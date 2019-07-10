from __future__ import absolute_import

import os
import re

import numpy as np

import pipeline as pipeline
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure.renderer.htmlrenderer import get_casa_version
from pipeline.infrastructure import casa_tasks

LOG = infrastructure.get_logger(__name__)


# utility to make miscinfo clean
def clean_extendable_keys(data, key, num_keys=None):
    """
    Remove extra entries in data. Logic is as follows:

        1. if num_keys is not given, take the number
           from the data using key ("n{key}")
        2. check if the entry whose keyword is "{key}X"
           where X denotes any integer
        3. remove the entry if X > num_keys

    Arguments:
        data {dict} -- Dictionary to be processed
        key {str} -- Key for the dictionary

    Keyword Arguments:
        num_keys {int} -- Number of expected entries for
                          given key. If not given (None),
                          get the number from data.
                          (default: {None})

    Returns:
        dict -- Reference to the data
    """
    if num_keys is None:
        number_key = 'n{}'.format(key)
        num_keys = data[number_key]

    # remove extra entries of "filnam0X"
    # it typically requires for single dish pipeline
    for k in list(data.keys()):
        if re.match('^{}[0-9]+$'.format(key), k):
            n = int(re.sub('^[a-zA-Z]+', '', k))
            if n > num_keys:
                data.pop(k, None)
    return data


# Add information to image header
def set_miscinfo(name, spw=None, field=None, type=None, iter=None, multiterm=None, intent=None, specmode=None,
                 robust=None, is_per_eb=None, context=None):
    """
    Define miscellaneous image information
    """
    if name != '':
        # Image name
        if multiterm:
            if name.find('.image.pbcor') != -1 and type == 'pbcorimage':
                imagename = name.replace('.image.pbcor', '.image.tt0.pbcor')
            else:
                imagename = '%s.tt0' % name
        else:
            imagename = name

        with casatools.ImageReader(imagename) as image:
            info = image.miscinfo()
            if imagename is not None:
                filename_components = os.path.basename(imagename).split('.')
                info['nfilnam'] = len(filename_components)
                for i in xrange(len(filename_components)):
                    info['filnam%02d' % (i+1)] = filename_components[i]

                # clean up extra "filnamX" entries
                info = clean_extendable_keys(info, 'filnam')

            if spw is not None:
                unique_spws = ','.join(np.unique(spw.split(',')))
                if context is not None and context.observing_run is not None:
                    spw_names = [
                        context.observing_run.virtual_science_spw_shortnames.get(
                            context.observing_run.virtual_science_spw_ids.get(int(spw_id), 'N/A'), 'N/A')
                        for spw_id in unique_spws.split(',')
                    ]
                else:
                    spw_names = ['N/A']
                info['spw'] = unique_spws
                info['nspwnam'] = len(spw_names)
                for i in xrange(len(spw_names)):
                    info['spwnam%02d' % (i+1)] = spw_names[i]

                # clean up extra "spwnamX" entries
                info = clean_extendable_keys(info, 'spwnam')

            if field is not None:
                # TODO: Find common key calculation. Long VLASS lists cause trouble downstream.
                #       Truncated list may cause duplicates.
                #       Temporarily (?) remove any '"' characters
                tmpfield = field.split(',')[0].replace('"', '')
                info['field'] = tmpfield

            if context is not None:
                # TODO: Use more generic approach like in the imaging heuristics
                if context.project_summary.telescope == 'ALMA':
                    info['npol'] = len(context.observing_run.measurement_sets[0].get_alma_corrstring().split(','))
                elif context.project_summary.telescope in ('VLA', 'EVLA'):
                    info['npol'] = len(context.observing_run.measurement_sets[0].get_vla_corrstring().split(','))
                else:
                    info['npol'] = -999

            if type is not None:
                info['type'] = type

            if iter is not None:
                info['iter'] = iter

            if intent is not None:
                info['intent'] = intent

            if specmode is not None:
                info['specmode'] = specmode

            if robust is not None:
                info['robust'] = robust

            if is_per_eb is not None:
                info['per_eb'] = is_per_eb

            # Pipeline / CASA information
            info['pipever'] = pipeline.revision
            info['casaver'] = get_casa_version()

            # Project information
            if context is not None:
                info['propcode'] = context.project_summary.proposal_code
                info['group'] = 'N/A'
                info['member'] = context.project_structure.ous_entity_id
                info['sgoal'] = 'N/A'

            # Some keywords should be present but are filled only
            # for other modes (e.g. single dish).
            info['offra'] = -999.0
            info['offdec'] = -999.0

            image.setmiscinfo(info)
