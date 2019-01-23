import collections
import copy
import glob
import math
import operator
import os.path
import re
import shutil
import uuid

import cleanhelper
import numpy as np
from imagerhelpers.imager_base import PySynthesisImager
from imagerhelpers.input_parameters import ImagerParameters

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.contfilehandler as contfilehandler
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.utils as utils
from pipeline.hif.heuristics import mosaicoverlap

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristics(object):
    """
    Image parameters heuristics base class. One instance is made per make/editimlist
    call. There are subclasses for different imaging modes such as ALMA
    or VLASS.
    """

    def __deepcopy__(self, memodict=None):
        # Save memory by using a shallow copy of the ObservingRun
        return self.__class__(
            copy.deepcopy(self.vislist),
            copy.deepcopy(self.spw),
            self.observing_run,
            self.imagename_prefix,
            copy.deepcopy(self.proj_params),
            self.contfile,
            self.linesfile,
            copy.deepcopy(self.imaging_params)
        )

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        """
        :param vislist: the list of MS names
        :type vislist: list of strings
        :param spw: the virtual spw specification (list of virtual spw IDs)
        :type spw_name: string or list
        """
        self.imaging_mode = 'BASE'

        self.observing_run = observing_run
        self.imagename_prefix = imagename_prefix
        self.proj_params = proj_params

        if isinstance(vislist, list):
            self.vislist = vislist
        else:
            self.vislist = [vislist]

        self.spw = spw
        self.contfile = contfile
        self.linesfile = linesfile

        self.imaging_params = imaging_params

        # split spw into list of spw parameters for 'clean' 
        spwlist = spw.replace('[', '').replace(']', '').strip()
        spwlist = spwlist.split("','")
        spwlist[0] = spwlist[0].strip("'")
        spwlist[-1] = spwlist[-1].strip("'")

        # find all the spwids present in the list
        p = re.compile(r"[ ,]+(\d+)")
        self.spwids = set()
        for spwclean in spwlist:
            spwidsclean = p.findall(' %s' % spwclean)
            spwidsclean = list(map(int, spwidsclean))
            self.spwids.update(spwidsclean)

    def primary_beam_size(self, spwid, intent):

        '''Calculate primary beam size in arcsec.'''

        cqa = casatools.quanta

        # get the diameter of the smallest antenna used among all vis sets
        antenna_ids = self.antenna_ids(intent)
        diameters = []
        for vis in self.vislist:
            ms = self.observing_run.get_ms(name=vis)
            antennas = ms.antennas
            for antenna in antennas:
                if antenna.id in antenna_ids[os.path.basename(vis)]:
                    diameters.append(antenna.diameter)
        smallest_diameter = np.min(np.array(diameters))

        # get spw info from first vis set, assume spws uniform
        # across datasets
        msname = self.vislist[0]
        real_spwid = self.observing_run.virtual2real_spw_id(spwid, self.observing_run.get_ms(msname))
        ms = self.observing_run.get_ms(name=msname)
        spw = ms.get_spectral_window(real_spwid)
        ref_frequency = float(
            spw.ref_frequency.to_units(measures.FrequencyUnits.HERTZ))

        # use the smallest antenna diameter and the reference frequency
        # to estimate the primary beam radius -
        # radius of first null in arcsec = 1.22*lambda/D
        primary_beam_size = \
            1.22 \
            * cqa.getvalue(cqa.convert(cqa.constants('c'), 'm/s')) \
            / ref_frequency \
            / smallest_diameter \
            * (180.0 * 3600.0 / math.pi)

        return primary_beam_size

    def cont_ranges_spwsel(self):
        """Determine spw selection parameters to exclude lines for mfs and cont images."""

        # initialize lookup dictionary for all possible source names
        cont_ranges_spwsel = {}
        for ms_ref in self.observing_run.get_measurement_sets():
            for source_name in [s.name for s in ms_ref.sources]:
                cont_ranges_spwsel[source_name] = {}
                for spwid in self.spwids:
                    cont_ranges_spwsel[source_name][str(spwid)] = ''

        contfile = self.contfile if self.contfile is not None else ''
        linesfile = self.linesfile if self.linesfile is not None else ''

        # read and merge continuum regions if contfile exists
        if os.path.isfile(contfile):
            LOG.info('Using continuum frequency ranges from %s to calculate continuum frequency selections.' % (contfile))

            contfile_handler = contfilehandler.ContFileHandler(contfile, warn_nonexist=True)

            # Collect the merged the ranges
            for field_name in cont_ranges_spwsel.iterkeys():
                for spw_id in cont_ranges_spwsel[field_name].iterkeys():
                    cont_ranges_spwsel[field_name][spw_id] = contfile_handler.get_merged_selection(field_name, spw_id)

        # alternatively read and merge line regions and calculate continuum regions
        elif os.path.isfile(linesfile):
            LOG.info('Using line frequency ranges from %s to calculate continuum frequency selections.' % (linesfile))

            p = re.compile('([\d.]*)(~)([\d.]*)(\D*)')
            try:
                line_regions = p.findall(open(linesfile, 'r').read().replace('\n', '').replace(';', '').replace(' ', ''))
            except Exception as e:
                line_regions = []
            line_ranges_GHz = []
            for line_region in line_regions:
                try:
                    fLow = casatools.quanta.convert('%s%s' % (line_region[0], line_region[3]), 'GHz')['value']
                    fHigh = casatools.quanta.convert('%s%s' % (line_region[2], line_region[3]), 'GHz')['value']
                    line_ranges_GHz.append((fLow, fHigh))
                except:
                    pass
            merged_line_ranges_GHz = [r for r in utils.merge_ranges(line_ranges_GHz)]

            # get source and spw info from first vis set, assume spws uniform
            # across datasets
            msname = self.vislist[0]
            ms = self.observing_run.get_ms(name=msname)
            for spwid in self.spwids:
                real_spwid = self.observing_run.virtual2real_spw_id(spwid, self.observing_run.get_ms(msname))
                spw = ms.get_spectral_window(real_spwid)
                # assemble continuum spw selection
                min_frequency = float(spw.min_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ))
                max_frequency = float(spw.max_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ))
                spw_sel_intervals = utils.spw_intersect([min_frequency, max_frequency], merged_line_ranges_GHz)
                spw_selection = ';'.join(['%s~%sGHz' % (spw_sel_interval[0], spw_sel_interval[1]) for spw_sel_interval in spw_sel_intervals])

                # Skip selection syntax completely if the whole spw is selected
                if (spw_selection == '%s~%sGHz' % (min_frequency, max_frequency)):
                    spw_selection = ''

                for source_name in [s.name for s in ms.sources]:
                    cont_ranges_spwsel[source_name][str(spwid)] = '%s LSRK' % (spw_selection)

        return cont_ranges_spwsel

    def field_intent_list(self, intent, field):
        intent_list = intent.split(',')
        field_list = utils.safe_split(field)

        field_intent_result = set()

        for vis in self.vislist:
            ms = self.observing_run.get_ms(name=vis)
            fields = ms.fields

            if intent.strip() is not '':
                for eachintent in intent_list:
                    re_intent = eachintent.replace('*', '.*')
                    field_intent_result.update(
                        [(fld.name, eachintent) for fld in fields if
                         re.search(pattern=re_intent, string=str(fld.intents))])

                if field.strip() is not '':
                    # remove items from field_intent_result that are not
                    # consistent with the fields in field_list
                    temp_result = set()
                    for eachfield in field_list:
                        re_field = utils.dequote(eachfield)
                        temp_result.update([fir for fir in field_intent_result if utils.dequote(fir[0]) == re_field])
                    field_intent_result = temp_result

            else:
                if field.strip() is not '':
                    for f in field_list:
                        fintents_list = [fld.intents for fld in fields if utils.dequote(fld.name) == utils.dequote(f)]
                        for fintents in fintents_list:
                            for fintent in fintents:
                                field_intent_result.update((f, fintent))

        # eliminate redundant copies of field/intent keys that map to the
        # same data - to prevent duplicate images being produced

        done_vis_scanids = []

        for field_intent in list(field_intent_result):
            field = field_intent[0]
            intent = field_intent[1]

            re_field = utils.dequote(field)

            vis_scanids = {}
            for vis in self.vislist:
                ms = self.observing_run.get_ms(name=vis)

                scanids = sorted(
                    [scan.id for scan in ms.scans
                     if intent in scan.intents and re_field in [utils.dequote(f.name) for f in scan.fields]])

                vis_scanids[vis] = scanids

            if vis_scanids in done_vis_scanids:
                LOG.warn(
                    'field: %s intent: %s is a duplicate - removing from imlist' %
                    (field, intent))
                field_intent_result.discard(field_intent)
            else:
                done_vis_scanids.append(vis_scanids)

        return field_intent_result

    def get_scanidlist(self, vis, field, intent):
        # Use scanids to select data with the specified intent
        # Note CASA clean now supports intent selection but leave
        # this logic in place and use it to eliminate vis that
        # don't contain the requested data.
        scanidlist = []
        visindexlist = []

        for i in xrange(len(vis)):
            allscanids = []
            for fieldname in field.split(','):
                re_field = utils.dequote(fieldname)
                ms = self.observing_run.get_ms(name=vis[i])
                scanids = [scan.id for scan in ms.scans if
                           intent in scan.intents and
                           ((re_field in [utils.dequote(f.name) for f in scan.fields]) or
                            (re_field in [str(f.id) for f in scan.fields]))]
                if scanids == []:
                    continue
                allscanids.extend(scanids)
            if allscanids != []:
                allscanids = ','.join(map(str, sorted(list(set(allscanids)))))
                scanidlist.append(allscanids)
                visindexlist.append(i)

        return scanidlist, visindexlist

    def largest_primary_beam_size(self, spwspec, intent):

        # get the spwids in spwspec
        p = re.compile(r"[ ,]+(\d+)")
        spwids = p.findall(' %s' % spwspec)
        spwids = set(map(int, spwids))

        # find largest beam among spws in spwspec
        largest_primary_beam_size = 0.0
        for spwid in spwids:
            largest_primary_beam_size = max(largest_primary_beam_size, self.primary_beam_size(spwid, intent))

        return largest_primary_beam_size

    def synthesized_beam(self, field_intent_list, spwspec, robust=0.5, uvtaper=[], pixperbeam=5.0, known_beams={},
                         force_calc=False):
        """Calculate synthesized beam for a given field / spw selection."""

        qaTool = casatools.quanta

        # Need to work on a local copy of known_beams to avoid setting the
        # method default value inadvertently
        local_known_beams = copy.deepcopy(known_beams)

        # reset state of imager
        casatools.imager.done()

        # get the spwids in spwspec - imager tool likes these rather than 
        # a string
        p = re.compile(r"[ ,]+(\d+)")
        spwids = p.findall(' %s' % spwspec)
        spwids = set(map(int, spwids))

        # Select only the highest frequency spw to get the smallest beam
        # ref_ms = self.observing_run.get_ms(self.vislist[0])
        # max_freq = 0.0
        # max_freq_spwid = -1
        # for spwid in spwids:
        #    real_spwid = self.observing_run.virtual2real_spw_id(spwid, ref_ms)
        #    spwid_centre_freq = ref_ms.get_spectral_window(real_spwid).centre_frequency.to_units(measures.FrequencyUnits.HERTZ)
        #    if spwid_centre_freq > max_freq:
        #        max_freq = spwid_centre_freq
        #        max_freq_spwid = spwid
        # spwids = [max_freq_spwid]

        # find largest primary beam size among spws in spwspec
        if list(field_intent_list) != []:
            largest_primary_beam_size = self.largest_primary_beam_size(spwspec, list(field_intent_list)[0][1])
        else:
            largest_primary_beam_size = self.largest_primary_beam_size(spwspec, 'TARGET')

        # put code in try-block to ensure that imager.done gets
        # called at the end
        cell = None
        valid_data = {}
        makepsf_beams = []
        try:
            for field, intent in field_intent_list:
                try:
                    if force_calc:
                        # Reset flag to avoid clearing the dictionary several times
                        force_calc = False
                        local_known_beams = {}
                        LOG.info('Got manual request to recalculate beams.')
                        raise Exception('Got manual request to recalculate beams.')
                    if local_known_beams['robust'] != robust:
                        LOG.info('robust value changed (old: %.1f, new: %.1f). Re-calculating beams.' % (local_known_beams['robust'], robust))
                        local_known_beams = {}
                        raise Exception('robust value changed (old: %.1f, new: %.1f). Re-calculating beams.' % (local_known_beams['robust'], robust))
                    if local_known_beams['uvtaper'] != uvtaper:
                        LOG.info('uvtaper value changed (old: %s, new: %s). Re-calculating beams.' % (str(local_known_beams['uvtaper']), str(uvtaper)))
                        local_known_beams = {}
                        raise Exception('uvtaper value changed (old: %s, new: %s). Re-calculating beams.' % (str(local_known_beams['uvtaper']), str(uvtaper)))
                    makepsf_beam = local_known_beams[field][intent][','.join(map(str, sorted(spwids)))]['beam']
                    LOG.info('Using previously calculated beam of %s for Field %s Intent %s SPW %s' %
                             (str(makepsf_beam), field, intent, ','.join(map(str, sorted(spwids)))))
                    if makepsf_beam != 'invalid':
                        makepsf_beams.append(makepsf_beam)
                except Exception as e:
                    local_known_beams['recalc'] = True
                    local_known_beams['robust'] = robust
                    local_known_beams['uvtaper'] = uvtaper

                    valid_data[(field, intent)] = False
                    valid_vis_list = []
                    valid_real_spwid_list = []
                    valid_virtual_spwid_list = []
                    # select data to be imaged
                    for vis in self.vislist:
                        for spwid in spwids:
                            ms = self.observing_run.get_ms(name=vis)
                            scanids = [str(scan.id) for scan in ms.scans
                                       if intent in scan.intents
                                       and field in [fld.name for fld in scan.fields]]
                            scanids = ','.join(scanids)
                            try:
                                real_spwid = self.observing_run.virtual2real_spw_id(spwid, self.observing_run.get_ms(vis))
                                antenna_ids = self.antenna_ids(intent, [os.path.basename(vis)])
                                taql = '||'.join(['ANTENNA1==%d' % i for i in antenna_ids[os.path.basename(vis)]])
                                rtn = casatools.imager.selectvis(vis=vis,
                                                                 field=field, spw=real_spwid, scan=scanids,
                                                                 taql=taql, usescratch=False, writeaccess=False)
                                if rtn is True:
                                    # flag to say that imager has some valid data to work
                                    # on
                                    valid_data[(field, intent)] = True
                                    valid_vis_list.append(vis)
                                    valid_real_spwid_list.append(real_spwid)
                                    valid_virtual_spwid_list.append(spwid)
                            except:
                                pass

                    if not valid_data[(field, intent)]:
                        # no point carrying on for this field/intent
                        LOG.debug('No data for field %s' % (field))
                        utils.set_nested_dict(local_known_beams,
                                              (field, intent, ','.join(map(str, sorted(spwids))), 'beam'),
                                              'invalid')
                        continue

                    # use imager.advise to get the maximum cell size
                    aipsfieldofview = '%4.1farcsec' % (2.0 * largest_primary_beam_size)
                    rtn = casatools.imager.advise(takeadvice=False, amplitudeloss=0.5, fieldofview=aipsfieldofview)
                    casatools.imager.done()
                    if not rtn[0]:
                        # advise can fail if all selected data are flagged
                        # - not documented but assuming bool in first field of returned
                        # record indicates success or failure
                        LOG.warning('imager.advise failed for field/intent %s/%s spw %s - no valid data?'
                                    % (field, intent, spwid))
                        valid_data[(field, intent)] = False
                        utils.set_nested_dict(local_known_beams,
                                              (field, intent, ','.join(map(str, sorted(spwids))), 'beam'),
                                              'invalid')
                    else:
                        cellv = qaTool.convert(rtn[2], 'arcsec')['value']
                        cellu = 'arcsec'
                        cellv /= (0.5 * pixperbeam)

                        # Now get better estimate from makePSF
                        tmp_psf_filename = str(uuid.uuid4())

                        antenna_ids = self.antenna_ids(intent, valid_vis_list)
                        antenna = [','.join(map(str, antenna_ids.get(os.path.basename(v), ''))) for v in valid_vis_list]
                        gridder = self.gridder(intent, field)
                        mosweight = self.mosweight(intent, field)
                        field_ids = self.field('TARGET', field, exclude_intent='ATMOSPHERE')
                        imsize = self.imsize(fields=field_ids, cell=['%.2g%s' % (cellv, cellu)], primary_beam=largest_primary_beam_size)
                        phasecenter = self.phasecenter(field_ids)
                        paramList = ImagerParameters(msname=valid_vis_list,
                                                     antenna=antenna,
                                                     spw=list(map(str, valid_real_spwid_list)),
                                                     field=field,
                                                     phasecenter=phasecenter,
                                                     imagename=tmp_psf_filename,
                                                     imsize=imsize,
                                                     cell='%.2g%s' % (cellv, cellu),
                                                     gridder=gridder,
                                                     mosweight=mosweight,
                                                     weighting='briggs',
                                                     robust=robust,
                                                     uvtaper=uvtaper,
                                                     specmode='mfs')
                        makepsf_imager = PySynthesisImager(params=paramList)
                        makepsf_imager.initializeImagers()
                        makepsf_imager.initializeNormalizers()
                        makepsf_imager.setWeighting()
                        makepsf_imager.makePSF()
                        makepsf_imager.deleteTools()

                        with casatools.ImageReader('%s.psf' % (tmp_psf_filename)) as image:
                            # Avoid bad PSFs
                            if all(qaTool.getvalue(qaTool.convert(image.restoringbeam()['minor'], 'arcsec')) > 1e-5):
                                restoringbeam = image.restoringbeam()
                                # CAS-11193: Round to 3 digits to avoid confusion when comparing
                                # heuristics against the beam weblog display (also using 3 digits)
                                restoringbeam_major_rounded = float('%.3g' % (qaTool.getvalue(qaTool.convert(restoringbeam['major'], 'arcsec'))))
                                restoringbeam_minor_rounded = float('%.3g' % (qaTool.getvalue(qaTool.convert(restoringbeam['minor'], 'arcsec'))))
                                restoringbeam_rounded = {'major': {'value': restoringbeam_major_rounded, 'unit': 'arcsec'},
                                                         'minor': {'value': restoringbeam_minor_rounded, 'unit': 'arcsec'},
                                                         'positionangle': restoringbeam['positionangle']}
                                makepsf_beams.append(restoringbeam_rounded)
                                utils.set_nested_dict(local_known_beams,
                                                      (field, intent, ','.join(map(str, sorted(spwids))), 'beam'),
                                                      restoringbeam_rounded)
                            else:
                                utils.set_nested_dict(local_known_beams,
                                                      (field, intent, ','.join(map(str, sorted(spwids))), 'beam'),
                                                      'invalid')

                        tmp_psf_images = glob.glob('%s.*' % tmp_psf_filename)
                        for tmp_psf_image in tmp_psf_images:
                            shutil.rmtree(tmp_psf_image)
        finally:
            casatools.imager.done()

        if makepsf_beams != []:
            # beam that's good for all field/intents
            smallest_beam = {'minor': '1e9arcsec', 'major': '1e9arcsec', 'positionangle': '0.0deg'}
            for beam in makepsf_beams:
                bmin_v = qaTool.getvalue(qaTool.convert(beam['minor'], 'arcsec'))
                if bmin_v < qaTool.getvalue(qaTool.convert(smallest_beam['minor'], 'arcsec')):
                    smallest_beam = beam
        else:
            smallest_beam = 'invalid'

        return smallest_beam, copy.deepcopy(local_known_beams)

    def cell(self, beam, pixperbeam=5.0):

        '''Calculate cell size.'''

        cqa = casatools.quanta
        try:
            cell_size = cqa.getvalue(cqa.convert(beam['minor'], 'arcsec')) / pixperbeam
            return ['%.2garcsec' % (cell_size)]
        except:
            return ['invalid']

    def nchan_and_width(self, field_intent, spwspec):
        if field_intent == 'TARGET':
            # get the spwids in spwspec
            p = re.compile(r"[ ,]+(\d+)")
            spwids = p.findall(' %s' % spwspec)
            spwids = list(set(map(int, spwids)))

            # Use the first spw for the time being. TBD if this needs to be improved.
            msname = self.vislist[0]
            real_spwid = self.observing_run.virtual2real_spw_id(spwids[0], self.observing_run.get_ms(msname))
            ms = self.observing_run.get_ms(name=msname)
            spw = ms.get_spectral_window(real_spwid)
            bandwidth = spw.bandwidth
            chan_widths = [c.getWidth() for c in spw.channels]

            # Currently imaging with a channel width of 15 MHz or the native
            # width if larger and at least 8 channels
            image_chan_width = measures.Frequency('15e6', measures.FrequencyUnits.HERTZ)
            min_nchan = 8
            if (any([chan_width >= image_chan_width for chan_width in chan_widths])):
                nchan = -1
                width = ''
            else:
                if (bandwidth >= min_nchan * image_chan_width):
                    nchan = int(round(float(bandwidth.to_units(measures.FrequencyUnits.HERTZ)) /
                                      float(image_chan_width.to_units(measures.FrequencyUnits.HERTZ))))
                    width = str(image_chan_width)
                else:
                    nchan = min_nchan
                    width = str(bandwidth / nchan)
        else:
            nchan = -1
            width = ''

        return nchan, width

    def has_data(self, field_intent_list, spwspec):
        # reset state of imager
        casatools.imager.done()

        # put code in try-block to ensure that imager.done gets
        # called at the end
        valid_data = {}
        try:
            # select data to be imaged
            for field_intent in field_intent_list:
                valid_data[field_intent] = False
                for vis in self.vislist:
                    ms = self.observing_run.get_ms(name=vis)
                    scanids = [str(scan.id) for scan in ms.scans if
                               field_intent[1] in scan.intents and
                               field_intent[0] in [fld.name for fld in scan.fields]]
                    if scanids != []:
                        scanids = ','.join(scanids)
                        real_spwspec = ','.join([str(self.observing_run.virtual2real_spw_id(spwid, ms)) for spwid in spwspec.split(',')])
                        try:
                            antenna_ids = self.antenna_ids(field_intent[1], [os.path.basename(vis)])
                            taql = '||'.join(['ANTENNA1==%d' % i for i in antenna_ids[os.path.basename(vis)]])
                            rtn = casatools.imager.selectvis(vis=vis, field=field_intent[0],
                                                             taql=taql, spw=real_spwspec,
                                                             scan=scanids, usescratch=False, writeaccess=False)
                            if rtn is True:
                                aipsfieldofview = '%4.1farcsec' % (2.0 * self.largest_primary_beam_size(spwspec, field_intent[1]))
                                # Need to run advise to check if the current selection is completely flagged
                                rtn = casatools.imager.advise(takeadvice=False, amplitudeloss=0.5, fieldofview=aipsfieldofview)
                                casatools.imager.done()
                                if rtn[0]:
                                    valid_data[field_intent] = True
                        except:
                            pass

                if not valid_data[field_intent]:
                    LOG.debug('No data for SpW %s field %s' %
                              (spwspec, field_intent[0]))

        finally:
            casatools.imager.done()

        return valid_data

    def gridder(self, intent, field):
        # the field heuristic which decides whether this is a mosaic or not
        # and sets self._mosaic (a bit convoluted...)
        self.field(intent, field)

        # also need to use mosaic gridder when gridding antennas with different
        # diameters (only for calibrators)
        if self._mosaic or (len(self.antenna_diameters()) > 1 and intent != 'TARGET'):
            # Setting this here because it is used in other places in the heuristics
            # TODO: this is flaky since it requires "gridder" to be called before
            #       other methods using self._mosaic
            self._mosaic = True
            return 'mosaic'
        else:
            return 'standard'

    def phasecenter(self, fields, centreonly=True, vislist=None):

        cme = casatools.measures
        cqa = casatools.quanta

        if vislist is None:
            vislist = self.vislist

        mdirections = []
        sdirections = []
        field_names = []
        for ivis, vis in enumerate(vislist):

            # Get the visibilities
            ms = self.observing_run.get_ms(name=vis)
            visfields = fields[ivis]
            if visfields == '':
                continue
            visfields = visfields.split(',')
            visfields = list(map(int, visfields))

            # Get the phase directions and convert to ICRS
            for field in visfields:
                # get field centres as measures
                fieldobj = ms.get_fields(field_id=field)[0]
                ref = cme.getref(fieldobj.mdirection)
                if ref == 'ICRS' or ref == 'J2000' or ref == 'B1950':
                    phase_dir = cme.measure(fieldobj.mdirection, 'ICRS')
                else:
                    phase_dir = fieldobj.mdirection
                mdirections.append(phase_dir)
                # Store source direction for ephemeris objects
                # for later correction of mosaic center position.
                if self.is_eph_obj(fieldobj.name):
                    ref = cme.getref(fieldobj.source.direction)
                    if ref == 'ICRS' or ref == 'J2000' or ref == 'B1950':
                        phase_dir = cme.measure(fieldobj.source.direction, 'ICRS')
                    else:
                        phase_dir = fieldobj.source.direction
                    sdirections.append(phase_dir)
                field_names.append(fieldobj.name)

        # Correct field directions for ephemeris source motion
        if sdirections != []:
            sdirection_m0_rad = []
            sdirection_m1_rad = []
            for sdirection in sdirections:
                sdirection_m0_rad.append(cqa.getvalue(cqa.convert(sdirection['m0'], 'rad')))
                sdirection_m1_rad.append(cqa.getvalue(cqa.convert(sdirection['m1'], 'rad')))
            avg_sdirection_m0_rad = np.average(sdirection_m0_rad)
            avg_sdirection_m1_rad = np.average(sdirection_m1_rad)
            for i in range(len(mdirections)):
                mdirections[i]['m0'] = cqa.sub(mdirections[i]['m0'], cqa.sub(sdirections[i]['m0'], cqa.quantity(avg_sdirection_m0_rad, 'rad'))) 
                mdirections[i]['m1'] = cqa.sub(mdirections[i]['m1'], cqa.sub(sdirections[i]['m1'], cqa.quantity(avg_sdirection_m1_rad, 'rad'))) 

        # sanity check - for single field images the field centres from
        # different measurement sets should be coincident and can
        # collapse the list 
        # Set the maximum separation to 200 microarcsec and test via
        # a tolerance rather than an equality.
        max_separation = cqa.quantity('200uarcsec')
        if not self._mosaic:
            max_separation_uarcsec =  cqa.getvalue(cqa.convert(max_separation, 'uarcsec'))[0] # in micro arcsec
            for mdirection in mdirections:
                separation = cme.separation(mdirection, mdirections[0])
                if cqa.gt(separation, max_separation):
                    separation_arcsec = cqa.getvalue(cqa.convert(separation, 'arcsec'))[0]
                    LOG.warning('The separation between %s field centers across EBs is %f arcseconds (larger than the limit of %.1f microarcseconds). This is only normal for an ephemeris source or a source with a large proper motion or parallax.' % (field_names[0], separation_arcsec, max_separation_uarcsec))
            mdirections = [mdirections[0]]

        # it should be easy to calculate some 'average' direction
        # from the contributing fields but it doesn't seem to be
        # at the moment - no conversion beween direction measures,
        # no calculation of a direction from a direction and an
        # offset. Consequently, what follows is a bit crude.

        # First, find the offset of all field from field 0.
        xsep = []
        ysep = []
        for mdirection in mdirections:
            pa = cme.posangle(mdirections[0], mdirection)
            sep = cme.separation(mdirections[0], mdirection)
            xs = cqa.mul(sep, cqa.sin(pa))
            ys = cqa.mul(sep, cqa.cos(pa))
            xs = cqa.convert(xs, 'arcsec')
            ys = cqa.convert(ys, 'arcsec')
            xsep.append(cqa.getvalue(xs))
            ysep.append(cqa.getvalue(ys))

        xsep = np.array(xsep)
        ysep = np.array(ysep)
        xspread = xsep.max() - xsep.min()
        yspread = ysep.max() - ysep.min()
        xcen = xsep.min() + xspread / 2.0
        ycen = ysep.min() + yspread / 2.0

        # get direction of image centre crudely by adding offset
        # of centre to ref values of first field.
        ref = cme.getref(mdirections[0])
        md = cme.getvalue(mdirections[0])
        m0 = cqa.quantity(md['m0'])
        m1 = cqa.quantity(md['m1'])

        m0 = cqa.add(m0, cqa.div('%sarcsec' % xcen, cqa.cos(m1)))
        m1 = cqa.add(m1, '%sarcsec' % ycen)

        # convert to strings (CASA 4.0 returns as list for some reason
        # hence 0 index)
        if ref == 'ICRS' or ref == 'J2000' or ref == 'B1950':
            m0 = cqa.time(m0, prec=10)[0]
        else:
            m0 = cqa.angle(m0, prec=9)[0]
        m1 = cqa.angle(m1, prec=9)[0]

        if centreonly:
            return '%s %s %s' % (ref, m0, m1)
        else:
            return '%s %s %s' % (ref, m0, m1), xspread, yspread

    def field(self, intent, field, exclude_intent=None, vislist=None):

        if vislist is None:
            vislist = self.vislist

        result = []

        for vis in vislist:
            ms = self.observing_run.get_ms(name=vis)
            fields = ms.fields

            field_list = []
            if field is not None:
                # field set explicitly
                if not isinstance(field, list):
                    field_names = [field]
                else:
                    field_names = field

                # convert to field ids
                for field_name in field_names:
                    field_list += [fld.id for fld in fields if
                                   field_name.replace(' ', '') == fld.name.replace(' ', '')]

            if intent is not None:
                # pattern matching to allow intents of form *TARGET* to work
                re_intent = intent.replace('*', '.*')
                if exclude_intent is not None:
                    re_exclude_intent = exclude_intent.replace('*', '.*')
                    field_list = [fld.id for fld in fields if
                                  fld.id in field_list and re_intent in fld.intents and re_exclude_intent not in fld.intents]
                else:
                    field_list = [fld.id for fld in fields if
                                  fld.id in field_list and re_intent in fld.intents]

            # this will be a mosaic if there is more than 1 field_id for any 
            # measurement set - probably needs more work, what if want to
            # mosaic together single fields in multiple measurement sets?
            self._mosaic = len(field_list) > 1

            field_string = ','.join(str(fld_id) for fld_id in field_list)
            result.append(field_string)

        return result

    def is_eph_obj(self, field):

        ms = None
        for msname in self.vislist:
            ms = self.observing_run.get_ms(msname)
            if field in [f.name for f in ms.fields]:
                break
        if ms is None:
            LOG.error('Image Heuristics Ephemeris Object Check: Field %s not found.' % (field))
            raise Exception('Image Heuristics Ephemeris Object Check: Field %s not found.' % (field))
        else:
            source_name = ms.get_fields(field)[0].source.name
            is_eph_obj = False
            for source in ms.sources:
                if (source.name == source_name) and (source.is_eph_obj):
                    is_eph_obj = True
            return is_eph_obj

    def aggregate_bandwidth(self, spwids=None):

        if spwids is None:
            local_spwids = self.spwids
        else:
            local_spwids = spwids

        msname = self.vislist[0]
        ms = self.observing_run.get_ms(name=msname)

        spw_frequency_ranges = []
        for spwid in local_spwids:
            real_spwid = self.observing_run.virtual2real_spw_id(spwid, ms)
            spw = ms.get_spectral_window(real_spwid)
            min_frequency_Hz = float(spw.min_frequency.convert_to(measures.FrequencyUnits.HERTZ).value)
            max_frequency_Hz = float(spw.max_frequency.convert_to(measures.FrequencyUnits.HERTZ).value)
            spw_frequency_ranges.append([min_frequency_Hz, max_frequency_Hz])

        aggregate_bandwidth_Hz = np.sum([r[1] - r[0] for r in utils.merge_ranges(spw_frequency_ranges)])

        return {'value': aggregate_bandwidth_Hz, 'unit': 'Hz'}

    def representative_target(self):

        cqa = casatools.quanta

        repr_ms = self.observing_run.get_ms(self.vislist[0])
        repr_target = repr_ms.representative_target

        reprBW_mode = 'nbin'
        if repr_target != (None, None, None) and repr_target[0] != 'none':
            real_repr_target = True
            repr_freq = repr_target[1]
            # Get representative source and spw
            repr_source, repr_spw = repr_ms.get_representative_source_spw()
            # Check if representative bandwidth is larger than spw bandwidth. If so, switch to fullcont.
            repr_spw_obj = repr_ms.get_spectral_window(repr_spw)
            repr_spw_bw = cqa.quantity(float(repr_spw_obj.bandwidth.convert_to(measures.FrequencyUnits.HERTZ).value), 'Hz')
            cont_spw_ids = self.observing_run.virtual_science_spw_ids.keys()
            agg_bw = self.aggregate_bandwidth(cont_spw_ids)
            if cqa.gt(repr_target[2], cqa.mul(agg_bw, 0.9)):
                reprBW_mode = 'all_spw'
            elif cqa.gt(repr_target[2], repr_spw_bw) and cqa.le(repr_target[2], cqa.mul(agg_bw, 0.9)):
                reprBW_mode = 'multi_spw'
            elif cqa.gt(repr_target[2], cqa.mul(repr_spw_bw, 0.2)):
                reprBW_mode = 'repr_spw'

            # Check if there is a non-zero min/max angular resolution
            minAcceptableAngResolution = cqa.convert(self.proj_params.min_angular_resolution, 'arcsec')
            maxAcceptableAngResolution = cqa.convert(self.proj_params.max_angular_resolution, 'arcsec')
            if cqa.getvalue(minAcceptableAngResolution) == 0.0 or cqa.getvalue(maxAcceptableAngResolution) == 0.0:
                desired_angular_resolution = cqa.convert(self.proj_params.desired_angular_resolution, 'arcsec')
                if cqa.getvalue(desired_angular_resolution) != 0.0:
                    minAcceptableAngResolution = cqa.mul(desired_angular_resolution, 0.8)
                    maxAcceptableAngResolution = cqa.mul(desired_angular_resolution, 1.2)
                else:
                    science_goals = self.observing_run.get_measurement_sets()[0].science_goals
                    minAcceptableAngResolution = cqa.convert(science_goals['minAcceptableAngResolution'], 'arcsec')
                    maxAcceptableAngResolution = cqa.convert(science_goals['maxAcceptableAngResolution'], 'arcsec')

            # Check if there is a non-zero sensitivity goal
            sensitivityGoal = cqa.convert(self.proj_params.desired_sensitivity, 'mJy')
            if cqa.getvalue(sensitivityGoal) == 0.0:
                science_goals = self.observing_run.get_measurement_sets()[0].science_goals
                sensitivityGoal = cqa.convert(science_goals['sensitivity'], 'mJy')

        else:
            real_repr_target = False
            # Fall back to existing source
            target_sources = [s.name for s in repr_ms.sources if 'TARGET' in s.intents]
            if target_sources == []:
                if repr_target[0] == 'none':
                    LOG.warning('No TARGET intent found. Trying to select a representative source from calibration intents.')
                    for repr_intent in ['PHASE', 'CHECK', 'AMPLITUDE', 'FLUX', 'BANDPASS']:
                        target_sources = [s.name for s in repr_ms.sources if repr_intent in s.intents]
                        if target_sources != []:
                            break
                    if target_sources == []:
                        raise Exception('Cannot select any representative target from calibration intents.')
                else:
                    raise Exception('Cannot select any representative target from TARGET intent.')

            repr_source = target_sources[0]
            repr_spw_obj = repr_ms.get_spectral_windows()[0]
            repr_spw = repr_spw_obj.id
            repr_chan_obj = repr_spw_obj.channels[int(repr_spw_obj.num_channels / 2)]
            repr_freq = cqa.quantity(float(repr_chan_obj.getCentreFrequency().convert_to(measures.FrequencyUnits.HERTZ).value), 'Hz')
            repr_bw = cqa.quantity(float(repr_chan_obj.getWidth().convert_to(measures.FrequencyUnits.HERTZ).value), 'Hz')
            repr_target = (repr_source, repr_freq, repr_bw)

            minAcceptableAngResolution = '0.0arcsec'
            maxAcceptableAngResolution = '0.0arcsec'
            sensitivityGoal = '0.0mJy'

            LOG.info('Image Heuristics: No representative target found. Choosing %s SPW %d.' % (repr_source, repr_spw))

        virtual_repr_spw = self.observing_run.real2virtual_spw_id(repr_spw, repr_ms)

        return repr_target, repr_source, virtual_repr_spw, repr_freq, reprBW_mode, real_repr_target, minAcceptableAngResolution, maxAcceptableAngResolution, sensitivityGoal

    def imsize(self, fields, cell, primary_beam, sfpblimit=None, max_pixels=None, centreonly=False):
        # get spread of beams
        if centreonly:
            xspread = yspread = 0.0
        else:
            ignore, xspread, yspread = self.phasecenter(fields, centreonly=centreonly)

        cqa = casatools.quanta

        cellx = cell[0]
        if len(cell) > 1:
            celly = cell[1]
        else:
            celly = cell[0]

        if (cellx == 'invalid') or (celly == 'invalid'):
            return [0, 0]

        # get cell and beam sizes in arcsec
        cellx_v = cqa.getvalue(cqa.convert(cellx, 'arcsec'))
        celly_v = cqa.getvalue(cqa.convert(celly, 'arcsec'))
        beam_radius_v = primary_beam

        # set size of image to spread of field centres plus a
        # border of 0.75 * beam radius (radius is to first null)
        # wide
        nxpix = int((1.5 * beam_radius_v + xspread) / cellx_v)
        nypix = int((1.5 * beam_radius_v + yspread) / celly_v)

        if (not self._mosaic) and (sfpblimit is not None):
            beam_fwhp = 1.12 / 1.22 * beam_radius_v
            nxpix = int(round(1.1 * beam_fwhp * math.sqrt(-math.log(sfpblimit) / math.log(2.)) / cellx_v))
            nypix = int(round(1.1 * beam_fwhp * math.sqrt(-math.log(sfpblimit) / math.log(2.)) / celly_v))

        if max_pixels is not None:
            nxpix = min(nxpix, max_pixels)
            nypix = min(nypix, max_pixels)

        # set nxpix, nypix to next highest 'composite number'
        nxpix = cleanhelper.cleanhelper.getOptimumSize(nxpix)
        nypix = cleanhelper.cleanhelper.getOptimumSize(nypix)

        return [nxpix, nypix]

    def imagename(self, output_dir=None, intent=None, field=None, spwspec=None, specmode=None, band=None):
        try:
            nameroot = self.imagename_prefix
            if nameroot == 'unknown':
                nameroot = 'oussid'
            # need to sanitize the nameroot here because when it's added
            # to filenamer as an asdm, os.path.basename is run on it with
            # undesirable results.
            nameroot = filenamer.sanitize(nameroot)
        except:
            nameroot = 'oussid'
        namer = filenamer.Image()
        namer._associations.asdm(nameroot)

        if output_dir:
            namer.output_dir(output_dir)

        namer.stage('STAGENUMBER')
        if intent:
            namer.intent(intent)
        if field:
            namer.source(field)
        if spwspec:
            # find all the spwids present in the list
            p = re.compile(r"[ ,]+(\d+)")
            spwids = p.findall(' %s' % spwspec)
            spw = '_'.join(map(str, sorted(map(int, set(spwids)))))
            namer.spectral_window(spw)
        if specmode:
            namer.specmode(specmode)

        # filenamer returns a sanitized filename (i.e. one with 
        # illegal characters replace by '_'), no need to check
        # the name components individually.
        imagename = namer.get_filename()
        return imagename

    def width(self, spwid):
        msname = self.vislist[0]
        real_spwid = self.observing_run.virtual2real_spw_id(spwid, self.observing_run.get_ms(msname))
        ms = self.observing_run.get_ms(name=msname)
        spw = ms.get_spectral_window(real_spwid)

        # negative widths appear to confuse clean,
        if spw.channels[1].high > spw.channels[0].high:
            width = spw.channels[1].high - spw.channels[0].high
        else:
            width = spw.channels[0].high - spw.channels[1].high
        # increase width slightly to try to avoid the error:
        # WARN SubMS::convertGridPars *** Requested new channel width is
        # smaller than smallest original channel width.
        # This should no longer be necessary (CASA >=4.7) and this width
        # method does not seem to be used anywhere anyways.
        # width = decimal.Decimal('1.0001') * width
        width = str(width)
        return width

    def ncorr(self, spwid):
        msname = self.vislist[0]
        real_spwid = self.observing_run.virtual2real_spw_id(spwid, self.observing_run.get_ms(msname))
        ms = self.observing_run.get_ms(name=msname)
        spw = ms.get_spectral_window(real_spwid)

        # Get the data description for this spw
        dd = ms.get_data_description(spw=spw)
        if dd is None:
            LOG.debug('Missing data description for spw %s ' % spwid)
            return 0

        # Determine the number of correlations
        #   Check that they are between 1 and 4
        ncorr = len(dd.corr_axis)
        if ncorr not in {1, 2, 4}:
            LOG.debug('Wrong number of correlations %s for spw %s ' % (ncorr, spwid))
            return 0

        return ncorr

    def pblimits(self, pb):

        pblimit_image = 0.2
        pblimit_cleanmask = 0.3

        if (pb not in [None, '']):
            try:
                iaTool = casatools.image
                iaTool.open(pb)
                nx, ny, np, nf = iaTool.shape()

                # First check if the center edge is masked. If so, then the
                # default pb level of 0.2 is fully within the image and no
                # adjustmnt is needed.
                if not iaTool.getchunk([nx / 2, 0, 0, nf / 2], [nx / 2, 0, 0, nf / 2], getmask=True).flatten()[0]:
                    return pblimit_image, pblimit_cleanmask

                pb_edge = 0.0
                i_pb_edge = -1
                # There are cases with zero edged PBs (due to masking),
                # check for first unmasked value.
                # Should no longer encounter the mask here due to the
                # above check, but keep code around for now.
                while ((pb_edge == 0.0) and (i_pb_edge < ny / 2)):
                    i_pb_edge += 1
                    pb_edge = iaTool.getchunk([nx/2, i_pb_edge, 0, nf/2], [nx/2, i_pb_edge, 0, nf/2]).flatten()[0]
                if (pb_edge > 0.2):
                    i_pb_image = max(i_pb_edge, int(0.05 * ny))
                    pblimit_image = iaTool.getchunk([nx/2, i_pb_image, 0, nf/2], [nx/2, i_pb_image, 0, nf/2]).flatten()[0]
                    i_pb_cleanmask = i_pb_image + int(0.05 * ny)
                    pblimit_cleanmask = iaTool.getchunk([nx/2, i_pb_cleanmask, 0, nf/2], [nx/2, i_pb_cleanmask, 0, nf/2]).flatten()[0]
            except Exception as e:
                LOG.warning('Could not analyze PB: %s. Using default pblimit values.' % (e))
            finally:
                iaTool.close()

        return pblimit_image, pblimit_cleanmask

    def deconvolver(self, specmode, spwspec):
        if (specmode == 'cont'):
            abs_min_frequency = 1.0e15
            abs_max_frequency = 0.0
            ms = self.observing_run.get_ms(name=self.vislist[0])
            msname = self.vislist[0]
            ms = self.observing_run.get_ms(name=msname)
            for spwid in spwspec.split(','):
                real_spwid = self.observing_run.virtual2real_spw_id(spwid, self.observing_run.get_ms(msname))
                spw = ms.get_spectral_window(real_spwid)
                min_frequency = float(spw.min_frequency.to_units(measures.FrequencyUnits.HERTZ))
                if (min_frequency < abs_min_frequency):
                    abs_min_frequency = min_frequency
                max_frequency = float(spw.max_frequency.to_units(measures.FrequencyUnits.HERTZ))
                if (max_frequency > abs_max_frequency):
                    abs_max_frequency = max_frequency
            if (2.0 * (abs_max_frequency - abs_min_frequency) / (abs_min_frequency + abs_max_frequency) > 0.1):
                return 'mtmfs'
            else:
                return 'hogbom'
        else:
            return 'hogbom'

    def robust(self):

        '''Default robust value.'''

        return 0.5

    def center_field_ids(self, msnames, field, intent, phasecenter, exclude_intent=None):

        '''Get per-MS IDs of field closest to the phase center.'''

        meTool = casatools.measures
        qaTool = casatools.quanta
        ref_field_ids = []

        # Phase center coordinates
        pc_direc = meTool.source(phasecenter)

        for msname in msnames:
            try:
                ms_obj = self.observing_run.get_ms(msname)
                if exclude_intent is None:
                    field_ids = [f.id for f in ms_obj.fields if (f.name == field) and (intent in f.intents)]
                else:
                    field_ids = [f.id for f in ms_obj.fields if (f.name == field) and (intent in f.intents) and (exclude_intent not in f.intents)]
                separations = [qaTool.getvalue(meTool.separation(pc_direc, f.mdirection)) for f in ms_obj.fields if f.id in field_ids]
                ref_field_ids.append(field_ids[separations.index(min(separations))])
            except:
                ref_field_ids.append(-1)

        return ref_field_ids

    def calc_topo_ranges(self, inputs):

        '''Calculate TOPO ranges for hif_tclean inputs.'''

        spw_topo_freq_param_lists = []
        spw_topo_chan_param_lists = []
        spw_topo_freq_param_dict = collections.defaultdict(dict)
        spw_topo_chan_param_dict = collections.defaultdict(dict)
        p = re.compile('([\d.]*)(~)([\d.]*)(\D*)')
        total_topo_freq_ranges = []
        topo_freq_ranges = []
        num_channels = []

        meTool = casatools.measures
        qaTool = casatools.quanta

        # Phase center coordinates
        pc_direc = meTool.source(inputs.phasecenter)

        # Get per-MS IDs of field closest to the phase center
        ref_field_ids = self.center_field_ids(inputs.vis, inputs.field, inputs.intent, pc_direc)

        # Get a cont file handler for the conversion to TOPO
        contfile_handler = contfilehandler.ContFileHandler(self.contfile)

        aggregate_lsrk_bw = '0.0GHz'

        msname = self.vislist[0]
        ms = self.observing_run.get_ms(name=msname)
        for spwid in inputs.spw.split(','):
            real_spwid = self.observing_run.virtual2real_spw_id(spwid, self.observing_run.get_ms(msname))
            spw_info = ms.get_spectral_window(real_spwid)

            num_channels.append(spw_info.num_channels)

            min_frequency = float(spw_info.min_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ))
            max_frequency = float(spw_info.max_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ))

            # Save spw width
            total_topo_freq_ranges.append((min_frequency, max_frequency))

            if 'spw%s' % (spwid) in inputs.spwsel_lsrk:
                if (inputs.spwsel_lsrk['spw%s' % (spwid)] not in ['ALL', '', 'NONE']):
                    freq_selection, refer = inputs.spwsel_lsrk['spw%s' % (spwid)].split()
                    if (refer == 'LSRK'):
                        # Convert to TOPO
                        topo_freq_selections, topo_chan_selections, aggregate_spw_lsrk_bw = contfile_handler.lsrk_to_topo(inputs.spwsel_lsrk['spw%s' % (spwid)], inputs.vis, ref_field_ids, spwid, self.observing_run)
                        spw_topo_freq_param_lists.append(['%s:%s' % (spwid, topo_freq_selection.split()[0]) for topo_freq_selection in topo_freq_selections])
                        spw_topo_chan_param_lists.append(['%s:%s' % (spwid, topo_chan_selection.split()[0]) for topo_chan_selection in topo_chan_selections])
                        for i in xrange(len(inputs.vis)):
                            spw_topo_freq_param_dict[os.path.basename(inputs.vis[i])][spwid] = topo_freq_selections[i].split()[0]
                            spw_topo_chan_param_dict[os.path.basename(inputs.vis[i])][spwid] = topo_chan_selections[i].split()[0]
                        # Count only one selection !
                        for topo_freq_range in topo_freq_selections[0].split(';'):
                            f1, sep, f2, unit = p.findall(topo_freq_range)[0]
                            topo_freq_ranges.append((float(f1), float(f2)))
                    else:
                        LOG.warning('Cannot convert frequency selection properly to TOPO. Using plain ranges for all MSs.')
                        spw_topo_freq_param_lists.append(['%s:%s' % (spwid, freq_selection)] * len(inputs.vis))
                        # TODO: Need to derive real channel ranges
                        spw_topo_chan_param_lists.append(['%s:0~%s' % (spwid, spw_info.num_channels - 1)] * len(inputs.vis))
                        for i in xrange(len(inputs.vis)):
                            spw_topo_freq_param_dict[os.path.basename(inputs.vis[i])][spwid] = freq_selection.split()[0]
                            # TODO: Need to derive real channel ranges
                            spw_topo_chan_param_dict[os.path.basename(inputs.vis[i])][spwid] = '0~%d' % (spw_info.num_channels - 1)
                        # Count only one selection !
                        aggregate_spw_lsrk_bw = '0.0GHz'
                        for freq_range in freq_selection.split(';'):
                            f1, sep, f2, unit = p.findall(freq_range)[0]
                            topo_freq_ranges.append((float(f1), float(f2)))
                            delta_f = qaTool.sub('%s%s' % (f2, unit), '%s%s' % (f1, unit))
                            aggregate_spw_lsrk_bw = qaTool.add(aggregate_spw_lsrk_bw, delta_f)
                else:
                    spw_topo_freq_param_lists.append([spwid] * len(inputs.vis))
                    spw_topo_chan_param_lists.append([spwid] * len(inputs.vis))
                    for msname in inputs.vis:
                        spw_topo_freq_param_dict[os.path.basename(msname)][spwid] = ''
                        spw_topo_chan_param_dict[os.path.basename(msname)][spwid] = ''
                    topo_freq_ranges.append((min_frequency, max_frequency))
                    aggregate_spw_lsrk_bw = '%sGHz' % (max_frequency - min_frequency)
                    if (inputs.spwsel_lsrk['spw%s' % (spwid)] != 'ALL') and (inputs.intent == 'TARGET') and (inputs.specmode in ('mfs', 'cont') and self.warn_missing_cont_ranges()):
                        LOG.warning('No continuum frequency selection for Target Field %s SPW %s' % (inputs.field, spwid))
            else:
                spw_topo_freq_param_lists.append([spwid] * len(inputs.vis))
                spw_topo_chan_param_lists.append([spwid] * len(inputs.vis))
                for msname in inputs.vis:
                    spw_topo_freq_param_dict[os.path.basename(msname)][spwid] = ''
                    spw_topo_chan_param_dict[os.path.basename(msname)][spwid] = ''
                topo_freq_ranges.append((min_frequency, max_frequency))
                aggregate_spw_lsrk_bw = '%sGHz' % (max_frequency - min_frequency)
                if (inputs.intent == 'TARGET') and (inputs.specmode in ('mfs', 'cont') and self.warn_missing_cont_ranges()):
                    LOG.warning('No continuum frequency selection for Target Field %s SPW %s' % (inputs.field, spwid))

            aggregate_lsrk_bw = qaTool.add(aggregate_lsrk_bw, aggregate_spw_lsrk_bw)

        spw_topo_freq_param = [','.join(spwsel_per_ms) for spwsel_per_ms in [[spw_topo_freq_param_list_per_ms[i] for spw_topo_freq_param_list_per_ms in spw_topo_freq_param_lists] for i in xrange(len(inputs.vis))]]
        spw_topo_chan_param = [','.join(spwsel_per_ms) for spwsel_per_ms in [[spw_topo_chan_param_list_per_ms[i] for spw_topo_chan_param_list_per_ms in spw_topo_chan_param_lists] for i in xrange(len(inputs.vis))]]

        # Calculate total bandwidth
        total_topo_bw = '0.0GHz'
        for total_topo_freq_range in utils.merge_ranges(total_topo_freq_ranges):
            total_topo_bw = qaTool.add(total_topo_bw, qaTool.sub('%sGHz' % (total_topo_freq_range[1]), '%sGHz' % (total_topo_freq_range[0])))

        # Calculate aggregate selected bandwidth
        aggregate_topo_bw = '0.0GHz'
        for topo_freq_range in utils.merge_ranges(topo_freq_ranges):
            aggregate_topo_bw = qaTool.add(aggregate_topo_bw, qaTool.sub('%sGHz' % (topo_freq_range[1]), '%sGHz' % (topo_freq_range[0])))

        return spw_topo_freq_param, spw_topo_chan_param, spw_topo_freq_param_dict, spw_topo_chan_param_dict, total_topo_bw, aggregate_topo_bw, aggregate_lsrk_bw

    def get_channel_flags(self, msname, field, spw):
        """
        Determine channel flags for a given field and spw selection in one MS.
        """

        # CAS-8997
        #
        # ms.selectinit(datadescid=x) is required to avoid the 'Data shape
        # varies...' warning message. There's no guarantee that a single
        # spectral window will resolve to a single data description, e.g.
        # the polarisation setup may change. There's not enough
        # information passed into this function to consistently identify the
        # data description that should be specified so on occasion the
        # warning message will reoccur.
        #
        # TODO refactor method signature to include data description ID
        #
        msDO = self.observing_run.get_ms(name=msname)
        real_spwid = self.observing_run.virtual2real_spw_id(spw, self.observing_run.get_ms(msname))
        spwDO = msDO.get_spectral_window(real_spwid)
        data_description_ids = {dd.id for dd in msDO.data_descriptions if dd.spw is spwDO}
        if len(data_description_ids) == 1:
            dd_id = data_description_ids.pop()
        else:
            msg = 'Could not resolve {!s} spw {!s} to a single data description'.format(msname, spwDO.id)
            LOG.warning(msg)
            dd_id = 0

        try:
            with casatools.MSReader(msname) as msTool:
                # CAS-8997 selectinit is required to avoid the 'Data shape varies...' warning message
                msTool.selectinit(datadescid=dd_id)
                # Antenna selection does not work (CAS-8757)
                # staql={'field': field, 'spw': real_spwid, 'antenna': '*&*'}
                staql = {'field': field, 'spw': str(real_spwid)}
                msTool.msselect(staql)
                msTool.iterinit(maxrows=500)
                msTool.iterorigin()

                channel_flags = np.array([True] * spwDO.num_channels)

                iterating = True
                while iterating:
                    flag_ants = msTool.getdata(['flag', 'antenna1', 'antenna2'])

                    # Calculate averaged flagging vector keeping all unflagged
                    # channels from any baseline.
                    if flag_ants != {}:
                        for i in xrange(flag_ants['flag'].shape[2]):
                            # Antenna selection does not work (CAS-8757)
                            if (flag_ants['antenna1'][i] != flag_ants['antenna2'][i]):
                                for j in xrange(flag_ants['flag'].shape[0]):
                                    channel_flags = np.logical_and(channel_flags, flag_ants['flag'][j, :, i])

                    iterating = msTool.iternext()

        except Exception as e:
            LOG.error('Exception while determining flags: %s' % e)
            channel_flags = np.array([])

        return channel_flags

    def get_first_unflagged_channel_from_center(self, channel_flags):
        """
        Find first unflagged channel from center channel.
        """

        i_low = i_high = None
        num_channels = len(channel_flags)
        center_channel = int(num_channels / 2)

        for i in xrange(center_channel, -1, -1):
            if channel_flags[i] == False:
                i_low = i
                break

        for i in xrange(center_channel, num_channels):
            if channel_flags[i] == False:
                i_high = i
                break

        if (i_low is None) and (i_high is None):
            return None
        elif (i_low is None):
            return i_high
        elif (i_high is None):
            return i_low
        else:
            if (center_channel - i_low) < (i_high - center_channel):
                return i_low
            else:
                return i_high

    def freq_intersection(self, vis, field, spw, frame='LSRK'):
        """
        Calculate LSRK frequency intersection of a list of MSs for a
        given field and spw. Exclude flagged channels.
        """
        freq_ranges = []
        channel_widths = []

        for msname in vis:
            real_spw = str(self.observing_run.virtual2real_spw_id(spw, self.observing_run.get_ms(msname)))

            # Get the channel flags (uses virtual spw ID !)
            channel_flags = self.get_channel_flags(msname, field, spw)

            # Get indices of unflagged channels
            nfi = np.where(channel_flags == False)[0]

            # Use unflagged edge channels to determine LSRK frequency range
            if nfi.shape != (0,):
                # Just the edges. Skip one extra channel in final frequency range.
                with casatools.SelectvisReader(msname, field=field,
                                               spw='%s:%d~%d' % (real_spw, nfi[0], nfi[-1])) as imager:
                    result = imager.advisechansel(getfreqrange=True, freqframe=frame)

                f0 = result['freqstart']
                f1 = result['freqend']

                freq_ranges.append((f0, f1))
                # The frequency range from advisechansel is from channel edge
                # to channel edge. To get the width, one needs to divide by the
                # number of channels in the selection.
                channel_widths.append((f1 - f0) / (nfi[-1] - nfi[0] + 1))

        intersect_range = utils.intersect_ranges(freq_ranges)
        if intersect_range != ():
            if0, if1 = intersect_range
            return if0, if1, max(channel_widths)
        else:
            return -1, -1, 0

    def warn_missing_cont_ranges(self):

        return False

    def get_bw_corr_factor(self, ms_do, spw, nchan):
        """
        Calculate effective bandwidth correction factor.
        """

        real_spwid = self.observing_run.virtual2real_spw_id(spw, ms_do)
        spw_do = ms_do.get_spectral_window(real_spwid)
        spwchan = spw_do.num_channels
        physicalBW_of_1chan = float(spw_do.channels[0].getWidth().convert_to(measures.FrequencyUnits.HERTZ).value)
        effectiveBW_of_1chan = float(spw_do.channels[0].effective_bw.convert_to(measures.FrequencyUnits.HERTZ).value)

        BW_ratio = effectiveBW_of_1chan / physicalBW_of_1chan

        if (BW_ratio <= 1.0):
            N_smooth = 0
        elif (utils.equal_to_n_digits(BW_ratio, 2.667, 4)):
            N_smooth = 1
        elif (utils.equal_to_n_digits(BW_ratio, 1.600, 4)):
            N_smooth = 2
        elif (utils.equal_to_n_digits(BW_ratio, 1.231, 4)):
            N_smooth = 4
        elif (utils.equal_to_n_digits(BW_ratio, 1.104, 4)):
            N_smooth = 8
        elif (utils.equal_to_n_digits(BW_ratio, 1.049, 4)):
            N_smooth = 16
        else:
            LOG.warning('Could not evaluate channel bandwidths ratio. Physical: %s Effective: %s Ratio: %s' % (physicalBW_of_1chan, effectiveBW_of_1chan, BW_ratio))
            N_smooth = 0

        if N_smooth > 0 and nchan > 1:
            optimisticBW = nchan * float(effectiveBW_of_1chan)
            approximateEffectiveBW = (nchan + 1.12 * (spwchan - nchan) / spwchan / N_smooth) * float(physicalBW_of_1chan)
            SCF = (optimisticBW / approximateEffectiveBW) ** 0.5
        else:
            SCF = 1.0

        return SCF, physicalBW_of_1chan, effectiveBW_of_1chan

    def calc_sensitivities(self, vis, field, intent, spw, nbin, spw_topo_chan_param_dict, specmode, gridder, cell, imsize, weighting, robust, uvtaper, center_only=False, known_sensitivities={}, force_calc=False):
        """Compute sensitivity estimate using CASA."""

        cqa = casatools.quanta

        # Need to work on a local copy of known_sensitivities to avoid setting the
        # method default value inadvertently
        local_known_sensitivities = copy.deepcopy(known_sensitivities)

        sensitivities = []
        eff_ch_bw = 0.0
        sens_bws = {}

        field_ids = self.field(intent, field, vislist=vis)  # list of strings with comma separated IDs per MS
        phasecenter = self.phasecenter(field_ids, vislist=vis)  # string
        if gridder == 'mosaic':
            center_field_ids = self.center_field_ids(vis, field, intent, phasecenter, exclude_intent='ATMOSPHERE') # list of integer IDs per MS
        else:
            center_field_ids = self.center_field_ids(vis, field, intent, phasecenter)  # list of integer IDs per MS

        for ms_index, msname in enumerate(vis):
            ms = self.observing_run.get_ms(name=msname)
            for intSpw in map(int, spw.split(',')):
                try:
                    real_spwid = self.observing_run.virtual2real_spw_id(intSpw, self.observing_run.get_ms(msname))
                    spw_do = ms.get_spectral_window(real_spwid)
                    sens_bws[intSpw] = 0.0
                    if (specmode == 'cube'):
                        # Use the center channel selection
                        if nbin != -1:
                            chansel = '%d~%d' % (int((spw_do.num_channels - nbin) / 2.0), int((spw_do.num_channels + nbin) / 2.0) - 1)
                            nchan_sel = nbin
                        else:
                            chansel = '%d~%d' % (int(spw_do.num_channels / 2.0), int(spw_do.num_channels / 2.0))
                            nchan_sel = 1
                    else:
                        if spw_topo_chan_param_dict.get(os.path.basename(msname), False):
                            if spw_topo_chan_param_dict[os.path.basename(msname)].get(str(intSpw), '') != '':
                                # Use continuum frequency selection
                                chansel = spw_topo_chan_param_dict[os.path.basename(msname)][str(intSpw)]
                                nchan_sel = np.sum([c[1]-c[0]+1 for c in [list(map(int, sel.split('~')))
                                                                          for sel in chansel.split(';')]])
                            else:
                                # Use full spw
                                chansel = '0~%d' % (spw_do.num_channels - 1)
                                nchan_sel = spw_do.num_channels
                        else:
                            # Use full spw
                            chansel = '0~%d' % (spw_do.num_channels - 1)
                            nchan_sel = spw_do.num_channels

                    chansel_full = '0~%d' % (spw_do.num_channels - 1)

                    # If necessary, calculate center field full spw sensitivity
                    try:
                        calc_sens = False
                        if force_calc:
                            # Reset flag to avoid clearing the dictionary several times
                            force_calc = False
                            local_known_sensitivities = {}
                            LOG.info('Got manual request to recalculate sensitivities.')
                            raise Exception('Got manual request to recalculate sensitivities.')
                        if local_known_sensitivities['robust'] != robust:
                            LOG.info('robust value changed (old: %.1f, new: %.1f). Re-calculating sensitivities.' % (local_known_sensitivities['robust'], robust))
                            local_known_sensitivities = {}
                            raise Exception('robust value changed (old: %.1f, new: %.1f). Re-calculating sensitivities.' % (local_known_sensitivities['robust'], robust))
                        if local_known_sensitivities['uvtaper'] != uvtaper:
                            LOG.info('uvtaper value changed (old: %s, new: %s). Re-calculating sensitivities.' % (str(local_known_sensitivities['uvtaper']), str(uvtaper)))
                            local_known_sensitivities = {}
                            raise Exception('uvtaper value changed (old: %s, new: %s). Re-calculating sensitivities.' % (str(local_known_sensitivities['uvtaper']), str(uvtaper)))
                        center_field_full_spw_sensitivity = cqa.getvalue(cqa.convert(local_known_sensitivities[os.path.basename(msname)][field][intent][intSpw]['sensitivityAllChans'], 'Jy/beam'))[0]
                        nchan_unflagged = local_known_sensitivities[os.path.basename(msname)][field][intent][intSpw]['nchanUnflagged']
                        eff_ch_bw = cqa.getvalue(cqa.convert(local_known_sensitivities[os.path.basename(msname)][field][intent][intSpw]['effChanBW'], 'Hz'))[0]
                        sens_bws[intSpw] = cqa.getvalue(cqa.convert(local_known_sensitivities[os.path.basename(msname)][field][intent][intSpw]['sensBW'], 'Hz'))[0]
                        LOG.info('Using previously calculated full SPW apparentsens value of %.3g Jy/beam'
                                 ' for EB %s Field %s Intent %s SPW %s' % (center_field_full_spw_sensitivity,
                                                                           os.path.basename(msname).replace('.ms', ''),
                                                                           field, intent, str(intSpw)))
                    except Exception as e:
                        calc_sens = True
                        center_field_full_spw_sensitivity, eff_ch_bw, sens_bws[intSpw] = self.get_sensitivity(ms, center_field_ids[ms_index], intent, intSpw, chansel_full, specmode, cell, imsize, weighting, robust, uvtaper)
                        channel_flags = self.get_channel_flags(msname, field, intSpw)
                        nchan_unflagged = np.where(channel_flags == False)[0].shape[0]
                        local_known_sensitivities['recalc'] = True
                        local_known_sensitivities['robust'] = robust
                        local_known_sensitivities['uvtaper'] = uvtaper
                        utils.set_nested_dict(local_known_sensitivities, (os.path.basename(msname), field, intent, intSpw, 'sensitivityAllChans'), '%.3g Jy/beam' % (center_field_full_spw_sensitivity))
                        utils.set_nested_dict(local_known_sensitivities, (os.path.basename(msname), field, intent, intSpw, 'nchanUnflagged'), nchan_unflagged)
                        utils.set_nested_dict(local_known_sensitivities, (os.path.basename(msname), field, intent, intSpw, 'effChanBW'), '%s Hz' % (eff_ch_bw))
                        utils.set_nested_dict(local_known_sensitivities, (os.path.basename(msname), field, intent, intSpw, 'sensBW'), '%s Hz' % (sens_bws[intSpw]))

                    # Correct from full spw to channel selection
                    chansel_corrected_center_field_sensitivity = center_field_full_spw_sensitivity * (float(nchan_unflagged) / float(nchan_sel)) ** 0.5
                    sens_bws[intSpw] = sens_bws[intSpw] * float(nchan_sel) / float(spw_do.num_channels)
                    LOG.info('Channel selection bandwidth heuristic (nbin or findcont; (spw BW / nchan_sel BW) ** 0.5):'
                             ' Correcting sensitivity for EB %s Field %s SPW %s by %.3g from %.3g Jy/beam to'
                             ' %.3g Jy/beam' % (os.path.basename(msname).replace('.ms', ''), field, str(intSpw),
                                                (float(nchan_unflagged) / float(nchan_sel)) ** 0.5,
                                                center_field_full_spw_sensitivity,
                                                chansel_corrected_center_field_sensitivity))

                    # Correct for effective bandwidth effects
                    bw_corr_factor, physicalBW_of_1chan, effectiveBW_of_1chan = self.get_bw_corr_factor(ms, intSpw, nchan_sel)
                    center_field_sensitivity = chansel_corrected_center_field_sensitivity * bw_corr_factor
                    if bw_corr_factor != 1.0:
                        LOG.info('Effective BW heuristic: Correcting sensitivity for EB %s Field %s SPW %s by %.3g from %.3g Jy/beam to %.3g Jy/beam' % (os.path.basename(msname).replace('.ms', ''), field, str(intSpw), bw_corr_factor, chansel_corrected_center_field_sensitivity, center_field_sensitivity))

                    if gridder == 'mosaic':
                        # Correct for mosaic overlap factor
                        source_name = [f.source.name for f in ms.fields if (utils.dequote(f.name) == utils.dequote(field) and intent in f.intents)][0]
                        diameter = np.median([a.diameter for a in ms.antennas])
                        overlap_factor = mosaicoverlap.mosaicOverlapFactorMS(ms, source_name, intSpw, diameter)
                        LOG.info('Dividing by mosaic overlap improvement factor of %s corrects sensitivity for EB %s'
                                 ' Field %s SPW %s from %.3g Jy/beam to %.3g Jy/beam.'
                                 '' % (overlap_factor, os.path.basename(msname).replace('.ms', ''), field, str(intSpw),
                                       center_field_sensitivity, center_field_sensitivity / overlap_factor))
                        center_field_sensitivity = center_field_sensitivity / overlap_factor

                        if calc_sens and not center_only:
                            # Calculate diagnostic sensitivities for first and last field
                            first_field_id = min(int(i) for i in field_ids[ms_index].split(','))
                            first_field_full_spw_sensitivity, first_field_eff_ch_bw, first_field_sens_bw = self.get_sensitivity(ms, first_field_id, intent, intSpw, chansel_full, specmode, cell, imsize, weighting, robust, uvtaper)
                            first_field_sensitivity = first_field_full_spw_sensitivity * (float(nchan_unflagged) / float(nchan_sel)) ** 0.5 * bw_corr_factor / overlap_factor
                            last_field_id = max(int(i) for i in field_ids[ms_index].split(','))
                            last_field_full_spw_sensitivity, last_field_eff_ch_bw, last_field_sens_bw = self.get_sensitivity(ms, last_field_id, intent, intSpw, chansel_full, specmode, cell, imsize, weighting, robust, uvtaper)
                            last_field_sensitivity = last_field_full_spw_sensitivity * (float(nchan_unflagged) / float(nchan_sel)) ** 0.5 * bw_corr_factor / overlap_factor

                            LOG.info('Corrected sensitivities for EB %s, Field %s, SPW %s for the first, central, and'
                                     ' last pointings are: %.3g / %.3g / %.3g Jy/beam'
                                     '' % (os.path.basename(msname).replace('.ms', ''), field, str(real_spwid),
                                           first_field_sensitivity, center_field_sensitivity, last_field_sensitivity))

                    sensitivities.append(center_field_sensitivity)
                except Exception as e:
                    # Simply pass as this could be a case of a source not
                    # being present in the MS.
                    pass

        if (len(sensitivities) > 0):
            sensitivity = 1.0 / np.sqrt(np.sum(1.0 / np.array(sensitivities) ** 2))
            # Calculate total bandwidth for this selection
            sens_bw = sum(sens_bws.values())
            if specmode == 'cont' and spw_topo_chan_param_dict == {}:
                # Correct for spw frequency overlaps
                agg_bw = cqa.getvalue(cqa.convert(self.aggregate_bandwidth(list(map(int, spw.split(',')))), 'Hz'))
                sensitivity = sensitivity * (sens_bw / agg_bw) ** 0.5
                sens_bw = agg_bw
            LOG.info('Final sensitivity estimate for Field %s, SPW %s specmode %s: %.3g Jy/beam', field, str(spw), specmode, sensitivity)
        else:
            defaultSensitivity = None
            LOG.warning('Exception in calculating sensitivity.')
            sensitivity = defaultSensitivity
            sens_bw = None

        return sensitivity, eff_ch_bw, sens_bw, copy.deepcopy(local_known_sensitivities)

    def get_sensitivity(self, ms_do, field, intent, spw, chansel, specmode, cell, imsize, weighting, robust, uvtaper):
        """
        Get sensitivity for a field / spw / chansel combination from CASA's
        apparentsens method and a correction for effective channel widths
        in case of online smoothing.

        This heuristic is currently optimized for ALMA data only.
        """

        real_spwid = self.observing_run.virtual2real_spw_id(spw, ms_do)
        chansel_sensitivities = []
        sens_bw = 0.0
        for chanrange in chansel.split(';'):

            scanids = [str(scan.id) for scan in ms_do.scans
                       if intent in scan.intents
                       and field in [fld.name for fld in scan.fields]]
            scanids = ','.join(scanids)
            antenna_ids = self.antenna_ids(intent, [os.path.basename(ms_do.name)])
            taql = '||'.join(['ANTENNA1==%d' % i for i in antenna_ids[os.path.basename(ms_do.name)]])

            try:
                with casatools.SelectvisReader(ms_do.name, spw='%s:%s' % (real_spwid, chanrange), field=field,
                                               scan=scanids, taql=taql) as imTool:
                    imTool.defineimage(mode=specmode if specmode == 'cube' else 'mfs', spw=real_spwid, cellx=cell[0],
                                       celly=cell[0], nx=imsize[0], ny=imsize[1])
                    imTool.weight(type=weighting, rmode='norm', robust=robust)
                    if uvtaper not in (None, []):
                        if len(uvtaper) == 1:
                            bmaj = bmin = uvtaper[0]
                            bpa = '0.0deg'
                        elif len(uvtaper) == 3:
                            bmaj, bmin, bpa = uvtaper
                        else:
                            raise Exception('Unknown uvtaper format: %s' % (str(uvtaper)))
                        imTool.filter(type='gaussian', bmaj=bmaj, bmin=bmin, bpa=bpa)
                    result = imTool.apparentsens()

                if result[1] == 0.0:
                    raise Exception('Empty selection')

                apparentsens_value = result[1]

                LOG.info('apparentsens result for EB %s Field %s SPW %s ChanRange %s: %s Jy/beam'
                         '' % (os.path.basename(ms_do.name).replace('.ms', ''), field, real_spwid, chanrange,
                               apparentsens_value))

                cstart, cstop = list(map(int, chanrange.split('~')))
                nchan = cstop - cstart + 1

                SCF, physicalBW_of_1chan, effectiveBW_of_1chan = self.get_bw_corr_factor(ms_do, spw, nchan)
                sens_bw += nchan * physicalBW_of_1chan

                chansel_sensitivities.append(apparentsens_value)

            except Exception as e:
                if (str(e) != 'Empty selection'):
                    LOG.info('Could not calculate sensitivity for EB %s Field %s SPW %s ChanRange %s: %s'
                             '' % (os.path.basename(ms_do.name).replace('.ms', ''), field, real_spwid, chanrange, e))

        if (len(chansel_sensitivities) > 0):
            return 1.0 / np.sqrt(np.sum(1.0 / np.array(chansel_sensitivities) ** 2)), effectiveBW_of_1chan, sens_bw
        else:
            return 0.0, effectiveBW_of_1chan, sens_bw

    def dr_correction(self, threshold, dirty_dynamic_range, residual_max, intent, tlimit):

        '''Adjustment of cleaning threshold due to dynamic range limitations.'''

        DR_correction_factor = 1.0
        maxEDR_used = False

        return threshold, DR_correction_factor, maxEDR_used

    def rms_threshold(self, rms, nsigma):
        return None

    def niter_correction(self, niter, cell, imsize, residual_max, threshold):
        # TODO: parameter should be clean mask to be able to count the pixels

        '''Adjustment of number of cleaning iterations due to mask size.'''

        return niter

    def get_autobox_params(self, intent, specmode, robust):

        '''Default auto-boxing parameters.'''

        sidelobethreshold = None
        noisethreshold = None
        lownoisethreshold = None
        negativethreshold = None
        minbeamfrac = None
        growiterations = None
        dogrowprune = None
        minpercentchange = None

        return sidelobethreshold, noisethreshold, lownoisethreshold, negativethreshold, minbeamfrac, growiterations, dogrowprune, minpercentchange

    def nterms(self):
        return None

    def cyclefactor(self):
        return None

    def cycleniter(self):
        return None

    def scales(self):
        return None

    def uvtaper(self, beam_natural=None, protect_long=None):
        return None

    def uvrange(self):
        return None

    def reffreq(self):
        return None

    def restfreq(self):
        return None

    def conjbeams(self):
        return None

    def pb_correction(self):
        return True

    def antenna_diameters(self, vislist=None):

        '''Count the antennas of given diameters per MS.'''

        if vislist is None:
            local_vislist = self.vislist
        else:
            local_vislist = vislist

        antenna_diameters = {}
        for vis in local_vislist:
            for antenna in self.observing_run.get_ms(vis).antennas:
                if antenna.diameter not in antenna_diameters:
                    antenna_diameters[antenna.diameter] = 0
                antenna_diameters[antenna.diameter] += 1

        return antenna_diameters

    def majority_antenna_ids(self, vislist=None):

        '''Get the IDs of the majority (by diameter) antennas per MS.'''

        if vislist is None:
            local_vislist = self.vislist
        else:
            local_vislist = vislist

        # Determine majority diameter
        antenna_diameters = self.antenna_diameters(local_vislist)
        majority_diameter = sorted(antenna_diameters.items(), key=operator.itemgetter(1))[-1][0]

        majority_antenna_ids = {}
        for vis in local_vislist:
            majority_antenna_ids[os.path.basename(vis)] = [antenna.id
                                                           for antenna in self.observing_run.get_ms(vis).antennas
                                                           if antenna.diameter == majority_diameter]

        return majority_antenna_ids

    def antenna_ids(self, intent, vislist=None):

        '''Get the antenna IDs to be used for imaging.'''

        if vislist is None:
            local_vislist = self.vislist
        else:
            local_vislist = vislist

        if intent != 'TARGET':
            # For calibrators use all antennas
            antenna_ids = {}
            for vis in local_vislist:
                antenna_ids[os.path.basename(vis)] = [antenna.id for antenna in self.observing_run.get_ms(vis).antennas]
            return antenna_ids
        else:
            # For science targets use majority antennas only
            return self.majority_antenna_ids(local_vislist)

    def check_psf(self, psf_name, field, spw):

        '''Check for bad psf fits.'''

        cqa = casatools.quanta

        with casatools.ImageReader(psf_name) as image:
            try:
                beams = image.restoringbeam()['beams']
                bmaj = np.array([cqa.getvalue(cqa.convert(b['*0']['major'], 'arcsec')) for b in beams.itervalues()])

                # Filter empty psf planes
                bmaj = bmaj[np.where(bmaj > 1e-6)]

                bmaj_median = np.median(bmaj)
                cond1 = np.logical_and(0.0 < bmaj, bmaj < 0.5 * bmaj_median)
                cond2 = bmaj > 2.0 * bmaj_median
                if np.logical_or(cond1, cond2).any():
                    LOG.warn('The PSF fit for one or more channels for field %s SPW %s failed, please check the'
                             ' results for this cube carefully, there are likely data issues.' % (field, spw))
            except:
                pass

    def usepointing(self):

        '''tclean flag to use pointing table.'''

        # Currently ALMA and VLA do not want to use the table (CAS-11840).
        return False

    def mosweight(self, intent, field):

        '''tclean flag to use mosaic weighting.'''

        # Currently only ALMA has decided to use this flag (CAS-11840). So
        # the default is set to False here.
        return False

    def keep_iterating(self, iteration, hm_masking, tclean_stopcode, dirty_dynamic_range, residual_max, residual_robust_rms, field, intent, spw, specmode):

        '''Determine if another tclean iteration is necessary.'''

        if iteration == 0:
            return True, hm_masking
        else:
            return False, hm_masking

    def threshold(self, iteration, threshold, rms_threshold, nsigma, hm_masking):

        if iteration == 0:
            return '0.0mJy'
        else:
            return threshold

    def nsigma(self, iteration, hm_masking):

        return None

    def savemodel(self, iteration):

        return None

    def stokes(self):
        return 'I'

    def mask(self):
        return ''

    def specmode(self):
        return 'mfs'
