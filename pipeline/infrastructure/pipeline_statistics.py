from pipeline import environment
from pipeline.infrastructure.renderer import stats_extractor
from pipeline.domain import measures
from . import logging
from collections import namedtuple
from typing import Dict, Tuple

import enum

import numpy as np

LOG = logging.get_logger(__name__)


class PipelineStatistics(object):
    class Level(enum.Enum):
        MOUS = 1
        EB = 2
        SPW = 3

    """A unit of pipeline statistics information"""
    def __init__(self, name, value, longdesc, origin='', units='',
                    level='', spw=None, mous=None, eb=None):
        self.name = name
        self.value = value
        self.longdesc = longdesc
        self.origin = origin
        self.units = units
        self.level = level
        self.spw = spw
        self.mous = mous
        self.eb = eb

        # TODO: if the structure is be not nested, some information about the mous, spw, etc needs to be added

        # Convert results to values that can be serialized by JSON
        # this could be done as part of the output instead
        if type(value) is set:
            self.value = list(self.value)
        elif type(value) is np.int64:
            self.value = int(self.value)
        elif type(value) is np.ndarray:
            self.value = list(self.value)

    def to_dict(self):
        stats_dict = {}

#        stats_dict['name'] = self.name is the next level up in the dict.
        stats_dict['longdescription'] = self.longdesc

        stats_dict['level'] = self.level

        if self.origin not in ["", None]:
            stats_dict['origin'] = self.origin

        if self.units not in ["", None]:
            stats_dict['units'] = self.units

        if self.value not in ["", None]:
            stats_dict['value'] = self.value

        if self.spw not in ["", None]:
            stats_dict['spw'] = self.spw

        if self.mous not in ["", None]:
            stats_dict['mous'] = self.mous

        if self.eb not in ["", None]:
            stats_dict['eb'] = self.eb

        return stats_dict


def _generate_stats(context):
    # Set 1: values that can be gathered directly from the context
    mous = context.get_oussid()
    ps1 = PipelineStatistics(
        name='project_id',
        value=context.observing_run.project_ids,
        longdesc='Proposal id number',
        origin="hifa_importdata",
        mous=mous,
        level='MOUS')

    ps2 = PipelineStatistics(
        name='pipeline_version',
        value=environment.pipeline_revision,
        longdesc="pipeline version string",
        origin="hifa_importdata",
        mous=mous,
        level='MOUS')

    ps3 = PipelineStatistics(
        name='pipeline_recipe',
        value=context.project_structure.recipe_name,
        longdesc="recipe name",
        origin="hifa_importdata",
        mous=mous,
        level='MOUS')

    ps4 = PipelineStatistics(
        name='casa_version',
        value=environment.casa_version_string,
        longdesc="casa version string",
        origin="hifa_importdata",
        mous=mous,
        level='MOUS')

    ps5 = PipelineStatistics(
        name='mous_uid',
        value=context.get_oussid(),
        longdesc="Member Obs Unit Set ID",
        origin="hifa_importdata",
        mous=mous,
        level="MOUS")

    stats_collection = []
    stats_collection.append(ps1)
    stats_collection.append(ps2)
    stats_collection.append(ps3)
    stats_collection.append(ps4)
    stats_collection.append(ps5)

    # per-EB
    ms_list = context.observing_run.get_measurement_sets()
    for ms in ms_list:
        eb = ms.name
        psm1 = PipelineStatistics(
            name='n_ant',
            value=len(ms.antennas),
            longdesc="Number of antennas per execution block",
            origin="hifa_importdata",
            eb=eb,
            level="EB")
        stats_collection.append(psm1)

        psm2 = PipelineStatistics(
            name='n_spw',
            value=len(ms.get_all_spectral_windows()),
            longdesc="number of spectral windows",
            origin="hifa_importdata",
            eb=eb,
            level="EB")
        stats_collection.append(psm2)

        psm3 = PipelineStatistics(
            name='n_scans',
            value=len(ms.get_scans()),
            longdesc="number of scans per science target",
            origin="hifa_importdata",
            eb=eb,
            level="EB")
        stats_collection.append(psm3)

        # per-SPW
        spw_list = ms.get_all_spectral_windows()
        for spw in spw_list:
            sps1 = PipelineStatistics(
                name='spw_width',
                value=float(spw.bandwidth.to_units(measures.FrequencyUnits.MEGAHERTZ)),
                longdesc="width of science spectral windows",
                origin="hifa_importdata",
                units="MHz",
                spw=spw.id,
                level="SPW")
            stats_collection.append(sps1)

            sps2 = PipelineStatistics(
                name='spw_freq',
                value=float(spw.centre_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ)),
                longdesc="central frequency for each science spectral window in TOPO",
                origin="hifa_importdata",
                units="GHz",
                spw=spw.id,
                level="SPW")
            stats_collection.append(sps2)

            sps3 = PipelineStatistics(
                name='spw_nchan',
                value=spw.num_channels,
                longdesc="number of channels in spectral windows",
                origin="hifa_importdata",
                spw=spw.id,
                level="SPW")
            stats_collection.append(sps3)

            sps4 = PipelineStatistics(
                name='nbin_online',
                value=spw.sdm_num_bin,
                longdesc="online nbin factors ",
                origin="hifa_importdata",
                spw=spw.id,
                level="SPW")
            stats_collection.append(sps4)

            chan_width_MHz = [chan * 1e-6 for chan in spw.channels.chan_widths]
            sps5 = PipelineStatistics(
                name='chan_width',
                value=chan_width_MHz,
                longdesc="frequency width of channels in spectral windows",
                origin="hifa_importdata",
                units="MHz",
                spw=spw.id,
                level="SPW")
            stats_collection.append(sps5)

    # Set 2: results objects needed
    stats_from_results = stats_extractor.get_stats_from_results(context)
    for elt in stats_from_results:
        if elt not in [[], [[]]]:
            result = elt[0][0]
            stats_collection.append(result)

    LOG.info(stats_collection)
    # construct final dictionary
    final_dict = {}
    for stat in stats_collection:
        final_dict[stat.name] = stat.to_dict()
    final_dict["version"] = 0.1
    return final_dict
