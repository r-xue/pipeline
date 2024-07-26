from pipeline import environment
from pipeline.infrastructure.renderer import stats_extractor
from pipeline.domain import measures
from . import logging
from typing import Dict

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

        # Convert results to values that can be serialized by JSON
        # this could be done as part of the output instead
        if type(value) is set:
            self.value = list(self.value)
        elif type(value) is np.int64:
            self.value = int(self.value)
        elif type(value) is np.ndarray:
            self.value = list(self.value)

    def to_dict(self, level_info: bool = False) -> Dict:
        """
        Convert an individual statistics item to dict
        
        level_info = True will include information about what
        MOUS/EB/SPW the value is from.
        """
        stats_dict = {} 

        stats_dict['longdescription'] = self.longdesc

        if self.origin not in ["", None]:
            stats_dict['origin'] = self.origin

        if self.units not in ["", None]:
            stats_dict['units'] = self.units

        if self.value not in ["", None]:
            stats_dict['value'] = self.value

        if level_info:
            stats_dict['name'] = self.name

            stats_dict['level'] = self.level

            if self.spw not in ["", None]:
                stats_dict['spw'] = self.spw

            if self.mous not in ["", None]:
                stats_dict['mous'] = self.mous

            if self.eb not in ["", None]:
                stats_dict['eb'] = self.eb

        return stats_dict


def generate_stats(context, output_format: str = "nested") -> Dict:
    """
    Gathers statistics from the context and returns a representation
    of them as a dict.

    output_format can be "nested" for a dict which contains information
    about whether values are per-MOUS, EB, SPW based on the structure
    of the output or "flat" for a flat structure with tags that
    indiciate the MOUS, EB, SPW a value is associated with.
    """
    stats_collection = []
    # First, gather statistics about the project and pipeline run info
    product_run_info = _generate_product_pl_run_info(context)
    stats_collection.extend(product_run_info)

    # Next, gather statistics that require the results objects
    stats_from_results = stats_extractor.get_stats_from_results(context)
    for elt in stats_from_results:
        stats_collection.append(elt)

    # Construct dictionary representation of all pipeline stats
    if output_format == "flat":
        final_dict = to_flat_dict(stats_collection)
    else:
        final_dict = to_nested_dict(stats_collection)

    return final_dict


def to_nested_dict(stats_collection) -> Dict:
    """
    Generates a "nested" output dict with EBs, SPWs, MOUSs
    Each level is represented in the structure of the output.
    """
    final_dict = {}
    for stat in stats_collection:
        if stat.level == "EB":
            if stat.mous not in final_dict:
                final_dict[stat.mous] = {}
            if stat.eb not in final_dict[stat.mous]:
                final_dict[stat.mous][stat.eb] = {}
            final_dict[stat.mous][stat.eb][stat.name] = stat.to_dict()
        elif stat.level == "MOUS":
            if stat.mous not in final_dict:
                final_dict[stat.mous] = {}
            final_dict[stat.mous][stat.name] = stat.to_dict()
        elif stat.level == "SPW":
            if stat.mous not in final_dict:
                final_dict[stat.mous] = {}
            if stat.eb not in final_dict[stat.mous]:
                final_dict[stat.mous][stat.eb] = {}
            if stat.spw not in final_dict[stat.mous][stat.eb]:
                final_dict[stat.mous][stat.eb][stat.spw] = {}
            final_dict[stat.mous][stat.eb][stat.spw][stat.name] = stat.to_dict()
        else:
            pass  # Shouldn't be possible to get here
    version_dict = _generate_header()
    final_dict['header'] = version_dict

    return final_dict


def to_flat_dict(stats_collection) -> Dict:
    """
    Generates a "flat" output dict with EBs, SPWs, MOUSs just 'tagged' and labeled
    not as part of the structure
    """
    final_dict = []
    for stat in stats_collection:
        final_dict.append(stat.to_dict(level_info=True))
    version_dict = _generate_header()
    final_dict.append(version_dict)
    return final_dict


def _generate_header() -> Dict:
    """
    Creates a header with information about the stats file
    """
    version_dict = {}
    version_dict["version"] = 0.1
    version_dict["name"] = "header"
#    version_dict["EB"] = []
#    version_dict["MOUS"] = []
#    version_dict["SPW"] = []
    return version_dict


def _generate_product_pl_run_info(context):
    #  Set 1: values that can be gathered directly from the context
    stats_collection = []
    mous = context.get_oussid()

    # MOUS-level
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
            mous=mous,
            level="EB")
        stats_collection.append(psm1)

        psm2 = PipelineStatistics(
            name='n_spw',
            value=len(ms.get_all_spectral_windows()),
            longdesc="number of spectral windows",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            level="EB")
        stats_collection.append(psm2)

        psm3 = PipelineStatistics(
            name='n_scans',
            value=len(ms.get_scans()),
            longdesc="number of scans per science target",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
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
                eb=eb,
                mous=mous,
                level="SPW")
            stats_collection.append(sps1)

            sps2 = PipelineStatistics(
                name='spw_freq',
                value=float(spw.centre_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ)),
                longdesc="central frequency for each science spectral window in TOPO",
                origin="hifa_importdata",
                units="GHz",
                spw=spw.id,
                eb=eb,
                mous=mous,
                level="SPW")
            stats_collection.append(sps2)

            sps3 = PipelineStatistics(
                name='spw_nchan',
                value=spw.num_channels,
                longdesc="number of channels in spectral windows",
                origin="hifa_importdata",
                eb=eb,
                mous=mous,
                spw=spw.id,
                level="SPW")
            stats_collection.append(sps3)

            sps4 = PipelineStatistics(
                name='nbin_online',
                value=spw.sdm_num_bin,
                longdesc="online nbin factors ",
                origin="hifa_importdata",
                spw=spw.id,
                eb=eb,
                mous=mous,
                level="SPW")
            stats_collection.append(sps4)

            chan_width_MHz = [chan * 1e-6 for chan in spw.channels.chan_widths]
            sps5 = PipelineStatistics(
                name='chan_width',
                value=chan_width_MHz[0],
                longdesc="frequency width of channels in spectral windows",
                origin="hifa_importdata",
                units="MHz",
                spw=spw.id,
                eb=eb,
                mous=mous,
                level="SPW")
            stats_collection.append(sps5)
    return stats_collection
