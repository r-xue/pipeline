import datetime

from pipeline import environment
from pipeline.infrastructure.renderer import stats_extractor
from pipeline.domain import measures
from pipeline.domain.datatype import DataType
from . import logging
from typing import Dict

import enum

import numpy as np

LOG = logging.get_logger(__name__)


class PipelineStatisticsLevel(enum.Enum):
    """
    An enum to specify which "level" of infomration the pipeline statistics applies to: 
    SPW, EB, or MOUS.
    """
    MOUS = enum.auto()
    EB = enum.auto()
    SPW = enum.auto()


class PipelineStatistics(object):

    """A unit of pipeline statistics information"""
    def __init__(self, name: str, value, longdesc: str, origin: str='', units: str='',
                    level: PipelineStatisticsLevel=None, spw: str=None, mous: str=None, eb: str=None):
        
        self.name = name
        self.value = value
        self.longdesc = longdesc
        # The origin is the name of the pipeline stage associateted with this statistics value
        self.origin = origin
        self.units = units
        # The level indicates whether a given quantity applies to the whole MOUS, EB, or SPW
        self.level = level
        # The spw, mous, and eb are set, if applicable.
        self.spw = spw
        self.mous = mous
        self.eb = eb

        # Convert initial value from the pipeline to a value that can be serialized by JSON
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
        if stat.level == PipelineStatisticsLevel.EB:
            if stat.mous not in final_dict:
                final_dict[stat.mous] = {}
                final_dict[stat.mous]["EB"] = {}
            if "EB" not in final_dict[stat.mous]:
                final_dict[stat.mous]["EB"] = {}
            if stat.eb not in final_dict[stat.mous]["EB"]:
                final_dict[stat.mous]["EB"][stat.eb] = {}
            final_dict[stat.mous]["EB"][stat.eb][stat.name] = stat.to_dict()
        elif stat.level == PipelineStatisticsLevel.MOUS:
            if stat.mous not in final_dict:
                final_dict[stat.mous] = {}
            final_dict[stat.mous][stat.name] = stat.to_dict()
        elif stat.level == PipelineStatisticsLevel.SPW:
            if stat.mous not in final_dict:
                final_dict[stat.mous] = {}
                final_dict[stat.mous]["SPW"] = {}
            if "SPW" not in final_dict[stat.mous]:
                final_dict[stat.mous]["SPW"] = {}
            if stat.spw not in final_dict[stat.mous]["SPW"]:
                final_dict[stat.mous]["SPW"][stat.spw] = {}
            final_dict[stat.mous]["SPW"][stat.spw][stat.name] = stat.to_dict()
        else:
            LOG.debug("In pipleine statics file creation, invalid level: {} specified.".format(stat.level))

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
    now = datetime.datetime.now(datetime.timezone.utc)
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S %Z")
    version_dict["stats_file_creation_date"] = dt_string
    return version_dict


def _generate_product_pl_run_info(context):
    #  Set 1: values that can be gathered directly from the context
    stats_collection = []
    mous = context.get_oussid()

    # MOUS-level
    ps1 = PipelineStatistics(
        name='project_id',
        value=context.observing_run.project_ids.pop(),
        longdesc='Proposal id number',
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps1)

    ps2 = PipelineStatistics(
        name='pipeline_version',
        value=environment.pipeline_revision,
        longdesc="pipeline version string",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps2)

    ps3 = PipelineStatistics(
        name='pipeline_recipe',
        value=context.project_structure.recipe_name,
        longdesc="recipe name",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps3)

    ps4 = PipelineStatistics(
        name='casa_version',
        value=environment.casa_version_string,
        longdesc="casa version string",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps4)

    ps5 = PipelineStatistics(
        name='mous_uid',
        value=context.get_oussid(),
        longdesc="Member Obs Unit Set ID",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps5)

    # Add per-EB information:

    # List of datatypes to use (in order) for fetching EB-level information.
    # The following function call will fetch all the MSes for ONLY the first 
    # datatype it finds in the list. This is needed so that information is 
    # not repeated for the ms and _targets.ms
    datatypes = [DataType.REGCAL_CONTLINE_ALL, DataType.REGCAL_CONTLINE_SCIENCE, DataType.SELFCAL_CONTLINE_SCIENCE, 
        DataType.REGCAL_LINE_SCIENCE, DataType.SELFCAL_LINE_SCIENCE, DataType.RAW]
    ms_list = context.observing_run.get_measurement_sets_of_type(datatypes)

    for ms in ms_list:
        eb = ms.name
        psm1 = PipelineStatistics(
            name='n_ant',
            value=len(ms.antennas),
            longdesc="Number of antennas per execution block",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.EB)
        stats_collection.append(psm1)

        psm2 = PipelineStatistics(
            name='n_spw',
            value=len(ms.get_all_spectral_windows()),
            longdesc="number of spectral windows",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.EB)
        stats_collection.append(psm2)

        psm3 = PipelineStatistics(
            name='n_scans',
            value=len(ms.get_scans()),
            longdesc="number of scans per science target",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.EB)
        stats_collection.append(psm3)

    # Per-SPW stats information
    # Use virtual spws so information can just be included once per MOUS
    ms = ms_list[0]
    virtual_spw_id = context.observing_run.real2virtual_spw_id(spw.id, ms)
    spw_list = ms.get_all_spectral_windows()
    for spw in spw_list:
        sps1 = PipelineStatistics(
            name='spw_width',
            value=float(spw.bandwidth.to_units(measures.FrequencyUnits.MEGAHERTZ)),
            longdesc="width of science spectral windows",
            origin="hifa_importdata",
            units="MHz",
            spw=virtual_spw_id,
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps1)

        sps2 = PipelineStatistics(
            name='spw_freq',
            value=float(spw.centre_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ)),
            longdesc="central frequency for each science spectral window in TOPO",
            origin="hifa_importdata",
            units="GHz",
            spw=virtual_spw_id,
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps2)

        sps3 = PipelineStatistics(
            name='spw_nchan',
            value=spw.num_channels,
            longdesc="number of channels in spectral windows",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            spw=virtual_spw_id,
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps3)

        sps4 = PipelineStatistics(
            name='nbin_online',
            value=spw.sdm_num_bin,
            longdesc="online nbin factors ",
            origin="hifa_importdata",
            spw=virtual_spw_id,
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps4)

        chan_width_MHz = [chan * 1e-6 for chan in spw.channels.chan_widths]
        sps5 = PipelineStatistics(
            name='chan_width',
            value=chan_width_MHz[0],
            longdesc="frequency width of channels in spectral windows",
            origin="hifa_importdata",
            units="MHz",
            spw=virtual_spw_id,
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps5)

    return stats_collection
