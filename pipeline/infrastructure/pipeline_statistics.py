import datetime
import enum
from typing import Dict, List, Set, Union

import numpy as np

from pipeline import environment
from pipeline.domain import measures
from pipeline.domain.datatype import DataType
from pipeline.infrastructure import utils

from . import logging

LOG = logging.get_logger(__name__)


class PipelineStatisticsLevel(enum.Enum):
    """
    An enum to specify which "level" of information an individual pipeline statistic applies to:
    SPW, EB, or MOUS.
    """
    MOUS = enum.auto()
    EB = enum.auto()
    SPW = enum.auto()


class PipelineStatistics(object):
    """A single unit of pipeline statistics information.

    Attributes:
        name: The name of this pipeline statistic
        value: The value for this pipeline statistic
        longdesc: A long description of the value
        origin: The stage that this value was calculated or populated in (Optional)
        units: The units associated with the value (Optional)
        level: A PipelineStatisticsLevel that specifies whether this value applies to a MOUS, EB, or SPW
        spw: The SPW this value applies to (if applicable) (Optional)
        eb: The EB or MS this value applies to (if applicable) (Optional)
        mous: The MOUS this value applies to
    """
    def __init__(self, name: str, value: Union[str, int, float, List, Dict, Set, np.int64, np.ndarray],
                 longdesc: str, origin: str='', units: str='',
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
        self.mous = mous
        self.eb = eb
        self.spw = spw

        # Convert initial value from the pipeline to a value that can be serialized by JSON
        # In the future, it may make sense to move this conversion to when the data is written out.
        if type(value) is set:
            self.value = list(self.value)
        elif type(value) is np.int64:
            self.value = int(self.value)
        elif type(value) is np.ndarray:
            self.value = list(self.value)

    def __str__(self) -> str:
        return 'PipelineStatistics({!s}, {!r}, {!r}, {!s})'.format(self.name, self.value, self.origin, self.units)

    def to_dict(self) -> Dict:
        """
        Convert an individual pipeline statistics item to a dictionary
        representation
        """
        stats_dict = {}

        if self.value not in ["", None]:
            stats_dict['value'] = self.value

        if self.units not in ["", None]:
            stats_dict['units'] = self.units

        if self.longdesc not in ["", None]: 
            stats_dict['longdescription'] = self.longdesc

        if self.origin not in ["", None]:
            stats_dict['origin'] = self.origin

        return stats_dict


def to_nested_dict(stats_collection) -> Dict:
    """
    Generates a "nested" output dict with EBs, SPWs, MOUSs
    Each level is represented in the structure of the output.
    """
    final_dict = {}

    # Step through the collect statistics values and construct
    # a dictionary representation. The output format is as follows:
    # { mous_name: {
    #    mous_property: { ...
    #    }
    #    "EB": {
    #       eb_name: {
    #           eb_property: {...
    #           }
    #       }
    #    }
    #    "SPW": {
    #       virtual_spw_id: {
    #           spw_property: {...
    #           }
    #       }
    #    }
    #  }
    #  header: {version: 0.1, creation_date: YYMMDD-HH:MM:SS Z}
    # }
    for stat in stats_collection:
        if stat.level == PipelineStatisticsLevel.MOUS:
            if stat.mous not in final_dict:
                final_dict[stat.mous] = {}
            final_dict[stat.mous][stat.name] = stat.to_dict()
        elif stat.level == PipelineStatisticsLevel.EB:
            if stat.mous not in final_dict:
                final_dict[stat.mous] = {}
                final_dict[stat.mous]["EB"] = {}
            if "EB" not in final_dict[stat.mous]:
                final_dict[stat.mous]["EB"] = {}
            if stat.eb not in final_dict[stat.mous]["EB"]:
                final_dict[stat.mous]["EB"][stat.eb] = {}
            final_dict[stat.mous]["EB"][stat.eb][stat.name] = stat.to_dict()
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

    # Generate and append a header with information about statistics file version and date created
    version_dict = _generate_header()
    final_dict['header'] = version_dict

    return final_dict


def _generate_header() -> Dict:
    """
    Creates a header with information about the pipeline stats file
    """
    version_dict = {}
    version_dict["version"] = 1.0
    now = datetime.datetime.now(datetime.timezone.utc)
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S %Z")
    version_dict["stats_file_creation_date"] = dt_string
    return version_dict


def generate_product_pl_run_info(context) -> List[PipelineStatistics]:
    """
    Gather statistics results for the pipleline run information and pipeline product information
    These can be directly obtained from the context.
    """
    stats_collection = []
    mous = context.get_oussid()

    # List of datatypes to use (in order) for fetching EB-level information.
    # The following function call will fetch all the MSes for ONLY the first
    # datatype it finds in the list. This is needed so that information is
    # not repeated for the ms and _targets.ms when both are present.
    datatypes = [DataType.REGCAL_CONTLINE_ALL, DataType.REGCAL_CONTLINE_SCIENCE, DataType.SELFCAL_CONTLINE_SCIENCE,
        DataType.REGCAL_LINE_SCIENCE, DataType.SELFCAL_LINE_SCIENCE, DataType.RAW]
    ms_list = context.observing_run.get_measurement_sets_of_type(datatypes)

    # Add MOUS-level information
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

    ps6 = PipelineStatistics(
        name='n_EB',
        value=len(context.observing_run.execblock_ids),
        longdesc="number of execution blocks",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps6)

    all_bands = sorted({spw.band for spw in ms_list[0].get_all_spectral_windows()})
    # TODO: or science spws, or for all MSes?
    ps7 = PipelineStatistics(
        name='bands',
        value=len(all_bands),
        longdesc="Band(s) used in observations. Usually 1, but maybe more than 1 for B2B observations",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps7)

    first_ms = ms_list[0]
    science_source_names = sorted({source.name for source in first_ms.sources if 'TARGET' in source.intents})
    ps8 = PipelineStatistics(
        name='n_targets',
        value=len(science_source_names),
        longdesc="total number of science targets in the MOUS",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps8)

    ps9 = PipelineStatistics(
        name='target_list',
        value=science_source_names,
        longdesc="list of science target names",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps9)

    pointings = {}
    science_sources = sorted({source for source in first_ms.sources if 'TARGET' in source.intents})
    for source in science_sources:
        pointings[source.name] = len([f for f in first_ms.fields
                                        if f.source_id == source.id])
    ps10 = PipelineStatistics(
        name='n_pointings',
        value=pointings,
        longdesc="number of mosaic pointings for each science target",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps10)

    ps11 = PipelineStatistics(
        name='rep_target',
        value=first_ms.representative_target[0],
        longdesc="representative target name",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps11)

    ps12 = PipelineStatistics(
        name='n_spw',
        value=len(first_ms.get_all_spectral_windows()),
        longdesc="number of spectral windows",
        origin="hifa_importdata",
        mous=mous,
        level=PipelineStatisticsLevel.MOUS)
    stats_collection.append(ps12)

    # Add per-EB information:
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

        psm3 = PipelineStatistics(
            name='n_scan',
            value=len(ms.get_scans()),
            longdesc="number of scans per science target",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.EB)
        stats_collection.append(psm3)

        l80 = np.percentile(ms.antenna_array.baselines_m, 80)
        psm10 = PipelineStatistics(
            name='L80',
            value=l80,
            longdesc="80% percentile baseline",
            origin="hifa_importdata",
            units="m",
            mous=mous,
            eb=eb,
            level=PipelineStatisticsLevel.EB)
        stats_collection.append(psm10)

    # Add per-SPW stats information
    # Virtual spws are used so information can just be included once per MOUS.
    ms = ms_list[0]
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
            level=PipelineStatisticsLevel.SPW)
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
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps2)

        sps3 = PipelineStatistics(
            name='spw_nchan',
            value=spw.num_channels,
            longdesc="number of channels in spectral windows",
            origin="hifa_importdata",
            eb=eb,
            mous=mous,
            spw=spw.id,
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps3)

        sps4 = PipelineStatistics(
            name='nbin_online',
            value=spw.sdm_num_bin,
            longdesc="online nbin factors ",
            origin="hifa_importdata",
            spw=spw.id,
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
            spw=spw.id,
            eb=eb,
            mous=mous,
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps5)

        dd = ms.get_data_description(spw=int(spw.id))
        numpols = dd.num_polarizations

        sps6 = PipelineStatistics(
            name='spw_npol',
            value=numpols,
            longdesc="number of polarizations in the data set",
            origin="hifa_importdata",
            mous=mous,
            eb=eb,
            spw=spw.id,
            level=PipelineStatisticsLevel.SPW)
        stats_collection.append(sps6)
    return stats_collection
