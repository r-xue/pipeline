"""Parameter classes of Imaging."""

import inspect
import logging
import os
from typing import TYPE_CHECKING, Dict, List, NewType, Union

import numpy

from pipeline.hsd.tasks.common import utils as sdutils
from pipeline.infrastructure import casa_tools
import pipeline.infrastructure as infrastructure

if TYPE_CHECKING:
    from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResults
    from pipeline.infrastructure import Context
    from pipeline.domain.datatable import DataTableImpl
    from pipeline.domain import MeasurementSet
    from pipeline.domain.singledish import MSReductionGroupDesc
    Direction = NewType('Direction', Dict[str, Union[str, float]])

LOG = infrastructure.get_logger(__name__)


def __debug(cls: object, obj: object, msg: str):
    """Output debug strings.

    Args:
        cls (object): caller class object
        obj (_type_): object to output
        msg (_type_): action message
    """
    outerframes = inspect.getouterframes(inspect.currentframe())
    for i, frame in enumerate(inspect.getouterframes(inspect.currentframe())):
        if not frame.filename.endswith(__file__):
            break
    source = os.path.basename(outerframes[i].filename)
    clsname = cls.__class__.__name__
    LOG.debug(f'{source}[{outerframes[i].lineno}] {msg}: {clsname}.{outerframes[i-1].function}: {type(obj)} {obj} '
              f'at {outerframes[i].function}')


class ObservedList(list):
    """Class inherit list to observe its behavior."""

    def __setitem__(self, index: int, value: object):
        """Overrode list.__setitem__().

        Args:
            index (int): index
            value (object): object to set at index
        """
        super().__setitem__(index, value)
        if LOG.level <= logging.DEBUG:
            __debug(self, value, 'list.setitem')

    def insert(self, index: int, value: object):
        """Overrode list.insert().

        Args:
            index (int): index
            value (object): object to insert at index
        """
        super().insert(index, value)
        if LOG.level <= logging.DEBUG:
            __debug(self, value, 'list.insert')

    def append(self, value: object):
        """Overrode list.append().

        Args:
            value (object): object to append
        """
        super().append(value)
        if LOG.level <= logging.DEBUG:
            __debug(self, value, 'list.append')

    def extend(self, value: list):
        """Overrode list.extend().

        Args:
            value (list): object to merge
        """
        super().extend(value)
        if LOG.level <= logging.DEBUG:
            __debug(self, value, 'list.extend')


class Parameters:
    """Abstract class of Parameter object."""

    def setvalue(self, name: str, value: object):
        """Set value by setattr().

        Args:
            name (str): property
            value (object): object
        """
        setattr(self, name, value)
        if LOG.level <= logging.DEBUG:
            __debug(self, value, 'set')


class CommonParameters(Parameters):
    """Common parameters class of prepare()."""

    def __init__(self):
        """Initialize an object."""
        self._args_spw = None         # spw selection per MS
        self._cqa = None              # LoggingQuanta: reference of casatools.quanta
        self._dt_dict = None          # Dict[str, DataTableImpl]: dictionary of input MS and corresponding datatable
        self._edge = None             # List[int]: edge channel of most recent SDBaselineResults or [0, 0]
        self._imagemode = None        # str: input image mode, str
        self._in_field = None         # str: comma-separated list of target field names that are extracted
                                      # from all input MSs
        self._infiles = None          # List[str]: list of input files
        self._is_nro = None           # numpy.bool_: flag of NRO data
        self._ms_list = None          # List[MeasurementSet]: list of ms to process
        self._ms_names = None         # List[str]: list of name of ms in ms_list
        self._reduction_group = None  # Dict[int, MSReductionGroupDesc]: eduction group object
        self._restfreq_list = None    # Union[str, List[str]]: list of rest frequency
        self._results = None          # SDImagingResults: instance of SDImagingResults

    @property
    def args_spw(self): return self._args_spw

    @args_spw.setter
    def args_spw(self, value): self.setvalue('_args_spw', value)

    @property
    def cqa(self) -> 'casa_tools.LoggingQuanta': return self._cqa

    @cqa.setter
    def cqa(self, value: 'casa_tools.LoggingQuanta'): self.setvalue('_cqa', value)

    @property
    def dt_dict(self) -> Dict[str, 'DataTableImpl']:
        return self._dt_dict

    @dt_dict.setter
    def dt_dict(self, value: Dict[str, 'DataTableImpl']):
        self.setvalue('_dt_dict', value)

    @property
    def edge(self) -> List[int]:
        return self._edge

    @edge.setter
    def edge(self, value: List[int]):
        self.setvalue('_edge', value)

    @property
    def imagemode(self) -> str:
        return self._imagemode

    @imagemode.setter
    def imagemode(self, value: str):
        self.setvalue('_imagemode', value)

    @property
    def in_field(self) -> str:
        return self._in_field

    @in_field.setter
    def in_field(self, value: str):
        self.setvalue('_in_field', value)

    @property
    def infiles(self) -> List[str]:
        return self._infiles

    @infiles.setter
    def infiles(self, value: List[str]):
        self.setvalue('_infiles', value)

    @property
    def is_nro(self) -> numpy.bool_:
        return self._is_nro

    @is_nro.setter
    def is_nro(self, value: numpy.bool_):
        self.setvalue('_is_nro', value)

    @property
    def ms_list(self) -> List['MeasurementSet']:
        return self._ms_list

    @ms_list.setter
    def ms_list(self, value: List['MeasurementSet']):
        self.setvalue('_ms_list', value)

    @property
    def ms_names(self) -> List[str]:
        return self._ms_names

    @ms_names.setter
    def ms_names(self, value: List[str]):
        self.setvalue('_ms_names', value)

    @property
    def reduction_group(self) -> Dict[int, 'MSReductionGroupDesc']:
        return self._reduction_group

    @reduction_group.setter
    def reduction_group(self, value: Dict[int, 'MSReductionGroupDesc']):
        self.setvalue('_reduction_group', value)

    @property
    def restfreq_list(self) -> Union[str, List[str]]:
        return self._restfreq_list

    @restfreq_list.setter
    def restfreq_list(self, value: Union[str, List[str]]):
        self.setvalue('_restfreq_list', value)

    @property
    def results(self) -> 'SDImagingResults':
        return self._results

    @results.setter
    def results(self, value: 'SDImagingResults'):
        self.setvalue('_results', value)


class ReductionGroupParameters(Parameters):
    """Parameters of Reduction Group Processing."""

    def __init__(self, group_id: str, group_desc: 'MSReductionGroupDesc'):
        """Initialize an object with group value.

        Args:
            group_id (str): Reduction group ID
            group_desc (MSReductionGroupDesc): MeasurementSet Reduction Group Desciption object
        """
        self._group_id = None               # Reduction group ID
        self.group_id = group_id
        self._group_desc = None             # MSReductionGroupDesc(spw_name:str, frequency_range:List[float], nchan:int,
                                            #                      field:str, member:List[MSReductionGroupMember]):
                                            #                      MSReductionGroupDesc object
        self.group_desc = group_desc
        self._antenna_list = None           # List[int]: List of antenna ID
        self._antids = None                 # List[int]: List of antenna ID
        self._ant_name = None               # str: Name of antenna
        self._asdm = None                   # str: ASDM name of reference MS
        self._cellx = None                  # Dict[str, Float]: cell size x, {'unit': 'arcsec', 'value': 6.4}
        self._celly = None                  # Dict[str, Float]: cell size y, {'unit': 'arcsec', 'value': 6.4}
        self._chanmap_range_list = None     # List[List[List[Float, Boolean]]]: List of channel map range
        self._channelmap_range_list = None  # List[List[List[Float, Boolean]]]: List of channel map range
        self._combined = None               # CombinedParameters: CombinedParameters object
        self._coord_set = False             # Boolean: Flag of Coord setting
        self._correlations = None           # str: a string figures correlation
        self._fieldid_list = None           # List[int]: List of field ID
        self._fieldids = None               # List[int]: List of field ID
        self._image_group = None            # Dict[str, List[MeasurementSet, List[str, List[Float, Boolean]], int]]:
                                            #  dictionary of image group of reduction group
        self._imagename = None              # str: image name
        self._imagename_nro = None          # Optional[str]: image name for NRO
        self._imager_result = None          # SDImagingResultItem: result object of imager
        self._imager_result_nro = None      # SDImagingResultItem: result object of imager for NRO
        self._member_list = None            # List[int]: List of reduction group ID
        self._members = None                # List[List[MeasurementSet, int, List[str]]]: image group of reduction group
        self._msobjs = None                 # List[MeasurementSet]: List of MeasurementSet
        self._name = None                   # str: name of MeasurementSet
        self._nx = None                     # Union[int, numpy.int64]: X of image shape
        self._ny = None                     # Union[int, numpy.int64]: Y of image shape
        self._org_direction = None          # Direction: directions of the origin for moving targets, like an ephemeris object
        self._phasecenter = None            # str: phase center of coord set
        self._polslist = None               # List[List[str]]: List of Polarization
        self._pols_list = None              # List[List[str]]: List of Polarization
        self._ref_ms = None                 # MeasurementSet: reference MS object
        self._restfreq = None               # str: Rest frequency
        self._rmss = None                   # List[[float]]: List of RMSs
        self._source_name = None            # str: name of source like 'M100'
        self._specmode = None               # str: spec mode like 'cube'
        self._spwid_list = None             # List[int]: List of Spectral window IDs
        self._spwids = None                 # List[int]: List of Spectral window IDs
        self._stokes_list = None            # List[str]: List of stokes
        self._tocombine = None              # ToCombinedParameters: ToCombinedParameters object
        self._validsps = None               # List[[int]]: List of valid spectrum
        self._v_spwids = None               # List[int]: List of Virtual Spectral window IDs

    @property
    def group_id(self): return self._group_id

    @group_id.setter
    def group_id(self, value): self.setvalue('_group_id', value)

    @property
    def group_desc(self): return self._group_desc

    @group_desc.setter
    def group_desc(self, value): self.setvalue('_group_desc', value)

    @property
    def antenna_list(self): return self._antenna_list

    @antenna_list.setter
    def antenna_list(self, value): self.setvalue('_antenna_list', value)

    @property
    def antids(self): return self._antids

    @antids.setter
    def antids(self, value): self.setvalue('_antids', value)

    @property
    def ant_name(self): return self._ant_name

    @ant_name.setter
    def ant_name(self, value): self.setvalue('_ant_name', value)

    @property
    def asdm(self): return self._asdm

    @asdm.setter
    def asdm(self, value): self.setvalue('_asdm', value)

    @property
    def cellx(self): return self._cellx

    @cellx.setter
    def cellx(self, value): self.setvalue('_cellx', value)

    @property
    def celly(self): return self._celly

    @celly.setter
    def celly(self, value): self.setvalue('_celly', value)

    @property
    def chanmap_range_list(self): return self._chanmap_range_list

    @chanmap_range_list.setter
    def chanmap_range_list(self, value): self.setvalue('_chanmap_range_list', value)

    @property
    def channelmap_range_list(self): return self._channelmap_range_list

    @channelmap_range_list.setter
    def channelmap_range_list(self, value): self.setvalue('_channelmap_range_list', value)

    @property
    def combined(self): return self._combined

    @combined.setter
    def combined(self, value): self.setvalue('_combined', value)

    @property
    def coord_set(self): return self._coord_set

    @coord_set.setter
    def coord_set(self, value): self.setvalue('_coord_set', value)

    @property
    def correlations(self): return self._correlations

    @correlations.setter
    def correlations(self, value): self.setvalue('_correlations', value)

    @property
    def fieldid_list(self): return self._fieldid_list

    @fieldid_list.setter
    def fieldid_list(self, value): self.setvalue('_fieldid_list', value)

    @property
    def fieldids(self): return self._fieldids

    @fieldids.setter
    def fieldids(self, value): self.setvalue('_fieldids', value)

    @property
    def image_group(self): return self._image_group

    @image_group.setter
    def image_group(self, value): self.setvalue('_image_group', value)

    @property
    def imagename(self): return self._imagename

    @imagename.setter
    def imagename(self, value): self.setvalue('_imagename', value)

    @property
    def imagename_nro(self): return self._imagename_nro

    @imagename_nro.setter
    def imagename_nro(self, value): self.setvalue('_imagename_nro', value)

    @property
    def imager_result(self): return self._imager_result

    @imager_result.setter
    def imager_result(self, value): self.setvalue('_imager_result', value)

    @property
    def imager_result_nro(self): return self._imager_result_nro

    @imager_result_nro.setter
    def imager_result_nro(self, value): self.setvalue('_imager_result_nro', value)

    @property
    def member_list(self): return self._member_list

    @member_list.setter
    def member_list(self, value): self.setvalue('_member_list', value)

    @property
    def members(self): return self._members

    @members.setter
    def members(self, value): self.setvalue('_members', value)

    @property
    def msobjs(self): return self._msobjs

    @msobjs.setter
    def msobjs(self, value): self.setvalue('_msobjs', value)

    @property
    def name(self): return self._name

    @name.setter
    def name(self, value): self.setvalue('_name', value)

    @property
    def nx(self): return self._nx

    @nx.setter
    def nx(self, value): self.setvalue('_nx', value)

    @property
    def ny(self): return self._ny

    @ny.setter
    def ny(self, value): self.setvalue('_ny', value)

    @property
    def org_direction(self): return self._org_direction

    @org_direction.setter
    def org_direction(self, value): self.setvalue('_org_direction', value)

    @property
    def phasecenter(self): return self._phasecenter

    @phasecenter.setter
    def phasecenter(self, value): self.setvalue('_phasecenter', value)

    @property
    def polslist(self): return self._polslist

    @polslist.setter
    def polslist(self, value): self.setvalue('_polslist', value)

    @property
    def pols_list(self): return self._pols_list

    @pols_list.setter
    def pols_list(self, value): self.setvalue('_pols_list', value)

    @property
    def ref_ms(self): return self._ref_ms

    @ref_ms.setter
    def ref_ms(self, value): self.setvalue('_ref_ms', value)

    @property
    def restfreq(self): return self._restfreq

    @restfreq.setter
    def restfreq(self, value): self.setvalue('_restfreq', value)

    @property
    def rmss(self): return self._rmss

    @rmss.setter
    def rmss(self, value): self.setvalue('_rmss', value)

    @property
    def source_name(self): return self._source_name

    @source_name.setter
    def source_name(self, value): self.setvalue('_source_name', value)

    @property
    def specmode(self): return self._specmode

    @specmode.setter
    def specmode(self, value): self.setvalue('_specmode', value)

    @property
    def spwid_list(self): return self._spwid_list

    @spwid_list.setter
    def spwid_list(self, value): self.setvalue('_spwid_list', value)

    @property
    def spwids(self): return self._spwids

    @spwids.setter
    def spwids(self, value): self.setvalue('_spwids', value)

    @property
    def stokes_list(self): return self._stokes_list

    @stokes_list.setter
    def stokes_list(self, value): self.setvalue('_stokes_list', value)

    @property
    def tocombine(self): return self._tocombine

    @tocombine.setter
    def tocombine(self, value): self.setvalue('_tocombine', value)

    @property
    def validsps(self): return self._validsps

    @validsps.setter
    def validsps(self, value): self.setvalue('_validsps', value)

    @property
    def v_spwids(self): return self._v_spwids

    @v_spwids.setter
    def v_spwids(self, value): self.setvalue('_v_spwids', value)


class CombinedImageParameters(Parameters):
    """Parameter class for combined image."""

    def __init__(self):
        """Initialize an object."""
        self._antids = ObservedList()           # List[int]: List of antenna ID
        self._fieldids = ObservedList()         # List[int]: List of field ID
        self._infiles = ObservedList()          # List[str]: List of input file names
        self._pols = ObservedList()             # List[List[str]]: List of Polarization
        self._rms_exclude = ObservedList()      # List[numpy.array[float]]: RMS mask frequency range
        self._spws = ObservedList()             # List[int]: List of Spectral window IDs
        self._v_spws = ObservedList()           # List[int]: List of Virtual Spectral window IDs
        self._v_spws_unique = ObservedList()    # List[int]: List of unique values of _v_spws

    def extend(self, _cp: CommonParameters, _rgp: ReductionGroupParameters):
        """Extend list properties using CP and RGP.

        Args:
            _cp (CommonParameters): CommonParameters object
            _rgp (ReductionGroupParameters): ReductionGroupParameters object
        """
        self._infiles.extend(_cp.infiles)
        self._antids.extend(_rgp.antids)
        self._fieldids.extend(_rgp.fieldids)
        self._spws.extend(_rgp.spwids)
        self._v_spws.extend(_rgp.v_spwids)
        self._pols.extend(_rgp.polslist)

    @property
    def antids(self): return self._antids

    @antids.setter
    def antids(self, value): self.setvalue('_antids', value)

    @property
    def fieldids(self): return self._fieldids

    @fieldids.setter
    def fieldids(self, value): self.setvalue('_fieldids', value)

    @property
    def infiles(self): return self._infiles

    @infiles.setter
    def infiles(self, value): self.setvalue('_infiles', value)

    @property
    def pols(self): return self._pols

    @pols.setter
    def pols(self, value): self.setvalue('_pols', value)

    @property
    def rms_exclude(self): return self._rms_exclude

    @rms_exclude.setter
    def rms_exclude(self, value): self.setvalue('_rms_exclude', value)

    @property
    def spws(self): return self._spws

    @spws.setter
    def spws(self, value): self.setvalue('_spws', value)

    @property
    def v_spws(self): return self._v_spws

    @v_spws.setter
    def v_spws(self, value): self.setvalue('_v_spws', value)

    @property
    def v_spws_unique(self): return self._v_spws_unique

    @v_spws_unique.setter
    def v_spws_unique(self, value): self.setvalue('_v_spws_unique', value)


class ToCombineImageParameters(Parameters):
    """Parameter class to combine image."""

    def __init__(self):
        """Initialize an object."""
        self._images = ObservedList()               # ObservedList[str]: list of image name
        self._images_nro = ObservedList()           # ObservedList[str]: list of image name for NRO
        self._org_directions = ObservedList()       # ObservedList[Direction]: list of origins
        self._org_directions_nro = ObservedList()   # ObservedList[Direction]: list of origins for NRO
        self._specmodes = ObservedList()            # ObservedList[str]: list of spec mode

    @property
    def images(self): return self._images

    @images.setter
    def images(self, value): self.setvalue('_images', value)

    @property
    def images_nro(self): return self._images_nro

    @images_nro.setter
    def images_nro(self, value): self.setvalue('_images_nro', value)

    @property
    def org_directions(self): return self._org_directions

    @org_directions.setter
    def org_directions(self, value): self.setvalue('_org_directions', value)

    @property
    def org_directions_nro(self): return self._org_directions_nro

    @org_directions_nro.setter
    def org_directions_nro(self, value): self.setvalue('_org_directions_nro', value)

    @property
    def specmodes(self): return self._specmodes

    @specmodes.setter
    def specmodes(self, value): self.setvalue('_specmodes', value)


class PostProcessParameters(Parameters):
    """Parameters for post proccessing of image generating."""

    def __init__(self):
        """Initialize an object."""
        self._beam = None                           # Dict[str, Dict[str, float]]
        self._brightnessunit = None                 # str: brightness unit like 'Jy/beam'
        self._chan_width = None                     # numpy.float64: channel width of faxis
        self._cs = None                             # coordsys: coordsys object
        self._faxis = None                          # int: spectral axis which is found
        self._imagename = None                      # str: image name
        self._image_rms = None                      # float: image statistics
        self._include_channel_range = None          # List[int]: List of channel ranges to calculate image statistics
        self._is_representative_source_spw = None   # Boolean: Flag of representative source spw
        self._is_representative_spw = None          # Boolean: Flag of representative spw
        self._nx = None                             # numpy.int64: X of image shape
        self._ny = None                             # numpy.int64: Y of image shape
        self._org_direction = None                  # Direction: directions of the origin for moving targets
        self._qcell = None                          # Dict[str, Dict[str, float]]
        self._raster_infos = None                   # List[RasterInfo]: list of RasterInfo(center/width/height/angle/row)
        self._region = None                         # str: region to calculate statistics
        self._rmss = None                           # List[[float]]: List of RMSs
        self._stat_chans = None                     # str: converted string from include_channel_range
        self._stat_freqs = None                     # str: statistics frequencies
        self._theoretical_rms = None                # Dict[str, float]: Theoretical RMSs
        self._validsps = None                       # List[[int]]: List of valid spectrum

    @property
    def beam(self): return self._beam

    @beam.setter
    def beam(self, value): self.setvalue('_beam', value)

    @property
    def brightnessunit(self): return self._brightnessunit

    @brightnessunit.setter
    def brightnessunit(self, value): self.setvalue('_brightnessunit', value)

    @property
    def chan_width(self): return self._chan_width

    @chan_width.setter
    def chan_width(self, value): self.setvalue('_chan_width', value)

    @property
    def cs(self): return self._cs

    @cs.setter
    def cs(self, value): self.setvalue('_cs', value)

    @property
    def faxis(self): return self._faxis

    @faxis.setter
    def faxis(self, value): self.setvalue('_faxis', value)

    @property
    def imagename(self): return self._imagename

    @imagename.setter
    def imagename(self, value): self.setvalue('_imagename', value)

    @property
    def image_rms(self): return self._image_rms

    @image_rms.setter
    def image_rms(self, value): self.setvalue('_image_rms', value)

    @property
    def include_channel_range(self): return self._include_channel_range

    @include_channel_range.setter
    def include_channel_range(self, value): self.setvalue('_include_channel_range', value)

    @property
    def is_representative_source_spw(self): return self._is_representative_source_spw

    @is_representative_source_spw.setter
    def is_representative_source_spw(self, value): self.setvalue('_is_representative_source_spw', value)

    @property
    def is_representative_spw(self): return self._is_representative_spw

    @is_representative_spw.setter
    def is_representative_spw(self, value): self.setvalue('_is_representative_spw', value)

    @property
    def nx(self): return self._nx

    @nx.setter
    def nx(self, value): self.setvalue('_nx', value)

    @property
    def ny(self): return self._ny

    @ny.setter
    def ny(self, value): self.setvalue('_ny', value)

    @property
    def org_direction(self): return self._org_direction

    @org_direction.setter
    def org_direction(self, value): self.setvalue('_org_direction', value)

    @property
    def qcell(self): return self._qcell

    @qcell.setter
    def qcell(self, value): self.setvalue('_qcell', value)

    @property
    def raster_infos(self): return self._raster_infos

    @raster_infos.setter
    def raster_infos(self, value): self.setvalue('_raster_infos', value)

    @property
    def region(self): return self._region

    @region.setter
    def region(self, value): self.setvalue('_region', value)

    @property
    def rmss(self): return self._rmss

    @rmss.setter
    def rmss(self, value): self.setvalue('_rmss', value)

    @property
    def stat_chans(self): return self._stat_chans

    @stat_chans.setter
    def stat_chans(self, value): self.setvalue('_stat_chans', value)

    @property
    def stat_freqs(self): return self._stat_freqs

    @stat_freqs.setter
    def stat_freqs(self, value): self.setvalue('_stat_freqs', value)

    @property
    def theoretical_rms(self): return self._theoretical_rms

    @theoretical_rms.setter
    def theoretical_rms(self, value): self.setvalue('_theoretical_rms', value)

    @property
    def validsps(self): return self._validsps

    @validsps.setter
    def validsps(self, value): self.setvalue('_validsps', value)


class TheoreticalImageRmsParameters(Parameters):
    """ Parameter class of calculate_theoretical_image_rms()."""

    def __init__(self, _pp: PostProcessParameters, context: 'Context'):
        """Initiarize the object.

        Args:
            _pp (PostProcessParameters): imaging post process parameters of prepare()
            context (Context): pipeline Context
        """
        self._cqa = casa_tools.quanta       # LoggingQuanta: LoggingQuanta object
        self._failed_rms = self.cqa.quantity(-1, _pp.brightnessunit)    # Dict[str, float]: Failed RMS value
        self._sq_rms = 0.0                  # float: Square RMS value
        self._N = 0.0                       # float: RMS counter
        self._time_unit = 's'               # str: time unit
        self._ang_unit = self.cqa.getunit(_pp.qcell[0])     # str: ang unit
        self._cx_val = self.cqa.getvalue(_pp.qcell[0])[0]   # float: cx
        self._cy_val = self.cqa.getvalue(self.cqa.convert(_pp.qcell[1], self.ang_unit))[0]  # float: cy
        self._bandwidth = numpy.abs(_pp.chan_width)         # float: band width
        self._context = context                             # Context: pipeline context
        self._is_nro = sdutils.is_nro(context)              # bool: NRO flag
        self._infile = None                 # str: input file
        self._antid = None                  # int antenna ID
        self._fieldid = None                # int: field ID
        self._spwid = None                  # int: spectrum ID
        self._pol_names = None              # List[str]: polarization names
        self._raster_info = None            # RasterInfo: RasterInfo object
        self._msobj = None                  # MeasurementSet: MeasuremetSet
        self._calmsobj = None               # MeasurementSet: calibrated MeasurementSet
        self._polids = None                 # List[int]: polarization ID
        self._error_msg = None              # str: error message
        self._dt = None                     # DataTableImpl: datatable object
        self._index_list = None             # numpy.array[int64]: index list
        self._effBW = None                  # float: effective BW
        self._mean_tsys_per_pol = None      # numpy.array[float]: mean of Tsys per polarization
        self._width = None                  # float: width
        self._height = None                 # float: height
        self._t_on_act = None               # float: Ton actual
        self._calst = None                  # IntercalCalState: interval calibration state object
        self._t_sub_on = None               # float: Tsub on
        self._t_sub_off = None              # float: Tsub off

    @property
    def cqa(self): return self._cqa

    @cqa.setter
    def cqa(self, value): self.setvalue('_cqa', value)

    @property
    def failed_rms(self): return self._failed_rms

    @failed_rms.setter
    def failed_rms(self, value): self.setvalue('_failed_rms', value)

    @property
    def sq_rms(self): return self._sq_rms

    @sq_rms.setter
    def sq_rms(self, value): self.setvalue('_sq_rms', value)

    @property
    def N(self): return self._N

    @N.setter
    def N(self, value): self.setvalue('_N', value)

    @property
    def time_unit(self): return self._time_unit

    @time_unit.setter
    def time_unit(self, value): self.setvalue('_time_unit', value)

    @property
    def ang_unit(self): return self._ang_unit

    @ang_unit.setter
    def ang_unit(self, value): self.setvalue('_ang_unit', value)

    @property
    def cx_val(self): return self._cx_val

    @cx_val.setter
    def cx_val(self, value): self.setvalue('_cx_val', value)

    @property
    def cy_val(self): return self._cy_val

    @cy_val.setter
    def cy_val(self, value): self.setvalue('_cy_val', value)

    @property
    def bandwidth(self): return self._bandwidth

    @bandwidth.setter
    def bandwidth(self, value): self.setvalue('_bandwidth', value)

    @property
    def context(self): return self._context

    @context.setter
    def context(self, value): self.setvalue('_context', value)

    @property
    def is_nro(self): return self._is_nro

    @is_nro.setter
    def is_nro(self, value): self.setvalue('_is_nro', value)

    @property
    def infile(self): return self._infile

    @infile.setter
    def infile(self, value): self.setvalue('_infile', value)

    @property
    def antid(self): return self._antid

    @antid.setter
    def antid(self, value): self.setvalue('_antid', value)

    @property
    def fieldid(self): return self._fieldid

    @fieldid.setter
    def fieldid(self, value): self.setvalue('_fieldid', value)

    @property
    def spwid(self): return self._spwid

    @spwid.setter
    def spwid(self, value): self.setvalue('_spwid', value)

    @property
    def pol_names(self): return self._pol_names

    @pol_names.setter
    def pol_names(self, value): self.setvalue('_pol_names', value)

    @property
    def raster_info(self): return self._raster_info

    @raster_info.setter
    def raster_info(self, value): self.setvalue('_raster_info', value)

    @property
    def msobj(self): return self._msobj

    @msobj.setter
    def msobj(self, value): self.setvalue('_msobj', value)

    @property
    def calmsobj(self): return self._calmsobj

    @calmsobj.setter
    def calmsobj(self, value): self.setvalue('_calmsobj', value)

    @property
    def polids(self): return self._polids

    @polids.setter
    def polids(self, value): self.setvalue('_polids', value)

    @property
    def error_msg(self): return self._error_msg

    @error_msg.setter
    def error_msg(self, value): self.setvalue('_error_msg', value)

    @property
    def dt(self): return self._dt

    @dt.setter
    def dt(self, value): self.setvalue('_dt', value)

    @property
    def index_list(self): return self._index_list

    @index_list.setter
    def index_list(self, value): self.setvalue('_index_list', value)

    @property
    def effBW(self): return self._effBW

    @effBW.setter
    def effBW(self, value): self.setvalue('_effBW', value)

    @property
    def mean_tsys_per_pol(self): return self._mean_tsys_per_pol

    @mean_tsys_per_pol.setter
    def mean_tsys_per_pol(self, value): self.setvalue('_mean_tsys_per_pol', value)

    @property
    def width(self): return self._width

    @width.setter
    def width(self, value): self.setvalue('_width', value)

    @property
    def height(self): return self._height

    @height.setter
    def height(self, value): self.setvalue('_height', value)

    @property
    def t_on_act(self): return self._t_on_act

    @t_on_act.setter
    def t_on_act(self, value): self.setvalue('_t_on_act', value)

    @property
    def calst(self): return self._calst

    @calst.setter
    def calst(self, value): self.setvalue('_calst', value)

    @property
    def t_sub_on(self): return self._t_sub_on

    @t_sub_on.setter
    def t_sub_on(self, value): self.setvalue('_t_sub_on', value)

    @property
    def t_sub_off(self): return self._t_sub_off

    @t_sub_off.setter
    def t_sub_off(self, value): self.setvalue('_t_sub_off', value)
