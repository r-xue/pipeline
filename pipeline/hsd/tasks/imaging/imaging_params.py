"""Parameter classes of Imaging."""

from typing import TYPE_CHECKING, Dict, List, NewType, Optional, Union

import numpy

from casatools import coordsys
from pipeline.hsd.tasks.common import utils as sdutils
from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResultItem
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils.debugwrapper import debugwrapper

if TYPE_CHECKING:
    from collections import namedtuple
    from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResults
    from pipeline.infrastructure import Context
    from pipeline.infrastructure.callibrary import IntervalCalState
    from pipeline.domain.datatable import DataTableImpl
    from pipeline.domain import MeasurementSet
    from pipeline.domain.singledish import MSReductionGroupDesc
    Direction = NewType('Direction', Dict[str, Union[str, float]])
    RasterInfo = NewType('RasterInfo', namedtuple('RasterInfo', 'center_ra center_dec width'
                                                                'height scan_angle row_separation row_duration'))
    ImageGroup = Dict[str, List[Union['MeasurementSet', int,
                                      List[Union[str, List[List[Union[float, bool]]]]]]]]


class ObservedList(list):
    """Class inherit list to observe its behavior.
    
    This class intends to observe/output list stuff for debugging more easier.
    If you want to search a needle in a haystack, please uncomment out @debugwrapper()."""

    # @debugwrapper(msg='list.setitem')
    def __setitem__(self, index: int, value: object):
        """Overrode list.__setitem__().

        Args:
            index : index
            value : object to set at index
        """
        super().__setitem__(index, value)

    # @debugwrapper(msg='list.insert')
    def insert(self, index: int, value: object):
        """Overrode list.insert().

        Args:
            index : index
            value : object to insert at index
        """
        super().insert(index, value)

    # @debugwrapper(msg='list.append')
    def append(self, value: object):
        """Overrode list.append().

        Args:
            value : object to append
        """
        super().append(value)

    # @debugwrapper(msg='list.extend')
    def extend(self, value: list):
        """Overrode list.extend().

        Args:
            value : object to merge
        """
        super().extend(value)


class Parameters:
    """Abstract class of Parameter object."""

    @debugwrapper(msg='set')
    def setvalue(self, name: str, value: object):
        """Set value by setattr().

        Args:
            name : property
            value : object
        """
        setattr(self, name, value)


class CommonParameters(Parameters):
    """Common parameters class of prepare()."""

    def __init__(self):
        """Initialize an object."""
        self._args_spw = None         # Spw selection per MS
        self._dt_dict = None          # Dict[str, DataTableImpl]: Dictionary of input MS and corresponding datatable
        self._edge = None             # List[int]: Edge channel of most recent SDBaselineResults or [0, 0]
        self._imagemode = None        # str: Input image mode, str
        self._in_field = None         # str: Comma-separated list of target field names that are extracted
                                      # from all input MSs
        self._infiles = None          # List[str]: List of input files
        self._is_nro = None           # numpy.bool_: Flag of NRO data
        self._ms_list = None          # List[MeasurementSet]: List of ms to process
        self._ms_names = None         # List[str]: List of name of ms in ms_list
        self._reduction_group = None  # Dict[int, MSReductionGroupDesc]: Eduction group object
        self._restfreq_list = None    # Union[str, List[str]]: List of rest frequency
        self._results = None          # SDImagingResults: Instance of SDImagingResults

    @property
    def args_spw(self): return self._args_spw

    @args_spw.setter
    def args_spw(self, value): self.setvalue('_args_spw', value)

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

    def __init__(self, group_id: int, group_desc: 'MSReductionGroupDesc'):
        """Initialize an object with group value.

        Args:
            group_id : Reduction group ID
            group_desc : MeasurementSet Reduction Group Desciption object
        """
        self._group_id = None               # int: Reduction group ID
        self.group_id = group_id
        self._group_desc = None             # MSReductionGroupDesc(spw_name:str, frequency_range:List[float], nchan:int,
                                            #                      field:str, member:List[MSReductionGroupMember]):
                                            #                      MSReductionGroupDesc object
        self.group_desc = group_desc
        self._antenna_list = None           # List[int]: List of antenna ID
        self._antids = None                 # List[int]: List of antenna ID
        self._ant_name = None               # str: Name of antenna
        self._asdm = None                   # str: ASDM name of reference MS
        self._cellx = None                  # Dict[str, float]: Cell size x, {'unit': 'arcsec', 'value': 6.4}
        self._celly = None                  # Dict[str, float]: Cell size y, {'unit': 'arcsec', 'value': 6.4}
        self._chanmap_range_list = None     # List[List[List[Union[float, bool]]]]: List of channel map range
        self._channelmap_range_list = None  # List[List[List[Union[float, bool]]]]: List of channel map range
        self._combined = None               # CombinedImageParameters: CombinedImageParameters object
        self._coord_set = False             # bool: Flag of Coord setting
        self._correlations = None           # str: A string figures correlation
        self._fieldid_list = None           # List[int]: List of field ID
        self._fieldids = None               # List[int]: List of field ID
        self._image_group = None            # ImageGroup: Dictionary of image group of reduction group
        self._imagename = None              # str: Image name
        self._imagename_nro = None          # Optional[str]: Image name for NRO
        self._imager_result = None          # SDImagingResultItem: Result object of imager
        self._imager_result_nro = None      # SDImagingResultItem: Result object of imager for NRO
        self._member_list = None            # List[int]: List of reduction group ID
        self._members = None                # List[List[MeasurementSet, int, List[str]]]: Image group of reduction group
        self._msobjs = None                 # List[MeasurementSet]: List of MeasurementSet
        self._name = None                   # str: Name of MeasurementSet
        self._nx = None                     # Union[int, numpy.int64]: X of image shape
        self._ny = None                     # Union[int, numpy.int64]: Y of image shape
        self._org_direction = None          # Direction: Directions of the origin for moving targets, like an ephemeris object
        self._phasecenter = None            # str: Phase center of coord set
        self._polslist = None               # List[List[str]]: List of Polarization
        self._pols_list = None              # List[List[str]]: List of Polarization
        self._ref_ms = None                 # MeasurementSet: Reference MS object
        self._restfreq = None               # str: Rest frequency
        self._rmss = None                   # List[float]: List of RMSs
        self._source_name = None            # str: Name of source like 'M100'
        self._specmode = None               # str: Spec mode like 'cube'
        self._spwid_list = None             # List[int]: List of Spectral window IDs
        self._spwids = None                 # List[int]: List of Spectral window IDs
        self._stokes_list = None            # List[str]: List of stokes
        self._tocombine = None              # ToCombineImageParameters: ToCombineImageParameters object
        self._validsps = None               # List[List[int]]: List of valid spectrum
        self._v_spwids = None               # List[int]: List of Virtual Spectral window IDs

    @property
    def group_id(self) -> int: return self._group_id

    @group_id.setter
    def group_id(self, value: int): self.setvalue('_group_id', value)

    @property
    def group_desc(self) -> 'MSReductionGroupDesc': return self._group_desc

    @group_desc.setter
    def group_desc(self, value: 'MSReductionGroupDesc'): self.setvalue('_group_desc', value)

    @property
    def antenna_list(self) -> List[int]: return self._antenna_list

    @antenna_list.setter
    def antenna_list(self, value: List[int]): self.setvalue('_antenna_list', value)

    @property
    def antids(self) -> List[int]: return self._antids

    @antids.setter
    def antids(self, value: List[int]): self.setvalue('_antids', value)

    @property
    def ant_name(self) -> str: return self._ant_name

    @ant_name.setter
    def ant_name(self, value: str): self.setvalue('_ant_name', value)

    @property
    def asdm(self) -> str: return self._asdm

    @asdm.setter
    def asdm(self, value: str): self.setvalue('_asdm', value)

    @property
    def cellx(self) -> Dict[str, float]: return self._cellx

    @cellx.setter
    def cellx(self, value: Dict[str, float]): self.setvalue('_cellx', value)

    @property
    def celly(self) -> Dict[str, float]: return self._celly

    @celly.setter
    def celly(self, value: Dict[str, float]): self.setvalue('_celly', value)

    @property
    def chanmap_range_list(self) -> List[List[List[Union[float, bool]]]]:
        return self._chanmap_range_list

    @chanmap_range_list.setter
    def chanmap_range_list(self, value: List[List[List[Union[float, bool]]]]):
        self.setvalue('_chanmap_range_list', value)

    @property
    def channelmap_range_list(self) -> List[List[List[Union[float, bool]]]]:
        return self._channelmap_range_list

    @channelmap_range_list.setter
    def channelmap_range_list(self, value: List[List[List[Union[float, bool]]]]):
        self.setvalue('_channelmap_range_list', value)

    @property
    def combined(self) -> 'CombinedImageParameters': return self._combined

    @combined.setter
    def combined(self, value: 'CombinedImageParameters'): self.setvalue('_combined', value)

    @property
    def coord_set(self) -> bool: return self._coord_set

    @coord_set.setter
    def coord_set(self, value: bool): self.setvalue('_coord_set', value)

    @property
    def correlations(self) -> str: return self._correlations

    @correlations.setter
    def correlations(self, value: str): self.setvalue('_correlations', value)

    @property
    def fieldid_list(self) -> List[int]: return self._fieldid_list

    @fieldid_list.setter
    def fieldid_list(self, value: List[int]): self.setvalue('_fieldid_list', value)

    @property
    def fieldids(self) -> List[int]: return self._fieldids

    @fieldids.setter
    def fieldids(self, value: List[int]): self.setvalue('_fieldids', value)

    @property
    def image_group(self) -> 'ImageGroup': return self._image_group

    @image_group.setter
    def image_group(self, value: 'ImageGroup'): self.setvalue('_image_group', value)

    @property
    def imagename(self) -> str: return self._imagename

    @imagename.setter
    def imagename(self, value: str): self.setvalue('_imagename', value)

    @property
    def imagename_nro(self) -> Optional[str]: return self._imagename_nro

    @imagename_nro.setter
    def imagename_nro(self, value: Optional[str]): self.setvalue('_imagename_nro', value)

    @property
    def imager_result(self) -> 'SDImagingResultItem': return self._imager_result

    @imager_result.setter
    def imager_result(self, value: 'SDImagingResultItem'): self.setvalue('_imager_result', value)

    @property
    def imager_result_nro(self) -> Optional['SDImagingResultItem']: return self._imager_result_nro

    @imager_result_nro.setter
    def imager_result_nro(self, value: Optional['SDImagingResultItem']): self.setvalue('_imager_result_nro', value)

    @property
    def member_list(self) -> List[int]: return self._member_list

    @member_list.setter
    def member_list(self, value: List[int]): self.setvalue('_member_list', value)

    @property
    def members(self) -> List[List[Union['MeasurementSet', int, List[str]]]]: return self._members

    @members.setter
    def members(self, value: List[List[Union['MeasurementSet', int, List[str]]]]): self.setvalue('_members', value)

    @property
    def msobjs(self) -> List['MeasurementSet']: return self._msobjs

    @msobjs.setter
    def msobjs(self, value: List['MeasurementSet']): self.setvalue('_msobjs', value)

    @property
    def name(self) -> str: return self._name

    @name.setter
    def name(self, value: str): self.setvalue('_name', value)

    @property
    def nx(self) -> Union[int, numpy.int64]: return self._nx

    @nx.setter
    def nx(self, value: Union[int, numpy.int64]): self.setvalue('_nx', value)

    @property
    def ny(self) -> Union[int, numpy.int64]: return self._ny

    @ny.setter
    def ny(self, value: Union[int, numpy.int64]): self.setvalue('_ny', value)

    @property
    def org_direction(self) -> 'Direction': return self._org_direction

    @org_direction.setter
    def org_direction(self, value: 'Direction'): self.setvalue('_org_direction', value)

    @property
    def phasecenter(self) -> str: return self._phasecenter

    @phasecenter.setter
    def phasecenter(self, value: str): self.setvalue('_phasecenter', value)

    @property
    def polslist(self) -> List[List[str]]: return self._polslist

    @polslist.setter
    def polslist(self, value: List[List[str]]): self.setvalue('_polslist', value)

    @property
    def pols_list(self) -> List[List[str]]: return self._pols_list

    @pols_list.setter
    def pols_list(self, value: List[List[str]]): self.setvalue('_pols_list', value)

    @property
    def ref_ms(self) -> 'MeasurementSet': return self._ref_ms

    @ref_ms.setter
    def ref_ms(self, value: 'MeasurementSet'): self.setvalue('_ref_ms', value)

    @property
    def restfreq(self) -> str: return self._restfreq

    @restfreq.setter
    def restfreq(self, value: str): self.setvalue('_restfreq', value)

    @property
    def rmss(self) -> List[float]: return self._rmss

    @rmss.setter
    def rmss(self, value: List[float]): self.setvalue('_rmss', value)

    @property
    def source_name(self) -> str: return self._source_name

    @source_name.setter
    def source_name(self, value: str): self.setvalue('_source_name', value)

    @property
    def specmode(self) -> str: return self._specmode

    @specmode.setter
    def specmode(self, value: str): self.setvalue('_specmode', value)

    @property
    def spwid_list(self) -> List[int]: return self._spwid_list

    @spwid_list.setter
    def spwid_list(self, value: List[int]): self.setvalue('_spwid_list', value)

    @property
    def spwids(self) -> List[int]: return self._spwids

    @spwids.setter
    def spwids(self, value: List[int]): self.setvalue('_spwids', value)

    @property
    def stokes_list(self) -> List[int]: return self._stokes_list

    @stokes_list.setter
    def stokes_list(self, value: List[int]): self.setvalue('_stokes_list', value)

    @property
    def tocombine(self) -> 'ToCombineImageParameters': return self._tocombine

    @tocombine.setter
    def tocombine(self, value: 'ToCombineImageParameters'): self.setvalue('_tocombine', value)

    @property
    def validsps(self) -> List[List[int]]: return self._validsps

    @validsps.setter
    def validsps(self, value: List[List[int]]): self.setvalue('_validsps', value)

    @property
    def v_spwids(self) -> List[int]: return self._v_spwids

    @v_spwids.setter
    def v_spwids(self, value: List[int]): self.setvalue('_v_spwids', value)


class CombinedImageParameters(Parameters):
    """Parameter class for combined image."""

    def __init__(self):
        """Initialize an object."""
        self._antids = ObservedList()           # ObservedList[int]: List of antenna ID
        self._fieldids = ObservedList()         # ObservedList[int]: List of field ID
        self._infiles = ObservedList()          # ObservedList[str]: List of input file names
        self._pols = ObservedList()             # List[List[str]]: List of Polarization
        self._rms_exclude = ObservedList()      # ObservedList[numpy.ndarray[float]]: RMS mask frequency range
        self._spws = ObservedList()             # ObservedList[int]: List of Spectral window IDs
        self._v_spws = ObservedList()           # ObservedList[int]: List of Virtual Spectral window IDs
        self._v_spws_unique = ObservedList()    # ObservedList[int]: List of unique values of _v_spws

    def extend(self, _cp: CommonParameters, _rgp: ReductionGroupParameters):
        """Extend list properties using CP and RGP.

        Args:
            _cp : CommonParameters object
            _rgp : ReductionGroupParameters object
        """
        self._infiles.extend(_cp.infiles)
        self._antids.extend(_rgp.antids)
        self._fieldids.extend(_rgp.fieldids)
        self._spws.extend(_rgp.spwids)
        self._v_spws.extend(_rgp.v_spwids)
        self._pols.extend(_rgp.polslist)

    @property
    def antids(self) -> 'ObservedList[int]': return self._antids

    @antids.setter
    def antids(self, value: 'ObservedList[int]'): self.setvalue('_antids', value)

    @property
    def fieldids(self) -> 'ObservedList[int]': return self._fieldids

    @fieldids.setter
    def fieldids(self, value: 'ObservedList[int]'): self.setvalue('_fieldids', value)

    @property
    def infiles(self) -> 'ObservedList[str]': return self._infiles

    @infiles.setter
    def infiles(self, value: 'ObservedList[str]'): self.setvalue('_infiles', value)

    @property
    def pols(self) -> 'ObservedList[List[str]]': return self._pols

    @pols.setter
    def pols(self, value: 'ObservedList[List[str]]'): self.setvalue('_pols', value)

    @property
    def rms_exclude(self) -> 'ObservedList[numpy.ndarray[float]]': return self._rms_exclude

    @rms_exclude.setter
    def rms_exclude(self, value: 'ObservedList[numpy.ndarray[float]]'): self.setvalue('_rms_exclude', value)

    @property
    def spws(self) -> 'ObservedList[int]': return self._spws

    @spws.setter
    def spws(self, value: 'ObservedList[int]'): self.setvalue('_spws', value)

    @property
    def v_spws(self) -> 'ObservedList[int]': return self._v_spws

    @v_spws.setter
    def v_spws(self, value: 'ObservedList[int]'): self.setvalue('_v_spws', value)

    @property
    def v_spws_unique(self) -> 'ObservedList[int]': return self._v_spws_unique

    @v_spws_unique.setter
    def v_spws_unique(self, value: 'ObservedList[int]'): self.setvalue('_v_spws_unique', value)


class ToCombineImageParameters(Parameters):
    """Parameter class to combine image."""

    def __init__(self):
        """Initialize an object."""
        self._images = ObservedList()               # ObservedList[str]: List of image name
        self._images_nro = ObservedList()           # ObservedList[str]: List of image name for NRO
        self._org_directions = ObservedList()       # ObservedList[Direction]: List of origins
        self._org_directions_nro = ObservedList()   # ObservedList[Direction]: List of origins for NRO
        self._specmodes = ObservedList()            # ObservedList[str]: List of spec mode

    @property
    def images(self) -> 'ObservedList[str]': return self._images

    @images.setter
    def images(self, value: 'ObservedList[str]'): self.setvalue('_images', value)

    @property
    def images_nro(self) -> 'ObservedList[str]': return self._images_nro

    @images_nro.setter
    def images_nro(self, value: 'ObservedList[str]'): self.setvalue('_images_nro', value)

    @property
    def org_directions(self) -> 'ObservedList[str]': return self._org_directions

    @org_directions.setter
    def org_directions(self, value: 'ObservedList[str]'): self.setvalue('_org_directions', value)

    @property
    def org_directions_nro(self) -> 'ObservedList[str]': return self._org_directions_nro

    @org_directions_nro.setter
    def org_directions_nro(self, value: 'ObservedList[str]'): self.setvalue('_org_directions_nro', value)

    @property
    def specmodes(self) -> 'ObservedList[str]': return self._specmodes

    @specmodes.setter
    def specmodes(self, value: 'ObservedList[str]'): self.setvalue('_specmodes', value)


class PostProcessParameters(Parameters):
    """Parameters for post processing of image generating."""

    def __init__(self):
        """Initialize an object."""
        self._beam = None                           # Dict[str, Dict[str, float]]: Beam data
        self._brightnessunit = None                 # str: Brightness unit like 'Jy/beam'
        self._chan_width = None                     # numpy.float64: Channel width of faxis
        self._cs = None                             # coordsys: Coordsys object
        self._faxis = None                          # int: Spectral axis which is found
        self._imagename = None                      # str: Image name
        self._image_rms = None                      # float: Image statistics
        self._include_channel_range = None          # List[int]: List of channel ranges to calculate image statistics
        self._is_representative_source_spw = None   # bool: Flag of representative source spw
        self._is_representative_spw = None          # bool: Flag of representative spw
        self._nx = None                             # numpy.int64: X of image shape
        self._ny = None                             # numpy.int64: Y of image shape
        self._org_direction = None                  # Direction: Directions of the origin for moving targets
        self._qcell = None                          # Dict[str, Dict[str, float]]: Cell data
        self._raster_infos = None                   # List[RasterInfo]: List of RasterInfo(center/width/height/angle/row)
        self._region = None                         # str: Region to calculate statistics
        self._rmss = None                           # List[float]: List of RMSs
        self._stat_chans = None                     # str: Converted string from include_channel_range
        self._stat_freqs = None                     # str: Statistics frequencies
        self._theoretical_rms = None                # Dict[str, float]: Theoretical RMSs
        self._validsps = None                       # List[int]: List of valid spectrum

    def done(self):
        if isinstance(self._cs, coordsys):
            self._cs.done()

    @property
    def beam(self) -> Dict[str, Dict[str, float]]: return self._beam

    @beam.setter
    def beam(self, value: Dict[str, Dict[str, float]]): self.setvalue('_beam', value)

    @property
    def brightnessunit(self) -> str: return self._brightnessunit

    @brightnessunit.setter
    def brightnessunit(self, value: str): self.setvalue('_brightnessunit', value)

    @property
    def chan_width(self) -> numpy.float64: return self._chan_width

    @chan_width.setter
    def chan_width(self, value: numpy.float64): self.setvalue('_chan_width', value)

    @property
    def cs(self) -> 'coordsys': return self._cs

    @cs.setter
    def cs(self, value: 'coordsys'): self.setvalue('_cs', value)

    @property
    def faxis(self) -> int: return self._faxis

    @faxis.setter
    def faxis(self, value: int): self.setvalue('_faxis', value)

    @property
    def imagename(self) -> str: return self._imagename

    @imagename.setter
    def imagename(self, value: str): self.setvalue('_imagename', value)

    @property
    def image_rms(self) -> float: return self._image_rms

    @image_rms.setter
    def image_rms(self, value: float): self.setvalue('_image_rms', value)

    @property
    def include_channel_range(self) -> List[int]: return self._include_channel_range

    @include_channel_range.setter
    def include_channel_range(self, value: List[int]): self.setvalue('_include_channel_range', value)

    @property
    def is_representative_source_spw(self) -> bool: return self._is_representative_source_spw

    @is_representative_source_spw.setter
    def is_representative_source_spw(self, value: bool): self.setvalue('_is_representative_source_spw', value)

    @property
    def is_representative_spw(self) -> bool: return self._is_representative_spw

    @is_representative_spw.setter
    def is_representative_spw(self, value: bool): self.setvalue('_is_representative_spw', value)

    @property
    def nx(self) -> numpy.int64: return self._nx

    @nx.setter
    def nx(self, value: numpy.int64): self.setvalue('_nx', value)

    @property
    def ny(self) -> numpy.int64: return self._ny

    @ny.setter
    def ny(self, value: numpy.int64): self.setvalue('_ny', value)

    @property
    def org_direction(self) -> 'Direction': return self._org_direction

    @org_direction.setter
    def org_direction(self, value: 'Direction'): self.setvalue('_org_direction', value)

    @property
    def qcell(self) -> Dict[str, Dict[str, float]]: return self._qcell

    @qcell.setter
    def qcell(self, value: Dict[str, Dict[str, float]]): self.setvalue('_qcell', value)

    @property
    def raster_infos(self) -> List['RasterInfo']: return self._raster_infos

    @raster_infos.setter
    def raster_infos(self, value: List['RasterInfo']): self.setvalue('_raster_infos', value)

    @property
    def region(self) -> str: return self._region

    @region.setter
    def region(self, value: str): self.setvalue('_region', value)

    @property
    def rmss(self) -> List[float]: return self._rmss

    @rmss.setter
    def rmss(self, value: List[float]): self.setvalue('_rmss', value)

    @property
    def stat_chans(self) -> str: return self._stat_chans

    @stat_chans.setter
    def stat_chans(self, value: str): self.setvalue('_stat_chans', value)

    @property
    def stat_freqs(self) -> str: return self._stat_freqs

    @stat_freqs.setter
    def stat_freqs(self, value: str): self.setvalue('_stat_freqs', value)

    @property
    def theoretical_rms(self) -> Dict[str, float]: return self._theoretical_rms

    @theoretical_rms.setter
    def theoretical_rms(self, value: Dict[str, float]): self.setvalue('_theoretical_rms', value)

    @property
    def validsps(self) -> List[int]: return self._validsps

    @validsps.setter
    def validsps(self, value: List[int]): self.setvalue('_validsps', value)


class TheoreticalImageRmsParameters(Parameters):
    """ Parameter class of calculate_theoretical_image_rms()."""

    def __init__(self, _pp: PostProcessParameters, context: 'Context'):
        """Initiarize the object.

        Args:
            _pp : imaging post process parameters of prepare()
            context : pipeline Context
        """
        self._cqa = casa_tools.quanta       # LoggingQuanta: LoggingQuanta object
        self._failed_rms = self.cqa.quantity(-1, _pp.brightnessunit)    # Dict[str, float]: Failed RMS value
        self._sq_rms = 0.0                  # float: Square RMS value
        self._N = 0.0                       # float: RMS counter
        self._time_unit = 's'               # str: Time unit
        self._ang_unit = self.cqa.getunit(_pp.qcell[0])     # str: Ang unit
        self._cx_val = self.cqa.getvalue(_pp.qcell[0])[0]   # float: cx
        self._cy_val = self.cqa.getvalue(self.cqa.convert(_pp.qcell[1], self.ang_unit))[0]  # float: cy
        self._bandwidth = numpy.abs(_pp.chan_width)         # float: Band width
        self._context = context                             # Context: Pipeline context
        self._is_nro = sdutils.is_nro(context)              # bool: NRO flag
        self._infile = None                 # str: Input file
        self._antid = None                  # int Antenna ID
        self._fieldid = None                # int: Field ID
        self._spwid = None                  # int: Spectrum ID
        self._pol_names = None              # List[str]: Polarization names
        self._raster_info = None            # RasterInfo: RasterInfo object
        self._msobj = None                  # MeasurementSet: MeasuremetSet
        self._calmsobj = None               # MeasurementSet: Calibrated MeasurementSet
        self._polids = None                 # List[int]: Polarization ID
        self._error_msg = None              # str: Error message
        self._dt = None                     # DataTableImpl: Datatable object
        self._index_list = None             # numpy.ndarray[int64]: Index list
        self._effBW = None                  # float: Effective BW
        self._mean_tsys_per_pol = None      # numpy.ndarray[float]: Mean of Tsys per polarization
        self._width = None                  # float: Width
        self._height = None                 # float: Height
        self._t_on_act = None               # float: Ton actual
        self._calst = None                  # IntervalCalState: Interval calibration state object
        self._t_sub_on = None               # float: Tsub on
        self._t_sub_off = None              # float: Tsub off

    @property
    def cqa(self) -> casa_tools.quanta: return self._cqa

    @cqa.setter
    def cqa(self, value: casa_tools.quanta): self.setvalue('_cqa', value)

    @property
    def failed_rms(self) -> Dict[str, float]: return self._failed_rms

    @failed_rms.setter
    def failed_rms(self, value: Dict[str, float]): self.setvalue('_failed_rms', value)

    @property
    def sq_rms(self) -> float: return self._sq_rms

    @sq_rms.setter
    def sq_rms(self, value: float): self.setvalue('_sq_rms', value)

    @property
    def N(self) -> float: return self._N

    @N.setter
    def N(self, value: float): self.setvalue('_N', value)

    @property
    def time_unit(self) -> str: return self._time_unit

    @time_unit.setter
    def time_unit(self, value: str): self.setvalue('_time_unit', value)

    @property
    def ang_unit(self) -> str: return self._ang_unit

    @ang_unit.setter
    def ang_unit(self, value: str): self.setvalue('_ang_unit', value)

    @property
    def cx_val(self) -> float: return self._cx_val

    @cx_val.setter
    def cx_val(self, value: float): self.setvalue('_cx_val', value)

    @property
    def cy_val(self) -> float: return self._cy_val

    @cy_val.setter
    def cy_val(self, value: float): self.setvalue('_cy_val', value)

    @property
    def bandwidth(self) -> float: return self._bandwidth

    @bandwidth.setter
    def bandwidth(self, value: float): self.setvalue('_bandwidth', value)

    @property
    def context(self) -> 'Context': return self._context

    @context.setter
    def context(self, value: 'Context'): self.setvalue('_context', value)

    @property
    def is_nro(self) -> bool: return self._is_nro

    @is_nro.setter
    def is_nro(self, value: bool): self.setvalue('_is_nro', value)

    @property
    def infile(self) -> str: return self._infile

    @infile.setter
    def infile(self, value: str): self.setvalue('_infile', value)

    @property
    def antid(self) -> int: return self._antid

    @antid.setter
    def antid(self, value: int): self.setvalue('_antid', value)

    @property
    def fieldid(self) -> int: return self._fieldid

    @fieldid.setter
    def fieldid(self, value: int): self.setvalue('_fieldid', value)

    @property
    def spwid(self) -> int: return self._spwid

    @spwid.setter
    def spwid(self, value: int): self.setvalue('_spwid', value)

    @property
    def pol_names(self) -> List[str]: return self._pol_names

    @pol_names.setter
    def pol_names(self, value: List[str]): self.setvalue('_pol_names', value)

    @property
    def raster_info(self) -> 'RasterInfo': return self._raster_info

    @raster_info.setter
    def raster_info(self, value: 'RasterInfo'): self.setvalue('_raster_info', value)

    @property
    def msobj(self) -> 'MeasurementSet': return self._msobj

    @msobj.setter
    def msobj(self, value: 'MeasurementSet'): self.setvalue('_msobj', value)

    @property
    def calmsobj(self) -> 'MeasurementSet': return self._calmsobj

    @calmsobj.setter
    def calmsobj(self, value: 'MeasurementSet'): self.setvalue('_calmsobj', value)

    @property
    def polids(self) -> List[int]: return self._polids

    @polids.setter
    def polids(self, value: List[int]): self.setvalue('_polids', value)

    @property
    def error_msg(self) -> str: return self._error_msg

    @error_msg.setter
    def error_msg(self, value: str): self.setvalue('_error_msg', value)

    @property
    def dt(self) -> 'DataTableImpl': return self._dt

    @dt.setter
    def dt(self, value: 'DataTableImpl'): self.setvalue('_dt', value)

    @property
    def index_list(self) -> 'numpy.ndarray[numpy.int64]': return self._index_list

    @index_list.setter
    def index_list(self, value: 'numpy.ndarray[numpy.int64]'): self.setvalue('_index_list', value)

    @property
    def effBW(self) -> float: return self._effBW

    @effBW.setter
    def effBW(self, value: float): self.setvalue('_effBW', value)

    @property
    def mean_tsys_per_pol(self) -> 'numpy.ndarray[float]': return self._mean_tsys_per_pol

    @mean_tsys_per_pol.setter
    def mean_tsys_per_pol(self, value: 'numpy.ndarray[float]'): self.setvalue('_mean_tsys_per_pol', value)

    @property
    def width(self) -> float: return self._width

    @width.setter
    def width(self, value: float): self.setvalue('_width', value)

    @property
    def height(self) -> float: return self._height

    @height.setter
    def height(self, value: float): self.setvalue('_height', value)

    @property
    def t_on_act(self) -> float: return self._t_on_act

    @t_on_act.setter
    def t_on_act(self, value: float): self.setvalue('_t_on_act', value)

    @property
    def calst(self) -> 'IntervalCalState': return self._calst

    @calst.setter
    def calst(self, value: 'IntervalCalState'): self.setvalue('_calst', value)

    @property
    def t_sub_on(self) -> float: return self._t_sub_on

    @t_sub_on.setter
    def t_sub_on(self, value: float): self.setvalue('_t_sub_on', value)

    @property
    def t_sub_off(self) -> float: return self._t_sub_off

    @t_sub_off.setter
    def t_sub_off(self, value: float): self.setvalue('_t_sub_off', value)
