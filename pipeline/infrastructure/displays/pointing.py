"""Pointing methods and classes."""

import math
import os
from typing import List, Optional, Tuple

from matplotlib.axes._axes import Axes
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator, AutoLocator
import numpy as np

from pipeline.domain.datatable import DataTableImpl as DataTable
from pipeline.domain.datatable import OnlineFlagIndex
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
from pipeline.infrastructure.displays.plotstyle import casa5style_plot
from pipeline.infrastructure.renderer.logger import Plot

LOG = infrastructure.get_logger(__name__)

RArotation = 90
DECrotation = 0

DPISummary = 90

dsyb = '$^\circ$'
hsyb = ':'
msyb = ':'

def Deg2HMS(x: float, prec: int=0) -> List[str, str, str]:
    """
    Convert an angle in degree to hour angle.
    Example:
    >>> Deg2HMS(20.123, prec=7)
    ['01', '20', '29.5']

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        List of　strings of hour, minute, and second values in a specified
        precision.
    """
    # Transform degree to HHMMSS.sss format
    xx = x % 360
    cqa = casatools.quanta
    angle = cqa.angle(cqa.quantity(xx, 'deg'), prec=prec, form=['time'])[0]
    return angle.split(':')


def HHMM(x: float, pos=None) -> str:
    """
    Convert an angle in degree to hour angle with HHMM format.
    HHMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> HHMM(20.123)
    '01:20'

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        formatted strings of hour, minute values.
    """
    (h, m, s) = Deg2HMS(x, prec=6)
    return '%s%s%s' % (h, hsyb, m)


def __format_hms(x: str, prec: int=0) -> str:
    """
    Convert an angle in degree to hour angle with hms format.
    Example:
    >>> __format_hms(10.123)
    '00:40:30'

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.
    """
    (h, m, s) = Deg2HMS(x, prec)
    return '%s%s%s%s%s' % (h, hsyb, m, msyb, s)


def HHMMSS(x: str, pos=None) -> str:
    """
    Convert an angle in degree to hour angle with hms format.
    Example:
    >>> HHMMSS(10.123)
    '00:40:30'

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.
    """
    return __format_hms(x, prec=6)


def HHMMSSs(x: str, pos=None):
    """
    Convert an angle in degree to hour angle with hms format.
    Example:
    >>> HHMMSSs(10.123)
    '00:40:29.5'

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.
    """
    return __format_hms(x, prec=7)


def HHMMSSss(x: str, pos=None) -> str:
    """
    Convert an angle in degree to hour angle with hms format.
    Example:
    >>> HHMMSSss(10.123)
    '00:40:29.52'

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.
    """
    return __format_hms(x, prec=8)


def HHMMSSsss(x: str, pos=None) -> str:
    """Convert an angle in degree to hour angle with hms format.
    Example:
    >>> HHMMSSsss(10.123)
    '00:40:29.520'

    Args:
        x: An angle in degree.
        prec: Significant digits.
    Returns:
        formatted strings of hour, minute, and second values in a specified
        precision.
    """
    return __format_hms(x, prec=9)


def Deg2DMS(x: float, prec: int=0) -> List[str, str, str]:
    """
    Convert an angle in degree to dms angle (ddmmss.s).
    Example:
    >>> Deg2DMS('+01.02.23.4', prec=1)
    ['+01', '02', '23.4']

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        prec: Significant digits.
    Returns:
        A list of strings of degree, arcminute, and arcsecond values in a
        specified precision.
    """
    xxx = (x + 90) % 180 - 90
    xx = abs(xxx)
    sign = '-' if xxx < 0 else '+'
    cqa = casatools.quanta
    dms_angle = cqa.angle(cqa.quantity(xx, 'deg'), prec=prec)[0]
    seg = dms_angle.split('.')
    assert len(seg) < 5 and len(seg) > 0
    # force degree in %02d format and add sign. qa.angle always retrun positive angle
    seg[0] = ( '%s%02d' % (sign, int(seg[0][1:])) )
    if len(seg) == 4:
        return (seg[0], seg[1], str('.').join(seg[2:]))
    else:
        return seg


def DDMM(x: float, pos=None) -> str:
    """Convert an angle in degree to dms angle with DDMM format.
    DDMM* function is used to set axis formatter of matplotlib plots.
    The functions will be turned into matplotlib.ticker.FuncFormatter.
    The function should take two inputs, a tick value x and position pos.
    see also: https://matplotlib.org/3.3.0/api/ticker_api.html#matplotlib.ticker.FuncFormatter

    Example:
    >>> Deg2DMS(10.123)
    "+10$^\\circ$07'"

    Args:
        x: An angle in degree.
        pos:
    Returns:
        A dms angle with DDMM format.
    """
    (d, m, s) = Deg2DMS(x, prec=6)
    return '%s%s%s\'' % (d, dsyb, m)


def __format_dms(x: float, prec: int=0) -> str:
    """
    Convert an angle in degree to dms angle in specified precision.

    Example:
    >>> __format_dms(10.123)
    '+10$^\\circ$07\'23"'

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        prec: Significant digits.
    Returns:
        String of degree, arcminute, and arcsecond values in a
        specified precision.
    """
    (d, m, s) = Deg2DMS(x, prec)
    # format desimal part of arcsec value separately
    xx = s.split('.')
    s = xx[0]
    ss = '' if len(xx) ==1 else '.%s' % xx[1]
    return '%s%s%s\'%s\"%s' % (d, dsyb, m, s, ss)


def DDMMSS(x: str, pos=None) -> str:
    """
    Convert an angle in degree to dms angle with DDMMSS.

    Example:
    >>> DDMMSS(10.123)
    '+10$^\\circ$07\'23"'

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        pos:
    Returns:
        String of degree, arcminute, and arcsecond values with DDMMSS.
    """
    return __format_dms(x, prec=6)

def DDMMSSs(x: str, pos=None) -> str:
    """
    Convert an angle in degree to dms angle with DDMMSSs.

    Example:
    >>> DDMMSSs(10.123)
    '+10$^\\circ$07\'22".8'

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        pos:
    Returns:
        String of degree, arcminute, and arcsecond values with DDMMSSs.
    """
    return __format_dms(x, prec=7)


def DDMMSSss(x: str, pos=None) -> str:
    """
    Convert an angle in degree to dms angle with DDMMSSss.

    Example:
    >>> DDMMSSss(10.123)
    '+10$^\\circ$07\'22".80'

    Args:
        x: An angle in degree. Degree string is always associated with a sign.
        pos:
    Returns:
        String of degree, arcminute, and arcsecond values with DDMMSSss.
    """
    return __format_dms(x, prec=8)


def XYlabel(span: float, direction_reference: str, ofs_coord: bool=False
            ) -> Tuple[Union[GLGBlabel, RADEClabel]]:
    """
    Create labels for the x- and y-axes in plot.

    Args:
        span:
        direction_reference:
        ofs_coord:
    Returns:
        labels for the x- and y-axes in plot.
    """
    if direction_reference.upper() == 'GALACTIC':
        return GLGBlabel(span)
    else:
        return RADEClabel(span, ofs_coord)


def GLGBlabel(span: float
    ) -> Tuple[MultipleLocator, MultipleLocator, FuncFormatter, FuncFormatter]:
    """
    Create GLGB formart label.

    Args:
        span:
    Returns:
        (GLlocator, GBlocator, GLformatter, GBformatter) for Galactic coordinate
    """
    # RAtick = [15.0, 5.0, 2.5, 1.25, 1/2.0, 1/4.0, 1/12.0, 1/24.0, 1/48.0, 1/120.0, 1/240.0, 1/480.0, 1/1200.0,
    #           1/2400.0, 1/4800.0, 1/12000.0, 1/24000.0, 1/48000.0, -1.0]
    XYtick = [20.0, 10.0, 5.0, 2.0, 1.0, 1/3.0, 1/6.0, 1/12.0, 1/30.0, 1/60.0, 1/180.0, 1/360.0, 1/720.0, 1/1800.0,
              1/3600.0, 1/7200.0, 1/18000.0, 1/36000.0, -1.0]
    #for RAt in RAtick:
    #    if span > (RAt * 3.0) and RAt > 0:
    #        RAlocator = MultipleLocator(RAt)
    #        break
    #if RAt < 0: RAlocator = MultipleLocator(1/96000.)
    #if RAt < 0: RAlocator = AutoLocator()
    for t in XYtick:
        if span > (t * 3.0) and t > 0:
            GLlocator = MultipleLocator(t)
            GBlocator = MultipleLocator(t)
            break
    #if DECt < 0: DEClocator = MultipleLocator(1/72000.0)
    if t < 0:
        GLlocator = AutoLocator()
        GBlocator = AutoLocator()

    if span < 0.0001:
        GLformatter = FuncFormatter(DDMMSSss)
        GBformatter = FuncFormatter(DDMMSSss)
    elif span < 0.001:
        GLformatter = FuncFormatter(DDMMSSs)
        GBformatter = FuncFormatter(DDMMSSs)
    elif span < 0.01:
        GLformatter = FuncFormatter(DDMMSS)
        GBformatter = FuncFormatter(DDMMSS)
    elif span < 1.0:
        GLformatter = FuncFormatter(DDMMSS)
        #GBformatter=FuncFormatter(DDMM)
        GBformatter = FuncFormatter(DDMMSS)
    else:
        GLformatter = FuncFormatter(DDMM)
        GBformatter = FuncFormatter(DDMM)

    return (GLlocator, GBlocator, GLformatter, GBformatter)


def RADEClabel(span: float,
        ofs_coord: bool
    ) -> Tuple[MultipleLocator, MultipleLocator, FuncFormatter, FuncFormatter]:
    """
    Args:
        span:
        ofs_coord:
    Returns:
        (RAlocator, DEClocator, RAformatter, DECformatter)
    """
    RAtick = [15.0, 5.0, 2.5, 1.25, 1/2.0, 1/4.0, 1/12.0, 1/24.0, 1/48.0, 1/120.0, 1/240.0, 1/480.0, 1/1200.0, 1/2400.0,
              1/4800.0, 1/12000.0, 1/24000.0, 1/48000.0, -1.0]
    DECtick = [20.0, 10.0, 5.0, 2.0, 1.0, 1/3.0, 1/6.0, 1/12.0, 1/30.0, 1/60.0, 1/180.0, 1/360.0, 1/720.0, 1/1800.0,
               1/3600.0, 1/7200.0, 1/18000.0, 1/36000.0, -1.0]
    for RAt in RAtick:
        if span > (RAt * 3.0) and RAt > 0:
            RAlocator = MultipleLocator(RAt)
            break
    #if RAt < 0: RAlocator = MultipleLocator(1/96000.)
    if RAt < 0: RAlocator = AutoLocator()
    for DECt in DECtick:
        if span > (DECt * 3.0) and DECt > 0:
            DEClocator = MultipleLocator(DECt)
            break
    #if DECt < 0: DEClocator = MultipleLocator(1/72000.0)
    if DECt < 0: DEClocator = AutoLocator()

    if span < 0.0001:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMMSSss)
        else:
            RAformatter = FuncFormatter(HHMMSSsss)
        DECformatter = FuncFormatter(DDMMSSss)
    elif span < 0.001:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMMSSs)
        else:
            RAformatter = FuncFormatter(HHMMSSss)
        DECformatter = FuncFormatter(DDMMSSs)
    elif span < 0.01:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMMSS)
        else:
            RAformatter = FuncFormatter(HHMMSSs)
        DECformatter = FuncFormatter(DDMMSS)
    elif span < 1.0:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMMSS)
        else:
            RAformatter = FuncFormatter(HHMMSS)
        #DECformatter=FuncFormatter(DDMM)
        DECformatter = FuncFormatter(DDMMSS)
    else:
        if ofs_coord:
            RAformatter = FuncFormatter(DDMM)
        else:
            RAformatter = FuncFormatter(HHMM)
        DECformatter = FuncFormatter(DDMM)

    return (RAlocator, DEClocator, RAformatter, DECformatter)


class MapAxesManagerBase(object):
    @property
    def direction_reference(self) -> str:
        """Get direction reference."""
        return self._direction_reference

    @direction_reference.setter
    def direction_reference(self, value: str) -> None:
        """Set direction reference."""
        if isinstance(value, str):
            self._direction_reference = value

    @property
    def ofs_coord(self) -> bool:
        """Get bool if the class is OFS coordinate."""
        return self._ofs_coord

    @ofs_coord.setter
    def ofs_coord(self, value: bool) -> None:
        """Set bool if the class is OFS coordinate."""
        if isinstance(value, bool):
            self._ofs_coord = value

    def __init__(self) -> None:
        self._direction_reference = None
        self._ofs_coord = None

    def get_axes_labels(self) -> Tuple[str, str]:
        """
        Returns:
            xlabel: xlabel in plot. Default is 'RA'.
            ylabel: ylabel in plot. Default is 'Dec'.
        """
        # default label is RA/Dec
        xlabel = 'RA'
        ylabel = 'Dec'
        if isinstance(self.direction_reference, str):
            if self.direction_reference in ['J2000', 'ICRS']:
                if self.ofs_coord:
                    xlabel = 'Offset-RA ({0})'.format(self.direction_reference)
                    ylabel = 'Offset-Dec ({0})'.format(self.direction_reference)
                else:
                    xlabel = 'RA ({0})'.format(self.direction_reference)
                    ylabel = 'Dec ({0})'.format(self.direction_reference)
            elif self.direction_reference.upper() == 'GALACTIC':
                xlabel = 'GL'
                ylabel = 'GB'
        return xlabel, ylabel


class PointingAxesManager(MapAxesManagerBase):
    MATPLOTLIB_FIGURE_ID = 9005

    @property
    def direction_reference(self) -> str:
        """Get direction reference."""
        return self._direction_reference

    @direction_reference.setter
    def direction_reference(self, value: str) -> None:
        """Set direction reference."""
        if isinstance(value, str):
            self._direction_reference = value

    @property
    def ofs_coord(self) -> bool:
        """Get bool if the class is OFS coordinate."""
        return self._ofs_coord

    @ofs_coord.setter
    def ofs_coord(self, value: bool):
        """Set bool if the class is OFS coordinate."""
        if isinstance(value, bool):
            self._ofs_coord = value

    def __init__(self) -> None:
        self._axes = None
        self.is_initialized = False
        self._direction_reference = None
        self._ofs_coord = None

    def init_axes(self, xlocator, ylocator, xformatter, yformatter, xrotation, yrotation, aspect, xlim=None, ylim=None,
                  reset=False) -> None:
        """
        Set matplotlib axes.

        Args:
            xlocator:
            ylocator:
            xformatter:
            yformatter:
            xrotation:
            yrotation:
            aspect:
            xlim:
            ylim:
            reset:
        """
        if self._axes is None:
            self._axes = self.__axes()

        if xlim is not None:
            self._axes.set_xlim(xlim)

        if ylim is not None:
            self._axes.set_ylim(ylim)

        if self.is_initialized == False or reset:
            # 2008/9/20 DEC Effect
            self._axes.set_aspect(aspect)
            self._axes.xaxis.set_major_formatter(xformatter)
            self._axes.yaxis.set_major_formatter(yformatter)
            self._axes.xaxis.set_major_locator(xlocator)
            self._axes.yaxis.set_major_locator(ylocator)
            xlabels = self._axes.get_xticklabels()
            plt.setp(xlabels, 'rotation', xrotation, fontsize=8)
            ylabels = self._axes.get_yticklabels()
            plt.setp(ylabels, 'rotation', yrotation, fontsize=8)

    @property
    def axes(self) -> Axes:
        if self._axes is None:
            self._axes = self.__axes()
        return self._axes

    def __axes(self) -> Axes:
        a = plt.axes([0.15, 0.2, 0.7, 0.7])
        xlabel, ylabel = self.get_axes_labels()
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title('')
        return a


def draw_beam(axes, r: float, aspect: float, x_base: float, y_base: float,
              offset: float=1.0):
    """
    Draw beam.

    Args:
        axes: pyplot instance of the current axes.
        r:
        aspect:
        x_base:
        y_base:
        offset:
    Returns:
    """
    xy = np.array([[r * (math.sin(t * 0.13) + offset) * aspect + x_base,
                       r * (math.cos(t * 0.13) + offset) + y_base]
                      for t in range(50)])
    plt.gcf().sca(axes)
    line = plt.plot(xy[:, 0], xy[:, 1], 'r-')
    return line[0]


def draw_pointing(axes_manager: PointingAxesManager,
                    RA: float,
                    DEC: float,
                    FLAG: Optional[int]=None,
                    plotfile: Optional[str]=None,
                    connect: bool=True,
                    circle: List[Optional[float]]=[],
                    ObsPattern: bool=False,
                    plotpolicy: str='ignore'
                ) -> None:
    """
    Draw pointing plots using matplotlib, export the plots and delete the matplotlib objects.

    Args:
        axes_manager: PointingAxesManager() instance.
        RA:
        DEC:
        FLAG:
        plotfile: A file path.
        connect:
        circle:
        ObsPattern:
        plotpolicy: The plotpolicy can be filled with 'plot', 'ignore' or
            'greyed'.
    """
    span = max(max(RA) - min(RA), max(DEC) - min(DEC))
    xmax = min(RA) - span / 10.0
    xmin = max(RA) + span / 10.0
    ymax = max(DEC) + span / 10.0
    ymin = min(DEC) - span / 10.0
    (RAlocator, DEClocator, RAformatter, DECformatter) = XYlabel(span, axes_manager.direction_reference, ofs_coord=axes_manager.ofs_coord)

    Aspect = 1.0 / math.cos(DEC[0] / 180.0 * 3.141592653)

    # Plotting routine
    if connect is True:
        Mark = 'g-o'
    else:
        Mark = 'bo'
    axes_manager.init_axes(RAlocator, DEClocator,
                           RAformatter, DECformatter,
                           RArotation, DECrotation,
                           Aspect,
                           xlim=(xmin, xmax),
                           ylim=(ymin, ymax))
    a = axes_manager.axes
    if ObsPattern == False:
        a.title.set_text('Telescope Pointing on the Sky')
    else:
        a.title.set_text('Telescope Pointing on the Sky\nPointing Pattern = %s' % ObsPattern)
    plot_objects = []

    if plotpolicy == 'plot':
        # Original
        plot_objects.extend(
            plt.plot(RA, DEC, Mark, markersize=2, markeredgecolor='b', markerfacecolor='b')
            )
    elif plotpolicy == 'ignore':
        # Ignore Flagged Data
        filter = FLAG == 1
        plot_objects.extend(
            plt.plot(RA[filter], DEC[filter], Mark, markersize=2, markeredgecolor='b', markerfacecolor='b')
            )
    elif plotpolicy == 'greyed':
        # Change Color
        if connect is True:
            plot_objects.extend(plt.plot(RA, DEC, 'g-'))
        filter = FLAG == 1
        plot_objects.extend(
            plt.plot(RA[filter], DEC[filter], 'o', markersize=2, markeredgecolor='b', markerfacecolor='b')
            )
        filter = FLAG == 0
        if np.any(filter == True):
            plot_objects.extend(
                plt.plot(RA[filter], DEC[filter], 'o', markersize=2, markeredgecolor='grey', markerfacecolor='grey')
                )
    # plot starting position with beam and end position
    if len(circle) != 0:
        plot_objects.append(
                draw_beam(a, circle[0], Aspect, RA[0], DEC[0], offset=0.0)
            )
        Mark = 'ro'
        plot_objects.extend(
            plt.plot(RA[-1], DEC[-1], Mark, markersize=4, markeredgecolor='r', markerfacecolor='r')
            )
    plt.axis([xmin, xmax, ymin, ymax])
    if plotfile is not None:
        plt.savefig(plotfile, format='png', dpi=DPISummary)

    for obj in plot_objects:
        obj.remove()


class SingleDishPointingChart(object):
    def __init__(self,
                    context,
                    ms,
                    antenna,
                    target_field_id=None,
                    reference_field_id=None,
                    target_only: bool=True,
                    ofs_coord: bool=False
                ) -> None:
        """
        Initialize SingleDishPointingChart class.

        Args:
            context: pipeline.Pipeline().context
            ms: measurementSet instance.
            antenna:
            target_field_id:
            reference_field_id:
            target_only:
            ofs_coord:
        """
        self.context = context
        self.ms = ms
        self.antenna = antenna
        self.target_field = self.__get_field(target_field_id)
        self.reference_field = self.__get_field(reference_field_id)
        self.target_only = target_only
        self.ofs_coord = ofs_coord
        self.figfile = self._get_figfile()
        self.axes_manager = PointingAxesManager()

    def __get_field(self, field_id: Optional[int]):
        if field_id is not None:
            fields = self.ms.get_fields(field_id)
            assert len(fields) == 1
            field = fields[0]
            LOG.debug('found field domain for %s'%(field_id))
            return field
        else:
            return None

    @casa5style_plot
    def plot(self, revise_plot: bool=False) -> Optional[Plot]:
        """
        Generate a plot object.

        Results:
            A Plot object.
        """
        if revise_plot == False and os.path.exists(self.figfile):
            return self._get_plot_object()

        ms = self.ms
        antenna_id = self.antenna.id

        datatable_name = os.path.join(self.context.observing_run.ms_datatable_name, ms.basename)
        datatable = DataTable()
        datatable.importdata(datatable_name, minimal=False, readonly=True)

        target_spws = ms.get_spectral_windows(science_windows_only=True)
        # Search for the first available SPW, antenna combination
        # observing_pattern is None for invalid combination.
        spw_id = None
        for s in target_spws:
            field_patterns = list(ms.observing_pattern[antenna_id][s.id].values())
            if field_patterns.count(None) < len(field_patterns):
                # at least one valid field exists.
                spw_id = s.id
                break
        if spw_id is None:
            LOG.info('No data with antenna=%d and spw=%s found in %s' % (antenna_id, str(target_spws), ms.basename))
            LOG.info('Skipping pointing plot')
            return None
        else: LOG.debug('Generate pointing plot using antenna=%d and spw=%d of %s' % (antenna_id, spw_id, ms.basename))
        beam_size = casatools.quanta.convert(ms.beam_sizes[antenna_id][spw_id], 'deg')
        beam_size_in_deg = casatools.quanta.getvalue(beam_size)
        obs_pattern = ms.observing_pattern[antenna_id][spw_id]
        antenna_ids = datatable.getcol('ANTENNA')
        spw_ids = datatable.getcol('IF')
        if self.target_field is None or self.reference_field is None:
            # plot pointings regardless of field
            if self.target_only == True:
                srctypes = datatable.getcol('SRCTYPE')
                func = lambda j, k, l: j == antenna_id and k == spw_id and l == 0
                vfunc = np.vectorize(func)
                dt_rows = vfunc(antenna_ids, spw_ids, srctypes)
            else:
                func = lambda j, k: j == antenna_id and k == spw_id
                vfunc = np.vectorize(func)
                dt_rows = vfunc(antenna_ids, spw_ids)
        else:
            field_ids = datatable.getcol('FIELD_ID')
            if self.target_only == True:
                srctypes = datatable.getcol('SRCTYPE')
                field_id = [self.target_field.id]
                func = lambda f, j, k, l: f in field_id and j == antenna_id and k == spw_id and l == 0
                vfunc = np.vectorize(func)
                dt_rows = vfunc(field_ids, antenna_ids, spw_ids, srctypes)
            else:
                field_id = [self.target_field.id, self.reference_field.id]
                func = lambda f, j, k: f in field_id and j == antenna_id and k == spw_id
                vfunc = np.vectorize(func)
                dt_rows = vfunc(field_ids, antenna_ids, spw_ids)

        if self.ofs_coord == True:
            racol = 'OFS_RA'
            deccol = 'OFS_DEC'
        else:
            racol = 'RA'
            deccol = 'DEC'
        LOG.debug('column names: {}, {}'.format(racol, deccol))
        if racol not in datatable.colnames() or deccol not in datatable.colnames():
            return None

        RA = datatable.getcol(racol)[dt_rows]
        if len(RA) == 0:  # no row found
            LOG.warn('No data found with antenna=%d, spw=%d, and field=%s in %s.' %
                     (antenna_id, spw_id, str(field_id), ms.basename))
            LOG.warn('Skipping pointing plots.')
            return None
        DEC = datatable.getcol(deccol)[dt_rows]
        FLAG = np.zeros(len(RA), dtype=int)
        rows = np.where(dt_rows == True)[0]
        assert len(RA) == len(rows)
        for (i, row) in enumerate(rows):
            pflags = datatable.getcell('FLAG_PERMANENT', row)
            # use flag for pol 0
            FLAG[i] = pflags[0][OnlineFlagIndex]

        self.axes_manager.direction_reference = datatable.direction_ref
        self.axes_manager.ofs_coord = self.ofs_coord

        plt.clf()
        draw_pointing(self.axes_manager, RA, DEC, FLAG, self.figfile, circle=[0.5*beam_size_in_deg],
                      ObsPattern=obs_pattern, plotpolicy='greyed')
        plt.close()

        return self._get_plot_object()

    def _get_figfile(self) -> str:
        """
        Generate file path to export a plot.

        Returns:
            file path to export a plot.
        """
        session_part = self.ms.session
        ms_part = self.ms.basename
        antenna_part = self.antenna.name
        if self.target_field is None or self.reference_field is None:
            identifier = antenna_part
        else:
            clean_name = self.target_field.clean_name
            identifier = antenna_part + '.%s'%(clean_name)
        if self.target_only == True:
            if self.ofs_coord == True:
                basename = 'offset_target_pointing.%s'%(identifier)
            else:
                basename = 'target_pointing.%s'%(identifier)
        else:
            basename = 'whole_pointing.%s'%(identifier)
        figfile = os.path.join(self.context.report_dir,
                               'session%s' % session_part,
                               ms_part,
                               '%s.png'%(basename))
        return figfile

    def _get_plot_object(self) -> Plot:
        """
        Generate a Plot object.

        Returns:
            A Plot object.
        """
        intent = 'target' if self.target_only == True else 'target,reference'
        if self.target_field is None or self.reference_field is None:
            field_name = ''
        else:
            if self.target_only or self.target_field.name == self.reference_field.name:
                field_name = self.target_field.name
            else:
                field_name = self.target_field.name + ',' + self.reference_field.name
        if self.ofs_coord == True:
            xaxis = 'Offset R.A.'
            yaxis = 'Offset Declination'
        else:
            xaxis = 'R.A.'
            yaxis = 'Declination'
        return Plot(self.figfile,
                           x_axis=xaxis,
                           y_axis=yaxis,
                           parameters={'vis': self.ms.basename,
                                       'antenna': self.antenna.name,
                                       'field': field_name,
                                       'intent': intent})
