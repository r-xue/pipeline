from __future__ import absolute_import
import re
import os
import string

import matplotlib.ticker as ticker
from matplotlib.colors import ColorConverter, Colormap, Normalize
import numpy as np
from numpy import ma
import pylab as plt

from pipeline.infrastructure.jobrequest import casa_tasks
import pipeline.infrastructure.renderer.logger as logger

_valid_chars = "_.%s%s" % (string.ascii_letters, string.digits)
def _char_replacer(s):
    '''A small utility function that echoes the argument or returns '_' if the
    argument is in a list of forbidden characters.
    '''
    if s not in _valid_chars:
        return '_'
    return s

def sanitize(text):
    filename = ''.join(_char_replacer(c) for c in text)
    return filename

flag_color = {'outlier': 'red',
              'high outlier':'orange',
              'low outlier':'yellow',
              'too many flags':'lightblue',
              'nmedian':'darkred'}
              

class ImageDisplay(object):

    def plot(self, context, results, reportdir, prefix='',
      change='Flagging'):
        if not results:
            return []

        stagenumber = context.stage

        plots = []        

        vis = results.vis
        flagops = results.flagops()
        descriptionlist = results.descriptions()
        descriptionlist.sort()

        for description in descriptionlist:
            if len(flagops) > 0:
                nsubplots = 3
                ignore = self._plot_panel(nsubplots, 1,
                  description, results.first(description),
                  'Before %s' % change, stagenumber)

                flagsSet = self._plot_panel(nsubplots, 2,
                  description, results.last(description),
                  'After', stagenumber, flagops)
            else:
                nsubplots = 2
                ignore = self._plot_panel(nsubplots, 1,
                  description, results.first(description),
                  '', stagenumber)

            # plot the titles and key
            plt.subplot(1, 3, 3)
            plt.axis('off')
            yoff = 1.1
            yoff = self.plottext(0.1, yoff, 'STAGE: %s' %
              stagenumber, 30)
            yoff = self.plottext(0.1, yoff, description, 30)

            # flaggingkey
            yoff -= 0.1

            ax = plt.gca()
            ax.fill([0.1, 0.2, 0.2, 0.1], [yoff, yoff,
              yoff+1.0/80.0, yoff+1.0/80.0], facecolor='indigo',
              edgecolor='indigo')
            yoff = self.plottext(0.25, yoff, 'No data', 35, mult=0.9)

            ax.fill([0.1, 0.2, 0.2, 0.1], [yoff, yoff, yoff+1.0/80.0,
              yoff+1.0/80.0], facecolor='violet', edgecolor='violet')
            yoff = self.plottext(0.25, yoff, 'cannot calculate', 45, mult=0.8)

            # key for data flagged during this stage
            if len(flagops) > 0:
                rulesplotted = set()
                
                yoff = self.plottext(0.1, yoff, 'Flagged here:', 35, mult=0.9)
                yoff = self.plottext(0.1, yoff, 'rules:', 45, mult=0.8)
                for flagop in flagops:
                    if flagop.rulename == 'ignore':
                        continue
 
                    if (flagop.rulename, flagop.ruleaxis,
                      flag_color[flagop.rulename]) not in rulesplotted:
                        color = flag_color[flagop.rulename]
                        ax.fill([0.1, 0.2, 0.2, 0.1], [yoff, yoff,
                          yoff+1.0/80.0, yoff+1.0/80.0],
                          facecolor=color, edgecolor=color)
                        if flagop.ruleaxis is not None:
                            yoff = self.plottext(0.25, yoff, '%s axis - %s' %
                              (flagop.ruleaxis, flagop.rulename), 45, mult=0.8)
                        else:
                            yoff = self.plottext(0.25, yoff, flagop.rulename,
                              45, mult=0.8)
                        rulesplotted.update([(flagop.rulename, flagop.ruleaxis,
                          color)])

            yoff = 0.6
            yoff = self.plottext(0.1, yoff, 'Antenna key:', 40)
            yoffstart = yoff
            xoff = 0.1
            # Get the MS object.
            ms = context.observing_run.get_ms(name=vis)
            antennas = ms.antennas
            for antenna in antennas:
                yoff = self.plottext(xoff, yoff, '%s:%s' % (antenna.id,
                  antenna.name), 40)
                if yoff < 0.0:
                    yoff = yoffstart
                    xoff += 0.30
             
            # reset axis limits which otherwise will have been pulled
            # around by the plotting of the key
            plt.axis([0.0,1.0,0.0,1.0])

            # save the image (remove odd characters from filename to cut
            # down length)
            xtitle = results.first(description).axes[0].name
            ytitle = results.first(description).axes[1].name
            plotfile = '%s_%s_%s_v_%s_%s.png' % (prefix,
              results.first(description).datatype, ytitle, xtitle, description)
            plotfile = sanitize(plotfile)
            plt.savefig(os.path.join(reportdir, plotfile))

            plt.clf()
            plt.close(1)

            plot = logger.Plot(plotfile,
              x_axis=xtitle, y_axis=ytitle,
              field=results.first(description).fieldname,
              parameters={'intent': results.first(description).intent,
              'spw': results.first(description).spw,
              'pol': results.first(description).pol,
              'ant': results.first(description).ant,
              'type': results.first(description).datatype,
              'file': os.path.basename(results.first(description).filename)})
            plots.append(plot)

        return plots

    def _plot_panel(self, nplots, plotnumber, description,
      image, subtitle, stagenumber, flagops=[]):
        """Plot the 2d data into one panel.

        Keyword arguments:
        nplots             -- The number of sub-plots on the page.
        plotnumber         -- The index of this sub-plot.
        description        -- Title associated with image.
        image              -- The 2d data.
        subtitle           -- The title to be given to this subplot.
        stagenumber        -- The number of the recipe stage using the object.
        flagops            -- Flag operationss applied to this view.
        """  
        cc = ColorConverter()
        sentinels = {}

        flag = image.flag
        data = image.data
        xtitle = image.axes[0].name
        xdata = image.axes[0].data
        ytitle = image.axes[1].name
        ydata = image.axes[1].data
        dataunits = image.units
        datatype = image.datatype
        indices = np.indices(np.shape(data))
        i = indices[0]
        j = indices[1]

        # set sentinels at points with no data/violet. These should be
        # overwritten by other flag colours in a moment.
        data[flag!=0] = 2.0
        sentinels[2.0] = cc.to_rgb('violet')

        # sentinels to mark flagging.

        # look at the flags in reverse so that the first time a point is
        # flagged is what appears on the plot. Work on local copies to avoid
        # unpredictable effects on global data.

        pixelsflagged = False

        flagrules = set()
        for flagop in flagops:
            flagrules.update([flagop.rulename])

        flagrules = list(flagrules)
        flagrules.sort()
        flagrules = np.array(flagrules)
        flagind = np.indices(np.shape(flagrules))[0]

        for flagop in flagops:
            # ignore if the flagop.rulename says so
            if flagop.rulename == 'ignore':
                continue

            # ignore if flag does not have colour associated with it
            if flag_color.get(flagop.rulename) is None:
                continue

            # basic checks of flag applicability
            if flagop.spw != image.spw:
                continue
            if flagop.cell_index != image.cell_index:
                continue           
            if flagop.axisnames == [xtitle, ytitle]:

                # flag does apply, set pixel colour accordingly
                sentinelvalue = 10.0 + float(
                  flagind[flagrules==flagop.rulename][0])
                sentinels[sentinelvalue] = cc.to_rgb(
                  flag_color[flagop.rulename])

                for flagcoord in flagop.flagcoords:
                    flagx = flagcoord[0]
                    flagy = flagcoord[1]

                    fset = self._setsentinel(i, j, xdata, ydata, data, flagx,
                      flagy, sentinelvalue) 
                    pixelsflagged = pixelsflagged or fset

        # plot points with no data indigo. 
        nodata = image.nodata
        data[nodata!=0] = 5.0
        sentinels[5.0] = cc.to_rgb('indigo')

        # set my own colormap and normalise to plot sentinels
        cmap = _SentinelMap(plt.cm.gray, sentinels=sentinels)
        norm = _SentinelNorm(sentinels=sentinels.keys())

        # calculate vmin, vmax without the sentinels. Leaving norm to do
        # this is not sufficient; the standard Normalize gets called 
        # by something in matplotlib and initialises vmin and vmax incorrectly.
        sentinel_mask = np.zeros(np.shape(data), np.bool)
        for sentinel in sentinels.keys():
            sentinel_mask += (data==sentinel)
        actual_data = data[np.logical_not(sentinel_mask)]
        if len(actual_data):
            vmin = actual_data.min()
            vmax = actual_data.max()
        else:
            vmin = vmax = 0.0

        # make antenna x antenna plots square
        aspect = 'auto'
        shrink = 0.6
        fraction = 0.15
        if ('ANTENNA' in xtitle.upper()) and ('ANTENNA' in ytitle.upper()):
            aspect = 'equal'
            shrink = 0.4
            fraction = 0.1

        # plot data - data transpose to get [x,y] into [row,column] expected by
        # matplotlib
        plt.subplot(1, nplots, plotnumber)
    
        plt.imshow(np.transpose(data), cmap=cmap, norm=norm, vmin=vmin,
          vmax=vmax, interpolation='nearest', origin='lower', aspect=aspect)
        lims = plt.axis()

        plt.xlabel(xtitle, size=20)
        for label in plt.gca().get_xticklabels():
            label.set_fontsize(16)
        plt.title(subtitle, fontsize='large')
        if plotnumber==1:
            plt.ylabel(ytitle, size=20)
        for label in plt.gca().get_yticklabels():
            label.set_fontsize(16)

        # plot wedge, make tick numbers smaller, label with units
        if vmin==vmax:
            cb = plt.colorbar(shrink=shrink, fraction=fraction,
              ticks=[-1,0,1])
        else:
            if (vmax - vmin > 0.001) and (vmax - vmin) < 10000:
                cb = plt.colorbar(shrink=shrink, fraction=fraction)
            else:
                cb = plt.colorbar(shrink=shrink, fraction=fraction,
                  format='%.1e')
        for label in cb.ax.get_yticklabels():
            label.set_fontsize(12)
        if plotnumber==1:
            if dataunits is not None:
                cb.set_label('%s (%s)' % (datatype, dataunits), fontsize=14)
            else:
                cb.set_label(datatype, fontsize=12)

        if ytitle.upper() == 'TIME':
            plt.gca().yaxis.set_major_locator(plt.NullLocator())
            if plotnumber==1:
                base_time = 86400.0*np.floor (ydata[0]/86400.0)
                tim_plot = ydata - base_time
                for i,t in enumerate(tim_plot):
                    h = int(np.floor(t/3600.0))
                    t -= h * 3600.0
                    m = int(np.floor(t/60.0))
                    t -= m * 60.0
                    s = int(np.floor(t))
                    tstring = '%sh%sm%ss' % (h,m,s)
                    plt.text(lims[0]-0.25, i, tstring, fontsize=14,
                      ha='right', va='bottom')

        # reset lims to values for image, stops box being pulled off edge
        # of image by other plotting commands.
        plt.axis(lims)

        return pixelsflagged

    def plottext(self, xoff, yoff, text, maxchars, ny_subplot=1, mult=1):
        """Utility method to plot text and put line breaks in to keep the
        text within a given limit.

        Keyword arguments:
        xoff       -- world x coord where text is to start.
        yoff       -- world y coord where text is to start.
        text       -- Text to print.
        maxchars   -- Maximum number of characters before a newline is
                      inserted.
        ny_subplot -- Number of sub-plots along the y-axis of the page.
        mult       -- Factor by which the text fontsize is to be multiplied.
        """
        words = text.rsplit()
        words_in_line = 0
        line = ''
        ax = plt.gca()
        for i in range(len(words)):
            temp = line + words[i] + ' '
            words_in_line += 1
            if len(temp) > maxchars:
                if words_in_line == 1:
                    while len(temp) > 0:
                        ax.text(xoff, yoff, temp[:maxchars], va='center',
                          fontsize=mult*16,
                          transform=ax.transAxes, clip_on=False)
                        temp = temp[min(len(temp), maxchars):]
                        yoff -= 0.03 * ny_subplot * mult
                    words_in_line = 0
                else:
                    ax.text(xoff, yoff, line, va='center', fontsize=mult*16,
                      transform=ax.transAxes, clip_on=False)
                    yoff -= 0.03 * ny_subplot * mult
                    line = words[i] + ' '
                    words_in_line = 1
            else:
                line = temp
        if len(line) > 0:
            ax.text(xoff, yoff, line, va='center', fontsize=mult*16,
              transform=ax.transAxes, clip_on=False)
            yoff -= 0.02 * ny_subplot * mult
        yoff -= 0.02 * ny_subplot * mult
        return yoff
        
    def _setsentinel(self, i, j, x, y, data, flagx, flagy, sentinel_value): 
        """Set pixels to sentinel value. Returns True if any values
        are actually set.

        Keyword arguments:
        i               -- i indices of data  
        j               -- j indices of data
        x               -- x-axis
        y               -- y-axis
        data            -- data array
        flagx           -- |data at these values of x,y to be shown flagged 
        flagy           -- |
        sentinel_value  -- The sentinel value to set in the target data
        """
        sentinelset = False

        i2flag = i[np.abs(flagx - x) < 0.1, np.abs(flagy - y) < 0.1]
        j2flag = j[np.abs(flagx - x) < 0.1, np.abs(flagy - y) < 0.1]
        if len(i2flag) > 0:
            data[i2flag, j2flag] = sentinel_value
            sentinelset = True

        return sentinelset


class _SentinelMap(Colormap):
    """Utility class for plotting sentinel pixels in colours."""

    def __init__(self, cmap, sentinels={}):
        """Constructor.
 
        Keyword arguments:
        """
        self.name = 'SentinelMap'
        cmap._init()
        self.cmap = cmap
        self._lut = cmap._lut
        self.N = cmap.N
        self.sentinels = sentinels
        self._isinit = True

    def __call__(self, scaledData, alpha=1.0, bytes=False):
        """Utility method.
        """
        rgba = self.cmap(scaledData, alpha, bytes)
        if bytes:
            mult = 255
        else:
            mult = 1

        for sentinel,rgb in self.sentinels.items():
            r,g,b = rgb 
            if np.ndim(rgba) == 3:
                rgba[:,:,0][scaledData==sentinel] = r * mult
                rgba[:,:,1][scaledData==sentinel] = g * mult
                rgba[:,:,2][scaledData==sentinel] = b * mult
                if alpha is not None:
                    rgba[:,:,3] = alpha * mult
            elif np.ndim(rgba) == 2:
                rgba[:,0][scaledData==sentinel] = r * mult
                rgba[:,1][scaledData==sentinel] = g * mult
                rgba[:,2][scaledData==sentinel] = b * mult
                if alpha is not None:
                    rgba[:,3] = alpha * mult

        return rgba


class _SentinelNorm(Normalize):
    """Normalise but leave sentinel values unchanged.
    """

    def __init__(self, vmin=None, vmax=None, clip=True, sentinels=[]):
        self.vmin = vmin
        self.vmax = vmax
        self.clip = clip
        self.sentinels = sentinels

    def __call__(self, value, clip=None):

        # remove sentinels, keeping a mask of where they were.
        sentinel_mask = np.zeros(np.shape(value), np.bool)
        for sentinel in self.sentinels:
            sentinel_mask += (value==sentinel)
        sentinel_values = value[sentinel_mask]

        actual_data = value[np.logical_not(sentinel_mask)]
        if len(actual_data):
            value[sentinel_mask] = actual_data.min()
        value = ma.asarray(value)
        value = Normalize.__call__(self, value, clip)

        # restore sentinels
        value[sentinel_mask] = sentinel_values
        return value

