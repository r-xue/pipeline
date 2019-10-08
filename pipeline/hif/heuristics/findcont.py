import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.utils as utils
from pipeline.extern.findContinuum import findContinuum
from pipeline.extern.findContinuum import countChannelsInRanges
from pipeline.extern.findContinuum import numberOfChannelsInCube

LOG = infrastructure.get_logger(__name__)


class FindContHeuristics(object):
    def __init__(self, context):
        self.context = context

    def find_continuum(self, dirty_cube, pb_cube=None, psf_cube=None, single_continuum=False):
        with casatools.ImageReader(dirty_cube) as image:
            stats = image.statistics()

        if stats['min'][0] == stats['max'][0]:
            LOG.error('Cube %s is constant at level %s.' % (dirty_cube, stats['max'][0]))
            return ['NONE'], 'none'

        # Run continuum finder on cube
        channel_selection, png_name, aggregate_bw, all_continuum = \
            findContinuum(img=dirty_cube,
                          pbcube=pb_cube,
                          psfcube=psf_cube,
                          singleContinuum=single_continuum,
                          returnAllContinuumBoolean=True)

        # PIPE-74
        channel_counts = countChannelsInRanges(channel_selection)
        if 1 == len(channel_counts):
            single_range_channel_fraction = channel_counts[0]/float(numberOfChannelsInCube(dirty_cube))
        else:
            single_range_channel_fraction = 999.

        if channel_selection == '':
            frequency_ranges_GHz = ['NONE']
        else:
            if all_continuum:
                frequency_ranges_GHz = ['ALL']
            else:
                frequency_ranges_GHz = []

            frequency_ranges_GHz.extend([{'range': item, 'refer': 'LSRK'} for item in utils.chan_selection_to_frequencies(dirty_cube, channel_selection, 'GHz')])

        return frequency_ranges_GHz, png_name, single_range_channel_fraction
