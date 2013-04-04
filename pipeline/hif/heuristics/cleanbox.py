import math
import numpy as np
import os.path
import types

import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure as infrastructure

LOG = infrastructure.get_logger(__name__)


class CleanBoxHeuristics(object):

    def __init__(self):
        # list of cleaned fluxes for successive cleaning loops
        self.sum_list = []

        # max and min peaks in residual
        self.max_peak = None
        self.min_peak = None

        # clean threshold
        self.threshold = None

    def clean_more(self, loop, new_threshold, sum, residual_max, residual_min,
      non_cleaned_rms, island_peaks):
        """Do we need to clean more deeply?
        """
        clean_more = True

        # not if the cleaned flux increased by less than 3%
        self.sum_list.append(sum)
        sum_array = np.array(self.sum_list)
        LOG.debug('model sums so far: %s' % sum_array)
        if clean_more and len(sum_array) > 6 and loop > 0:
            sum_change = (sum_array[-5:] - sum_array[-6:-1]) / sum_array[-1]
            LOG.debug('sum change: %s' % sum_change)
            mean_sum_change = np.mean(sum_change)

            if mean_sum_change < 0.03:
                LOG.info(
                  'terminate clean; cleaned flux increased by less than 3%')
                clean_more = False

        # not if the threshold is lower than the 1.5 *
        # residual rms - except for the first loop
        if clean_more and \
          abs(residual_max) < 1.3 * abs(residual_min) and \
          new_threshold < 1.5 * non_cleaned_rms and loop > 0:
            LOG.info(
              'terminate clean; next threshold (%s) < 1.5 * rms (%s)' % (
              new_threshold, non_cleaned_rms))
            clean_more = False

        # are the new islands the tops of noise peaks? Two ways of assessing
        # this.
        previous_max_peak = self.max_peak
        previous_min_peak = self.min_peak
        peaks = np.array(island_peaks.values())
        self.max_peak = max(peaks)
        self.min_peak = min(peaks)

        # first way, are there lots of peaks with similar peak values?
        if clean_more and loop > 0:
            try:
                if len(peaks) == 30 and self.max_peak < 1.2 * self.min_peak:
                    LOG.info('terminate clean; finding noise peaks (method 1)')
                    clean_more = False
            except:
                pass

        # second way, is there a big overlap between the range of peak values
        # from this loop and the last one?
        if clean_more and loop > 0:
            overlap_max = min(self.max_peak, previous_max_peak)
            overlap_min = max(self.min_peak, previous_min_peak)
            overall_max = max(self.max_peak, previous_max_peak)
            overall_min = min(self.min_peak, previous_min_peak)
            LOG.debug('overlap max:%s min:%s' % (overlap_max, overlap_min))
            LOG.debug('overall max:%s min:%s' % (overall_max, overall_min))

            if (self.max_peak < 2 * self.min_peak) and \
              ((overlap_max - overlap_min) / (overall_max - overall_min) >0.3):
                LOG.info('terminate clean; finding noise peaks (method 2)')
                clean_more = False

        # not so good if we've run out of iterations.
        if clean_more and loop > 10:
            LOG.warning('terminate clean; nloop > 10')
            clean_more = False

        # clean diverging?
        old_threshold = self.threshold
        self.threshold = new_threshold
        if clean_more and loop > 0 and new_threshold > old_threshold:
            LOG.warning('terminate clean; diverging')
            clean_more = False

        if clean_more:
            LOG.debug('continue cleaning; new threshold %sJy' % new_threshold)

        return clean_more

def psf_sidelobe_ratio(psf, island_threshold=0.1, peak_threshold=0.1):
    """Adapted from an algorithm by Amy Kimball, NRAO.
    """
    with casatools.ImageReader(psf) as image:

        # the psf is a cube. For now assume all planes are identical, 
        # except for the extreme channels which often appear empty.
        # Look at the middle plane only.
        nchan = image.shape()[3]
        target_chan = nchan / 2

        # searchmask is used to select the part of the image to be 
        # searched for peaks, initially all pixels above the 
        # 'island threshold'.
        # get mask
        searchmask = image.getregion(mask='%s/"%s" > %s' %
          (os.path.dirname(psf), os.path.basename(psf), island_threshold),
          getmask=True)[:,:,0,target_chan]

        # get pixels
        pixels = image.getregion()[:,:,0,target_chan]
        nx, ny = np.shape(pixels)

        grid = np.indices(np.shape(searchmask))

        # look for 2 highest islands.
        nislands = 0
        islandpix = {}
        islandpeak = {}
        while nislands < 2:

            if not(np.any(searchmask)):
                # no more pixels above island threshold: we're done
                break

            # find the next peak and its location
            argpeak = np.argmax(pixels[searchmask > 0])
            peakpos = (grid[0][searchmask>0][argpeak],
              grid[1][searchmask>0][argpeak])
            peak = pixels[peakpos]

            if peak < peak_threshold:
                # peak is below peak threshold for clean boxing: we're done
                break

            # make this the first pixel of the island, remove it from the mask
            island_pix = [peakpos]
            searchmask[peakpos] = False

            # find all above-threshold contiguous pixels of this island
            # python lets us loop over items in a list while appending to
            # the list!
            for pixel in island_pix:
                thisx = pixel[0]
                thisy = pixel[1]

                # search the pixels surrounding the pixel of interest
                xlook1 = max(0,thisx-1)      # in case we're at the image edge
                xlook2 = min(thisx+2,nx-1)   #            |
                ylook1 = max(0,thisy-1)      #            |
                ylook2 = min(thisy+2,ny-1)   #            V

                # only look at the four pixels that share an edge with
                # pixel-of-interest
                contig_pix = []
                contig_pix += [(thisx, ylook1)]
                contig_pix += [(thisx, ylook2-1)]
                contig_pix += [(xlook1, thisy)]
                contig_pix += [(xlook2-1, thisy)]

                for pix in contig_pix:
                    if searchmask[pix]:
                        island_pix.append(pix)
                        searchmask[pix] = False

            # reach here after finding all pixels in an island.
            # ignore the island if it comprises one isolated pixel
            if len(island_pix) == 1:
                continue

            # otherwise add the island to those found
            islandpix[nislands] = island_pix
            islandpeak[nislands] = peak
            nislands += 1
 
        # reach here after hopefully having 2 islands, the main peak and
        # first sidelobe
        if len(islandpeak.keys()) > 1:
            sidelobe_ratio = islandpeak[1] / islandpeak[0]
            LOG.info('psf peak:%s first sidelobe:%s sidelobe ratio:%s' % (
              islandpeak[0], islandpeak[1], sidelobe_ratio))
            if sidelobe_ratio > 0.7:
                # too high a value leads to problems with small clean
                # islands and slow convergence
                sidelobe_ratio = 0.7
                LOG.warning('sidelobe ratio too high, reset to 0.7')
        else:
            sidelobe_ratio = 0.5
            LOG.warning('psf analysis failure, sidelobe ratio set to 0.5')

    return sidelobe_ratio

def threshold_and_mask(residual, old_mask, new_mask, sidelobe_ratio,
 npeak=30, fluxscale=None):
    """Adapted from an algorithm by Amy Kimball, NRAO.

    Starting with peak in image, find islands; contiguous pixels above
    threshold. If peak bright enough to cleanbox then adds chosen region
    shape to mask. Continues with next peak pixel until Npeak peaks have
    been found.

    # Won't always add Npeak new islands if current islands still
    # contain peaks.  Isolated bright pixels (<2.5*peak_threshold) are
    #ignored.

    Keyword arguments:
    residual         -- Name of file containing residual map.
    old_mask         -- Name of file to contain mask.
    peak_threshold   -- threshold for island peaks.
    sidelobe_ratio   -- ratio of peak to first sidelobe in psf.
    fluxscale        -- Fluxscale map from mosaic clean. If set, used to locate
                        edges of map where spikes may occur.
    """
	
    # Get a searchmask to be used in masking image during processing.
    # An explicitly separate mask is used so as not to risk messing
    # up any mask that arrives with the residual.
    # Set all its values to 1 (=True=good).
    searchmask = casatools.image.newimagefromimage(infile=residual,
      outfile='searchmask', overwrite=True)
    searchmask.calc('1') 

    # Ignore parts of image where fluxscale map is less than 0.1
    # NOTE: logic may appear strange because the LEL 'replace'
    # function replaces masked pixels (i.e. bad pixels) 
    searchmask.calc('replace(searchmask["%s" > 0.1], 0)' % fluxscale)

    # Ignore edges of image in an effort to prevent divergence; spikes
    # sometimes appear there
    shape = searchmask.shape()
    searchmask.calc('replace(searchmask[indexin(0,[5:%s])], 0)' % shape[0])
    searchmask.calc('replace(searchmask[indexin(0,[0:%s])], 0)' % (shape[0]-5))
    searchmask.calc('replace(searchmask[indexin(1,[5:%s])], 0)' % shape[1])
    searchmask.calc('replace(searchmask[indexin(1,[0:%s])], 0)' % (shape[1]-5))

    with casatools.ImageReader(residual) as image:
        # find the max pixel value and derive the threshold for the clean mask
        # 'islands'.

        statistics = image.statistics(mask=searchmask, robust=False)
        maxpix = statistics['max'][0]
        island_threshold = sidelobe_ratio * maxpix

        # Update the mask to show only pixels above the threshold for island
        # membership
        searchmask.calc('replace(searchmask["%s" > %s], 0)' % (residual,
          island_threshold))

        island_pix = {}
        island_peaks = {}
        islandx = {}
        islandy = {}
        plane_threshold = {}

        plane_grid = np.indices([shape[0], shape[1], 1, 1])

        # Look for islands in each plane of the image/cube.
        searchmask_max = searchmask.statistics(axes=[0,1,2])['max']
        for plane in range(shape[3]):
            n_plane_islands = 0
            island_pix[plane] = {}
            island_peaks[plane] = {}
            islandx[plane] = {}
            islandy[plane] = {}

            # are there any island pixels in this plane?
            if searchmask_max[plane] < 0.5:
                continue

            plane_pixels = image.getchunk(blc=[0,0,0,plane],
              trc=[-1,-1,-1,plane])
            plane_searchmask = searchmask.getchunk(blc=[0,0,0,plane],
              trc=[-1,-1,-1,plane])
            # convert to bool
            plane_searchmask = plane_searchmask > 0.5

            # Look for islands in this plane
            while n_plane_islands < 30:
                if not(np.any(plane_searchmask)):
                    # no more pixels above island threshold: we're done
                    break

                # find the next island
                new_island_peak, new_peak_x, new_peak_y, new_island_pix = \
                  find_island(plane_searchmask, plane_pixels, plane_grid)

                # add island to list unless it is an isolated pixel
                if len(new_island_pix) > 1:
                    island_pix[plane][n_plane_islands] = new_island_pix
                    island_peaks[plane][n_plane_islands] = new_island_peak
                    islandx[plane][n_plane_islands] = new_peak_x
                    islandy[plane][n_plane_islands] = new_peak_y
                    n_plane_islands += 1

            # reach this point after all islands have been found in the 
            # plane. Calculate the ideal threshold for the next clean in
            # this plane.
            # For point sources we can clean island 0 safely down to peak 1
            # peak * sidelobe ratio without fear of mistakenly cleaning a
            # sidelobe of island 1. If no island 1 detected then assume 
            # it's safe to clean down to peak 0 * sidelobe ratio squared.
            if len(island_peaks[plane]) > 1:
                plane_threshold[plane] = island_peaks[plane][1] * \
                  sidelobe_ratio
            elif len(island_peaks[plane]) > 0:
                plane_threshold[plane] = island_peaks[plane][0] * \
                  pow(sidelobe_ratio, 2)
            else:
                plane_threshold[plane] = 0.0

        # free the searchmask
        searchmask.done(remove=True)

        # threshold is global to all planes, select the maximum found as this
        # should be safe for all
        threshold = max(plane_threshold.values())
        imax = np.argmax(np.array(plane_threshold.values()))

        # Reduce the 'plane island' information. Starting with an island
        # with a high peak value link it together with islands on other 
        # planes where there is an overlap between islands pixels on adjacent
        # planes. 

        # Obtain linked islands with the 'npeak' highest peaks
        nislands = 0
        island_tree_peaks = {}
        island_trees = {}

        while nislands < npeak:
            # find the brightest (remaining) island
            max_peak = None
            max_plane = None
            for plane in range(shape[3]):
                keys = island_peaks[plane].keys() 
                if len(keys) > 0:
                    # the brightest island has the lowest key 
                    brightest = min(keys)
                    if max_peak is None or \
                      island_peaks[plane][brightest] > max_peak:
                        max_peak = island_peaks[plane][brightest]
                        max_plane = plane
                        max_plane_brightest = brightest

            if max_plane is None:
               # no islands left
               break

            # begin island_tree with this island on this plane
            root = (max_plane, max_plane_brightest)
            island_tree = {root:[]}

            # build the tree of islands linked by to this one by overlap
            # between adjacent planes
            build_island_tree(island_tree, root, island_pix)

            # store the island tree
            island_tree_peaks[nislands] = max_peak
            island_trees[nislands] = dict(island_tree)
            nislands += 1

            # remove the plane islands just linked together so that they
            # will not be counted twice
            for node in island_tree.keys():
                ignore = island_peaks[node[0]].pop(node[1], None)

        # Create new mask
        if new_mask is not None:
            if old_mask is None:
                # construct empty mask the same shape as the map
                nm = image.newimagefromimage(infile=residual,
                  outfile=new_mask, overwrite=True)
            else:
                nm = image.newimagefromimage(infile=old_mask,
                  outfile=new_mask, overwrite=True)

            # add new islands cumulatively - Kumar says this is best as it
            # allows clean to correct mistakes in previous iterations
            for plane in range(shape[3]):
                maskpix = nm.getchunk(blc=[0,0,0,plane],
                  trc=[-1,-1,-1,plane])
                if old_mask is None:
                    maskpix[:] = 0.0

                for island_tree in island_trees.values():
                    plane_nodes = [k for k in island_tree.keys() if k[0]==plane] 
                    for node in plane_nodes:
                        for pix in island_pix[node[0]][node[1]]:
                            maskpix[pix] = 1.0
                        # remove the node from the tree as it will not be
                        # needed further
                        ignore = island_tree.pop(node, None)

                maskpix = nm.putchunk(blc=[0,0,0,plane], pixels=maskpix)

#            uncomment following if want next mask to have only one island
#            added
#            for pix in island_pix[0]:
#                maskpix[pix] = 1.0

            nm.close()
            nm.done()

        LOG.debug('new threshold is: %s' % threshold)
        LOG.debug('%s %s' % (threshold, island_tree_peaks))
        return threshold, island_tree_peaks

def build_island_tree(island_tree, node, island_pix):
    # island is a single island on plane
    # see if it joins to islands on adjacent planes

    # first, planes with lower index, using try-block to handle
    # limits to plane dimension
    plane = node[0]
    node_set = set(island_pix[plane][node[1]])
    new_nodes = []
    try:
        for k,candidate in island_pix[plane-1].items():
            candidate_set = set(candidate)
            if not node_set.isdisjoint(candidate_set):
                # add this island if it is not already a node
                # in the tree
                if (plane-1,k) not in island_tree.keys():
                    # link to the new node from the current node
                    island_tree[node].append({(plane-1,k): []})
                    # add the new node
                    island_tree[(plane-1,k)] = []
                    # note that we have added a new node 
                    new_nodes.append((plane-1,k))

        # at this point any islands in plane-1 that are linked to the 
        # island in the input plane should have been added to island_tree.
        # Now search for islands connected to the new nodes.
        for new_node in new_nodes:
            build_island_tree(island_tree, new_node, island_pix)
    except:
        pass

    # second, planes with higher index
    new_nodes = []
    try:
        for k,candidate in island_pix[plane+1].items():
            candidate_set = set(candidate)
            if not node_set.isdisjoint(candidate_set):
                if (plane+1,k) not in island_tree.keys():
                    island_tree[node].append({(plane+1,k): []})
                    island_tree[(plane+1,k)] = []
                    new_nodes.append((plane+1,k))

        for new_node in new_nodes:
            build_island_tree(island_tree, new_node, island_pix)
    except:
        pass

def find_island(searchmask, pixels, grid):
    island_pix = []
    peak = None
    peak_x = None
    peak_y = None

    if not(np.any(searchmask)):
        # no pixels above island threshold: we're done
        return peak, peak_x, peak_y, island_pix

    nx, ny, nstokes, nchan = np.shape(pixels)

    # find the next peak and its location
    argpeak = np.argmax(pixels[searchmask])
    peakpos = (grid[0][searchmask][argpeak], grid[1][searchmask][argpeak],
      grid[2][searchmask][argpeak], grid[3][searchmask][argpeak])
    peak = pixels[peakpos]

    # start a new 'island', clear the mask for this pixel
    island_pix = [peakpos]
    searchmask[peakpos] = False

    # find all above-threshold contiguous pixels of this island
    # python lets us loop over items in a list while appending to
    # the list!
    for pixel in island_pix:
        thisx = pixel[0]
        thisy = pixel[1]
        thiss = pixel[2]
        thisc = pixel[3]

        # search the pixels surrounding the pixel of interest
        xlook1 = max(0,thisx-1)       # in case we're at the image edge...
        xlook2 = min(thisx+1,nx-1)    #            |
        ylook1 = max(0,thisy-1)       #            |
        ylook2 = min(thisy+1,ny-1)    #            |
        clook1 = max(0,thisc-1)       #            |
        clook2 = min(thisc+1,nchan-1) #            v

        # only look at the six pixels that share an edge with
        # pixel-of-interest
        contig_pix = []
        contig_pix += [(thisx, ylook1, 0, thisc)]
        contig_pix += [(thisx, ylook2, 0, thisc)]
        contig_pix += [(xlook1, thisy, 0, thisc)]
        contig_pix += [(xlook2, thisy, 0, thisc)]
        contig_pix += [(thisx, thisy, 0, clook1)]
        contig_pix += [(thisx, thisy, 0, clook2)]

        for pix in contig_pix:
            if searchmask[pix]:
                island_pix.append(pix)
                searchmask[pix] = False

    # reach here after the whole island has been found

    peak_x = peakpos[0]
    peak_y = peakpos[1]

    return peak, peak_x, peak_y, island_pix

def analyse_clean_result(model, restored, residual, fluxscale, cleanmask):

        # get the sum of the model image to find how much flux has been
        # cleaned
        model_sum = None
        with casatools.ImageReader(model) as image:
            model_stats = image.statistics(robust=False)
            model_sum = model_stats['sum'][0]
            LOG.debug('sum of model: %s' % model_sum)

        LOG.debug('fixing coordsys of fluxscale and cleanmask')
        with casatools.ImageReader(residual) as image:
            csys = image.coordsys()
        if fluxscale is not None:
            with casatools.ImageReader(fluxscale) as image:
                image.setcoordsys(csys.torecord())
        if cleanmask is not None:
            with casatools.ImageReader(cleanmask) as image:
                image.setcoordsys(csys.torecord())

        with casatools.ImageReader(residual) as image:
            # get the rms of the residual image inside the cleaned area
            LOG.todo('cannot use dirname in mask')
            clean_rms = None
            if cleanmask is not None and os.path.exists(cleanmask):
                if fluxscale is not None and os.path.exists(fluxscale):
                    statsmask = '"%s" > 0.1 && "%s" > 0.1' % (
                      os.path.basename(cleanmask), os.path.basename(fluxscale))
                else:
                    statsmask = '"%s" > 0.1' % (os.path.basename(cleanmask))
                resid_clean_stats = image.statistics(mask=statsmask, 
                  robust=False)
                try:
                    clean_rms = resid_clean_stats['rms'][0]
                    LOG.info('residual rms inside cleaned area: %s' %
                      clean_rms)
                except:
                    pass

            # and the rms of the residual image outside the cleaned area
            non_clean_rms = None
            if cleanmask is not None and os.path.exists(cleanmask):
                if fluxscale is not None and os.path.exists(fluxscale):
                    statsmask = '"%s" < 0.1 && "%s" > 0.1' % (
                      os.path.basename(cleanmask), os.path.basename(fluxscale))
                else:
                    statsmask = '"%s" < 0.1' % (os.path.basename(cleanmask))
            else:
                if fluxscale is not None and os.path.exists(fluxscale):
                    statsmask = '"%s" > 0.1' % os.path.basename(fluxscale)
                else:
                    statsmask = ''
            resid_stats = image.statistics(mask=statsmask, robust=False)
            try:
                non_clean_rms = resid_stats['rms'][0]
                if cleanmask is not None:
                    LOG.info('residual rms outside cleaned area: %s' % 
                      non_clean_rms)
                else:
                    LOG.info('residual rms: %s' %  non_clean_rms)
            except:
                pass

            # get the max, min of the residual image (avoiding the edges
            # where spikes can occur)
            if fluxscale is not None and os.path.exists(fluxscale):
                residual_stats = image.statistics(
                  mask='"%s" > 0.1' % os.path.basename(fluxscale),
                  robust=False)
            else:
                residual_stats = image.statistics(robust=False)
            try:
                residual_max = residual_stats['max'][0]
                residual_min = residual_stats['min'][0]
            except:
                residual_max = None
                residual_min = None
            LOG.debug('residual max:%s min:%s' % (residual_max, residual_min))

            # get 2d rms of residual
            pixels = image.getregion(axes=[3])
            rms2d = np.std(pixels)
            LOG.debug('2d rms of residual:%s' % rms2d)

        # get max of cleaned result
        with casatools.ImageReader(restored) as image:
            clean_stats = image.statistics()
            image_max = clean_stats['max'][0]
            LOG.debug('clean image max: %s' % image_max)

        return model_sum, clean_rms, non_clean_rms, residual_max,\
          residual_min, rms2d, image_max
