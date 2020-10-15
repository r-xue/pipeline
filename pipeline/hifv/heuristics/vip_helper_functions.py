"""
VLASS-SE VIP script helper functions.

Unchanged functions can be found on the New Mexico nmpost cluster:
/home/vlass/packages/vip/mask-from-catalog/mask_from_catalog.py
/users/jmarvil/scripts/edit_pybdsf_islands.py
/users/jmarvil/scripts/run_bdsf.py
"""
import time
from glob import glob

import numpy as np
from scipy.stats import linregress

import pyfits
import bdsf

import pipeline.infrastructure.casatools as casatools


def run_bdsf(infile=""):
    """
    Detect bright point sources and blobs on map.

    :param infile:
    :return:
    """
    img = bdsf.process_image(infile, thresh_isl=2, thresh_pix=5, rms_box=[512, 512], thresh='hard',
                             adaptive_rms_box=False, mean_map='zero', peak_fit=True, split_isl=False,
                             group_by_isl=True)
    img.write_catalog(format='fits', outfile=infile.replace('.fits', '.cat.fits'))
    img.write_catalog(format='ds9', outfile=infile.replace('.fits', '.cat.ds9.reg'))
    img.export_image(outfile=infile + '.rms', img_type='rms', clobber=True)
    img.export_image(outfile=infile + '.island.mask', img_type='island_mask', clobber=True)


def mask_from_catalog(inext='iter0.model.tt0', outext="mask_from_cat.mask",
                      catalog_fits_file='/home/vlass/packages/VLASS1Q.fits',
                      catalog_search_size=1.0):
    """
    Construct a clean mask from a sky catalog and reference image.

    :param inext: A string to select the input image.  *inext should return a single CASA image.
    :param outext: A string to replace inext that will determine the filename of the output mask image.
    :param catalog_fits_file: Path to a PyBDSF sky catalog in FITS format.
    :param catalog_search_size: The half-width (in degrees) of the catalog search centered on the
        image's reference pixel.
    :return: returns nothing

    Example 1:
    sys.path.append('<path-to-folder-containing-this-file>')
    from mask_from_catalog import mask_from_catalog
    mask_from_catalog(...)

    Example 2:
    execfile('<path-to-folder-containing-this-file>/mask_from_catalog.py')
    mask_from_catalog(...)

    Unchanged, CASA 5 compatible file is located at
    /home/vlass/packages/vip/mask-from-catalog/mask_from_catalog.py
    """
    myia = casatools.image()
    myim = casatools.imager()
    myrg = casatools.regionmanager()
    myqa = casatools.quanta()

    import pdb; pdb.set_trace()

    # the intention is that this finds the model from the 'iter0' dirty image
    # but it really just needs any image for the shape and csys
    model_for_mask = glob('*' + inext)

    hdu = pyfits.open(catalog_fits_file)

    catalog_dat = hdu[1].data

    if not (len(model_for_mask) == 1):
        raise Exception('Expected an ' + inext + ' image to use for masking')

    mask_name = model_for_mask[0].replace(inext, outext)

    mask_im = myia.newimagefromimage(infile=model_for_mask[0], outfile=mask_name, overwrite=True)
    print('Created new mask image: {0}'.format(mask_name))

    mask_shape = mask_im.shape()
    mask_csys = mask_im.coordsys()

    mask_dat = mask_im.getchunk() * 0  # should already equal zero, but let's just make sure
    mask_im.putchunk(mask_dat)

    myia.done()

    mask_refval_radians = mask_csys.referencevalue(format='q')['quantity']

    mask_refval_ra_deg = myqa.convert(mask_refval_radians['*1'], 'deg')['value']
    mask_refval_dec_deg = myqa.convert(mask_refval_radians['*2'], 'deg')['value']

    catalog_within_dec_range = catalog_dat[(catalog_dat['DEC'] < mask_refval_dec_deg + catalog_search_size) & (
            catalog_dat['DEC'] > mask_refval_dec_deg - catalog_search_size)]
    print('Reduced catalog from {0} rows to {1} rows within +/- {2} degrees declination of image phase center'.format(
        catalog_dat.shape[0], catalog_within_dec_range.shape[0], catalog_search_size))

    # *** not tested for images near RA=0 or the NCP except in Josh's head ***
    # Catalog selection will use an RA range in degrees
    # after dividing by the most extreme cos(dec)
    # Selection will overshoot two image corners
    # and these will be rejected later using myia.topixel

    mask_dec_for_ra_search = np.max(
        np.abs([mask_refval_dec_deg - catalog_search_size, mask_refval_dec_deg + catalog_search_size]))

    if (mask_dec_for_ra_search < 90):
        catalog_ra_search_min = mask_refval_ra_deg - catalog_search_size / np.cos(np.pi * mask_dec_for_ra_search / 180.)
        catalog_ra_search_max = mask_refval_ra_deg + catalog_search_size / np.cos(np.pi * mask_dec_for_ra_search / 180.)
    else:
        catalog_ra_search_min = 0
        catalog_ra_search_max = 360

    if (catalog_ra_search_min > 0) and (catalog_ra_search_max < 360):
        catalog_within_ra_and_dec_range = catalog_within_dec_range[
            (catalog_within_dec_range['RA'] > catalog_ra_search_min) & (
                    catalog_within_dec_range['RA'] < catalog_ra_search_max)]
    elif (catalog_ra_search_min < 0):
        catalog_within_ra_and_dec_range = catalog_within_dec_range[
            (catalog_within_dec_range['RA'] > catalog_ra_search_min % 360) | (
                    catalog_within_dec_range['RA'] < catalog_ra_search_max)]
    elif (catalog_ra_search_max > 360):
        catalog_within_ra_and_dec_range = catalog_within_dec_range[
            (catalog_within_dec_range['RA'] > catalog_ra_search_min) | (
                    catalog_within_dec_range['RA'] < catalog_ra_search_max % 360)]
    else:
        catalog_within_ra_and_dec_range = catalog_within_dec_range

    print('Further reduced catalog from {0} rows to {1} rows within +/- {2} degrees RA of image phase center'.format(
        catalog_within_dec_range.shape[0], catalog_within_ra_and_dec_range.shape[0], catalog_search_size))

    # catalog rows to region text file
    parsed_catalog_crtf = 'mask_from_cat.crtf'
    rejected_rows = []
    with open(parsed_catalog_crtf, 'w') as out1:
        out1.writelines('#CRTFv0\n')
        ellipse_region = "ellipse [[{0}deg, {1}deg], [{2}deg, {3}deg], {4}deg]\n"

        for row in catalog_within_ra_and_dec_range:
            numeric_dir = {'numeric': [np.pi * row['RA'] / 180., np.pi * row['DEC'] / 180.]}
            pixel_dir = mask_im.topixel(numeric_dir)

            if np.all(pixel_dir['numeric'][:2] > 0) and np.all(pixel_dir['numeric'][:2] < mask_shape[0]):
                out1.writelines(ellipse_region.format(row['RA'], row['DEC'], row['Maj'], row['Min'], row['PA']))
            else:
                rejected_rows.append(row)

    print('Rejected {0} additional catalog rows outside the image'.format(len(rejected_rows)))
    print('Wrote reduced catalog to region file: {0}'.format(parsed_catalog_crtf))

    mask_im.done()

    # region text file to mask
    myRGN = myrg.fromtextfile(filename=parsed_catalog_crtf, shape=mask_shape, csys=mask_csys.torecord())
    myim.regiontoimagemask(mask=mask_name, region=myRGN)
    print('Used region file to add masks to mask image')

    myim.done()
    myrg.done()
    myqa.done()


def edit_pybdsf_islands(catalog_fits_file='', r_squared_threshold=0.99,
                        n_gauss_threshold=10, gauss_size_threshold=100):
    """
    Reject islands from a sky catalog based on simple heuristics.
    Writes out a new catalog ending in '.edited.fits'
    Also creates ds9 region files for accepted and rejected components.

    :param catalog_fits_file: Path to a PyBDSF sky catalog in FITS format.
    :param r_squared_threshold: Upper limit of correlation coefficient.
        Islands having components along a line are rejected.
    :param n_gauss_threshold: Maximum number of components per island.
        Islands with more than this many components are rejected.
    :param gauss_size_threshold: Maximum component size in arcsec.
        Islands with one or more component larger than this size are rejected.
    :return: returns nothing

    Example 1:
    sys.path.append('<path-to-folder-containing-this-file>')
    from edit_pybdsf_islands import edit_pybdsf_islands
    edit_pybdsf_islands(...)

    Example 2:
    execfile('<path-to-folder-containing-this-file>/edit_pybdsf_islands.py')
    edit_pybdsf_islands(...)

    Copy of script located at:
    /users/jmarvil/scripts/edit_pybdsf_islands.py
    """
    hdu = pyfits.open(catalog_fits_file)
    catalog_dat = hdu[1].data

    islands, island_counts = np.unique(catalog_dat['Isl_id'], return_counts=True)

    # flag islands based on Gaussians with large b_maj
    # LAS of B-config at S-band is 58" for full synthesis, 29" for snapshot
    # https://science.nrao.edu/facilities/vla/docs/manuals/oss/performance/resolution
    large_islands, large_island_counts = np.unique(
        catalog_dat['Isl_id'][catalog_dat['Maj'] * 3600 > gauss_size_threshold],
        return_counts=True)

    # flag islands with too many Gaussians
    numerous_islands = islands[island_counts > n_gauss_threshold]

    # for islands with more than 3 components
    # check if components are all on a line
    linear_islands = []
    hits = np.where(island_counts >= 3)[0]
    for island in islands[hits]:
        x = catalog_dat[catalog_dat['Isl_id'] == island]['Xposn']
        y = catalog_dat[catalog_dat['Isl_id'] == island]['Yposn']
        slope, intercept, r_value, p_value, std_err = linregress(x, y)

        if r_value ** 2 > r_squared_threshold:
            linear_islands.append(island)

    print('large islands: ', list(large_islands))
    print('linear_islands: ', list(linear_islands))
    print('numerous_islands: ', list(numerous_islands))

    rejected_islands = list(set.union(set(large_islands), set(linear_islands), set(numerous_islands)))
    print('rejected_islands: ', list(rejected_islands))

    cat_to_ds9_rgn(catalog_dat[np.in1d(catalog_dat['Isl_id'], rejected_islands)],
                   outfile=catalog_fits_file.replace('.fits', '') + '.rejected.ds9.reg',
                   region_color='red')

    print('wrote region file of rejected islands to: {0}'.format(catalog_fits_file.replace('.fits', '')
                                                                 + '.rejected.ds9.reg'))

    cat_to_ds9_rgn(catalog_dat[~np.in1d(catalog_dat['Isl_id'], linear_islands)],
                   outfile=catalog_fits_file.replace('.fits', '') + '.accepted.ds9.reg',
                   region_color='green')

    print('wrote region file of accepted islands to: {0}'.format(catalog_fits_file.replace('.fits', '')
                                                                 + '.accepted.ds9.reg'))

    hdu[1].data = catalog_dat[~np.in1d(catalog_dat['Isl_id'], linear_islands)]

    with open(catalog_fits_file.replace('.fits', '') + '.edited.fits', 'w') as out1:
        hdu.writeto(out1)

    hdu.close()

    print('wrote catalog of accepted islands to: {0}'.format(catalog_fits_file.replace('.fits', '') + '.edited.fits'))


def cat_to_ds9_rgn(catalog_fits_file, outfile='ds9.reg', region_color='red'):
    """
    Write each component to a ds9 region

    Copy of script located at:
    /users/jmarvil/scripts/edit_pybdsf_islands.py
    """
    region = 'ellipse({0},{1},{2}",{3}",{4}) # text={{i{5}_g{6}}}\n'

    with open(outfile, 'w') as out1:
        out1.write('# Region file format: DS9 version 4.0\n')
        out1.write(
            'global color={0} font="helvetica 10 normal" select=1 highlite=1 edit=1 move=1 delete=1 include=1 fixed=0 source\n'.format(
                region_color))
        out1.write('fk5\n')
        for line in catalog_fits_file:
            out1.write(region.format(
                line.field('RA'),
                line.field('DEC'),
                line.field('Maj') * 3600.,
                line.field('Min') * 3600.,
                line.field('PA') + 90.,
                line.field('Isl_id'),
                line.field('Gaus_id')
            ))
    return


class Timer:
    """
    Contextualisable class for simple runtime measurement.
    """
    def __init__(self, message="Run for {:.4f} seconds", logger=print):
        self.message = message
        self.logger = logger
        self._start_time = None

    def start(self):
        """Start timer."""
        if self._start_time:
            raise Exception("Timer is already running! Stop it with .stop() method.")
        self._start_time = time.perf_counter()

    def stop(self):
        """Stop timer and return elapsed time."""
        if not self._start_time:
            raise Exception("Timer is not running! Start it with .start() method.")
        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        self.logger(self.message.format(elapsed_time))
        return elapsed_time

    def __enter__(self):
        """Initialise context timer."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Finalise context timer and print elapsed time."""
        self.stop()
