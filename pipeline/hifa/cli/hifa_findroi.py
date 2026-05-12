import pipeline.h.cli.utils as utils


# docstring and type hints: inherits from hifa.tasks.findroi.findroi.FindROIInputs.__init__
@utils.cli_wrapper
def hifa_findroi(vis=None, field=None, spw=None, parallel=None, timebin_sec=None, min_nchan=None,
                 npix=None, fov_pb_mult=None, ref_sigma=None, mom0_thresh_sigma=None, gate_sigma=None,
                 pos_evidence_thr=None, neg_evidence_thr=None, evidence_thr=None,
                 evidence_bin_scales=None, ref_zero_frac_thr=None, mom0_zero_frac_thr=None,
                 ref_smooth_width=None, mom0_smooth_width=None, neg_extent_delta_thr=None,
                 neg_extent_trim_frac=None, acf_edge_trim_frac=None, acf_smooth_frac=None,
                 fwhm_global_fallback_factor=None, fwhm_min_chan=None, rolling_rms_window=None,
                 rolling_rms_target_unsmoothed=None, rolling_rms_target_smoothed=None,
                 uv_taper_auto=None, uv_taper_sigma_uv=None, uv_taper_fwhm_cell=None,
                 roi_thresh=None, roi_cont_thresh=None, tmp_dir=None, tmp_overwrite=None,
                 save_moment0=None, save_cube=None, save_results_path=None, verbose=None):
    """Detect spectral-line regions of interest for ALMA science targets.

    This task is intended to run after ``hifa_importdata``. By default it uses
    the pipeline context to process all science target sources and science
    spectral windows. It writes a native findROI stage-product pickle, ROI DAT
    files, and summary plots without changing downstream pipeline context.

    Returns:
        The results object for the pipeline task is returned.

    Examples:
        1. Run with recommended settings after importdata:

        >>> hifa_findroi()

        2. Restrict processing to one virtual science spectral window:

        >>> hifa_findroi(spw='25')

    """
