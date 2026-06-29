from __future__ import annotations

import copy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import task_registry

from pipeline.hif.heuristics import findroi as heuristics

from .resultobjects import FindROIResult

LOG = infrastructure.get_logger(__name__)


class FindROIInputs(vdp.StandardInputs):
    """Inputs for the hif_findroi stage."""

    processing_data_type = [
        DataType.SELFCAL_CONTLINE_SCIENCE,
        DataType.REGCAL_CONTLINE_SCIENCE,
        DataType.REGCAL_CONTLINE_ALL,
        DataType.RAW,
    ]

    timebin_sec = vdp.VisDependentProperty(default=240)
    min_nchan = vdp.VisDependentProperty(default=128)
    field = vdp.VisDependentProperty(default='target')
    spw = vdp.VisDependentProperty(default='')
    npix = vdp.VisDependentProperty(default=256)
    fov_pb_mult = vdp.VisDependentProperty(default=1.5)
    ref_sigma = vdp.VisDependentProperty(default=3.0)
    mom0_thresh_sigma = vdp.VisDependentProperty(default=5.0)
    gate_sigma = vdp.VisDependentProperty(default=1.0)
    pos_evidence_thr = vdp.VisDependentProperty(default=5.0)
    neg_evidence_thr = vdp.VisDependentProperty(default=7.0)
    evidence_thr = vdp.VisDependentProperty(default=None)
    evidence_bin_scales = vdp.VisDependentProperty(default=(1, 2, 3, 4, 5, 6, 8, 10, 20, 40, 60, 80))
    ref_zero_frac_thr = vdp.VisDependentProperty(default=0.05)
    mom0_zero_frac_thr = vdp.VisDependentProperty(default=0.05)
    ref_smooth_width = vdp.VisDependentProperty(default=4)
    mom0_smooth_width = vdp.VisDependentProperty(default=4)
    neg_extent_delta_thr = vdp.VisDependentProperty(default=5.0)
    neg_extent_trim_frac = vdp.VisDependentProperty(default=0.10)
    acf_edge_trim_frac = vdp.VisDependentProperty(default=0.10)
    acf_smooth_frac = vdp.VisDependentProperty(default=0.20)
    fwhm_global_fallback_factor = vdp.VisDependentProperty(default=5.0)
    fwhm_min_chan = vdp.VisDependentProperty(default=4)
    rolling_rms_window = vdp.VisDependentProperty(default=10)
    rolling_rms_target_unsmoothed = vdp.VisDependentProperty(default=0.87)
    rolling_rms_target_smoothed = vdp.VisDependentProperty(default=0.66)
    uv_taper_auto = vdp.VisDependentProperty(default=True)
    uv_taper_sigma_uv = vdp.VisDependentProperty(default=None)
    uv_taper_fwhm_cell = vdp.VisDependentProperty(default=2.0)
    roi_thresh = vdp.VisDependentProperty(default=7.0)
    roi_cont_thresh = vdp.VisDependentProperty(default=7.0)
    tmp_dir = vdp.VisDependentProperty(default='')
    tmp_overwrite = vdp.VisDependentProperty(default=True)
    save_moment0 = vdp.VisDependentProperty(default=True)
    save_cube = vdp.VisDependentProperty(default=False)
    save_results_path = vdp.VisDependentProperty(default=None)
    verbose = vdp.VisDependentProperty(default=True)
    parallel = sessionutils.parallel_inputs_impl()

    # docstring and type hints: supplements hif_findroi
    def __init__(
        self,
        context,
        output_dir=None,
        vis=None,
        field=None,
        spw=None,
        parallel=None,
        timebin_sec=None,
        min_nchan=None,
        npix=None,
        fov_pb_mult=None,
        ref_sigma=None,
        mom0_thresh_sigma=None,
        gate_sigma=None,
        pos_evidence_thr=None,
        neg_evidence_thr=None,
        evidence_thr=None,
        evidence_bin_scales=None,
        ref_zero_frac_thr=None,
        mom0_zero_frac_thr=None,
        ref_smooth_width=None,
        mom0_smooth_width=None,
        neg_extent_delta_thr=None,
        neg_extent_trim_frac=None,
        acf_edge_trim_frac=None,
        acf_smooth_frac=None,
        fwhm_global_fallback_factor=None,
        fwhm_min_chan=None,
        rolling_rms_window=None,
        rolling_rms_target_unsmoothed=None,
        rolling_rms_target_smoothed=None,
        uv_taper_auto=None,
        uv_taper_sigma_uv=None,
        uv_taper_fwhm_cell=None,
        roi_thresh=None,
        roi_cont_thresh=None,
        tmp_dir=None,
        tmp_overwrite=None,
        save_moment0=None,
        save_cube=None,
        save_results_path=None,
        verbose=None,
    ):
        super().__init__()
        self.context = context
        self.output_dir = output_dir
        self.vis = vis
        self.field = field
        self.spw = spw
        self.parallel = parallel
        self.timebin_sec = timebin_sec
        self.min_nchan = min_nchan
        self.npix = npix
        self.fov_pb_mult = fov_pb_mult
        self.ref_sigma = ref_sigma
        self.mom0_thresh_sigma = mom0_thresh_sigma
        self.gate_sigma = gate_sigma
        self.pos_evidence_thr = pos_evidence_thr
        self.neg_evidence_thr = neg_evidence_thr
        self.evidence_thr = evidence_thr
        self.evidence_bin_scales = evidence_bin_scales
        self.ref_zero_frac_thr = ref_zero_frac_thr
        self.mom0_zero_frac_thr = mom0_zero_frac_thr
        self.ref_smooth_width = ref_smooth_width
        self.mom0_smooth_width = mom0_smooth_width
        self.neg_extent_delta_thr = neg_extent_delta_thr
        self.neg_extent_trim_frac = neg_extent_trim_frac
        self.acf_edge_trim_frac = acf_edge_trim_frac
        self.acf_smooth_frac = acf_smooth_frac
        self.fwhm_global_fallback_factor = fwhm_global_fallback_factor
        self.fwhm_min_chan = fwhm_min_chan
        self.rolling_rms_window = rolling_rms_window
        self.rolling_rms_target_unsmoothed = rolling_rms_target_unsmoothed
        self.rolling_rms_target_smoothed = rolling_rms_target_smoothed
        self.uv_taper_auto = uv_taper_auto
        self.uv_taper_sigma_uv = uv_taper_sigma_uv
        self.uv_taper_fwhm_cell = uv_taper_fwhm_cell
        self.roi_thresh = roi_thresh
        self.roi_cont_thresh = roi_cont_thresh
        self.tmp_dir = tmp_dir
        self.tmp_overwrite = tmp_overwrite
        self.save_moment0 = save_moment0
        self.save_cube = save_cube
        self.save_results_path = save_results_path
        self.verbose = verbose


@task_registry.set_equivalent_casa_task('hif_findroi')
class FindROI(basetask.StandardTaskTemplate):
    Inputs = FindROIInputs

    is_multi_vis_task = True

    def prepare(self):
        inputs = self.inputs
        tmp_dir = heuristics.default_tmp_dir(inputs.context, inputs.output_dir, inputs.tmp_dir)
        LOG.info('Writing hif_findroi artifacts under %s', tmp_dir)

        stage_product = heuristics.run_findroi_mpi(
            vis=inputs.vis,
            context=inputs.context,
            executor=self._executor.copy(exclude_context=True),
            timebin_sec=inputs.timebin_sec,
            min_nchan=inputs.min_nchan,
            field=inputs.field,
            spw=inputs.spw,
            npix=inputs.npix,
            fov_pb_mult=inputs.fov_pb_mult,
            ref_sigma=inputs.ref_sigma,
            mom0_thresh_sigma=inputs.mom0_thresh_sigma,
            gate_sigma=inputs.gate_sigma,
            pos_evidence_thr=inputs.pos_evidence_thr,
            neg_evidence_thr=inputs.neg_evidence_thr,
            evidence_thr=inputs.evidence_thr,
            evidence_bin_scales=tuple(inputs.evidence_bin_scales),
            ref_zero_frac_thr=inputs.ref_zero_frac_thr,
            mom0_zero_frac_thr=inputs.mom0_zero_frac_thr,
            ref_smooth_width=inputs.ref_smooth_width,
            mom0_smooth_width=inputs.mom0_smooth_width,
            neg_extent_delta_thr=inputs.neg_extent_delta_thr,
            neg_extent_trim_frac=inputs.neg_extent_trim_frac,
            acf_edge_trim_frac=inputs.acf_edge_trim_frac,
            acf_smooth_frac=inputs.acf_smooth_frac,
            fwhm_global_fallback_factor=inputs.fwhm_global_fallback_factor,
            fwhm_min_chan=inputs.fwhm_min_chan,
            rolling_rms_window=inputs.rolling_rms_window,
            rolling_rms_target_unsmoothed=inputs.rolling_rms_target_unsmoothed,
            rolling_rms_target_smoothed=inputs.rolling_rms_target_smoothed,
            uv_taper_auto=inputs.uv_taper_auto,
            uv_taper_sigma_uv=inputs.uv_taper_sigma_uv,
            uv_taper_fwhm_cell=inputs.uv_taper_fwhm_cell,
            roi_thresh=inputs.roi_thresh,
            roi_cont_thresh=inputs.roi_cont_thresh,
            tmp_dir=tmp_dir,
            tmp_overwrite=inputs.tmp_overwrite,
            save_moment0=inputs.save_moment0,
            save_cube=inputs.save_cube,
            parallel=inputs.parallel,
            save_results_path=inputs.save_results_path,
            verbose=inputs.verbose,
        )

        if stage_product is None:
            return FindROIResult(errors=['No successful hif_findroi SPW results were produced.'])

        artifacts = copy.deepcopy(stage_product.get('metadata', {}).get('artifacts', {}))
        errors = list(stage_product.get('metadata', {}).get('errors', []))
        summary = heuristics.summarize_stage_product(stage_product)
        return FindROIResult(
            stage_product_path=artifacts.get('results_pickle'),
            artifacts=artifacts,
            summary=summary,
            errors=errors,
        )

    def analyse(self, result):
        return result
