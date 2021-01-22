VLASS-SE-CONT imaging workflow
------------------------------

This document describes the implementation of the workflow presented in VLASS Memo 15 in a 
CASA script to the pipeline codebase.

The three major peculiarities in the workflow: 
1) special masking procedure based on the external pyBDSF blob finding algorithm
2) with awproject gridder the manual computation of PSFs with the wabwp tclean parameter turned off using different CFCaches for the two cases
3) the use of self calibration.

### Task level workflow

The Memo 15 workflow consists of 3 imaging steps: 1) compute image (with Tier-1 mask, derived from QL database) that is 
used for self-calibration, 2) image self calibrated data column and use the result for creating the Tier-2 mask, and 3) 
compute the final image using self-calibrated data column, Tier-1 mask and combined Tier-1 and Tier-2 masks.

The pipeline parameters are controlled via a parameter file, which is interpreted by the `hif_editimlist` task. An example follows.

    # SEIP_parameter.list
    imagename='VLASS1.2.se.Txxxx.J133335+164400.v1'
    phasecenter='J2000 13:33:35.814 +16.44.04.255'
    imaging_mode='VLASS-SE-CONT'
    imsize=[1024, 1024]
    cell=['2.0arcsec']
    cfcache='/lustre/aoc/projects/vlass/cfcache/cfcache_spw2-17_smaller_imsize1024_cell2.0arcsec_w32_conjT.cf, /lustre/aoc/projects/vlass/cfcache/cfcache_spw2-17_smaller_imsize1024_cell2.0arcsec_w32_conjT_psf_wbawp_False.cf'
    pblimit=0.02
    niter=50

The strictly necessary parameters are `imaging_mode`, `imagename`, `phasecenter` and `cfcache` (note that it is a two 
element list, appropriate caches need to be used for the selected imsize). The other parameters are determined by 
heuristics and in the example they are set manually in order to speed up computation (in exchange for reduced fidelity).

Note: the CFCaches should not be used from network storage drives (e.g. lustre) because it may lead slow 
and blocked file access. To circumvent the issue, CFCaches are provided from local disk storage on the NMPOST
cluster.

The imaging steps include a `hif_editimlist`, an optional `hifv_vlassmasking` and a `hif_makeimages` task call:

    # First imaging step
    hif_editimlist(parameter_file='SEIP_parameter.list')
    hifv_vlassmasking(maskingmode='vlass-se-tier-1')
    hif_makeimages( hm_masking='manual')

    # Self calibration steps

    # Second imaging cycle (after self-cal)
    hif_editimlist(parameter_file='SEIP_parameter.list')
    hif_makeimages( hm_masking='manual')

    # Final imaging step
    hif_editimlist(parameter_file='SEIP_parameter.list')
    hifv_vlassmasking(maskingmode='vlass-se-tier-2')
    hif_makeimages( hm_masking='manual')

##### hif_editimlist()

The `context.clean_list_pending` list is filled with a single imaging target (`imlist_entry`). The following changes occur compared to the 
normal task operation:

 - `"sSTAGENUMBER"` string is prepended to imagename parameter value, this will be replaced by stage numbers
 - `vlass_stage` property of the heuristics object (ImageParamsHeuristicsVlassSeCont) is set by method 
   `EditimlistInputs._get_task_stage_ordinal()`. This counts the MakeImagesResult instances in context.result list (n)
   and returns n+1. The imaging heuristics is determined by the `vlass_stage` property together with 
   the `iteration` number during imaging.
 - `imlist_entry['cfcache_nowb']` key is added in addition to the normal `imlist_entry['cfcache']` key.
   The heuristics method `get_cfcaches()` is used to parse the string provided in the parameter file to 
   these two keys. Other modes than VLASS-CONT-SE always set`cfcache_nowb=None`.

##### hifv_vlassmasking()

The task requires the `vlass_ql_database` parameter. If it is not defined, then the default 
`/home/vlass/packages/VLASS1Q.fits` is used (can be found on NMPOST machines). The mask name is constructed 
from the `context.clean_list_pending['imagename']` key with `".QLcatmask-tier1.mask"` string appended.

When merging mask results to context, the `context.clean_list_pending['mask']` key is overwritten with the 
newly constructed mask name. Therefore, the next `hif_makeimages` task call will  use this mask.

##### hif_makeimages()

The following changes are implemented for the VLASS-SE-CONT mode:

 - A new parameter, `clearlist` is added. If `False` then `context.clean_list_pending` is not emptied during 
merging hifv_makeimages result to context. This is not needed in the pipeline currently and can be omitted (TODO).
 - New `Tclean._do_iterative_vlass_se_imaging()` method implements the VLASS-SE-CONT imaging sequence in `awproject` 
mode. This method is invoked only if `image_heuristics.imaging_mode == 'VLASS-SE-CONT'`. The sequence incorporates the following steps: 
   - compute PSF without frequency dependent A-terms, i.e. `cfcache=cfcache_noawbp` and `wbawp=False` tclean parameters (via Tclean._do_clean() class method)
   - `iter0`: initialize clean using normal CFCache (`niter=0`, `calcpsf=True`, `calcres=True`)
   - replace `iter0` PSF with non-frequency dependent A-terms PSF
   - `iter1`: continue
        - tclean-1: clean with mask to nsigma and niter set by heuristics
        - tclean-2 (optional, only if `iter=1` and `vlass_stage=1`): save model to `modelcolumn` in single threaded mode.
   - `iter2` (optional: only if `vlass_stage=3`): same as `iter1`, but use second element of the mask list (see below).
   
Note that the context.clean_list_pending['mask'] key is a string in imaging stages 1 and 2 (`vlass_stage in [1,2]`) 
and list in stage 3 (`vlass_stage=3`), where the first element is the Tier-1 mask, the second element is the 
combined Tier-1 and Tier-2 mask.

The final image is the `vlass_stage=3` `iter=2` image product.