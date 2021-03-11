VLASS-SE-CONT imaging workflow
------------------------------

This document describes the implementation of the workflow presented in VLASS Memo 15 in a 
CASA script to the pipeline codebase.

The three major peculiarities in the workflow: 
1) special masking procedure based on the external pyBDSF blob finding algorithm
2) with awproject gridder the manual computation of PSFs with the wabwp tclean parameter turned off using different CFCaches for the two cases
3) the use of self calibration.

### Imaging modes

The pipeline supports 3 VLASS-SE-CONT imaging modes (i.e. hif_editimlist parameter). The modes differ in gridder related tclean 
parameters. 

- VLASS-SE-CONT: default imaging mode
- VLASS-SE-CONT-AWP-032: at the moment same as default imaging mode, i.e. tclean parameters `gridder='awproject'`, `wprojplanes=32`.
  Essentially this implements the VLASS Memo 15 imaging workflow and configuration.
- VLASS-SE-CONT-AWP-001: alternative `gridder='awproject'` imaging mode. It differs from the default mode by setting 
`wprojplanes=1` in order to speed up imaging.
- An additional, `gridder='mosaic'` based mode is discussed, but as of Feb. 2021 no decision is reached.

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
- `user_cycleniter_final_image_nomask` property of the heuristics object (ImageParamsHeuristicsVlassSeCont) is set 
  according to the parameter list key `cycleniter_final_image_nomask`. The value is not None, then it is used instead of 
  the default cycleniter tclean parameter in the third imaging stage last clean interation (cleaning without used mask). 
  No other cycleniter values are affected. If cycleniter is set explicitly, then it will used in all tclean calls, 
  except the above mentioned final one. The value set for `user_cycleniter_final_image_nomask` is stored in the heuristics 
  object (ImageParamsHeuristicsVlassSeCont.user_cycleniter_final_image_nomask).
- `clean_no_mask_selfcal_image` if True, then perform a final clean in the first imaging stage without a user mask.

##### hifv_vlassmasking()

The task requires the `vlass_ql_database` parameter. If it is not defined, then the default 
`/home/vlass/packages/VLASS1Q.fits` is used (can be found on NMPOST machines). The mask name is constructed 
from the `context.clean_list_pending['imagename']` key with `".QLcatmask-tier1.mask"` string appended.

The details of how the constructed mask is added to `context.clean_list_pending['mask']` depends on the imaging mode and 
whether cleaning without mask (i.e. pbmask only) is used:

1. `maskingmode='vlass-se-tier-1'` and no cleaning without mask is requested, then `context.clean_list_pending['mask']` 
   is overwritten by the new mask name.
2. `maskingmode='vlass-se-tier-1'` and cleaning without mask is requested, then `context.clean_list_pending['mask']` 
   is a list with new mask name in first position and `'pb'` string in second (last) position.
3. `maskingmode='vlass-se-tier-2'`, no cleaning without mask is requested and `context.clean_list_pending['mask']` 
   is `''`, then key value is overwritten with new mask name. (Not used.)
4. `maskingmode='vlass-se-tier-2'`, cleaning without mask is requested (`context.clean_list_pending['mask']` 
   is `'pb'`), then list is returned with new mask in first place followed by `'pb'`. (Not used.)
5. `maskingmode='vlass-se-tier-2'`  and `context.clean_list_pending['mask']` is a list, then insert new mask name to 
   the second position. (this applies also when cleaning without mask is requested).

##### hif_makeimages()

The following changes are implemented for the VLASS-SE-CONT mode:

 - A new parameter, `clearlist` is added. If `False` then `context.clean_list_pending` is not emptied during 
merging hifv_makeimages result to context. This is not needed in the pipeline currently and can be omitted (TODO).
 - New `Tclean._do_iterative_vlass_se_imaging()` method implements the VLASS-SE-CONT imaging sequence in `awproject` 
mode. This method is invoked only if `image_heuristics.imaging_mode == 'VLASS-SE-CONT'`. The sequence incorporates the following steps: 
   - compute PSF without frequency dependent A-terms, i.e. `cfcache=cfcache_noawbp` and `wbawp=False` tclean parameters (via Tclean._do_clean() class method)
   - `iter0`: initialize clean using normal CFCache (`niter=0`, `calcpsf=True`, `calcres=True`)
   - replace `iter0` PSF with non-frequency dependent A-terms PSF
   - `iter1`: continue (clean with 1st mask)
        - tclean-1: clean with mask to nsigma and niter set by heuristics
        - tclean-2 (optional, only if `iter=1` and `vlass_stage=1`): save model to `modelcolumn` in single threaded mode.
   - `iter2` (optional: only if `vlass_stage=3`): same as `iter1`, but use second element of the mask list (see below).
   
Note that the `context.clean_list_pending['mask']` key is a string in imaging stages 2 (`vlass_stage=2`, Tier-1 mask), list in stage 3 
(`vlass_stage=3`, first element is Tier-1 mask, second element is the combined Tier-1 and Tier-2 mask, third element is `'pb'`), and either string 
or list in imaging stage 1 (`vlass_stage=1`), depending on the `clean_no_mask_selfcal_image` `hif_editimlist` parameter. 
If `clean_no_mask_selfcal_image=True` then the `'mask'` key value is a list in imaging stage 1: first element is the Tier-1 mask, second element is `'pb'`.


### Masking

See above description of hifv_vlassmasking() and hif_makeimages() tasks.

Obtaining and ordering masks for VLASS-SE-CONT is an overly complicated process that involves several tasks and pipeline 
stages.

Tasks involved: `hif_editimlist`, `hifv_vlassmasking`, `hif_tclean` (as called by `hif_makimages`). Furthermore, the `hif_editimlist` 
parameter input file might also contain the `clean_no_mask_selfcal_image` bool parameter, which control the final mask used for cleaning.

If `clean_no_mask_selfcal_image=True` then the `'pb'` placeholder is added to the mask list. This stands always on the last position (i.e. final mask), 
and always applied (regadless of parameter value in the third imaging stage).

When the mask is `'pb'`, then CASA tclean task is called with `mask=''`, `usemask='pb'`, `pbmask=0.4` and `cycleniter=100` parameters.