VLASS-SE-CUBE imaging workflow
------------------------------

This documentation describes the VLASS [Coarse Cube Imaging Pipeline](https://open-confluence.nrao.edu/pages/viewpage.action?pageId=63897714) (CCIP) workflow.

In general, the CCIP workflow uses similar imaging heuristics as the VLASS-SE-CONT workflow, with two major differences:

1. The CCIP workflow skips several selfcal/masking-related stages in VLASS-SE-CONT, and replaces them with a single equivalent `hifv_restorepims()` stage that utilizes the products from VLASS-SE-CONT (e.g. reimaging_esource.tgz and tier1/tier2 masks).
   `hifv_restorepims()` restores the `flag`, `corrected`, and `weight` columns in a PIMS to the same state of the final imaging-ready MS in the VLASS-SE-CONT production run.
   
2. The imaging operation will go through individual specified SPW groups and generate/analyze/export full-Stokes images, using slightly less expensive algorithms (mosaic gridder, nterm=1). However, the per-iteration imaging masks and sequence of each spw group are the same as that used in the final imaging stage (`vlass_stage=3`) of VLASS-SE-CONT. 


## Task Changes

### hifv_restorepims

The task will perform the following operations:

 * Fill the model column in a PIMS using the selfcal model image from VLASS-SE-CONT

 * Restore flags from `${FSID}_split_split.ms.flagversions/statwt_1`

 * Re-run `statwt` using the same settings from the VLASS-SE-CONT production run

 * Reapply selfcal table

If any required resource is missing, Pipeline will throw an exception in the task and Pipeline will halt and exit. However, the weblog will still be rendered and the exception message from the weblog will help identify the missing file(s).

### hif_editimlist

### hif_makeimages

### hif_makermsimages

### hif_makecutoutimages

### hifv_analyzestokescubes

### hifv_exportvlassdata


## Workflow Summary


