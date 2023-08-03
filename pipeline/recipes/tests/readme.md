
## Testing Prrocedures

* Examples of SRDP Json files for creating custom recipes
  * `test_hifa_cubeimage.json` # uid___A001_X1467_X256
  * `test_hifv_contimage.json`

* SRDP-like procedures:
    no data restorations/targetsplit; cube imaging created from `hif_makeimlist` rather than `hif_editimlist`
  * `test_procedure_hifa_image.xml`
  * `test_procedure_hifv_contimage.xml`

* Testing procedures for only testing the `hif_selfcal` task
  * `test_procedure_hifv_selfcal.xml`
  * `test_procedure_hifa_selfcal.xml`

* A standard "calimage" workflow with `hif_selfcal` stage

  * `test_procedure_hifa_calimage.xml`
