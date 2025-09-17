
## Testing Procedures

* Examples of SRDP JSON files for creating custom recipes
  * `test_hifa_cubeimage.json` # uid___A001_X1467_X256
  * `test_hifv_contimage.json`

* SRDP-like procedures: no restoration / target-split / findcont for speeded-up testing; needs to be used with *_targets.ms, along with cont.dat
  * `test_procedure_hifa_image.xml` (with selfcal)
  * `test_procedure_hifv_contimage.xml` (with selfcal)
  * `test_procedure_hifv_cubeimage.xml` (without selfcal)

* Testing procedures primarily designed to test the `hif_selfcal` task from target-only calibrated data.
  * `test_procedure_hifv_selfcal*.xml`
  * `test_procedure_hifa_selfcal*.xml`

* Testing recipes in Python implementations, demonstrating the Pipeline CLI task interface.
  * `test_hifv_contimage.py` - Python equivalent of `procedure_hifv_contimage.xml` (might be outdated)
  * `test_hifv_calimage_cont.py` - Python workflow equivalent to `procedure_hifv_calimage_cont.xml` (might be outdated)
  * `test_hifv.py` - Python workflow equivalent to `procedure_hifv.xml` (might be outdated)
