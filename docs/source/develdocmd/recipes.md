# Pipeline Recipes

| Project, Procedure/Template | Description | NRAO-JIRA Ticket |
|----------|----------|----------|
| ***VLA*** | | |
| procedure_hifv.xml | VLA PI calibration | N/A |
| procedure_hifv_contimage.xml | VLA PI user defined target continuum imaging. Input is a calibrated MS. | PIPE-784 |
| procedure_hifv_contimage_selfcal.xml | VLA PI continuum imaging with selfcal. Input is a calibrated MS | PIPE-1952 |
| procedure_hifv_calimage_cont.xml | VLA PI calibration + user defined target continuum imaging | PIPE-783 |
| procedure_hifv_calimage_cont_selfcal.xml | VLA PI calibration + target continuum imaging with selfcal	| PIPE-1952 |
| procedure_hifv_calimage_cont_cube.xml | VLA PI calibration, target continuum imaging, and cube imaging | PIPE-1344 |
| procedure_hifv_calimage_cont_cube_selfcal.xml | VLA PI calibration, target continuum imaging, and cube imaging with selfcal |PIPE-2296 |
| procedure_hifv_cont_cube_image.xml | VLA PI  target continuum imaging and cube imaging. Input is a calibrated MS. | PIPE-2295 |
| procedure_hifv_cont_cube_image_selfcal.xml | VLA PI  target continuum imaging and cube imaging with selfcal. Input is a calibrated MS. | PIPE-2295 |
| template_hifv_cubeimage.xml | SRDP template recipe for user-defined cube imaging | PIPE-2277 |
| template_hifv_contimage.xml | VLA PI recipe for user defined target continuum imaging. Input is a calibrated MS. Archive template version. | PIPE-731 |
| template_hifv_deliver_ms.xml | SRDP template recipe for user requested MS delivery via SSA interface | PIPE-72 |
| ***VLASS*** | | |
| procedure_hifvcalvlass.xml | VLASS calibration | CAS-9625 |
| procedure_hifvcalvlass_compression.xml | alternate VLASS calibration when compression is needed | PIPE-506 |
| procedure_vlassQLIP.xml | VLASS Quick Look Imaging | CAS-9631 |
| procedure_vlassSEIP.xml | VLASS Single Epoch Continuum Imaging | PIPE-718 |
| procedure_vlassCCIP.xml | VLASS Coarse Cube Imaging | PIPE-1357 |
| ***ALMA-IF*** | | |
| procedure_hifa_cal.xml | ALMA interferometric calibration | PIPE-1590 |
| procedure_hifa_image.xml | ALMA interferometric target imaging | N/A |
| procedure_hifa_calimage.xml | ALMA interferometric calibration + target imaging | PIPE-1590 |
| procedure_hifa_calsurvey.xml | ALMA interferometric calibrator survey processing (data usually needs editing intents to add TARGET and set that in SBSummary) | PIPE-1590 |
| procedure_hifa_polcal.xml | ALMA polarization interferometric calibration | PIPE-1776 |
| procedure_hifa_polcalimage.xml | ALMA polarization interferometric calibration + target imaging | PIPE-1776 |
| procedure_hifa_polcal_totalintensity.xml | "Polarization friendly" ALMA interferometric calibration | PIPE-606 / PIPE-1978 |
| procedure_hifa_polcalimage_totalintensity.xml | "Polarization friendly" ALMA interferometric calibration + target imaging (not used regularly in operations in C7,8 but good to test periodically because we do offer it as supported) | PIPE-606 / PIPE-1978 |
| procedure_hifa_cal_diffgain.xml | ALMA interferometric calibration for differential gain mode observations (Band-to-Band or Bandwidth Switching) | PIPE-2079 / PIPE-2098 |
| procedure_hifa_calimage_diffgain.xml | ALMA interferometric calibration for differential gain mode observations (Band-to-Band or Bandwidth Switching) + target imaging | PIPE-2079 / PIPE-2098 |
| template_hifa_deliver_ms.xml | SRDP template recipe for user requested MS delivery via SSA interface | PIPE-72 |
| template_hifa_cubeimage.xml | SRDP template recipe to support ALMA optimized cube generation via SSA interface | PIPE-2
| ***ALMA-SD*** | | |
| procedure_hsd_calimage.xml | ALMA Single Dish calibration + target imaging | N/A |
| ***Nobeyama-SD*** | | |
| procedure_hsdn_calimage.xml | Nobeyama Single Dish calibration + target imaging | CAS-10763 |
