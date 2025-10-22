import pytest

from tests.regression.regression_tester import PipelineRegression, setup_flux_antennapos


@pytest.mark.regression
@pytest.mark.slow
@pytest.mark.alma
@pytest.mark.sd
def test_2019_2_00093_S__uid___A002_Xe850fb_X2df8_regression(data_directory):
    """Run longer regression test on this ALMA SD dataset

    Recipe name: procedure_hsd_calimage
    Dataset: 2019.2.00093.S: uid___A002_Xe850fb_X2df8, uid___A002_Xe850fb_X36e4, uid___A002_Xe850fb_X11e13
    """
    test_directory = f'{data_directory}/alma_sd/2019.2.00093.S/'
    ref_directory = 'pl-regressiontest/2019.2.00093.S/'

    pr = PipelineRegression(
        visname=['uid___A002_Xe850fb_X2df8', 'uid___A002_Xe850fb_X36e4', 'uid___A002_Xe850fb_X11e13'],
        recipe='procedure_hsd_calimage.xml',
        input_dir=test_directory,
        project_id="2019_2_00093_S",
        expectedoutput_dir=ref_directory
        )

    pr.run()

@pytest.mark.regression
@pytest.mark.slow
@pytest.mark.alma
@pytest.mark.sd
def test_2019_1_01056_S__uid___A002_Xe1d2cb_X110f1_regression(data_directory):
    """Run weekly regression test on this ALMA SD dataset

    Recipe name: procedure_hsd_calimage
    Dataset: 2019.1.01056.S: uid___A002_Xe1d2cb_X110f1, uid___A002_Xe1d2cb_X11d0a, uid___A002_Xe1f219_X6eeb
    """
    test_directory = f'{data_directory}/alma_sd/2019.1.01056.S/'
    ref_directory = 'pl-regressiontest/2019.1.01056.S/'

    pr = PipelineRegression(
        visname=['uid___A002_Xe1d2cb_X110f1', 'uid___A002_Xe1d2cb_X11d0a', 'uid___A002_Xe1f219_X6eeb'], 
        recipe='procedure_hsd_calimage.xml',
        input_dir=test_directory,
        project_id="2019_1_01056_S",
        expectedoutput_dir=f'{ref_directory}'
        )

    setup_flux_antennapos(test_directory, pr.output_dir)
    pr.run()

@pytest.mark.regression
@pytest.mark.slow
@pytest.mark.alma
@pytest.mark.sd
def test_2016_1_01489_T__uid___A002_Xbadc30_X43ee_regression(data_directory):
    """Run weekly regression test on this ALMA SD dataset

    Recipe name: procedure_hsd_calimage
    Dataset: 2016.1.01489.T: uid___A002_Xbadc30_X43ee, uid___A002_Xbaedce_X7694
    """
    test_directory = f'{data_directory}/alma_sd/2016.1.01489.T/'
    ref_directory = 'pl-regressiontest/2016.1.01489.T/'

    pr = PipelineRegression(
        visname=['uid___A002_Xbadc30_X43ee', 'uid___A002_Xbaedce_X7694'],
        recipe='procedure_hsd_calimage.xml',
        input_dir=test_directory,
        project_id="2016_1_01489_T",
        expectedoutput_dir=ref_directory
        )

    pr.run()
