import shutil

import pytest

from pipeline.infrastructure import casa_tools
from tests.regression.regression_tester import RegressionTester


# 7m tests
@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.seven
def test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small__PPR__regression():
    """Run ALMA cal+image regression on a small test dataset with a PPR file.

    PPR:                        pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/PPR.xml
    Dataset:                    uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms
    """
    pr = RegressionTester(
        visname=['uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms'],
        ppr='pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/PPR.xml',
        input_dir='pl-unittest',
        expectedoutput_dir='pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/',
        )

    pr.run()


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.seven
def test_uid___A002_Xef72bb_X9d29__renorm_restore_procedure_hifa_image__regression():
    """Restore renorm from Cycle 8 (with current pipeline)

    Recipe name:                procedure_hifa_image
    Dataset:                    uid___A002_Xef72bb_X9d29
    """

    input_dir = 'pl-regressiontest/uid___A002_Xef72bb_X9d29'
    ref_directory = 'pl-regressiontest/uid___A002_Xef72bb_X9d29'

    pr = RegressionTester(
        visname=['uid___A002_Xef72bb_X9d29'],
        recipe='procedure_hifa_image.xml',
        input_dir=input_dir,
        expectedoutput_dir=ref_directory,
        )

    # copy files into products folder for restore
    if not pr.compare_only:
        input_products = casa_tools.utils.resolve(f'{input_dir}/products')
        shutil.copytree(input_products, f'{pr.output_dir}/rawdata')

    pr.run()


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.seven
def test_uid___A002_Xc845c0_X7366__cycle5_restore_procedure_hifa_image__regression():
    """
    Restore from Cycle 5 (with current pipeline)

    Recipe name:                procedure_hifa_image
    Dataset:                    uid___A002_Xc845c0_X7366
    """

    input_dir = 'pl-regressiontest/uid___A002_Xc845c0_X7366'
    ref_directory = 'pl-regressiontest/uid___A002_Xc845c0_X7366'

    pr = RegressionTester(
        visname=['uid___A002_Xc845c0_X7366'],
        recipe='procedure_hifa_image.xml',
        input_dir=input_dir,
        expectedoutput_dir=ref_directory,
        )

    # copy files for the restore into products folder
    if not pr.compare_only:
        input_products = casa_tools.utils.resolve(f'{input_dir}/products')
        shutil.copytree(input_products, f'{pr.output_dir}/rawdata')

    pr.run()


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.seven
def test_uid___A002_Xc46ab2_X15ae__selfcal_restore_procedure_hifa_image__regression():
    """Restore selfcal from Cycle 10 (with current pipeline)

    Recipe name:                procedure_hifa_image
    Dataset:                    uid___A002_Xc46ab2_X15ae
    """

    input_dir = 'pl-regressiontest/uid___A002_Xc46ab2_X15ae_selfcal_restore'
    ref_directory = 'pl-regressiontest/uid___A002_Xc46ab2_X15ae_selfcal_restore'

    pr = RegressionTester(
        visname=['uid___A002_Xc46ab2_X15ae'],
        recipe='procedure_hifa_image.xml',
        input_dir=input_dir,
        expectedoutput_dir=ref_directory,
        )

    # copy files into products folder for restore
    if not pr.compare_only:
        input_products = casa_tools.utils.resolve(f'{input_dir}/products')
        shutil.copytree(input_products, f'{pr.output_dir}/rawdata')

    pr.run()


# 12m tests
@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.twelve
def test_E2E6_1_00010_S__uid___A002_Xd0a588_X2239__procedure_hifa_image__regression():
    """Run ALMA cal+image regression on a 12m moderate-size test dataset in ASDM.

    Recipe name:                procedure_hifa_calimage
    Dataset:                    E2E6.1.00010.S: uid___A002_Xd0a588_X2239
    """

    input_dir = 'pl-regressiontest/E2E6.1.00010.S'
    ref_directory = 'pl-regressiontest/E2E6.1.00010.S'

    pr = RegressionTester(
        visname=['uid___A002_Xd0a588_X2239'],
        recipe='procedure_hifa_calimage.xml',
        input_dir=input_dir,
        expectedoutput_dir=ref_directory,
        )

    pr.run()


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.twelve
def test_csv_3899_eb2_small__procedure_hifa_calimage__regression():
    """PIPE-2245: Run small ALMA cal+image regression to cover various heuristics

    Dataset:                    CSV-3899-EB2-small
    """

    input_dir = 'pl-regressiontest/CSV-3899-EB2-small'

    pr = RegressionTester(
        visname=['uid___A002_X1181695_X1c6a4_8ant.ms'],
        recipe='procedure_hifa_calimage.xml',
        input_dir=input_dir,
        output_dir='csv_3899_eb2_small',
        expectedoutput_dir=input_dir,
        )

    pr.run(omp_num_threads=1)


@pytest.mark.skip(reason="Recent failure needs longer investigation")
@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.twelve
def test_uid___A002_Xee1eb6_Xc58d_original__procedure_hifa_calsurvey__regression():
    """Run ALMA cal+survey regression on a calibration survey test dataset

    Recipe name:                procedure_hifa_calsurvey
    Dataset:                    uid___A002_Xee1eb6_Xc58d_original.ms
    """
    input_directory = 'pl-regressiontest/uid___A002_Xee1eb6_Xc58d_calsurvey/'
    pr = RegressionTester(
        visname=['uid___A002_Xee1eb6_Xc58d_original.ms'],
        recipe='procedure_hifa_calsurvey.xml',
        input_dir=input_directory,
        output_dir='uid___A002_Xee1eb6_Xc58d_calsurvey_output',
        expectedoutput_dir=input_directory,
        )

    pr.run()
