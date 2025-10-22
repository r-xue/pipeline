import shutil

import pytest

from pipeline.infrastructure import casa_tools
from tests.regression.regression_tester import PipelineRegression


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.sd
def test_uid___A002_X85c183_X36f__procedure_hsd_calimage__regression():
    """Run ALMA single-dish cal+image regression on the obseration data of M100.

    Recipe name:                procedure_hsd_calimage
    Dataset:                    uid___A002_X85c183_X36f
    """
    pr = PipelineRegression(
        visname=['uid___A002_X85c183_X36f'],
        recipe='procedure_hsd_calimage.xml',
        input_dir='pl-regressiontest/uid___A002_X85c183_X36f',
        expectedoutput_dir=('pl-regressiontest/uid___A002_X85c183_X36f')
        )

    pr.run()


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.sd
def test_uid___A002_X85c183_X36f_SPW15_23__PPR__regression():
    """Run ALMA single-dish restoredata regression on the observation data of M100.

    Dataset:                    uid___A002_X85c183_X36f_SPW15_23
    """
    input_dir = 'pl-regressiontest/uid___A002_X85c183_X36f_SPW15_23'
    pr = PipelineRegression(
        visname=['uid___A002_X85c183_X36f_SPW15_23.ms'],
        ppr=f'{input_dir}/PPR.xml',
        input_dir=input_dir,
        expectedoutput_dir=('pl-regressiontest/uid___A002_X85c183_X36f_SPW15_23')
        )

    # copy files use restore task into products folder
    if not pr.compare_only:
        input_products = casa_tools.utils.resolve(f'{input_dir}/products')
        shutil.copytree(input_products, f'{pr.output_dir}/products')

    pr.run()


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.sd
def test_uid___mg2_20170525142607_180419__procedure_hsdn_calimage__regression():
    """Run ALMA single-dish cal+image regression for standard nobeyama recipe.

    Recipe name:                procedure_hsdn_calimage
    Dataset:                    mg2-20170525142607-180419
    """
    pr = PipelineRegression(
        visname=['mg2-20170525142607-180419.ms'],
        recipe='procedure_hsdn_calimage.xml',
        input_dir='pl-regressiontest/mg2-20170525142607-180419',
        expectedoutput_file=('pl-regressiontest/mg2-20170525142607-180419/' +
                             'mg2-20170525142607-180419.casa-6.6.6-16-pipeline-2025.0.2.7.results.txt'))
    pr.run()


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.alma
@pytest.mark.sd
def test_uid___mg2_20170525142607_180419__PPR__regression():
    """Run ALMA single-dish cal+image regression for restore nobeyama recipe.

    Dataset:                    mg2-20170525142607-180419
    """

    input_dir = 'pl-regressiontest/mg2-20170525142607-180419'

    pr = PipelineRegression(
        visname=['mg2-20170525142607-180419.ms'],
        ppr=f'{input_dir}/PPR.xml',
        input_dir=input_dir,
        expectedoutput_file=(f'{input_dir}/' +
                             'mg2-20170525142607-180419_PPR.casa-6.6.6-16-pipeline-2025.0.1.18.results.txt'),
        output_dir='mg2-20170525142607-180419_PPR')

    # copy files use restore task into products folder
    if not pr.compare_only:
        input_products = casa_tools.utils.resolve(f'{input_dir}/products')
        shutil.copytree(input_products, f'{pr.output_dir}/products')

    pr.run()
