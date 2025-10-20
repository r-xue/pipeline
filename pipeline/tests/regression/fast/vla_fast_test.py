import shutil

import pytest

from pipeline.infrastructure import casa_tools
from pipeline.tests.regression.regression_tester import PipelineRegression


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.vla
def test_13A_537__procedure_hifv__regression():
    """Run VLA calibration regression for standard procedure_hifv.xml recipe.

    Recipe name:                procedure_hifv
    Dataset:                    13A-537/13A-537.sb24066356.eb24324502.56514.05971091435
    """

    input_dir = 'pl-regressiontest/13A-537'
    pr = PipelineRegression(
        visname=['13A-537.sb24066356.eb24324502.56514.05971091435'],
        recipe='procedure_hifv.xml',
        input_dir=input_dir,
        expectedoutput_dir=input_dir,
        output_dir='13A_537__procedure_hifv__regression'
        )

    pr.run(telescope='vla', omp_num_threads=1)


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.vla
def test_13A_537__calibration__PPR__regression():
    """Run VLA calibration regression with a PPR file.

    PPR name:                   PPR_13A-537.xml
    Dataset:                    13A-537/13A-537.sb24066356.eb24324502.56514.05971091435
    """

    input_dir = 'pl-regressiontest/13A-537'

    pr = PipelineRegression(
        visname=['13A-537.sb24066356.eb24324502.56514.05971091435'],
        ppr=f'{input_dir}/PPR_13A-537.xml',
        input_dir=input_dir,
        expectedoutput_dir=input_dir,
        output_dir='13A_537__calibration__PPR__regression'
        )

    pr.run(telescope='vla', omp_num_threads=1)


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.vla
def test_13A_537__restore__PPR__regression():
    """Run VLA calibration restoredata regression with a PPR file
    NOTE: results file frozen to CASA/Pipeline version below since products were created
    with that Pipeline version

    PPR name:                   PPR_13A-537_restore.xml
    Dataset:                    13A-537/13A-537.sb24066356.eb24324502.56514.05971091435
    Expected results version:   casa-6.2.1.7-pipeline-2021.2.0.128
    """
    input_dir = 'pl-regressiontest/13A-537'
    pr = PipelineRegression(
        visname=['13A-537.sb24066356.eb24324502.56514.05971091435'],
        ppr=f'{input_dir}/PPR_13A-537_restore.xml',
        input_dir=input_dir,
        expectedoutput_file=f'{input_dir}/restore/' +
                             '13A-537.casa-6.2.1.7-pipeline-2021.2.0.128.restore.results.txt',
        output_dir='13A_537__restore__PPR__regression'
        )

    # copy files use restore task into products folder
    if not pr.compare_only:
        input_products = casa_tools.utils.resolve(f'{input_dir}/products')
        shutil.copytree(input_products, f'{pr.output_dir}/products')

    pr.run(telescope='vla')


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.vla
def test_13A_537__restore__post1553__PPR__regression():
    """Run VLA calibration restoredata regression with a PPR file

    PPR name:                   PPR_13A-537_restore.xml
    Dataset:                    13A-537/13A-537.sb24066356.eb24324502.56514.05971091435
    """
    input_dir = 'pl-regressiontest/13A-537'
    pr = PipelineRegression(
        visname=['13A-537.sb24066356.eb24324502.56514.05971091435'],
        ppr=f'{input_dir}/PPR_13A-537_restore.xml',
        input_dir=input_dir,
        expectedoutput_dir=f'{input_dir}/restore/',
        output_dir='13A_537__restore__post1553__PPR__regression'
        )

    # copy files use restore task into products folder
    if not pr.compare_only:
        input_products = casa_tools.utils.resolve(f'{input_dir}/post1553_products')
        shutil.copytree(input_products, f'{pr.output_dir}/products')

    pr.run(telescope='vla')
