import os
import shutil

import pytest

from pipeline.infrastructure import casa_tools
from tests.regression.regression_tester import PipelineRegression


@pytest.mark.regression
@pytest.mark.fast
@pytest.mark.vlass
def test_vlass_quicklook():
    """Run VLASS quicklook regression

    Recipe name: procedure_vlassQLIP.xml
    Dataset: TSKY0001.sb32295801.eb32296475.57549.31722762731_split_withcorrectdata.ms
    """

    input_dir = 'pl-regressiontest/vlass_quicklook'
    ref_directory = 'pl-regressiontest/vlass_quicklook'

    pr = PipelineRegression(
        visname=['TSKY0001.sb32295801.eb32296475.57549.31722762731_split_withcorrectdata.ms'],
        recipe='procedure_vlassQLIP.xml',
        input_dir=input_dir,
        expectedoutput_dir=ref_directory
        )

    # Copy parameter list file into the working directory

    try:
        os.mkdir(f'{pr.output_dir}/working/')
    except FileExistsError:
        pass
    if not pr.compare_only:
        parameter_list_file = casa_tools.utils.resolve(
            f'{input_dir}/TSKY0001.sb32295801.eb32296475.57549.31722762731_split_QLIP_parameter.list')
        shutil.copyfile(parameter_list_file, casa_tools.utils.resolve(f'{pr.output_dir}/working/QLIP_parameter.list'))
    pr.run(telescope='vla')
