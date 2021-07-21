"""
Pipeline Regression framework.

PipelineRegression class runs on pytest framework, so it needs to implement
test_* methods for testing. 
"""

import os
import shutil
import pytest
from typing import Tuple, Optional, List

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.recipereducer
from pipeline.infrastructure.renderer import regression
import pipeline.infrastructure.executeppr as almappr
import pipeline.infrastructure.executevlappr as vlappr

LOG = infrastructure.get_logger(__name__)


class PipelineRegression(object):
    """Pipeline regression test class called from pytest."""

    def __init__(self, recipe: str, input_dir: str, visname: str, expectedoutput: str, output_dir=None):
        """Constractor of PilelineRegression.
        
        Args:
            recipe: recipe XML file name
            input_dir: path to directory contains input files
            visname: name of MeadurementSet
            expectedoutput: path to a file that defines expected output of a test
            output_dir: path to directory to output. If None, it sets visname
        """
        self.recipe = recipe
        self.input_dir = input_dir
        self.visname = visname
        self.expectedoutput = expectedoutput
        self.testinput = f'{input_dir}/{visname}'
        self.current_path = os.getcwd()
        self.output_dir = output_dir if output_dir else visname
        self.__initialize_working_folder()

    def __initialize_working_folder(self):
        """Initialize a root folder for task execution."""
        if os.path.isdir(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.mkdir(self.output_dir)
        os.chdir(self.output_dir)

    def __sanitize_regression_string(self, instring: str) -> Tuple:
        """Sanitize to get numeric values, remove newline chars and change to float.

        Args:
            instring: input string
        Returns:
            tuple(key, value, optional tolerance)
        """
        keyval = instring.split(':::')[0]

        keystring = keyval.split('=')[0]
        value = float(keyval.split('=')[-1])
        try:
            tolerance = float(instring.split(':::')[-1].replace('\n', ''))
        except ValueError:
            tolerance = None

        return keystring, value, tolerance

    def run(self, ppr: Optional[str] = None, telescope: str = 'alma', default_relative_tolerance: float = 1e-7):
        """
        Run test with PPR if supplied or recipereducer if no PPR and compared to expected results.

        The inputs and expectd output are usually found in the pipeline data repository.

        Args:
            ppr: PPR file name
            telescope: string 'alma' or 'vla'
            default_relative_tolerance: default relative tolerance of output value
        """
        # Run the Pipeline using cal+imag ALMA IF recipe
        # set datapath in ~/.casa/config.py, e.g. datapath = ['/users/jmasters/pl-testdata.git']
        input_vis = casa_tools.utils.resolve(self.testinput)

        try:
            # run the pipeline for new results
            if ppr:
                self.__run_ppr(input_vis, ppr, telescope)
            else:
                self.__run_reducer(input_vis)

            # Get new results
            new_results = self.__get_results_of_from_current_context()

            # new results file path
            new_file = f'{self.visname}.NEW.results.txt'

            # Store new results in a file
            self.__save_new_results_to(new_file, new_results)

            # Compare new results with expected results
            self.__compare_results(new_file, default_relative_tolerance)
        finally:
            os.chdir(self.current_path)

    def __compare_results(self, new_file: str, relative_tolerance: float):
        """
        Compare results between new one loaded from file and old one.

        Args:
            new_file : file path of new results
            relative_tolerance : relative tolerance of output value
        """
        expected = casa_tools.utils.resolve(self.expectedoutput)
        with open(expected) as expected_fd, open(new_file) as new_fd:
            expected_results = expected_fd.readlines()
            new_results = new_fd.readlines()
            errors = []
            for old, new in zip(expected_results, new_results):
                oldkey, oldval, tol = self.__sanitize_regression_string(old)
                newkey, newval, _ = self.__sanitize_regression_string(new)
                assert oldkey == newkey
                tolerance = tol if tol else relative_tolerance
                LOG.info(f'Comparing {oldval} to {newval} with a rel. tolerance of {tolerance}')
                if oldval != pytest.approx(newval, rel=tolerance):
                    errorstr = f"{oldkey}\n\tvalues differ by > a relative difference of {tolerance}\n\texpected: {oldval}\n\tnew:      {newval}"
                    errors.append(errorstr)
            [LOG.warning(x) for x in errors]
            assert not errors

    def __save_new_results_to(self, new_file: str, new_results: List[str]):
        """
        Compare results between new one and old one, both results are loaded from specified files.

        Args:
            new_file : file path of new results to save
            new_results : List[str] of new results
        """
        with open(new_file, 'w') as fd:
            fd.writelines([str(x) + '\n' for x in new_results])

    def __get_results_of_from_current_context(self) -> List[str]:
        """
        Get results of current execution from context.

        Returns: List[str] of new results
        """
        context = pipeline.Pipeline(context='last').context
        new_results = sorted(regression.extract_regression_results(context))
        return new_results

    def __run_ppr(self, input_vis: str, ppr: str, telescope: str):
        """
        Execute the recipe defined by PPR.

        Args:
            input_vis : MS name
            ppr : PPR file name
            telescope : string 'alma' or 'vla'
        """
        for dd in ('rawdata', 'products', 'working'):
            try:
                os.mkdir(dd)
            except FileExistsError:
                LOG.warning(f"Directory \'{dd} exists.  Continuing")
        ppr_path = casa_tools.utils.resolve(ppr)
        shutil.copyfile(ppr_path, os.path.basename(ppr_path))
        os.symlink(input_vis, f'rawdata/{os.path.basename(input_vis)}')
        os.chdir('working')
        os.environ['SCIPIPE_ROOTDIR'] = os.getcwd()
        if telescope is 'alma':
            almappr.executeppr(f'../{os.path.basename(ppr_path)}', importonly=False)
        elif telescope is 'vla':
            vlappr.executeppr(f'../{os.path.basename(ppr_path)}', importonly=False)
        else:
            LOG.error("Telescope is not 'alma' or 'vla'.  Can't run executeppr.")

    def __run_reducer(self, input_vis: str):
        """
        Execute the recipe by recipereducer.

        Args:
            input_vis : MS name
        """
        LOG.warning("Running without Pipeline Processing Request (PPR).  Using recipereducer instead.")
        try:
            os.mkdir('working')
        except FileExistsError:
            LOG.warning(f"Directory working exists.  Continuing")
        os.chdir('working')
        pipeline.recipereducer.reduce(vis=[input_vis], procedure=self.recipe)

# The methods below are test methods called from pytest.

# def test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small__procedure_hifa_calimage__regression():
#     """Run ALMA cal+image regression on a small test dataset.
#
#     Recipe name:                procedure_hifa_calimage
#     Dataset:                    uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms
#     Expected results version:   casa-6.1.1-15-pipeline-2020.1.0.40
#     """
#     pr = PipelineRegression(recipe='procedure_hifa_calimage.xml',
#                             input_dir='pl-unittest',
#                             visname='uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms',
#                             expectedoutput=('pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/' +
#                                             'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.casa-6.1.1-15-pipeline-2020.1.0.40.results.txt'))
#
#     pr.run(ppr='pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/PPR.xml')
#
#
# def test_uid___A002_X85c183_X36f__procedure_hsd_calimage__regression():
#     """Run ALMA single-dish cal+image regression on the obseration data of M100.
#
#     Recipe name:                procedure_hsd_calimage
#     Dataset:                    uid___A002_X85c183_X36f
#     Expected results version:   casa-6.2.0-119-pipeline-2020.2.0.23
#     """
#     pr = PipelineRegression(recipe='procedure_hsd_calimage.xml',
#                             input_dir='pl-regressiontest/uid___A002_X85c183_X36f',
#                             visname='uid___A002_X85c183_X36f',
#                             expectedoutput=('pl-regressiontest/uid___A002_X85c183_X36f/' +
#                                             'uid___A002_X85c183_X36f.casa-6.2.0-119-pipeline-2021.2.0.23.results.txt'))
#
#     pr.run()
#
#
# def test_uid___A002_X85c183_X36f_SPW15_23_procedure_hsd_restoredata__regression():
#     """Run ALMA single-dish restoredata regression on the observation data of M100.
#
#     Recipe name:                procedure_hsd_calimage
#     Dataset:                    uid___A002_X85c183_X36f_SPW15_23
#     Expected results version:   casa-6.2.0-119-pipeline-2020.2.0.23
#     """
#     input_dir = 'pl-regressiontest/uid___A002_X85c183_X36f_SPW15_23'
#     pr = PipelineRegression(recipe='procedure_hsd_calimage.xml',
#                             input_dir=input_dir,
#                             visname='uid___A002_X85c183_X36f_SPW15_23.ms',
#                             expectedoutput=('pl-regressiontest/uid___A002_X85c183_X36f_SPW15_23/' +
#                                             'uid___A002_X85c183_X36f_SPW15_23.casa-6.2.0-119-pipeline-2021.2.0.23.results.txt'))
#
#     # copy files use restore task into products folder
#     input_products = casa_tools.utils.resolve(f'{input_dir}/products')
#     shutil.copytree(input_products, './products')
#
#     pr.run(ppr='pl-regressiontest/uid___A002_X85c183_X36f_SPW15_23/PPR.xml')
#
#
# def test_uid___mg2_20170525142607_180419__procedure_hsdn_calimage__regression():
#     """Run ALMA single-dish cal+image regression for standard nobeyama recipe.
#
#     Recipe name:                procedure_hsdn_calimage
#     Dataset:                    mg2-20170525142607-180419
#     Expected results version:   casa-6.2.0-119-pipeline-2020.2.0.23
#     """
#     pr = PipelineRegression(recipe='procedure_hsdn_calimage.xml',
#                             input_dir='pl-regressiontest/mg2-20170525142607-180419',
#                             visname='mg2-20170525142607-180419.ms',
#                             expectedoutput=('pl-regressiontest/mg2-20170525142607-180419/' +
#                                             'mg2-20170525142607-180419.casa-6.2.0-119-pipeline-2021.2.0.23.results.txt'))
#     pr.run()
#

def test_uid___mg2_20170525142607_180419_PPR__procedure_hsdn_calimage__regression():
    """Run ALMA single-dish cal+image regression for restore nobeyama recipe.

    Recipe name:                procedure_hsdn_calimage
    Dataset:                    mg2-20170525142607-180419
    Expected results version:   casa-6.2.0-119-pipeline-2020.2.0.23
    """

    input_dir = 'pl-regressiontest/mg2-20170525142607-180419'

    pr = PipelineRegression(recipe='procedure_hsdn_calimage.xml',
                            input_dir=input_dir,
                            visname='mg2-20170525142607-180419.ms',
                            expectedoutput=(f'{input_dir}/' +
                                            'mg2-20170525142607-180419_PPR.casa-6.2.0-119-pipeline-2021.2.0.23.results.txt'),
                            output_dir='mg2-20170525142607-180419_PPR')

    # copy files use restore task into products folder
    input_products = casa_tools.utils.resolve(f'{input_dir}/products')
    shutil.copytree(input_products, './products')

    pr.run(ppr=f'{input_dir}/PPR.xml')
