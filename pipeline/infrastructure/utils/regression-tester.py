import os
import shutil
import pytest
from typing import Tuple, Optional

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.recipereducer
from pipeline.infrastructure.renderer import regression
import pipeline.infrastructure.executeppr as almappr
import pipeline.infrastructure.executevlappr as vlappr

LOG = infrastructure.get_logger(__name__)

class PipelineRegression(object):

    def __init__(self, recipe: str, input_dir: str, visname: str, expectedoutput: str):
        self.recipe = recipe
        self.input_dir = input_dir
        self.visname = visname
        self.expectedoutput = expectedoutput
        self.testinput = f'{input_dir}/{visname}'
        
    def __sanitize_regression_string(self, instring: str) -> Tuple:
        """sanitize to get numeric values, remove newline chars and change to float

        Returns:
            tuple(key, value, optional tolerance)
        """
        keyval = instring.split(':::')[0]

        keystring = keyval.split('=')[0]
        value = float(keyval.split('=')[-1])
        try:
            tolerance = float(instring.split(':::')[-1].replace('\n',''))
        except ValueError:
            tolerance = None

        return keystring, value, tolerance

    def run(self, ppr: Optional[str] = None, telescope: str  = 'alma', default_relative_tolerance: float = 1e-7):
        """Run test with PPR if supplied or recipereducer if no PPR and compared to expected results

        The inputs and expectd output are usually found in the pipeline data repository.
        """

        # Run the Pipeline using cal+imag ALMA IF recipe
        # set datapath in ~/.casa/config.py, e.g. datapath = ['/users/jmasters/pl-testdata.git']
        input_vis = casa_tools.utils.resolve(self.testinput)

        #run the pipeline for new results
        if ppr:
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
        else:
            LOG.warning("Running without Pipeline Processing Request (PPR).  Using recipereducer instead.")
            pipeline.recipereducer.reduce(vis=[input_vis], procedure=self.recipe)

        # Get new results
        context = pipeline.Pipeline(context='last').context
        new_results = sorted(regression.extract_regression_results(context))

        # Store new results in a file
        new_file = f'{self.visname}.NEW.results.txt'
        with open(new_file,'w') as fd:
            fd.writelines([str(x)+'\n' for x in new_results])

        expected = casa_tools.utils.resolve(self.expectedoutput)
        with open(expected) as expected_fd, open(new_file) as new_fd:
            expected_results = expected_fd.readlines()
            new_results = new_fd.readlines()
            errors = []
            for old, new  in zip(expected_results, new_results):
                oldkey, oldval, tol = self.__sanitize_regression_string(old)
                newkey, newval, _ = self.__sanitize_regression_string(new)
                assert oldkey == newkey
                tolerance = tol if tol else default_relative_tolerance
                LOG.info(f'Comparing {oldval} to {newval} with a rel. tolerance of {tolerance}')
                if oldval != pytest.approx(newval, rel=tolerance):
                    errorstr = f"{oldkey}\n\tvalues differ by > a relative difference of {tolerance}\n\texpected: {oldval}\n\tnew:      {newval}"
                    errors.append(errorstr)
            [LOG.warning(x) for x in errors]
            assert not errors


def test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small__procedure_hifa_calimage__regression():
    """Run ALMA cal+image regression on a small test dataset

    Recipe name:                procedure_hifa_calimage
    Dataset:                    uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms
    Expected results version:   casa-6.1.1-15-pipeline-2020.1.0.40
    """

    pr = PipelineRegression(recipe='procedure_hifa_calimage.xml',
                            input_dir='pl-unittest', visname='uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms',
                            expectedoutput=('pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/' +
                                            'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.casa-6.1.1-15-pipeline-2020.1.0.40.results.txt'))

    pr.run(ppr='pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/PPR.xml')


def test_uid___A002_X85c183_X36f__procedure_hsd_calimage__regression():
    """Run ALMA single-dish cal+image regression on the obseration data of M100

    Recipe name:                procedure_hsd_calimage
    Dataset:                    uid___A002_X85c183_X36f
    Expected results version:   casa-6.2.0-119-pipeline-2020.2.0.23
    """

    pr = PipelineRegression(recipe='procedure_hsd_calimage.xml',
                            input_dir='pl-regressiontest/uid___A002_X85c183_X36f', visname='uid___A002_X85c183_X36f',
                            expectedoutput=('pl-regressiontest/uid___A002_X85c183_X36f/' +
                                            'uid___A002_X85c183_X36f.casa-6.2.0-119-pipeline-2021.2.0.23.results.txt'))
    
    pr.run()

