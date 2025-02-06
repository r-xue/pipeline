from pathlib import Path

from pipeline.domain.measurementset import MeasurementSet
from pipeline.infrastructure import logging
from .. import qa_utils as qau
# imports required for WIP testing, to be removed once migration is complete
from ..ampphase_vs_freq_qa import score_all_scans
from ..qa import outliers_to_qa_scores, QAScoreEvalFunc, summarise_scores

LOG = logging.get_logger(__name__)


#List of SSO objects
SSOfieldnames = ['Ceres', 'Pallas', 'Vesta', 'Venus', 'Mars', 'Jupiter', 'Uranus', 'Neptune', 'Ganymede', 'Titan', 'Callisto', 'Juno', 'Europa']


def get_qa_scores(
        ms: MeasurementSet,
        outlier_score: float=0.5,
        output_path: Path = Path(''),
        memory_gb: str='2.0',
        flag_all=False,
        timestamp=''
):
    """
    Calculate amp/phase vs freq and time outliers for an EB and convert to QA scores.

    This is the key entry point for applycal QA metric calculation. It
    delegates to the detailed metric implementation in ampphase_vs_freq_qa.py 
    and ampphase_vs_time_qa.py to detect outliers, 
    converting the outlier descriptions to normalised QA
    scores.
    """
    # TODO: confirm that DIFFGAINREF, DIFFGAINSRC, POLANGLE, POLLEAKAGE should be added to this list
    pipe_intents = ['BANDPASS', 'AMPLITUDE', 'PHASE', 'CHECK', 'POLARIZATION']

    print(f'Calculating scores for MS: {ms.basename}')
    #if there are any average visibilities saved, they are in buffer_folder
    buffer_folder = output_path / 'databuffer'
    #All outlier objects in this list
    outliers = []
    #all outlier scores objects will be saved here
    all_scores = []
    #Define debug filename
    debug_path = output_path / f'PIPE356_outliers.pipe.txt'

    #Define intents that need to be processed
    #avoiding intents with repeated scans
    intents2proc = qau.get_intents_to_process(ms, pipe_intents)

    #Go and process each of these intents
    for intent in intents2proc:
        print('Processing intent '+str(intent))
        outliers_for_intent = score_all_scans(ms, intent, memory_gb=memory_gb,
                                              saved_visibilities=buffer_folder, flag_all=flag_all)
        outliers.extend(outliers_for_intent)

        if not flag_all:
            with open(debug_path, 'a') as debug_file:
                for o in outliers:
                    msg = (f'{o.vis} {o.intent} scan={o.scan} spw={o.spw} ant={o.ant} '
                           f'pol={o.pol} reason={o.reason} sigma_deviation={o.num_sigma} '
                           f'delta_physical={o.delta_physical} amp_freq_sym_off={o.amp_freq_sym_off}')
                    debug_file.write('{}\n'.format(msg))

    #Create QA evaluation function
    qaevalf = QAScoreEvalFunc(ms, pipe_intents, outliers)
    # convert outliers to QA scores
    all_scores.extend(outliers_to_qa_scores(ms, outliers, outlier_score))

    #Get summary QA scores
    final_scores = summarise_scores(all_scores, ms, qaevalf = qaevalf)

    return all_scores, final_scores, qaevalf
