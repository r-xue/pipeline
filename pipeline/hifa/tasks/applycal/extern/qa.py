import functools
import operator
import os
from pathlib import Path
from typing import Dict, List

import numpy as np

import pipeline.infrastructure.pipelineqa as pqa
from pipeline.domain.measurementset import MeasurementSet
from pipeline.domain.measures import FrequencyUnits
from pipeline.infrastructure import logging
from .. import mswrapper, ampphase_vs_freq_qa
from .. import qa as original_qa, qa_utils as qau
# imports required for WIP testing, to be removed once migration is complete
from ..ampphase_vs_freq_qa import Outlier, get_best_fits_per_ant, score_all
from ..qa import outliers_to_qa_scores, REASONS_TO_TEXT, QAScoreEvalFunc

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

def score_all_scans(
        ms: MeasurementSet,
        intent: str,
        memory_gb: str = '2.0',
        saved_visibilities: Path = Path(''),
        flag_all: bool = False
) -> list[Outlier]:
    """
    Calculate amp/phase vs freq and time outliers for an EB and filter out outliers.
    :param ms: name of ms file 
    :param intent: intent for scans
    :memory_gb: max memory allowed in gb
    :saved_visibilities: folder where saved average visibilities are, if any
    :param outlier_score: score to assign to generated QAScores
    :return: list of Outlier objects
    """
    outliers = []
    wrappers = {}
    scans = sorted(ms.get_scans(scan_intent=intent), key=operator.attrgetter('id'))

    if not scans:
        return outliers

    unit_factor = qau.get_unit_factor(ms)
    antenna_ids = [antenna.id for antenna in scans[0].antennas]

    for scan in scans:
        spws = sorted([spw for spw in scan.spws if spw.type in ('FDM', 'TDM')],
                      key=operator.attrgetter('id'))
        for spw in spws:
            LOG.info('Applycal QA analysis: processing {} scan {} spw {}'.format(ms.basename, scan.id, spw.id))

            channel_frequencies = np.array([float((c.high + c.low).to_units(FrequencyUnits.HERTZ) / 2) for c in spw.channels])

            # are there saved averaged visbilities?
            saved_visibility = saved_visibilities / f'buf.{ms.basename}.{int(scan.id)}.{spw.id}.pkl'
            if os.path.exists(saved_visibility):
                print("loading visibilities")
                wrapper = mswrapper.MSWrapper(ms, scan.id, spw.id)
                wrapper.load(saved_visibility)
            else:
                print('Creating averaged visibilities, since they do not exist yet...')
                wrapper = mswrapper.MSWrapper.create_averages_from_ms(ms.basename, int(scan.id), spw.id, memory_gb)
                wrapper.save(saved_visibility)

            wrappers.setdefault(spw.id, []).append(wrapper)

            # amp/phase vs frequency fits per scan
            frequency_fit = get_best_fits_per_ant(wrapper, channel_frequencies)

            # partial function to construct outlier, so we don't have to repeat
            # these arguments for all outliers connected to this ms, intent, spw
            outlier_fn = functools.partial(
                Outlier,
                vis={ms.basename, },
                intent={intent, },
                spw={spw.id, },
                scan={scan.id, }
            )

            scan_outliers = score_all(frequency_fit, outlier_fn, unit_factor, flag_all)
            outliers.extend(scan_outliers)

    # now we get scores for the average over average visibilities across all scans
    for spw_id, spw_wrappers in wrappers.items():
        if len(spw_wrappers) == 1:
            LOG.info('Applycal QA analysis: skipping {} scan average for spw {} due to single scan'.format(ms.basename, spw_id))
            continue

        LOG.info('Applycal QA analysis: processing {} scan average spw {}'.format(ms.basename, spw_id))

        all_scans = '_'.join(str(scan.id) for scan in scans) #string with list of all scans separated by underscore
        ddi = ms.get_data_description(spw=spw_id)
        pickle_file = saved_visibilities / f'buf.{ms.basename}.{all_scans}.{ddi.id}.pkl'
        if os.path.exists(pickle_file):
            print('All-scan visibilities for ' + str(all_scans) + ' exist, reading them...')
            all_scan_wrapper = mswrapper.MSWrapper(ms, all_scans, spw_id)
            all_scan_wrapper.load(pickle_file)
        else:
            print('All-scan visibilities for ' + str(all_scans) + ' DO NOT exist, creating them...')
            all_scan_wrapper = mswrapper.MSWrapper.create_averages_from_combination(spw_wrappers, antenna_ids)
            all_scan_wrapper.save(pickle_file)

        spw = ms.get_spectral_window(spw_id)
        channel_frequencies = np.array([float((c.high + c.low).to_units(FrequencyUnits.HERTZ) / 2) for c in spw.channels])
        all_scan_frequency_fits = get_best_fits_per_ant(all_scan_wrapper, channel_frequencies)
        outlier_fn = functools.partial(
            Outlier,
            vis={ms.basename, },
            intent={intent, },
            spw={spw_id, },
            scan={-1, }  #for lack of a better idenfifier, '-1' means 'all scans'
        )

        scan_outliers = ampphase_vs_freq_qa.score_all(all_scan_frequency_fits, outlier_fn, unit_factor, flag_all)
        outliers.extend(scan_outliers)

    return outliers

def summarise_scores(
        all_scores: List[pqa.QAScore],
        ms: MeasurementSet,
        qaevalf: QAScoreEvalFunc = None
) -> Dict[pqa.WebLogLocation, List[pqa.QAScore]]:
    """
    Process a list of QAscores, replacing the detailed and highly specific
    input scores with compressed representations intended for display in the
    web log accordion, and even more generalised summaries intended for
    display as warning banners.
    """
    final_scores = original_qa.summarise_scores(all_scores, ms)
    # TODO TBC: erase WebLogLocation.HIDDEN scores like prototype?

    #Use continuum scoring function, if one is given.
    if qaevalf is not None:
        # TODO TBC: operate exclusively on .ACCORDION scores, like prototype?
        for scores in final_scores.values():
            for fq in scores:
                newscore = qaevalf(fq)
                fq.score = newscore
                fq.longmsg = qaevalf.long_msg
                fq.shortmsg = qaevalf.short_msg

    # TODO TBC: prototype did not summarise to .BANNER scores. Repeat?

    return final_scores

def get_max_scores(all_scores: List[pqa.QAScore]) -> Dict[str, List[pqa.QAScore]]:
    '''Process the entire list of QA scores for a dataset and extract the ones with maximum
    metric score value for each metric.
    '''

    #List of metric names to search for
    metricnamelist = [item for item in REASONS_TO_TEXT if '.' in item and not ',' in item]
    #Extract metric names for indexing all_scores data
    metricname = np.array([score.origin.metric_name for score in all_scores])
    scoreval = np.array([score.origin.metric_score for score in all_scores])
    maxqascores = []

    for m in metricnamelist:
        sel = (metricname == m)
        #If there is some QAscore, search for the one with maximum metric score
        if np.sum(sel) > 0:
            idxmax = np.argsort(scoreval[sel])[-1]
            maxqascores.append(np.array(all_scores)[sel][idxmax])

    return maxqascores
