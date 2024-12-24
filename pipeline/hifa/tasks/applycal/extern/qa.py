import collections
import functools
import math
import os
from pathlib import Path
from typing import Dict, List

import numpy as np

import pipeline.infrastructure.pipelineqa as pqa
from pipeline.domain.measurementset import MeasurementSet
from . import ampphase_vs_freq_qa
from . import mswrapper
from . import qa_utils as qau

# imports required for WIP testing, to be removed once migration is complete
from ..ampphase_vs_freq_qa import Outlier
from .. import qa as original_qa
from ..qa import QAMessage, outliers_to_qa_scores, REASONS_TO_TEXT

#Dictionaries necessary for the QAScoreEvalFunc class
#scores_thresholds holds the list of metrics to actually use for calculating the score, each pointing
#to the threholds used for them, so that the metric/threhold ratio can be calculated
score_thresholds = {'amp_vs_freq.slope': ampphase_vs_freq_qa.AMPLITUDE_SLOPE_THRESHOLD,
                    'amp_vs_freq.intercept': ampphase_vs_freq_qa.AMPLITUDE_INTERCEPT_THRESHOLD,
                    'phase_vs_freq.slope': ampphase_vs_freq_qa.PHASE_SLOPE_THRESHOLD,
                    'phase_vs_freq.intercept': ampphase_vs_freq_qa.PHASE_INTERCEPT_THRESHOLD}

#Dictionary of minimum QA scores values accepted for each intent
intent_minscore = {'BANDPASS': 0.34, 'AMPLITUDE': 0.34, 'PHASE': 0.34, 'CHECK': 0.85, 'POLARIZATION': 0.34, 'AMP_SYM_OFFSET': 0.8}

#List of SSO objects
SSOfieldnames = ['Ceres', 'Pallas', 'Vesta', 'Venus', 'Mars', 'Jupiter', 'Uranus', 'Neptune', 'Ganymede', 'Titan', 'Callisto', 'Juno', 'Europa']


class QAScoreEvalFunc(object):

    def __init__(self, ms: MeasurementSet, spwsetup, outliers: List[Outlier]):
        """
        QAScoreEvalFunc is a function that given the dataset parameters and a list of outlier
        objects, generates an object that can be evaluated to obtain a QA score evaluation
        for any subset of the dataset.


        """
        #Define constants for QA score evaluation
        self.m4factors = {'amp_vs_freq.intercept': 100.0, 'amp_vs_freq.slope': 100.0*2.0,
                          'phase_vs_freq.intercept': 100.0/180.0, 'phase_vs_freq.slope': 100.0*2.0/180.0}
        self.QAEVALF_MIN = 0.33
        self.QAEVALF_MAX = 1.0
        self.QAEVALF_SCALE = 1.55

        #Save basic data, MS name and spwsetup dictionary.
        self.ms = ms
        self.spwsetup = spwsetup
        
        #Create the relevant vector array for QA scores evaluation, and save them
        self.outliers = outliers
        self.noutliers = len(outliers)
        self.metricnames = np.array([list(o.reason)[0].replace('gt90deg_offset_','') for o in outliers])
        self.gt90degoffset = np.array([('gt90deg_offset' in list(o.reason)[0]) for o in outliers])
        self.metricscores = np.array([np.abs(o.num_sigma) for o in outliers])
        self.delta_phys = np.array([np.abs(o.delta_physical) for o in outliers])
        self.is_amp_sym_off = np.array([o.amp_freq_sym_off for o in outliers])
        self.metricthresholds = np.array([score_thresholds[m] if m in score_thresholds.keys() else 9999.0 for m in self.metricnames])
        self.mtratio = self.metricscores/self.metricthresholds
        self.scan = np.array([list(o.scan)[0] for o in outliers])
        self.spw = np.array([list(o.spw)[0] for o in outliers])
        self.intent = np.array([list(o.intent)[0] for o in outliers])
        self.ant = np.array([list(o.ant)[0] for o in outliers])
        self.pol = np.array([list(o.pol)[0] for o in outliers])
        self.allintents = [i for i in self.spwsetup['scan'].keys() if len(self.spwsetup['scan'][i]) > 0]
        self.long_msg = 'EVALUATE_TO_GET_LONGMSG'
        self.short_msg = 'EVALUATE_TO_GET_SHORTMSG'
        #Initialize metrics/data dictionary
        mlist = score_thresholds.keys()
        self.qascoremetrics = {}
        for i in self.allintents:
            self.qascoremetrics[i] = {}
            for s in self.spwsetup['spwlist']:
                self.qascoremetrics[i][s] = {}
                for m in mlist:
                    self.qascoremetrics[i][s][m] = {}


    def __call__(self, qascore: pqa.QAScore):

        #If given a list of QA scores, evaluate them all and return an array of the results
        if type(qascore) == list:
            output = [self.__call__(q) for q in qascore]
            return np.array(output)

        mlist = score_thresholds.keys()
        #Get data selection from QA score
        selscan = np.array(list(qascore.applies_to.scan))
        selspw = np.array(list(qascore.applies_to.spw))
        selintent = np.array(list(qascore.applies_to.intent))
        selant = np.array(list(qascore.applies_to.ant))
        selmetric = qascore.origin.metric_name

        #Case of no data selected as outlier for this metric,
        #fill values with default values for non-outlier QA scores
        if (len(selscan) == 0) and (len(selspw) == 0) and (len(selintent) == 0) and (len(selant) == 0):
            for i in self.allintents:
                self.qascoremetrics[i]['subscore'] = 1.0
                self.qascoremetrics['finalscore'] = 1.0
                for s in self.spwsetup['spwlist']:
                    for m in mlist:
                        self.qascoremetrics[i][s][m]['significance'] = 0.0
                        self.qascoremetrics[i][s][m]['is_amp_sym_off'] = False
                        self.qascoremetrics[i][s][m]['outliers'] = False
            self.qascoremetrics['finalscore'] = 1.0
            self.long_msg = qascore.longmsg
            self.short_msg = qascore.shortmsg
            return self.qascoremetrics['finalscore']

        testspw = lambda x: x in selspw
        testscan = lambda x: x in selscan
        testintent = lambda x: x in selintent
        testant = lambda x: x in selant
        basesel = np.array(list(map(testspw, self.spw))) & np.array(list(map(testscan, self.scan))) & np.array(list(map(testintent, self.intent))) & np.array(list(map(testant, self.ant)))
        #npols = self.spwsetup[selspw[0]]['npol']
        nants = len(self.spwsetup['antids'])

        for i in selintent:
            intentscans = np.intersect1d(selscan, np.array(self.spwsetup['scan'][i]))
            nscans = len(intentscans)
            for s in selspw:
                for m in mlist:
                    #For this metric, select the pool of outliers from the "applies_to" attribute
                    sel = (basesel & (self.metricnames == m) & (self.mtratio > 1.0))
                    nsel = np.sum(sel)
                    if nsel > 0:
                        idxmax = np.argsort(self.metricscores[sel])[-1]
                        #Get ratio Metric/Threshold for maximum value -> significance
                        self.qascoremetrics[i][s][m]['significance'] = self.mtratio[sel][idxmax]
                        #Generate message for this max outlier
                        thismaxoutlieridx = np.arange(self.noutliers)[sel][idxmax]
                        thismaxoutlier = self.outliers[thismaxoutlieridx]
                        thisqamsg = QAMessage(self.ms, thismaxoutlier, reason=list(thismaxoutlier.reason)[0])
                        self.qascoremetrics[i][s][m]['long_msg'] = thisqamsg.full_message
                        self.qascoremetrics[i][s][m]['short_msg'] = thisqamsg.short_message
                        #copy the boolean is_amp_sym_offset from this QA scores
                        self.qascoremetrics[i][s][m]['is_amp_sym_off'] = self.is_amp_sym_off[sel][idxmax]
                        self.qascoremetrics[i][s][m]['outliers'] = True
                    else:
                        metric_axes, outlier_description, extra_description = REASONS_TO_TEXT[m]
                        # Correct capitalisation as we'll prefix the metric with 'No '
                        metric_axes = metric_axes.lower()
                        self.qascoremetrics[i][s][m]['short_msg'] = 'No {} outliers'.format(metric_axes)
                        self.qascoremetrics[i][s][m]['long_msg'] = 'No {} {} detected for {}'.format(metric_axes, outlier_description, self.ms.basename)
                        self.qascoremetrics[i][s][m]['significance'] = 0.0
                        self.qascoremetrics[i][s][m]['is_amp_sym_off'] = False
                        self.qascoremetrics[i][s][m]['outliers'] = False

            longmsgsubscores = np.array([self.qascoremetrics[i][s][selmetric]['long_msg'] for s in selspw])
            shortmsgsubscores = np.array([self.qascoremetrics[i][s][selmetric]['short_msg'] for s in selspw])
            sig_subscores = np.array([self.qascoremetrics[i][s][selmetric]['significance'] for s in selspw])
            is_amp_sym_off_subscores = np.array([self.qascoremetrics[i][s][selmetric]['is_amp_sym_off'] for s in selspw])
            anyoutliers = any([self.qascoremetrics[i][s][selmetric]['outliers'] for s in selspw])
            #combine metric factors into one for each
            #Currently just using the maximum of each.
            idxmax = np.argsort(sig_subscores)[-1]
            #copy message from the outlier with maximum metric value
            self.qascoremetrics[i]['long_msg'] = longmsgsubscores[idxmax]
            self.qascoremetrics[i]['short_msg'] = shortmsgsubscores[idxmax]
            significance = np.max(sig_subscores)
            #Determine whether for this QA scores we set this boolean is_amp_symmetric_offset
            #In order to be symmetric for all the data considered in the QA score,
            #it needs to be symmetric for any outlier in the pool.
            is_amp_sym_off_all = all(is_amp_sym_off_subscores)
            if anyoutliers:
                #Decide the minimum QA score for this subscore
                #Unless it is a non-polarization intent with symmetric amplitude outliers,
                #should be determined by the intent_minscore dictionary from the intent
                if (selmetric == 'amp_vs_freq.intercept') and (i != '*POLARIZATION*') and is_amp_sym_off_all:
                    thisminscore = intent_minscore['AMP_SYM_OFFSET']
                else:
                    thisminscore = intent_minscore[i]
                auxqascore = self.QAEVALF_MIN + 0.5*(self.QAEVALF_MAX-self.QAEVALF_MIN)*(1 + math.erf(-np.log10(significance/self.QAEVALF_SCALE)))
                self.qascoremetrics[i]['subscore'] = max(thisminscore, auxqascore)
            else:
                self.qascoremetrics[i]['subscore'] = 1.0

        #Obtain final QA score value for this QA score object
        finalset = [self.qascoremetrics[i]['subscore'] for i in selintent]
        if len(finalset) > 0:
            self.qascoremetrics['finalscore'] = min(finalset)
        else:
            self.qascoremetrics['finalscore'] = 1.0
        #Generate summary line
        if len(selintent) == 1:
            self.long_msg = self.qascoremetrics[selintent[0]]['long_msg']
            self.short_msg = self.qascoremetrics[selintent[0]]['short_msg']
        elif len(selintent) == 0:
            self.long_msg = ''
            self.short_msg = ''
        else:
            print('Multiple intents for this QAscore!!')
            print(repr(qascore))
            self.long_msg = ''
            self.short_msg = ''

        return self.qascoremetrics['finalscore']


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
    #this is still using function from analysisUtils, but should probably be replaced
    spwsetup = qau.getSpecSetup(ms, intents=pipe_intents, bfolder=buffer_folder)
    #All outlier objects in this list
    outliers = []
    #all outlier scores objects will be saved here
    all_scores = []
    #Define debug filename
    debug_path = output_path / f'PIPE356_outliers.pipe.{timestamp}.txt'

    #Define intents that need to be processed
    #avoiding intents with repeated scans
    intents2proc = qau.get_intents_to_process(ms, pipe_intents)

    #Go and process each of these intents
    for intent in intents2proc:
        print('Processing intent '+str(intent))
        outliers_for_intent = score_all_scans(ms.basename, intent, spwsetup, memory_gb=memory_gb,
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
    qaevalf = QAScoreEvalFunc(ms, spwsetup, outliers)
    # convert outliers to QA scores
    all_scores.extend(outliers_to_qa_scores(ms, outliers, outlier_score))

    #Get summary QA scores
    final_scores = summarise_scores(all_scores, ms, qaevalf = qaevalf)

    return all_scores, final_scores, qaevalf

def score_all_scans(
        ms: str,
        intent: str,
        spwsetup: dict,
        memory_gb: str = '2.0',
        saved_visibilities: Path = Path(''),
        flag_all: bool = False
) -> list[Outlier]:
    """
    Calculate amp/phase vs freq and time outliers for an EB and filter out outliers.
    :param ms: name of ms file 
    :param intent: intent for scans
    :spwsetup: dictionary with information regarding the spectral set-up, remnant from old script. should probably be a class
    :memory_gb: max memory allowed in gb
    :saved_visibilities: folder where saved average visibilities are, if any
    :param outlier_score: score to assign to generated QAScores
    :return: list of Outlier objects
    """
    #all outliers will be here
    outliers = []

    #spwsetup dictionary contains:
    scanlist = spwsetup['scan'][intent]
    antennaids = spwsetup['antids']
    nants = len(antennaids)
    spwlist = spwsetup['spwlist']
    msname = ms.split('/')[-1]
    nscans = len(scanlist)
    unitdicts = qau.getUnitsDicts(spwsetup)

    if scanlist: #if there are any scans for the required intent

        print('Starting analysis of intent: '+str(intent)+' with scan list: '+str(scanlist))
        for spw in spwlist: #for all spectral windows in it

            #partial function to construct outlier, so we don't have to repeat these arguments for all outliers connected to this ms, intent, spw
            outlier_fn = functools.partial(
                Outlier,
                vis={msname, },
                intent={intent, },
                spw={int(spw), },
                # TODO remove this attribute added for backwards compatibility
                phase_offset_gt90deg=None
            )

            print('Processing SPW '+str(spw)+' , intent '+str(intent))
            npol = spwsetup[spw]['npol']
            nchan = spwsetup[spw]['nchan']
            channel_frequencies=spwsetup[spw]['chanfreqs']
            ddi = spwsetup[spw]['ddi']
            fieldid=spwsetup['fieldid'][intent][0]

            if nscans > 1:
                #string to check whether a file of averaged visibilities for all scans exists
                all_scans = str(scanlist).replace(', ','_')[1:-1] #string with list of all scans separated by underscore
                all_scans_saved_visibilities = saved_visibilities / f'buf.{msname}.{all_scans}.{ddi}.{fieldid}.pkl'
                all_scans_visibility_exists = os.path.exists(all_scans_saved_visibilities) #do the average visibili ties for all scans already exist

            #all mswrapper objects of this ms, intent, will go here. this is in case we need to average the visibilities of all scans
            wrapper_list=[]

            for scan in scanlist:

                print('Starting QA of scan '+str(scan))
                #are there saved averaged visbilities? 
                saved_visibility = saved_visibilities / f'buf.{msname}.{scan}.{ddi}.{fieldid}.pkl'
                if os.path.exists(saved_visibility):
                    #then load them
                    print("loading visibilities")
                    wrapper = mswrapper.MSWrapper(ms, scan, spw)
                    wrapper.load(saved_visibility)
                else:
                    #then create them    
                    print('Creating averaged visibilities, since they do not exist yet...')
                    wrapper = mswrapper.MSWrapper.create_averages_from_ms(ms, scan, spw, memory_gb, antennaids, npol, nchan)
                    wrapper.save(saved_visibility)

                #add scan parameter to outlier partial function
                outlier_fn_for_scan = functools.partial(outlier_fn, scan={scan, })
                #amp/phase vs frequency fits per scan            
                frequency_fit = ampphase_vs_freq_qa.get_best_fits_per_ant(wrapper, channel_frequencies)

                #amp/phase vs frequency scores
                scan_outliers = ampphase_vs_freq_qa.score_all(frequency_fit, outlier_fn_for_scan, unitdicts, flag_all)
                outliers.extend(scan_outliers)

                #in case we need to average over average visibilities to get scores over all scans
                if (nscans > 1) and not all_scans_visibility_exists:
                    wrapper_list.append(wrapper)

            #now we get scores for the average over average visibilities across all scans
            if (nscans > 1) and all_scans_visibility_exists:
                print('All-scan visibilities for '+str(all_scans)+' exist, reading them...')
                all_scan_wrapper = mswrapper.MSWrapper(ms, all_scans, spw)
                all_scan_wrapper.load(all_scans_saved_visibilities)
            elif nscans > 1:
                print('All-scan visibilities for '+str(all_scans)+' DO NOT exist, creating them...')
                all_scan_wrapper = mswrapper.MSWrapper.create_averages_from_combination(wrapper_list, antennaids, npol, nchan)
                all_scan_wrapper.save(all_scans_saved_visibilities)

            if nscans > 1:
                #amp/phase vs frequency scores for all scans              
                all_scan_frequency_fits = ampphase_vs_freq_qa.get_best_fits_per_ant(all_scan_wrapper, channel_frequencies)
                #for lack of a better number, '-1' means 'all scans'
                outlier_fn_for_all_scans = functools.partial(outlier_fn, scan={-1, })
                scan_outliers = ampphase_vs_freq_qa.score_all(all_scan_frequency_fits, outlier_fn_for_all_scans, unitdicts, flag_all)
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
    metricnamelist = [item for item in REASONS_TO_TEXT.keys() if '.' in item and not ',' in item]
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
