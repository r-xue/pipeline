import collections
import copy
import functools
import itertools
import math
import os
from typing import Dict, Iterable, List, Reversible

import numpy as np

from . import ampphase_vs_freq_qa
from . import mswrapper
from . import pipelineqa as pqa
from . import qa_utils as qau

# Maps outlier reasons to a text snippet that can be used in a QAScore message
# Maps outlier reasons to a text snippet that can be used in a QAScore message
REASONS_TO_TEXT = {
    'amp_vs_freq.intercept,amp.slope': ('Amp vs frequency', 'zero point and slope outliers', ''),
    'amp_vs_freq.intercept': ('Amp vs frequency', 'zero point outliers', ''),
    'amp_vs_freq.slope': ('Amp vs frequency', 'slope outliers', ''),
    'amp_vs_freq': ('Amp vs frequency', 'outliers', ''),
    'phase_vs_freq.intercept,phase_vs_freq.slope': ('Phase vs frequency', 'zero point and slope outliers', ''),
    'phase_vs_freq.intercept': ('Phase vs frequency', 'zero point outliers', ''),
    'phase_vs_freq.slope': ('Phase vs frequency', 'slope outliers', ''),
    'phase_vs_freq': ('Phase vs frequency', 'outliers', ''),
    'gt90deg_offset_phase_vs_freq.intercept,phase_vs_freq.slope': ('Phase vs frequency', 'zero point and slope outliers', '; phase offset > 90deg detected'),
    'gt90deg_offset_phase_vs_freq.intercept': ('Phase vs frequency', 'zero point outliers', '; phase offset > 90deg detected'),
    'gt90deg_offset_phase_vs_freq.slope': ('Phase vs frequency', 'slope outliers', '; phase offset > 90deg detected'),
    'gt90deg_offset_phase_vs_freq': ('Phase vs frequency', 'outliers', '; phase offset > 90deg detected'),
}


Outlier = collections.namedtuple(
    'Outlier',
    ['vis', 'intent', 'scan', 'spw', 'ant', 'pol', 'num_sigma', 'delta_physical', 'amp_freq_sym_off', 'reason']
)

# Tuple to hold data selection parameters. The field order is important as it
# sets in which order the dimensions are rolled up. With the order below,
# scores are merged first by pol, then ant, then spw, etc.
DataSelection = collections.namedtuple('DataSelection', 'vis intent scan spw ant pol')


# The key data structure used to consolidate and merge QA scores: a dict
# mapping data selections to the QA scores that cover that data selection. The
# DataSelection keys are simple tuples, with index relating to a data
# selection parameter (e.g., vis=[0], intent=[1], scan=[2], etc.).
DataSelectionToScores = Dict[DataSelection, List[pqa.QAScore]]

#Dictionaries necessary for the QAScoreEvalFunc class
#scores_thresholds holds the list of metrics to actually use for calculating the score, each pointing
#to the threholds used for them, so that the metric/threhold ratio can be calculated
score_thresholds = {'amp_vs_freq.slope': ampphase_vs_freq_qa.AMPLITUDE_SLOPE_THRESHOLD,
                    'amp_vs_freq.intercept': ampphase_vs_freq_qa.AMPLITUDE_INTERCEPT_THRESHOLD,
                    'phase_vs_freq.slope': ampphase_vs_freq_qa.PHASE_SLOPE_THRESHOLD,
                    'phase_vs_freq.intercept': ampphase_vs_freq_qa.PHASE_INTERCEPT_THRESHOLD}

#Dictionary of minimum QA scores values accepted for each intent
intent_minscore = {'*BANDPASS*': 0.34, '*FLUX*': 0.34, '*PHASE*': 0.34, '*CHECK*': 0.85, '*POLARIZATION*': 0.34, 'AMP_SYM_OFFSET': 0.8}

#List of SSO objects
SSOfieldnames = ['Ceres', 'Pallas', 'Vesta', 'Venus', 'Mars', 'Jupiter', 'Uranus', 'Neptune', 'Ganymede', 'Titan', 'Callisto', 'Juno', 'Europa']


class QAScoreEvalFunc(object):

    def __init__(self, vis, spwsetup, outliers: List[Outlier]):
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
        self.vis = vis
        self.msname = vis.split('/')[-1]
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
                        thisqamsg = QAMessage(self.msname, thismaxoutlier, reason=list(thismaxoutlier.reason)[0])
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
                        self.qascoremetrics[i][s][m]['long_msg'] = 'No {} {} detected for {}'.format(metric_axes, outlier_description, self.msname)
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


def get_qa_scores(ms, outlier_score=0.5, output_path="", memory_gb='2.0',applycalQAversion="",intents=['*BANDPASS*', '*FLUX*', '*PHASE*', '*CHECK*', '*POLARIZATION*'], flag_all=False, timestamp=''):
    

    """
    Calculate amp/phase vs freq and time outliers for an EB and convert to QA scores.

    This is the key entry point for applycal QA metric calculation. It
    delegates to the detailed metric implementation in ampphase_vs_freq_qa.py 
    and ampphase_vs_time_qa.py to detect outliers, 
    converting the outlier descriptions to normalised QA
    scores.
    """
    msname = ms.split('/')[-1]
    print('Calculating scores for MS: '+msname)
    #if there are any average visibilities saved, they are in buffer_folder
    buffer_folder = output_path + "/databuffer"
    #this is still using function from analysisUtils, but should probably be replaced
    spwsetup = qau.getSpecSetup(ms, intentlist=intents, bfolder=buffer_folder, applycalQAversion=applycalQAversion)
    #All outlier objects in this list
    outliers = []
    #all outlier scores objects will be saved here
    all_scores = []
    #Define debug filename
    debug_path = output_path+'/PIPE356_outliers.pipe.'+timestamp+'.txt'

    #Define intents that need to be processed
    #avoiding intents with repeated scans
    intents2proc = qau.get_intents_to_process(spwsetup, intents = intents)

    #Go and process each of these intents
    for intent in intents2proc:
        print('Processing intent '+str(intent))
        outliers_for_intent = score_all_scans(ms, intent, spwsetup, memory_gb=memory_gb,
                                              applycalQAversion=applycalQAversion,
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
    all_scores.extend(outliers_to_qa_scores(ms, outliers, outlier_score, qafunction = None))

    #Get summary QA scores
    final_scores = summarise_scores(all_scores, ms, qaevalf = qaevalf)

    return all_scores, final_scores, qaevalf

def score_all_scans(ms, intent, spwsetup, memory_gb='2.0',applycalQAversion="",saved_visibilities="", flag_all=False):
   
    """
    Calculate amp/phase vs freq and time outliers for an EB and filter out outliers.
    :param ms: name of ms file 
    :param intent: intent for scans
    :spwsetup: dictionary with information regarding the spectral set-up, remnant from old script. should probably be a class
    :memory_gb: max memory allowed in gb
    :applycalQAversion: version of script, optional and remnant from old script. The version is used to find posible saved average visibilites.
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
                spw={spw, }
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
                all_scans_saved_visibilities = saved_visibilities+'/buf.'+msname+'.'+str(all_scans)+'.'+str(ddi)+'.'+str(fieldid)+'.v'+str(applycalQAversion)+'.pkl'
                all_scans_visibility_exists = os.path.exists(all_scans_saved_visibilities) #do the average visibili ties for all scans already exist

            #all mswrapper objects of this ms, intent, will go here. this is in case we need to average the visibilities of all scans
            wrapper_list=[]

            for scan in scanlist:

                print('Starting QA of scan '+str(scan))
                #are there saved averaged visbilities? 
                saved_visibility = saved_visibilities+'/buf.'+msname+'.'+str(scan)+'.'+str(ddi)+'.'+str(fieldid)+'.v'+str(applycalQAversion)+'.pkl'
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

class QAMessage:
    """
    QAMessage constructs a user-friendly QA message for an Outlier.

    The QAMessage instance has two attributes, full_message and short_message,
    that are of interest. full_message holds the text to be used when the
    message is the first to be printed. short_message holds the text to be
    used when this message is to be appended to the text of other QAMessages.
    Naturally, this assumes the the calling code only concatenates messages
    that originate from the same reason.
    """

    def __init__(self, ms, outlier, reason):
        metric_axes, outlier_description, extra_description = REASONS_TO_TEXT[reason]

        intent_msg = f' {outlier.intent} calibrator' if outlier.intent else ''
        spw_msg = f' spw {outlier.spw}' if outlier.spw else ''
        scan_msg = f' scan {outlier.scan}' if outlier.scan else ''
        corr_msg = f' scan {outlier.pol}' if outlier.pol else ''
        ant_names = sorted([str(a) for a in outlier.ant])
        ant_msg = f' {",".join(ant_names)}' if ant_names else ''

        if isinstance(outlier, Outlier):
            num_sigma_msg = '{0:.3f}'.format(outlier.num_sigma)
            delta_physical_msg = '{0:.3f}'.format(outlier.delta_physical)
            amp_freq_sym_off_msg = 'Y' if outlier.amp_freq_sym_off else 'N'
            significance_msg = f'; n_sig={num_sigma_msg}; d_phys={delta_physical_msg}; ampsymoff={amp_freq_sym_off_msg}'
        else:
            significance_msg = ''

        short_msg = f'{metric_axes} {outlier_description}'
        full_msg = f'{short_msg} for {ms}{intent_msg}{spw_msg}{ant_msg}{corr_msg}{scan_msg}{significance_msg}{extra_description}'

        self.short_message = short_msg
        self.full_message = full_msg

def outliers_to_qa_scores(ms,
                          outliers: List[Outlier],
                          outlier_score: float, qafunction: QAScoreEvalFunc = None) -> List[pqa.QAScore]:
    """
    Convert a list of consolidated Outliers into a list of equivalent
    QAScores.

    The MeasurementSet argument is required to convert antenna IDs to antenna
    names.

    All generated QAScores will be assigned the numeric score given in
    outlier_score.

    :param ms: MeasurementSet domain object for the DataSelections
    :param outliers: list of Outliers
    :param outlier_score: score to assign to generated QAScores
    :return:
    """
    hashable = []
    for outlier in outliers:
        # convert ['amp.slope','amp.intercept'] into 'amp.slope,amp.intercept', etc.
        hashable.append(Outlier(vis=outlier.vis,
                                intent=outlier.intent,
                                scan=outlier.scan,
                                spw=outlier.spw,
                                ant=outlier.ant,
                                pol=outlier.pol,
                                num_sigma=outlier.num_sigma,
                                delta_physical=outlier.delta_physical,
                                amp_freq_sym_off=outlier.amp_freq_sym_off,
                                reason=','.join(sorted(outlier.reason))))
    reasons = {outlier.reason for outlier in hashable}

    qa_scores = []
    for reason in reasons:
        outliers_for_reason = [outlier for outlier in hashable if outlier.reason == reason]
        if not outliers_for_reason:
            continue

        for outlier in outliers_for_reason:
            msgs = QAMessage(ms, outlier, reason=outlier.reason)

            applies_to = pqa.TargetDataSelection(vis=outlier.vis, scan=outlier.scan,
                                                 intent=outlier.intent, spw=outlier.spw,
                                                 ant=outlier.ant, pol=outlier.pol)

            score = pqa.QAScore(outlier_score, longmsg=msgs.full_message, shortmsg=msgs.short_message,
                                applies_to=applies_to, hierarchy=reason)
            score.origin = pqa.QAOrigin(metric_name=reason,
                                        metric_score=outlier.num_sigma,
                                        metric_units='sigma deviation from reference fit')
            #Use continuum scoring function, if one is given. (Deactivated)
            # if qafunction is not None:
            #     newscore = qafunction(score)
            #     score.score = newscore
            qa_scores.append(score)

    return qa_scores


def to_data_selection(tds: pqa.TargetDataSelection) -> DataSelection:
    """
    Convert a pipeline QA TargetDataSelection object to a DataSelection tuple.
    """
    hashable_vals = {attr: tuple(sorted(getattr(tds, attr))) for attr in DataSelection._fields}
    return DataSelection(**hashable_vals)


def summarise_scores(all_scores: List[pqa.QAScore], ms, qaevalf: QAScoreEvalFunc = None) -> Dict[str, List[pqa.QAScore]]:
    """
    Process a list of QAscores, replacing the detailed and highly specific
    input scores with compressed representations intended for display in the
    web log accordion, and even more generalised summaries intended for
    display as warning banners.
    """
    # list to hold the final QA scores: non-combined hidden scores, plus the
    # summarised (and less specific) accordion scores and banner scores
    final_scores: Dict[str, List[pqa.QAScore]] = {}

    # we don't want the non-combined scores reported in the web log. They're
    # useful for the QA report written to disk, but for the web log the
    # individual scores will be aggregated into general, less specific QA
    # scores.
    hidden_scores = copy.deepcopy(all_scores)
    # final_scores[pqa.WebLogLocation.HIDDEN] = hidden_scores

    # JH update to spec for PIPE-477:
    #
    # After looking at the current messages, I have come to the conclusion
    # that its really not necessary for the message to relate whether the
    # outlier is slope or offset or both, or that the pol is XX or YY or both,
    # since these are easily discerned once the antenna and scan are known. So
    # that level of detail can be suppressed to keep the number of accordion
    # messages down. I have changed the example in the description accordingly.

    final_scores = []
    for hierarchy_root in ['amp_vs_freq', 'phase_vs_freq']:
        # erase just the polarisation dimension for accordion messages,
        # leaving the messages specific enough to identify the plot that
        # caused the problem
        discard = ['pol']
        msgs = combine_scores(all_scores, hierarchy_root, discard, ms)
        final_scores.extend(msgs)

        # add a 1.0 accordion score for metrics that generated no outlier
        if not msgs:
            msname = ms.split('/')[-1]
            metric_axes, outlier_description, extra_description = REASONS_TO_TEXT[hierarchy_root]
            # Correct capitalisation as we'll prefix the metric with 'No '
            metric_axes = metric_axes.lower()
            short_msg = 'No {} outliers'.format(metric_axes)
            long_msg = 'No {} {} detected for {}'.format(metric_axes, outlier_description, msname)
            score = pqa.QAScore(1.0,
                                longmsg=long_msg,
                                shortmsg=short_msg,
                                hierarchy=hierarchy_root,
                                applies_to=pqa.TargetDataSelection(vis={msname}))
            score.origin = pqa.QAOrigin(metric_name=hierarchy_root,
                                        metric_score=0,
                                        metric_units='number of outliers')
            final_scores.append(score)

    #Use continuum scoring function, if one is given.
    if qaevalf is not None:
        for fq in final_scores:
            newscore = qaevalf(fq)
            fq.score = newscore
            fq.longmsg = qaevalf.long_msg
            fq.shortmsg = qaevalf.short_msg

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

def combine_scores(all_scores: List[pqa.QAScore],
                   hierarchy_base: str,
                   discard: List[str],
                   ms) -> List[pqa.QAScore]:
    """
    Combine and summarise a list of QA scores.

    QA scores that share a base metric type and/or differ only in the data
    selection dimensions given in discard are aggregated and summarised
    together.
    """
    all_scores = copy.deepcopy(all_scores)

    # We're going to merge slope and offset outliers together into a single
    # record. We do not need to group or partition the input data by QA score
    # as every outlier gets the same score. If that condition changes, the
    # algorithm below will need to change to group and process by score value
    # too.

    # create a filter function to leave the scores we want to process
    filter_fn = lambda qa_score: qa_score.hierarchy.startswith(f'{hierarchy_base}.')

    # get all QA scores generated by a '<metric> vs X' algorithm
    scores_for_metric = [o for o in all_scores if filter_fn(o)]

    # create a data structure that map data selections to the scores that
    # apply to them
    ds = map_data_selection_to_scores(scores_for_metric)

    # Combine data selections that differ only by polarisation. This erases
    # the distinction between pol=0 and pol=1, placing QA scores in a single
    # data selection that spans all polarisations (other data selection
    # dimensions aside).
    no_pols = discard_dimension(ds, discard)

    # hierarchically merge adjacent data selections
    merged_scores = compress_data_selections(no_pols, DataSelection._fields)

    # filter out to leave one QA score, the score with the highest-valued
    # metric, in the list. This also resets the QAScore.applies_to to match
    # the data selection.
    as_min = take_min_as_representative(merged_scores)

    # we can now discard the DataSelection keys, as the QAScore has now been
    # updated with the correct data selection
    qa_scores = [score for score_list in as_min.values() for score in score_list]

    # rewrite the score messages
    for qa_score in qa_scores:
        # QAMessage takes an Outlier namedtuple, but TargetDataSelection
        # shares enough of the Outlier interface (vis, spw, scan, intent,
        # etc.) that we can pass QAMessage directly without converting to
        # an Outlier
        msgs = QAMessage(ms, qa_score.applies_to, reason=hierarchy_base)
        qa_score.shortmsg = msgs.short_message
        qa_score.longmsg = msgs.full_message
        qa_score.hierarchy = hierarchy_base

    return qa_scores


def take_min_as_representative(to_merge: DataSelectionToScores) -> DataSelectionToScores:
    """
    Filter out all but the worst score per data selection.

    Note that this function also rewrites QAScore.applies_to to match the data
    selection the QA score applies to.

    This function operates on a dict that maps DataSelections to list of QA
    scores. For each list, it discards all but the worst score as determined
    by the metric. The selection will be biased towards metrics whose value
    distribution tends higher than other metrics, but it seem the best we can
    do.
    """
    result: DataSelectionToScores = {}
    for ds, all_scores_for_ds in to_merge.items():
        # get score with worst metric
        scores_and_metrics = [(1-o.score, o.origin.metric_score, o) for o in all_scores_for_ds]

        # PIPE-634: hif_applycal crashes with TypeError: '>' not supported
        # between instances of 'QAScore' and 'QAScore'
        #
        # When the score and metric score are equal, max starts comparing the
        # QAScores themselves, which fails as the comparison operators are
        # not implemented. From the perspective of a 'worst score' calculation
        # the scores are equal so it doesn't matter which one we take. Hence,
        # we can supply an ordering function which simply excludes the QAScore
        # object from the calculation.
        def omit_qascore_instance(t):
            return t[0], t[1]

        worst_score = max(scores_and_metrics, key=omit_qascore_instance)[2]

        c = copy.deepcopy(worst_score)
        c.applies_to = pqa.TargetDataSelection(**ds._asdict())

        result[ds] = [c]

    return result


def discard_dimension(to_merge: DataSelectionToScores, attrs: Iterable[str]) -> DataSelectionToScores:
    """
    Aggregate QA scores held in one or more DataSelection dimensions,
    discarding data selection indices for those dimensions.

    This function discards DataSelection dimensions. Say four QA scores were
    registered, one each to spws 16, 18, 20, and 22. Calling this function
    with attrs=['spw'] would combine those scores into a single DataSelection
    with spw='', i.e., spw data selection is left unspecified.
    """
    new_dsts = {}
    for data_selection, qa_scores in to_merge.items():
        new_attrs = {attr: tuple() for attr in attrs}
        new_ds = data_selection._replace(**new_attrs)

        # note that the QA scores themselves still have the data selection
        # specifiers in their .applies_to. For example, when asked to discard
        # pol, the DataSelection keys in the the result object would have
        # pol='' but the list of QA scores held as a value for that key would
        # still have pol=0 or pol=1, etc.
        if new_ds in new_dsts:
            new_dsts[new_ds].extend(qa_scores)
        else:
            new_dsts[new_ds] = copy.deepcopy(qa_scores)
    return new_dsts


def map_data_selection_to_scores(scores: Iterable[pqa.QAScore]) -> DataSelectionToScores:
    """
    Expand QAScores to a dict-based data structure that maps data selections
    to the QA scores applicable to that selection.

    :param scores: scores to decompose
    """
    return {to_data_selection(score.applies_to): [score] for score in scores}


def compress_data_selections(to_merge: DataSelectionToScores,
                             attrs_to_merge: Reversible[str]) -> DataSelectionToScores:
    """
    Combine adjacent data selections to give a new data structure that
    expresses the same data selection but in a more compressed form.

    A data selection applies over various dimensions: spw, field, pol, etc..
    This function merges data selections hierarchically, identifying adjacent
    data selections per data selection dimension given in attrs_to_merge, and
    concatenating those adjacent dimension indices together.
    """
    to_merge = copy.deepcopy(to_merge)

    # We can identify data selections that apply to the same data except for a
    # particular field by creating a new tuple that omits that field and
    # sorting/grouping on the reduced tuple. This function is used to create
    # the reduced tuple that 'ignores' the specified field(s)
    def get_keyfunc(cols_to_ignore: List[str]):
        def keyfunc(ds: DataSelection):
            return tuple(getattr(ds, field) for field in ds._fields if field not in cols_to_ignore)
        return keyfunc

    # hierarchical merging of tuple fields, grouping/merging in reverse order of
    # DataSelection fields. This has the effect of merging DataSelections from
    # the bottom up, e.g., first merge data selections that differ only in pol,
    # then merge data selections that differ only in ant, etc.
    keys_to_merge = to_merge.keys()
    for attr in reversed(attrs_to_merge):
        key_func = get_keyfunc([attr])
        keys_to_merge = sorted(keys_to_merge, key=key_func)
        to_add = {}
        to_del = []
        for k, g in itertools.groupby(keys_to_merge, key_func):
            # k is the data selection minus the ignored field, while g
            # iterates over the data selections matching k that differ only in
            # the ignored field. These selections in g can be merged.
            group = list(g)
            # convert from ((1, ), (2, ), (5, )) to (1, 2, 5)
            merged_vals = tuple(itertools.chain(*(getattr(g, attr) for g in group)))

            # we now need to reconstruct the full data selection tuple from the
            # reduced tuple in k plus the values we've just merged. We do this
            # by created a dict of DataSelection named arguments for everything
            # except the field we've merged...
            d = {o: p for p, o in zip(k, (f for f in DataSelection._fields if f != attr))}
            # ... and then adding the merged field with the merged value
            d[attr] = merged_vals

            merged_ds = DataSelection(**d)
            to_add[merged_ds] = list(itertools.chain(*[to_merge[o] for o in group]))
            to_del.extend(group)

        for k in to_del:
            del to_merge[k]
        to_merge.update(to_add)

        # replace the original unmerged data with our merged selections and
        # we're ready to go round again for the next field
        keys_to_merge = list(to_merge.keys())

    return to_merge
