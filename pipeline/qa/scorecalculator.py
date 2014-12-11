'''
Created on 9 Jan 2014

@author: sjw
'''
import collections
import datetime
import operator

import pipeline.domain.measures as measures
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa

__all__ = ['score_polintents',                                # ALMA specific
           'score_bands',                                     # ALMA specific
           'score_bwswitching',                               # ALMA specific
	   'score_tsysspwmap',                                # ALMA specific
	   'score_missing_derived_fluxes',                    # ALMA specific
	   'score_derived_fluxes_snr',                        # ALMA specific
	   'score_phaseup_mapping_fraction',                  # ALMA specific
	   'score_missing_phaseup_snrs',                      # ALMA specific
	   'score_missing_bandpass_snrs',                     # ALMA specific
	   'score_poor_phaseup_solutions',                    # ALMA specific
	   'score_poor_bandpass_solutions',                   # ALMA specific
	   'score_setjy_measurements',         
           'score_missing_intents',
           'score_ephemeris_coordinates',
           'score_online_shadow_agents',
           'score_total_data_flagged',
           'score_ms_model_data_column_present',
           'score_ms_history_entries_present',
           'score_contiguous_session']

LOG = logging.get_logger(__name__)

#- utility functions ---------------------------------------------------------

def log_qa(method):
    """
    Decorator that logs QA evaluations as they return with a log level of
    INFO for scores of 1.0 and WARNING for any other level.
    """
    def f(self, *args, **kw):
        # get the size of the CASA log before task execution
        qascore = method(self, *args, **kw)
        if qascore.score == 1.0:
            LOG.info(qascore.longmsg)
        else:
            LOG.warning(qascore.longmsg)
        return qascore

    return f

# struct to hold flagging statistics
AgentStats = collections.namedtuple("AgentStats", "name flagged total")

def calc_flags_per_agent(summaries):
    stats = []
    for idx in range(0, len(summaries)):
        flagcount = int(summaries[idx]['flagged'])
        totalcount = int(summaries[idx]['total'])

        # From the second summary onwards, subtract counts from the previous 
        # one
        if idx > 0:
            flagcount = flagcount - int(summaries[idx-1]['flagged'])
        
        stat = AgentStats(name=summaries[idx]['name'],
                          flagged=flagcount,
                          total=totalcount)
        stats.append(stat)

    return stats

def linear_score(x, x1, x2, y1=0.0, y2=1.0):
    """
    Calculate the score for the given data value, assuming the
    score follows a linear gradient between the low and high values.
    
    x values will be clipped to lie within the range x1->x2
    """
    x1 = float(x1)
    x2 = float(x2)
    y1 = float(y1)
    y2 = float(y2)
    
    clipped_x = sorted([x1, x, x2])[1]
    m = (y2-y1) / (x2-x1)
    c = y1 - m*x1
    return m*clipped_x + c

def score_data_flagged_by_agents(ms, summaries, min_frac, max_frac, 
                                 agents=None):
    """
    Calculate a score for the agentflagger summaries based on the fraction of
    data flagged by certain flagging agents.

    min_frac < flagged < max_frac maps to score of 1-0
    """
    agent_stats = calc_flags_per_agent(summaries)

    if agents is None:
        agents = []
    match_all_agents = True if len(agents) is 0 else False

    # sum the number of flagged rows for the selected agents     
    frac_flagged = reduce(operator.add, 
                          [float(s.flagged)/s.total for s in agent_stats
                           if s.name in agents or match_all_agents])

    score = linear_score(frac_flagged, min_frac, max_frac, 1.0, 0.0)
    percent = 100.0*frac_flagged
    longmsg = ('%0.2f%% data in %s flagged by %s flagging agents'
               '' % (percent, ms.basename, utils.commafy(agents, False)))
    shortmsg = '%0.2f%% data flagged' % percent
    
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)
    
#- exported scoring functions ------------------------------------------------

def score_ms_model_data_column_present(all_mses, mses_with_column):
    """
    Give a score for a group of mses based on the number with modeldata 
    columns present.
    None with modeldata - 100% with modeldata = 1.0 -> 0.5
    """
    num_with = len(mses_with_column)
    num_all = len(all_mses)
    f = float(num_with) / num_all

    if mses_with_column:
        # log a message like 'No model columns found in a.ms, b.ms or c.ms'
        basenames = [ms.basename for ms in mses_with_column]
        s = utils.commafy(basenames, quotes=False)
        longmsg = 'Model data column found in %s' % s
        shortmsg = '%s/%s have MODELDATA' % (num_with, num_all) 
    else:
        # log a message like 'Model data column was found in a.ms and b.ms'
        basenames = [ms.basename for ms in all_mses]
        s = utils.commafy(basenames, quotes=False, conjunction='or')
        longmsg = ('No model data column found in %s' % s)            
        shortmsg = 'MODELDATA empty' 

    score = linear_score(f, 0.0, 1.0, 1.0, 0.5)

    return pqa.QAScore(score, longmsg, shortmsg)


@log_qa
def score_ms_history_entries_present(all_mses, mses_with_history):
    """
    Give a score for a group of mses based on the number with history 
    entries present.
    None with history - 100% with history = 1.0 -> 0.5
    """
    num_with = len(mses_with_history)
    num_all = len(all_mses)

    if mses_with_history:
        # log a message like 'Entries were found in the HISTORY table for 
        # a.ms and b.ms'
        basenames = utils.commafy([ms.basename for ms in mses_with_history],
                                  quotes=False)
        if len(mses_with_history) is 1:
            longmsg = ('Unexpected entries were found in the HISTORY table of %s. '
                        'This measurement set may already be processed.'
                        '' % basenames)
        else:
            longmsg = ('Unexpected entries were found in the HISTORY tables of %s. '
                       'These measurement sets may already be processed.'
                        '' % basenames)                
        shortmsg = '%s/%s have HISTORY' % (num_with, num_all) 

    else:
        # log a message like 'No history entries were found in a.ms or b.ms'
        basenames = [ms.basename for ms in all_mses]
        s = utils.commafy(basenames, quotes=False, conjunction='or')
        longmsg = 'No HISTORY entries found in %s' % s
        shortmsg = 'No HISTORY entries'

    f = float(num_with) / num_all
    score = linear_score(f, 0.0, 1.0, 1.0, 0.5)

    return pqa.QAScore(score, longmsg, shortmsg)

@log_qa
def score_bwswitching(mses):
    """
    Score a MeasurementSet object based on the presence of 
    bandwidth switching observings. For bandwidth switched
    observations the TARGET and PHASE spws are different.
    """

    score = 1.0
    num_mses = len(mses)
    all_ok = True
    complaints = []

    # analyse each MS
    for ms in mses:

        # Get the science spws
        scispws = set([spw.id for spw in ms.get_spectral_windows(science_windows_only=True)])

        # Get phase calibrator science spw ids
        phasespws = []
        for scan in ms.get_scans(scan_intent='PHASE'):
            phasespws.extend([spw.id for spw in scan.spws])
        phasespws = set(phasespws).intersection(scispws)

        # Get science target science spw ids
        targetspws = []
        for scan in ms.get_scans(scan_intent='TARGET'):
            targetspws.extend([spw.id for spw in scan.spws])
        targetspws = set(targetspws).intersection(scispws)

        # Determine the difference between the two
        nophasecals = targetspws.difference(phasespws)
        if len(nophasecals) == 0:
            continue

        # Score the difference
        all_ok = False
        for _ in nophasecals:
            score += (-1.0 / num_mses / len(nophasecals))
        longmsg = ('%s contains no phase calibrations for target spws %s'
            '' % (ms.basename, utils.commafy(nophasecals, False)))
        complaints.append(longmsg)

    if all_ok:
        longmsg = ('Phase calibrations found for all target spws in %s.' % (
                   utils.commafy([ms.basename for ms in mses], False)))
        shortmsg = 'Phase calibrations found for all target spws' 
    else:
        longmsg = '%s.' % utils.commafy(complaints, False)
        shortmsg = 'No phase calibrations found for target spws %s' % list(nophasecals)
        
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_bands(mses):
    """
    Score a MeasurementSet object based on the presence of 
    ALMA bands with calibration issues.
    """

    # ALMA receiver bands. Warnings will be raised for any 
    # measurement sets containing the following bands.
    score = 1.0
    score_map = {'8'  : -1.0,
                 '9'  : -1.0}

    unsupported = set(score_map.keys())

    num_mses = len(mses)
    all_ok = True
    complaints = []

    # analyse each MS
    for ms in mses:
        msbands = []
        for spw in ms.get_spectral_windows(science_windows_only=True):
            # This does not work for old data
            #match = re.match(r'ALMA_RB_(?P<band>\d+)', spw.name)
            # Get rid of the leading 0 in the band number
            #bandnum = str(int(match.groupdict()['band']))
            bandnum = spw.band.split(' ')[2]
            msbands.append(bandnum)
        msbands = set(msbands)
        overlap = unsupported.intersection(msbands)
        if not overlap:
            continue
        all_ok = False
        for m in overlap:
            score += (score_map[m] / num_mses)
        longmsg = ('%s contains band %s data'
            '' % (ms.basename, utils.commafy(overlap, False)))
        complaints.append(longmsg)

    if all_ok:
        longmsg = ('No high frequency %s band data were found in %s.' % (list(unsupported),
                   utils.commafy([ms.basename for ms in mses], False)))
        shortmsg = 'No high frequency band data found' 
    else:
        longmsg = '%s.' % utils.commafy(complaints, False)
        shortmsg = 'High frequency band data found' 
        
    # Make score linear
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_polintents(mses):
    """
    Score a MeasurementSet object based on the presence of 
    polarization intents.
    """

    # Polarization intents. Warnings will be raised for any 
    # measurement sets containing these intents. Ignore the
    # array type for now.
    score = 1.0
    score_map = {'POLARIZATION'  : -1.0,
                 'POLANGLE'      : -1.0,
                 'POLLEAKAGE'    : -1.0}

    unsupported = set(score_map.keys())

    num_mses = len(mses)
    all_ok = True
    complaints = []

    # analyse each MS
    for ms in mses:
        # are these intents present in the ms
        overlap = unsupported.intersection(ms.intents)
        if not overlap:
            continue
        all_ok = False
        for m in overlap:
            score += (score_map[m] / num_mses)

        longmsg = ('%s contains %s polarization calibration intents'
            '' % (ms.basename, utils.commafy(overlap, False)))
        complaints.append(longmsg)

    if all_ok:
        longmsg = ('No polarization calibration intents were found in '
                   '%s.' % utils.commafy([ms.basename for ms in mses], False))
        shortmsg = 'No polarization calibrators found'
    else:
        longmsg = '%s.' % utils.commafy(complaints, False)
        shortmsg = 'Polarization calibrators found'
        
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_missing_intents(mses, array_type='ALMA_12m'):
    """
    Score a MeasurementSet object based on the presence of certain
    observing intents.
    """
    # Required calibration intents. Warnings will be raised for any 
    # measurement sets missing these intents 
    score = 1.0
    if array_type == 'ALMA_TP':
        score_map = {'ATMOSPHERE' : -1.0}
    else:
        score_map = {'PHASE'     : -1.0,
                     'BANDPASS'  : -0.1,
                     'AMPLITUDE' : -0.1}

    required = set(score_map.keys())

    num_mses = len(mses)
    all_ok = True
    complaints = []

    # analyse each MS
    for ms in mses:
        # do we have the necessary calibrators?
        if not required.issubset(ms.intents):
            all_ok = False
            missing = required.difference(ms.intents)
            for m in missing:
                score += (score_map[m] / num_mses)

            longmsg = ('%s is missing %s calibration intents'
                       '' % (ms.basename, utils.commafy(missing, False)))
            complaints.append(longmsg)
            
    if all_ok:
        longmsg = ('All required calibration intents were found in '
                   '%s.' % utils.commafy([ms.basename for ms in mses], False))
        shortmsg = 'All calibrators found'
    else:
        longmsg = '%s.' % utils.commafy(complaints, False)
        shortmsg = 'Calibrators missing'
        
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_ephemeris_coordinates(mses):

    """
    Score a MeasurementSet object based on the presence of possible
    ephemeris coordinates.
    """

    score = 1.0

    num_mses = len(mses)
    all_ok = True
    complaints = []
    zerodirection = casatools.measures.direction('j2000', '0.0deg', '0.0deg')

    # analyse each MS
    for ms in mses:

        # Examine each source
	for source in ms.sources:
	    if source.ra == casatools.quanta.formxxx(zerodirection['m0'], format='hms', prec=3) or \
	        source.dec == casatools.quanta.formxxx(zerodirection['m1'], format='dms', prec=2):
		all_ok = False
                score += (-1.0 / num_mses)
		longmsg =  ('Suspicious source coordinates for  %s in %s'
		    '' % (source.name, ms.basename))
		complaints.append(longmsg)

    if all_ok:
        longmsg = ('All source coordinates OK in '
                   '%s.' % utils.commafy([ms.basename for ms in mses], False))
        shortmsg = 'All source coordinates OK'
    else:
        longmsg = '%s.' % utils.commafy(complaints, False)
        shortmsg = 'Suspicious source coordinates'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_online_shadow_agents(ms, summaries):
    """
    Get a score for the fraction of data flagged by online and shadow agents.

    0 < score < 1 === 50% < frac_flagged < 20%
    """
    return score_data_flagged_by_agents(ms, summaries, 0.2, 0.5, ['online', 'shadow'])


@log_qa
def score_total_data_flagged(filename, summaries):
    """
    Calculate a score for the flagging task based on the total fraction of
    data flagged.
    
    0%-5% flagged   -> 1
    5%-50% flagged  -> 0.5
    50-100% flagged -> 0
    """    
    agent_stats = calc_flags_per_agent(summaries)

    # sum the number of flagged rows for the selected agents     
    frac_flagged = reduce(operator.add, 
                          [float(s.flagged)/s.total for s in agent_stats])

    if frac_flagged > 0.5:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.5, 1.0, 0.5)

    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was flagged' % (percent, filename)
    shortmsg = '%0.2f%% data flagged' % percent
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)


@log_qa
def score_fraction_newly_flagged(filename, summaries):
    """
    Calculate a score for the flagging task based on the fraction of
    data newly flagged.
    
    0%-5% flagged   -> 1
    5%-50% flagged  -> 0.5
    50-100% flagged -> 0
    """    
    agent_stats = calc_flags_per_agent(summaries)

    # sum the number of flagged rows for the selected agents     
    frac_flagged = reduce(operator.add, 
                          [float(s.flagged)/s.total for s in agent_stats[1:]], 0)
        
    if frac_flagged > 0.5:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.5, 1.0, 0.5)

    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was newly flagged' % (percent, filename)
    shortmsg = '%0.2f%% data flagged' % percent
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)
@log_qa


def linear_score_fraction_newly_flagged(filename, summaries):
    """
    Calculate a score for the flagging task based on the fraction of
    data newly flagged.
    
    fraction flagged   -> score
    """    
    agent_stats = calc_flags_per_agent(summaries)

    # sum the number of flagged rows for the selected agents     
    frac_flagged = reduce(operator.add, 
                          [float(s.flagged)/s.total for s in agent_stats[1:]], 0)

    score = 1.0 - frac_flagged        

    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was newly flagged' % (percent, filename)
    shortmsg = '%0.2f%% data flagged' % percent
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)


@log_qa
def score_contiguous_session(mses, tolerance=datetime.timedelta(hours=1)):
    """
    Check whether measurement sets are contiguous in time. 
    """
    # only need to check when given multiple measurement sets
    if len(mses) < 2:
        return pqa.QAScore(1.0,
                           longmsg='%s forms one continuous observing session.' % mses[0].basename,
                           shortmsg='Unbroken observing session')

    # reorder MSes by start time
    by_start = sorted(mses, 
                      key=lambda m : utils.get_epoch_as_datetime(m.start_time)) 

    # create an interval for each one, including our tolerance    
    intervals = []    
    for ms in by_start:
        start = utils.get_epoch_as_datetime(ms.start_time)
        end = utils.get_epoch_as_datetime(ms.end_time)
        interval = measures.TimeInterval(start - tolerance, end + tolerance)
        intervals.append(interval)

    # check whether the intervals overlap
    bad_mses = []
    for i, (interval1, interval2) in enumerate(zip(intervals[0:-1], 
                                                   intervals[1:])):
        if not interval1.overlaps(interval2):
            bad_mses.append(utils.commafy([by_start[i].basename,
                                           by_start[i+1].basename]))

    if bad_mses:
        basenames = utils.commafy(bad_mses, False)
        longmsg = ('Measurement sets %s are not contiguous. They may be '
                   'miscalibrated as a result.' % basenames)
        shortmsg = 'Gaps between observations'
        score = 0.5
    else:
        basenames = utils.commafy([ms.basename for ms in mses])
        longmsg = ('Measurement sets %s are contiguous.' % basenames)
        shortmsg = 'Unbroken observing session'
        score = 1.0

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)


@log_qa
def score_wvrgcal(ms_name, wvr_score):
    if wvr_score < 1.0:
        score = 0
    else:
        score = linear_score(wvr_score, 1.0, 2.0, 0.5, 1.0)

    longmsg = 'RMS improvement was %0.2f for %s' % (wvr_score, ms_name)
    shortmsg = '%0.2fx improvement' % wvr_score
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_sdtotal_data_flagged(name, ant, spw, pol, frac_flagged):
    """
    Calculate a score for the flagging task based on the total fraction of
    data flagged.
    
    0%-5% flagged   -> 1
    5%-50% flagged  -> 0.5
    50-100% flagged -> 0
    """
    if frac_flagged > 0.5:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.5, 1.0, 0.5)
    
    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s (Ant=%s, SPW=%d, Pol=%d) was flagged' % (percent, name, ant, spw, pol)
    shortmsg = '%0.2f%% data flagged' % percent
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_tsysspwmap (ms, unmappedspws):

    '''
    Score is equal to the fraction of unmapped windows
    '''

    if len(unmappedspws) <= 0:
        score = 1.0
        longmsg = 'Tsys spw map is complete for %s ' % ms.basename
        shortmsg = 'Tsys spw map is complete'
    else:
        nscispws = len([spw.id for spw in ms.get_spectral_windows(science_windows_only=True)])
        if nscispws <= 0:
            score = 0.0
        else:
            score = float(nscispws - len (unmappedspws)) / float(nscispws)
        longmsg = 'Tsys spw map is incomplete for %s science window%s ' % (ms.basename, utils.commafy(unmappedspws, False, 's'))
        shortmsg = 'Tsys spw map is incomplete'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_setjy_measurements (ms, reqfields, reqintents, reqspws, measurements):

    '''
    Score is equal to the ratio of the number of actual flux
    measurements to expected number of flux measurements
    '''

    # Expected fields
    scifields = set ([field for field in ms.get_fields (reqfields, intent=reqintents)])

    # Expected science windows
    scispws = set([spw.id for spw in ms.get_spectral_windows(reqspws, science_windows_only=True)])

    # Loop over the expected fields
    nexpected = 0
    for scifield in scifields:
	validspws = set([spw.id for spw in scifield.valid_spws])
	nexpected = nexpected + len(validspws.intersection(scispws))

    # Loop over the measurements
    nmeasured = 0
    for key, value in measurements.iteritems():
        # Loop over the flux measurements
        for flux in value:
            nmeasured = nmeasured + 1

    # Compute score
    if nexpected == 0:
        score = 0.0
        longmsg = 'No flux calibrators for %s ' % ms.basename
        shortmsg = 'No flux calibrators'
    elif nmeasured == 0:
        score = 0.0
        longmsg = 'No flux measurements for %s ' % ms.basename
        shortmsg = 'No flux measurements'
    elif nexpected == nmeasured:
        score = 1.0
        longmsg = 'All expected flux calibrator measurements present for %s ' % ms.basename
        shortmsg = 'All expected flux calibrator measurements present'
    elif nmeasured < nexpected:
        score = float(nmeasured) / float(nexpected)
        longmsg = 'Missing flux calibrator measurements for %s %d/%d ' % (ms.basename, nmeasured, nexpected)
        shortmsg = 'Missing flux calibrator measurements'
    else:
	score = 0.0
        longmsg = 'Too many flux calibrator measurements for %s %d/%d' % (ms.basename, nmeasured, nexpected)
        shortmsg = 'Too many flux measurements'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_missing_derived_fluxes (ms, reqfields, reqintents, measurements):

    '''
    Score is equal to the ratio of actual flux
    measurement to expected flux measurements
    '''

    # Expected fields
    scifields = set ([field for field in ms.get_fields (reqfields, intent=reqintents)])

    # Expected science windows
    scispws = set([spw.id for spw in ms.get_spectral_windows(science_windows_only=True)])

    # Loop over the expected fields
    nexpected = 0
    for scifield in scifields:
	validspws = set([spw.id for spw in scifield.valid_spws])
	nexpected = nexpected + len(validspws.intersection(scispws))

    # Loop over measurements
    nmeasured = 0
    for key, value in measurements.iteritems():
        # Loop over the flux measurements
        for flux in value:
	    fluxjy = getattr (flux, 'I').to_units(measures.FluxDensityUnits.JANSKY)
	    uncjy = getattr (flux.uncertainty, 'I').to_units(measures.FluxDensityUnits.JANSKY)
	    if fluxjy <= 0.0 or uncjy <= 0.0: 
	         continue
            nmeasured = nmeasured + 1

    # Compute score
    if nexpected == 0:
        score = 0.0
        longmsg = 'No secondary calibrators for %s ' % ms.basename
        shortmsg = 'No secondary calibrators'
    elif nmeasured == 0:
        score = 0.0
        longmsg = 'No derived fluxes for %s ' % ms.basename
        shortmsg = 'No derived fluxes'
    elif nexpected == nmeasured:
        score = 1.0
        longmsg = 'All expected derived fluxes present for %s ' % ms.basename
        shortmsg = 'All expected derived fluxes present'
    elif nmeasured < nexpected:
        score = float(nmeasured) / float(nexpected)
        longmsg = 'Missing derived fluxes for %s %d/%d' % (ms.basename, nmeasured, nexpected)
        shortmsg = 'Missing derived fluxes'
    else:
	score = 0.0
        longmsg = 'Extra derived fluxes for %s %d/%d' % (ms.basename, nmeasured, nexpected)
        shortmsg = 'Extra derived fluxes'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_phaseup_mapping_fraction(ms, reqfields, reqintents, phaseup_spwmap):
    '''
    Compute the fraction of science spws that have not been
    mapped to other probably  wider windows.
    '''

    if not phaseup_spwmap:
        score = 1.0
        longmsg = 'No mapped narrow science spws for %s ' % ms.basename
        shortmsg = 'No mapped narrow science spws'
    else:
        # Expected fields
        #scifields = set ([field for field in ms.get_fields (reqfields, intent=reqintents)])

        # Expected science windows
        scispws = set([spw.id for spw in ms.get_spectral_windows(science_windows_only=True)])
	nexpected = len (scispws)

        # Loop over the expected fields
        #nexpected = 0
        #for scifield in scifields:
	    #validspws = set([spw.id for spw in scifield.valid_spws])
	    #nexpected = nexpected + len(validspws.intersection(scispws))

        nunmapped = 0
	for spwid in scispws:
	    if spwid == phaseup_spwmap[spwid]: 
	        nunmapped = nunmapped + 1
	
	if nunmapped >= nexpected:
            score = 1.0
            longmsg = 'No mapped science spws for %s ' % ms.basename
            shortmsg = 'No mapped science spws'
	else:
	    score =  float(nunmapped) / float(nexpected) 
            longmsg = 'There are %d mapped narrow science spws for %s ' % (nexpected - nunmapped, ms.basename)
            shortmsg = 'There are mapped narrow science spws'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_missing_phaseup_snrs(ms, spwids, phsolints):

    '''
    Score is the fraction of spws with phaseup SNR estimates
    '''

    # Compute the number of expected and missing SNR measurements
    nexpected = len(spwids)
    missing_spws = []
    for i in range (len(spwids)):
        if not phsolints[i]:
	    missing_spws.append(spwid[i])
    nmissing = len(missing_spws) 

    if nmissing <= 0:
        score = 1.0
        longmsg = 'No missing phaseup SNR estimates for %s ' % ms.basename
        shortmsg = 'No missing phaseup SNR estimates'
    else:
        score = float (nexpected - nmissing) / nexpected
        longmsg = 'Missing phaseup SNR estimates for spws %s in %s ' % \
	    (missing_spws, ms.basename)
        shortmsg = 'Missing phaseup SNR estimates'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_poor_phaseup_solutions(ms, spwids, nphsolutions, min_nsolutions):

    '''
    Score is the fraction of spws with poor phaseup solutions
    '''

    # Compute the number of expected and poor SNR measurements
    nexpected = len(spwids)
    poor_spws = []
    for i in range (len(spwids)):
        if not nphsolutions[i]:
	    poor_spws.append(spwid[i])
	elif nphsolutions[i] < min_nsolutions:
	    poor_spws.append(spwid[i])
    npoor = len(poor_spws) 

    if npoor <= 0:
        score = 1.0
        longmsg = 'No poorly determined phaseup solutions for %s ' % ms.basename
        shortmsg = 'No poorly determined phaseup solutions'
    else:
        score = float (nexpected - npoor) / nexpected
        longmsg = 'Poorly determined phaseup solutions for spws %s in %s ' % \
	    (poor_spws, ms.basename)
        shortmsg = 'Poorly determined phaseup solutions'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_missing_bandpass_snrs(ms, spwids, bpsolints):

    '''
    Score is the fraction of spws with bandpass SNR estimates
    '''

    # Compute the number of expected and missing SNR measurements
    nexpected = len(spwids)
    missing_spws = []
    for i in range (len(spwids)):
        if not bpsolints[i]:
	    missing_spws.append(spwid[i])
    nmissing = len(missing_spws) 

    if nmissing <= 0:
        score = 1.0
        longmsg = 'No missing bandpass SNR estimates for %s ' % ms.basename
        shortmsg = 'No missing bandpass SNR estimates'
    else:
        score = float (nexpected - nmissing) / nexpected
        longmsg = 'Missing bandpass SNR estimates for spws %s in%s ' % \
	    (missing_spws, ms.basename)
        shortmsg = 'Missing bandpass SNR estimates'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_poor_bandpass_solutions(ms, spwids, nbpsolutions, min_nsolutions):

    '''
    Score is the fraction of spws with poor bandpass solutions
    '''

    # Compute the number of expected and poor solutions
    nexpected = len(spwids)
    poor_spws = []
    for i in range (len(spwids)):
        if not nbpsolutions[i]:
	    poor_spws.append(spwid[i])
	elif nbpsolutions[i] < min_nsolutions:
	    poor_spws.append(spwid[i])
    npoor = len(poor_spws) 

    if npoor <= 0:
        score = 1.0
        longmsg = 'No poorly determined bandpass solutions for %s ' % \
	    ms.basename
        shortmsg = 'No poorly determined bandpass solutions'
    else:
        score = float (nexpected - npoor) / nexpected
        longmsg = 'Poorly determined bandpass solutions for spws %s in %s ' % \
	    (poor_spws, ms.basename)
        shortmsg = 'Poorly determined bandpass solutions'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

@log_qa
def score_derived_fluxes_snr (ms, measurements):

    '''
    Score the SNR of the derived flux measurements.
        1.0 if SNR > 20.0
	0.0 if SNR < 5.0
	linear scale between 0.0 and 1.0 in between
    '''

    # Loop over measurements
    nmeasured = 0
    score = 0.0
    minscore = 1.0
    for key, value in measurements.iteritems():
        # Loop over the flux measurements
        for flux in value:
	    fluxjy = getattr (flux, 'I').to_units(measures.FluxDensityUnits.JANSKY)
	    uncjy = getattr (flux.uncertainty, 'I').to_units(measures.FluxDensityUnits.JANSKY)
	    if fluxjy <= 0.0 or uncjy <= 0.0: 
	         continue
	    snr = fluxjy / uncjy
            nmeasured = nmeasured + 1
	    if float(snr) <= 5.0:
	        score1 = 0.0
	    elif float(snr)  >= 20.0:
	        score1 = 1.0
	    else:
	        score1 = linear_score (float(snr), 5.0, 20.0, 0.0, 1.0)
	    minscore = min (minscore, score1)
	    score = score + score1
    if nmeasured > 0:
        score = score / nmeasured

    if nmeasured == 0:
        score = 0.0
        longmsg = 'No derived fluxes for %s ' % ms.basename
        shortmsg = 'No derived fluxes'
    elif minscore >= 1.0:
        score = 1.0
        longmsg = 'No low SNR derived fluxes for %s ' % ms.basename
        shortmsg = 'No low SNR derived fluxes'
    else:
        longmsg = 'Low SNR derived fluxes for %s ' % ms.basename
        shortmsg = 'Low SNR derived fluxes'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)
