import collections
from itertools import chain
from typing import List, Tuple, Callable, Set

import pipeline.h.tasks.importdata.importdata as importdata
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from pipeline.domain.field import Field
from pipeline.domain.measurementset import MeasurementSet
from . import almaimportdata

LOG = logging.get_logger(__name__)


class ALMAImportDataListQAHandler(pqa.QAPlugin):
    result_cls = collections.Iterable
    child_cls = importdata.ImportDataResults
    generating_task = almaimportdata.ALMAImportData

    def handle(self, context, result):
        super().handle(context, result)

        # Check per-session parallactic angle coverage of polarisation calibration
        parallactic_threshold = result.inputs['minparang']
        # gather mses into a flat list
        mses = list(chain(*(r.mses for r in result)))

        # PIPE-597 spec states to test POLARIZATION intent
        intents_to_test = {'POLARIZATION'}
        parang_scores = _check_parallactic_angle_range(mses, intents_to_test, parallactic_threshold)

        result.qa.pool.extend(parang_scores)


class ALMAImportDataQAHandler(pqa.QAPlugin):
    result_cls = importdata.ImportDataResults
    child_cls = None
    generating_task = almaimportdata.ALMAImportData

    def handle(self, context, result):
        # Check for the presense of polarization intents
        recipe_name = context.project_structure.recipe_name
        polcal_scores = _check_polintents(recipe_name, result.mses)

        # Check for the presence of receiver bands with calibration issues
        score2 = _check_bands(result.mses)

        # Check for the presence of bandwidth switching
        score3 = _check_bwswitching(result.mses)

        # Check for science spw names matching the virtual spw ID lookup table
        score4 = _check_science_spw_names(result.mses,
                                          context.observing_run.virtual_science_spw_names)

        # Flux service usage
        score5 = _check_fluxservice(result)

        result.qa.pool.extend(polcal_scores)
        result.qa.pool.extend([score2, score3, score4, score5])


def _check_polintents(recipe_name: str, mses: List[MeasurementSet]) -> List[pqa.QAScore]:
    """
    Check each measurement set for polarization intents
    """
    return qacalc.score_polintents(recipe_name, mses)


def _check_parallactic_angle_range(mses: List[MeasurementSet], intents: Set[str], threshold: float) -> List[pqa.QAScore]:
    """
    Check that the parallactic angle coverage of the polarisation calibrator
    meets the required threshold.

    See PIPE-597 for full spec.

    :param mses: MeasurementSets to check
    :param intents: intents to measure
    :param threshold: minimum parallactic angle coverage
    :return: list of QAScores
    """
    # holds list of all QA scores for this metric
    all_scores: List[pqa.QAScore] = []

    intents_present = any([intents.intersection(ms.intents) for ms in mses])

    # group MSes per sessions, adding to default 'Shared' session if not
    # defined
    session_to_mses = collections.defaultdict(list)
    for ms in mses:
        session_to_mses[getattr(ms, 'session', 'Shared')].append(ms)

    # Check parallactic angle for each polcal in each session
    for session_name, session_mses in session_to_mses.items():
        for intent in intents:
            polcal_names = {polcal.name
                            for ms in session_mses
                            for polcal in ms.get_fields(intent=intent)}
            for polcal_name in polcal_names:
                parallactic_range = ous_parallactic_range(session_mses, polcal_name, intent)
                LOG.info(f'Parallactic angle range for {polcal_name} ({intent}) in session {session_name}: '
                         f'{parallactic_range}')
                session_scores = qacalc.score_parallactic_range(
                    intents_present, session_name, polcal_name, parallactic_range, threshold
                )
                all_scores.extend(session_scores)

    return all_scores


def _check_bands(mses) -> pqa.QAScore:
    """
    Check each measurement set for bands with calibration issues
    """
    return qacalc.score_bands(mses)


def _check_bwswitching(mses) -> pqa.QAScore:
    """
    Check each measurement set for bandwidth switching calibration issues
    """
    return qacalc.score_bwswitching(mses)


def _check_science_spw_names(mses, virtual_science_spw_names) -> pqa.QAScore:
    """
    Check science spw names
    """
    return qacalc.score_science_spw_names(mses, virtual_science_spw_names)


def _check_fluxservice(result) -> pqa.QAScore:
    """
    Check flux service usage
    """
    return qacalc.score_fluxservice(result)


#- functions to measure parallactic angle coverage of polarisation calibrator ------------------------------------------

# Type aliases for the parallactic angle computations.
ParallacticAngle = float
SignedAngle = float
PositiveDefiniteAngle = float


def ous_parallactic_range(mses: List[MeasurementSet], field_name: str, intent: str):
    """
    Get the parallactic angle range across all measurement sets for field f
    when observed with the specifiedintent.

    :param mses: MeasurementSets to process
    :param f: Field to inspect
    :param intent: observing intent to consider
    :return: angular range expressed as (min angle, max angle) tuple
    """
    angles = []
    for ms in mses:
        fields = ms.get_fields(task_arg=field_name)
        if len(fields) != 1:
            LOG.error('Cannot determine parallactic angle for %s field: %s', ms.basename, field_name)
            continue
        field = fields[0]

        try:
            angles.extend(parallactic_range_for_field(ms, field, intent))
        except ValueError as e:
            LOG.exception('Could not determine parallactic angle', exc_info=e)
            continue

    if not angles:
        return

    signed_range = range_after_processing(angles, to_signed)
    pd_range = range_after_processing(angles, to_positive_definite)

    return min((signed_range, pd_range))


def parallactic_range_for_field(ms: MeasurementSet, f: Field, intent: str) -> Tuple[float, float]:
    """
    Get the parallactic angle range for field f when observed with the specified
    intent.
    
    :param ms: MeasurementSet to process
    :param f: Field to inspect
    :param intent: observing intent to consider
    :return: angular range expressed as (min angle, max angle) tuple
    """
    # get the scans when the field was observed with the required intent
    scans = ms.get_scans(field=f.id, scan_intent=intent)
    if not scans:
        raise ValueError(f'No scans detected for field {f} with intent {intent}')

    # This uses the earliest start and latest end times of the scan domain
    # objects. In theory, times within a scan can be field and spw dependent so
    # they may not exactly match those of the field in question, but they
    # should be accurate enough for our purposes
    min_utc = min([s.start_time for s in scans], key=utils.get_epoch_as_datetime)
    max_utc = max([s.end_time for s in scans], key=utils.get_epoch_as_datetime)

    min_pa = parallactic_angle_at_epoch(f, min_utc)
    max_pa = parallactic_angle_at_epoch(f, max_utc)

    if min_pa > max_pa:
        min_pa, max_pa = max_pa, min_pa

    return min_pa, max_pa


def parallactic_angle_at_epoch(f: Field, e: dict) -> float:
    """
    Get the instantaneous parallactic angle for field f at epoch e.

    :param f: Field domain object
    :param e: CASA epoch
    :return: angular separation in degrees
    """
    me = casatools.measures
    qa = casatools.quanta

    try:
        me.doframe(me.observatory('ALMA'))
        me.doframe(e)

        pole_direction = me.direction('J2000', '0deg', '+90deg')
        pole_azel = me.measure(pole_direction, 'AZEL')
        field_azel = me.measure(f.mdirection, 'AZEL')

        separation = me.posangle(field_azel, pole_azel)
        sep_degs = qa.convert(separation, 'deg')

        return sep_degs['value']

    finally:
        me.done()


def range_after_processing(fs: List[float], g: Callable[[float], float]):
    """
    Get the range of a list of floats (fs) once processed by function g.
    """
    processed = [g(f) for f in fs]
    return max(processed) - min(processed)


def to_signed(angle: ParallacticAngle) -> SignedAngle:
    if angle > 180:
        return angle-360
    return angle


def to_positive_definite(angle: ParallacticAngle) -> PositiveDefiniteAngle:
    if angle < 0:
        return angle+360
    return angle

#- end parallactic angle coverage functions ----------------------------------------------------------------------------


