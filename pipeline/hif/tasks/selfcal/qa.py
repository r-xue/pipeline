"""QA score module for selfcal task."""
import collections
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import selfcal


LOG = logging.get_logger(__name__)


class SelfcalQAHandler(pqa.QAPlugin):
    result_cls = selfcal.SelfcalResults
    child_cls = None

    def handle(self, context, result):

        scores = []

        is_newmode = False
        targets = []
        targets_exception = []          # An exception is triggered during the selfcal-solver execution
        targets_attempt = []            # At least one solint is attempted in the selfcal-solver.
        targets_success = []            # Selfcal is successful and a solution is applied
        targets_improved = []           # Selfcal is applied and the RMS is improved
        targets_unimproved = []         # Selfcal is applied but the RMS is not improved

        for target in result.targets:

            slib = target['sc_lib']
            vislist = slib['vislist']
            solints = target['sc_solints']
            band = target['sc_band'].replace('_', ' ')

            targets.append((target['field_name'], band))
            if target['sc_exception']:
                targets_exception.append((target['field_name'], band))
                continue
            if not set(solints).isdisjoint(slib[vislist[-1]].keys()):
                targets_attempt.append((target['field_name'], band))
            else:
                continue
            if slib['SC_success']:
                targets_success.append((target['field_name'], band))
            else:
                continue
            if slib['RMS_final'] < 1.02*slib['RMS_orig'] and slib['RMS_final'] != -99.0 and slib['RMS_orig'] != -99.0:
                targets_improved.append((target['field_name'], band))
            else:
                targets_unimproved.append((target['field_name'], band))

        if not targets:
            score = None
            longmsg = 'No self-calibration attempted, modes not supported (e.g. ephemeris targets)'
            shortmsg = longmsg
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets_exception:
            score = 0.8
            longmsg = f'An exception is triggered when resolving self-calibration solutions: {targets_exception}'
            shortmsg = longmsg
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets and len(targets) == len(targets_attempt) and not targets_success:
            # pass
            score = 0.99
            longmsg = 'No field has sufficient SNR to attempt self-calibration. lomg'
            shortmsg = 'No field has sufficient SNR to attempt self-calibration. short'
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets_success and not targets_unimproved:
            score = 0.98
            targets_desc = utils.commafy([name+' / '+band for name, band in targets_improved], quotes=False)
            longmsg = f'Self-calibrations applied for {targets_desc}.'
            shortmsg = longmsg
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets_success and targets_unimproved:
            score = 0.85
            targets_desc1 = utils.commafy([name+' / '+band for name, band in targets_success], quotes=False)
            targets_desc2 = utils.commafy([name+' / '+band for name, band in targets_improved], quotes=False)
            targets_desc3 = utils.commafy([name+' / '+band for name, band in targets_unimproved], quotes=False)
            longmsg = f'Self-calibrations applied for {targets_desc1}. SNR and RMS improved for fields {targets_desc2}. SNR improved but RMS increased by more than 2% not for {targets_desc3}'
            shortmsg = longmsg
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if is_newmode:
            score = 0.90
            longmsg = 'new mode used during self-calibration: [mosaics / long baseline heuristics]'
            shortmsg = longmsg
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        result.qa.pool[:] = scores


class SelfcalListQAHandler(pqa.QAPlugin):
    result_cls = collections.abc.Iterable
    child_cls = selfcal.SelfcalResults

    def handle(self, context, result):

        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
