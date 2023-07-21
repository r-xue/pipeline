import collections

import numpy as np

import pipeline.h.tasks.exportdata.aqua as aqua
import pipeline.domain.measures as measures
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as scorecalc
import pipeline.qa.utility.scorers as scorers
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from . import resultobjects

LOG = logging.get_logger(__name__)


class TcleanQAHandler(pqa.QAPlugin):
    result_cls = resultobjects.TcleanResult
    child_cls = None

    def handle(self, context, result):

        qaTool = casa_tools.quanta

        # calculate QA score comparing RMS against clean threshold

        # Add offset of 0.34 to avoid any red scores
        imageScorer = scorers.erfScorer(1.0, 5.0, 0.34)

        # Basic imaging score
        # For the time being the VLA calibrator imaging is generating an error
        # due to the dynamic range limitation. Bypass the real score here.
        if 'VLASS' in result.imaging_mode:
            result.qa.pool[:] = [pqa.QAScore(1.0)]
        elif 'VLA' in result.imaging_mode and 'VLASS' not in result.imaging_mode:
            snr = result.image_max / result.image_rms
            score = scorecalc.linear_score(x=snr, x1=5, x2=100, y1=0.0, y2=1.0)  # CAS-10925
            # Set score messages and origin.
            longmsg = ('{} pbcor image max / non-pbcor image RMS = {:0.2f}'.format(result.sourcename, snr))
            shortmsg = 'snr = {:0.2f}'.format(snr)
            result.qa.pool[:] = [pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)]
        else:
            # Check for any cleaning errors and render a zero score
            if (result.error is not None):
                result.qa.pool.append(pqa.QAScore(0.0, longmsg=result.error.longmsg, shortmsg=result.error.shortmsg, weblog_location=pqa.WebLogLocation.UNSET))

            # Image RMS based score
            try:
                # For the score we compare the image RMS with the DR corrected
                # sensitivity as an estimate of the expected RMS.
                rms_score = imageScorer(result.image_rms / result.dr_corrected_sensitivity)

                if (np.isnan(rms_score)):
                    rms_score = 0.0
                    longmsg='Cleaning diverged, RMS is NaN. Field: %s Intent: %s SPW: %s' % (result.inputs['field'], result.intent, result.spw)
                    shortmsg='RMS is NaN'
                else:
                    if rms_score > 0.66:
                        longmsg = 'RMS vs. DR corrected sensitivity. Field: %s Intent: %s SPW: %s' % (result.inputs['field'], result.intent, result.spw)
                        shortmsg = 'RMS vs. sensitivity'
                    else:
                        # The level of 2.7 comes from the Erf scorer limits of 1 and 5.
                        # The level needs to be adjusted if these limits are modified.
                        longmsg = 'Observed RMS noise exceeds DR corrected sensitivity by more than 2.7. Field: %s Intent: %s SPW: %s' % (result.inputs['field'], result.intent, result.spw)
                        shortmsg = 'RMS vs. sensitivity'

                    # Adjust RMS based score if there were PSF fit errors
                    if result.bad_psf_channels is not None:
                        if result.bad_psf_channels.shape[0] <= 10:
                            rms_score -= 0.11
                            rms_score = max(0.0, rms_score)
                            longmsg = '%s. Between 1-10 channels show significantly deviant synthesized beam(s), this is usually indicative of bad data, if at cube edge can likely be ignored.' % (longmsg)
                            shortmsg = '%s. 1-10 channels masked.' % (shortmsg)
                        else:
                            rms_score -= 0.34
                            rms_score = max(0.0, rms_score)
                            longmsg = '%s. More than 10 channels show significantly deviant synthesized beams, this is usually indicative of bad data, and should be investigated.' % (longmsg)
                            shortmsg = '%s. > 10 channels masked.' % (shortmsg)

                origin = pqa.QAOrigin(metric_name='image rms / sensitivity',
                                      metric_score=(result.image_rms, result.dr_corrected_sensitivity),
                                      metric_units='Jy / beam')
            except Exception as e:
                LOG.warning('Exception scoring imaging result by RMS: %s. Setting score to -0.1.' % (e))
                rms_score = -0.1
                longmsg = 'Exception scoring imaging result by RMS: %s. Setting score to -0.1.' % (e)
                shortmsg = 'Exception scoring imaging result by RMS'
                origin = pqa.QAOrigin(metric_name='N/A', metric_score='N/A', metric_units='N/A')

            # Add score to pool
            result.qa.pool.append(pqa.QAScore(rms_score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))

            # MOM8_FC based score
            if result.mom8_fc is not None and result.mom8_fc_peak_snr is not None:
                try:
                    mom8_fc_score = scorecalc.score_mom8_fc_image(result.mom8_fc,
                                                                  result.mom8_fc_peak_snr,
                                                                  result.mom8_10_fc_histogram_asymmetry,
                                                                  result.mom8_fc_max_segment_beams,
                                                                  result.mom8_fc_frac_max_segment)
                    result.qa.pool.append(mom8_fc_score)
                except Exception as e:
                    LOG.warning('Exception scoring MOM8 FC image result: %s. Setting score to -0.1.' % (e))
                    result.qa.pool.append(pqa.QAScore(-0.1, longmsg='Exception scoring MOM8 FC image result: %s' % (e), shortmsg='Exception scoring MOM8 FC image result', weblog_location=pqa.WebLogLocation.UNSET))

            # Check source score
            # Be careful about the source name vs field name issue
            if result.intent == 'CHECK' and result.inputs['specmode'] == 'mfs':
                try:
                    mses = [context.observing_run.get_ms(name=vis) for vis in result.inputs['vis']]
                    fieldname = result.sourcename
                    spwid = int(result.spw)
                    imagename = result.image
                    rms = result.image_rms

                    # For now handling per EB case only
                    if len(result.vis) == 1:
                        try:
                            ms_do = context.observing_run.get_ms(result.vis[0])
                            field_id = [field.id for field in ms_do.fields if utils.dequote(field.name) == utils.dequote(fieldname)][0]
                            real_spwid = context.observing_run.virtual2real_spw_id(spwid, ms_do)
                            fluxresult = [fr for fr in ms_do.derived_fluxes[str(field_id)] if fr.spw_id == real_spwid][0]
                            gfluxscale = float(fluxresult.I.to_units(measures.FluxDensityUnits.MILLIJANSKY))
                            gfluxscale_err = float(fluxresult.uncertainty.I.to_units(measures.FluxDensityUnits.MILLIJANSKY))
                        except Exception as e:
                            gfluxscale = None
                            gfluxscale_err = None
                    else:
                        gfluxscale = None
                        gfluxscale_err = None

                    checkscore, offset, offset_err, beams, beams_err, fitflux, fitflux_err, fitpeak = scorecalc.score_checksources(mses, fieldname, spwid, imagename, rms, gfluxscale, gfluxscale_err)
                    result.qa.pool.append(checkscore)

                    result.check_source_fit = {'offset': offset, 'offset_err': offset_err, 'beams': beams, 'beams_err': beams_err, 'fitflux': fitflux, 'fitflux_err': fitflux_err, 'fitpeak': fitpeak, 'gfluxscale': gfluxscale, 'gfluxscale_err': gfluxscale_err}
                except Exception as e:
                    result.check_source_fit = {'offset': 'N/A', 'offset_err': 'N/A', 'beams': 'N/A', 'beams_err': 'N/A', 'fitflux': 'N/A', 'fitflux_err': 'N/A', 'fitpeak': 'N/A', 'gfluxscale': 'N/A', 'gfluxscale_err': 'N/A'}
                    LOG.warning('Exception scoring check source fit: %s. Setting score to -0.1.' % (e))
                    result.qa.pool.append(pqa.QAScore(-0.1, longmsg='Exception scoring check source fit: %s' % (e), shortmsg='Exception scoring check source fit'))

            # Polarization calibrators
            if result.intent == 'POLARIZATION' and result.inputs['specmode'] in ('mfs', 'cont') and result.imaging_mode == 'ALMA':
                try:
                    # Fit I, Q and U image planes
                    imagename = result.image.replace('.pbcor', '')

                    # Calculate POLI/POLA images
                    imstat_arg = {'imagename': result.residual, 'axes': [0, 1]}
                    job = casa_tasks.imstat(**imstat_arg)
                    calstat = job.execute(dry_run=False)
                    rms = calstat['rms']
                    prms = np.sqrt(rms[1]**2. + rms[2]**2.)

                    poli_imagename = imagename.replace('IQUV', 'POLI')
                    immath_arg = {'imagename': imagename, 'outfile': poli_imagename, 'mode': 'poli', 'sigma': '0.0Jy/beam'}
                    job = casa_tasks.immath(**immath_arg)
                    res = job.execute(dry_run=False)
                    pola_imagename = imagename.replace('IQUV', 'POLA')
                    immath_arg = {'imagename': imagename, 'outfile': pola_imagename, 'mode': 'pola', 'polithresh': '%.8fJy/beam' % (5.0*prms)}
                    job = casa_tasks.immath(**immath_arg)
                    res = job.execute(dry_run=False)

                    # Fit I, Q and U images
                    imfit_arg = {'imagename': imagename, 'stokes': 'I', 'box': '110,110,145,145'}
                    job = casa_tasks.imfit(**imfit_arg)
                    res_I = job.execute(dry_run=False)
                    if res_I is None or not res_I['converged']:
                        msg = f'Fitting Stokes I for {imagename} failed'
                        raise Exception(msg)

                    imfit_arg = {'imagename': imagename, 'stokes': 'Q', 'box': '115,115,130,130'}
                    job = casa_tasks.imfit(**imfit_arg)
                    res_Q = job.execute(dry_run=False)
                    if res_Q is None or not res_Q['converged']:
                        msg = f'Fitting Stokes Q for {imagename} failed'
                        raise Exception(msg)

                    imfit_arg = {'imagename': imagename, 'stokes': 'U', 'box': '110,110,145,145'}
                    job = casa_tasks.imfit(**imfit_arg)
                    res_U = job.execute(dry_run=False)
                    if res_U is None or not res_U['converged']:
                        msg = f'Fitting Stokes U for {imagename} failed'
                        raise Exception(msg)

                    # Extract the flux and error values for each Stokes
                    flux_I = res_I['results']['component0']['flux']['value'][0]
                    unit_I = res_I['results']['component0']['flux']['unit']
                    error_I = res_I['results']['component0']['flux']['error'][0]

                    flux_Q = res_Q['results']['component0']['flux']['value'][1]
                    unit_Q = res_Q['results']['component0']['flux']['unit']
                    error_Q = res_Q['results']['component0']['flux']['error'][1]

                    flux_U = res_U['results']['component0']['flux']['value'][2]
                    unit_U = res_U['results']['component0']['flux']['unit']
                    error_U = res_U['results']['component0']['flux']['error'][2]

                    # Now use these values to compute polarization intensity, angle and ratio, and their errors:
                    flux_pol_intens  = np.sqrt(flux_Q**2 + flux_U**2)
                    err_pol_intens = np.sqrt((flux_Q*error_U)**2 + (flux_U*error_Q)**2) / flux_pol_intens

                    pol_ratio     = flux_pol_intens / flux_I
                    err_pol_ratio = pol_ratio * np.sqrt((err_pol_intens/flux_pol_intens)**2 + (error_I/flux_I)**2)

                    pol_angle     = 0.5 * np.degrees(np.arctan2(flux_U, flux_Q))
                    err_pol_angle = 0.5 * np.degrees(err_pol_intens / flux_pol_intens)

                    result.polcal_fit = {'session': context.observing_run.get_ms(result.vis[0]).session,
                                         'converged': True,
                                         'flux_pol_intens': qaTool.quantity(flux_pol_intens, unit_I),
                                         'err_pol_intens': qaTool.quantity(err_pol_intens, unit_I),
                                         'pol_ratio': qaTool.convert(qaTool.quantity(pol_ratio, ''), '%'),
                                         'err_pol_ratio': qaTool.convert(qaTool.quantity(err_pol_ratio, ''), '%'),
                                         'pol_angle': qaTool.quantity(pol_angle, 'deg'),
                                         'err_pol_angle': qaTool.quantity(err_pol_angle, 'deg'),
                                         'err_msg': ''}
                except Exception as e:
                    LOG.error(str(e))
                    result.polcal_fit = {'session': context.observing_run.get_ms(result.vis[0]).session,
                                         'converged': False,
                                         'flux_pol_intens': 'N/A',
                                         'err_pol_intens': 'N/A',
                                         'pol_ratio': 'N/A',
                                         'err_pol_ratio': 'N/A',
                                         'pol_angle': 'N/A',
                                         'err_pol_angle': 'N/A',
                                         'err_msg': str(e)}

class TcleanListQAHandler(pqa.QAPlugin):
    result_cls = collections.Iterable
    child_cls = resultobjects.TcleanResult

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated


aqua_exporter = aqua.xml_generator_for_metric('ScoreChecksources', '{:0.3}')
aqua.register_aqua_metric(aqua_exporter)
