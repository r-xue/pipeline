import collections
import copy
import uuid

import matplotlib.pyplot as plt
import numpy as np
import shutil
import re

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
from pipeline.infrastructure import casa_tools, casa_tasks
import pkg_resources

LOG = infrastructure.get_logger(__name__)


class RflagDevHeuristic(api.Heuristic):
    """Heuristics for Rflag thresholds.
    see PIPE-685/987
    """

    def __init__(self, ms, ignore_sefd=False):
        self.vla_sefd = self._get_vla_sefd()
        self.ms = ms
        self.spw_rms_scale = self._get_spw_rms_scale(ignore_sefd=ignore_sefd)

    def calculate(self, rflag_report):

        _, baseband_spws_list = self.ms.get_vla_baseband_spws(science_windows_only=True, return_select_list=True)

        if 'rflag' in rflag_report['type'] == 'rflag':
            return self._correct_rflag_ftdev(rflag_report)
        else:
            LOG.error("Invalid input rflag report")
            return None

    @staticmethod
    def _get_vla_sefd():
        """Load the VLA SEFD profile.

        PIPE-987: See the ticket attachement for the description of SEFD data files
        Note: The band names here are capitalized, but the labels from SPW names are all caps.
        """
        sedf_path = pkg_resources.resource_filename('pipeline', 'hifv/heuristics/sefd')

        bands = ['4', 'P', 'L', 'S', 'C', 'X', 'Ku', 'K', 'Ka', 'Q']
        sefd = collections.OrderedDict()
        for band in bands:
            sefd[band] = np.loadtxt(sedf_path+'/'+band+'.txt', skiprows=1, comments='#')

        return sefd

    @staticmethod
    def _plot_vla_sefd(sefd, baseband_spws=None, figfile='vla_sefd.png'):
        """Generate the VLA SEFD summary plot."""

        fig, ax = plt.subplots(figsize=(8, 4))

        for band, sefd_per_band in sefd.items():
            ax.plot(sefd_per_band[:, 0], sefd_per_band[:, 1], label=band)

        ax.set_xlabel('Freq. [MHz]')
        ax.set_ylabel('SEFD [Jy]')
        ax.set_xscale('log')
        ax.set_yscale('log')
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        yrange = ax.get_ylim()

        freq_min = []
        freq_max = []
        if baseband_spws is not None:
            for band, basebands_per_band in baseband_spws.items():
                for baseband, spws_per_baseband in basebands_per_band.items():
                    for idx, spw_dict in enumerate(spws_per_baseband):
                        for spw_id, spw_freq_range in spw_dict.items():
                            dlog10 = np.log10(yrange[1]/yrange[0])
                            yhline = yrange[0]*10**(dlog10*(0.75+0.01*idx))
                            ax.hlines(yhline, float(spw_freq_range[0].value)/1e6,
                                      float(spw_freq_range[1].value)/1e6, color='k')
                            freq_min.append(float(spw_freq_range[0].value)/1e6)
                            freq_max.append(float(spw_freq_range[1].value)/1e6)
            ax.set_xlim([np.min(freq_min)/1.01, np.max(freq_max)*1.01])
        fig.savefig(figfile, bbox_inches='tight')
        plt.close(fig)

        return

    def _correct_rflag_ftdev(self, rflag_report):
        """Derive the corrected freqdev/timedev for applying rflag.

        Args:
            rflag_report:  the "rflag" report dictionary in the returne from flagdata(action='calculation',mode='rflag',..)
                           flagdata_result['report0']

        - The return report structure of flagdata(action='calculation',mode='rflag',..) from
        the freq-domain and time-domain analysis (see additional details in flagdata documenttaion) is exepcted
        to be:
            report['freqdev'|'timedev']:    (sum_i(nspw_of_field_i), 3) 
            report['*dev'][:, 0]:           FldId
            report['*dev'][:, 1]:           SpwId
            report['freqdev'][:, 2]:        Estimated freqdev/timedev
                                            note: this is not the "clipping" threshold (which is defined as dev*devscale).
        
        - the median-based rflag threshold reset scheme (within each baseband/field) is summarized in CAS-11598 and PIPE-685/987
        - A completely flagged spw+field data selection is still show up in flagdata reports, but the value will be 0.0.
        """

        new_report = copy.deepcopy(rflag_report)

        for ftdev in ['freqdev', 'timedev']:

            fields = new_report[ftdev][:, 0]
            spws = new_report[ftdev][:, 1]
            devs = new_report[ftdev][:, 2]
            devs_med = new_report[ftdev][:, 2].copy()
            bbs = np.full_like(spws, -1)

            _, baseband_spws_list = self.ms.get_vla_baseband_spws(science_windows_only=True, return_select_list=True)
            for bb_idx, bb_spws in enumerate(baseband_spws_list):
                bb_mask = np.isin(spws, bb_spws)
                bbs[bb_mask] = bb_idx

            spws_unique, spws_unique_inverse = np.unique(spws, return_inverse=True)

            spws_unique_rms_scale = np.array([self.spw_rms_scale[int(spw)]['rms_scale'] for spw in spws_unique])
            rms_scale = spws_unique_rms_scale[spws_unique_inverse]
            spws_unique_sefd_jy = np.array([self.spw_rms_scale[int(spw)]['sefd_jy'] for spw in spws_unique])
            sefd_jy = spws_unique_sefd_jy[spws_unique_inverse]
            spws_unique_chanwidth_mhz = np.array([self.spw_rms_scale[int(spw)]['chanwidth_mhz'] for spw in spws_unique])
            chanwidth_mhz = spws_unique_chanwidth_mhz[spws_unique_inverse]

            fields_bbs = np.column_stack((fields, bbs))
            _, fields_bbs_inverse = np.unique(fields_bbs, return_inverse=True, axis=0)
            for idx in np.unique(fields_bbs_inverse):
                select_mask = (fields_bbs_inverse == idx) & (devs > 0)
                if np.any(select_mask):
                    devs_med[select_mask] = rms_scale[select_mask]*np.median(devs[select_mask]/rms_scale[select_mask])
            devs_modified = np.minimum(devs, devs_med)
            new_report[ftdev] = np.column_stack((fields, spws, devs_modified))
            new_report[ftdev+'_info'] = {'field': fields,
                                         'spw': spws,
                                         'bb': bbs,
                                         'devs': devs,
                                         'sefd_jy': sefd_jy,
                                         'ch_mhz': chanwidth_mhz,
                                         'rms_scale': rms_scale,
                                         'devs_med': devs_med,
                                         'devs_use': devs_modified}
            ftdev_report = new_report[ftdev+'_info']

            str_message = ' {:<6s} '.format('field')
            str_message += ' {:<20s} '.format('spw_id/name')
            str_message += ' {:<3s} '.format('bb')
            colnum_names = ['devs', 'sefd_jy', 'ch_mhz', 'rms_scale', 'devs_med', 'devs_use']
            str_message += (' {:<10s} '*len(colnum_names)).format(*colnum_names)
            LOG.debug('rflag '+ftdev+' heuristic')
            LOG.debug(str_message)
            for idx, field in enumerate(ftdev_report['field']):
                str_message = ' {:<6.0f} '.format(ftdev_report['field'][idx])
                spw_id = ftdev_report['spw'][idx]
                spw_name = self.ms.get_spectral_window(spw_id).name
                str_message += ' {:<3.0f} {:<16s} '.format(spw_id, spw_name)
                colnum_values = [ftdev_report[name][idx] for name in colnum_names]
                str_message += ' {:<3.0f} '.format(ftdev_report['bb'][idx])
                str_message += (' {:<10.4g} '*len(colnum_names)).format(*colnum_values)
                LOG.debug(str_message)

        return new_report

    def _get_spw_rms_scale(self, science_windows_only=True, ignore_sefd=False):
        """[summary]

        Args:
            ms ([type]): [description]
            sefd_database ([type]): [description]
            science_windows_only (bool, optional): [description]. Defaults to True.

        Returns:
            [type]: [description]
        
        By default, the rms scaling factor per spw (spw_rms_scale) is defined as: SEFD_jy/chanwidth_mhz^0.5.
        If ignore_sefd=True, spw_rms_scale would be simplily as: 1/chanwidth_mhz^0.5. This is equiavelent to the VLASS specific assumption of a uniform SEFD (see PIPE-685/987).
        Note: spw_rms_scale is only useful in a relative sense when comparing threstical rms from different spws from same scans.
        """

        spw_rms_scale = dict()

        baseband_spws, baseband_spws_list = self.ms.get_vla_baseband_spws(
            science_windows_only=science_windows_only, return_select_list=True)
        for band in baseband_spws:
            for baseband in baseband_spws[band]:
                for spwitem in baseband_spws[band][baseband]:
                    for spw_id, spw_info in spwitem.items():

                        chanwidth_mhz = float(spw_info[3].to_units(measures.FrequencyUnits.MEGAHERTZ))
                        if ignore_sefd:
                            spw_rms_scale[spw_id] = {'rms_scale': 1./chanwidth_mhz**0.5,
                                                     'sefd_jy': 1.,
                                                     'chanwidth_mhz': chanwidth_mhz}
                            LOG.debug('spw {:>3}   ChanWidth = {:6.2f} MHz   rms_scale = {:8.2f}'.format(
                                spw_id, chanwidth_mhz, spw_rms_scale[spw_id]['rms_scale']))
                            continue

                        try:
                            sedf_per_band = self.vla_sefd[band.capitalize()]
                            mean_freq_mhz = float(spw_info[2].to_units(measures.FrequencyUnits.MEGAHERTZ))
                            chanwidth_mhz = float(spw_info[3].to_units(measures.FrequencyUnits.MEGAHERTZ))
                            spw_sefd = np.interp(mean_freq_mhz, sedf_per_band[:, 0],
                                                 sedf_per_band[:, 1], left=np.nan, right=np.nan)
                            if mean_freq_mhz < min(sedf_per_band[:, 0]) or mean_freq_mhz > max(sedf_per_band[:, 0]):
                                LOG.warn(
                                    "The mean frequency of spw {!s} from {!s}#{!s}  is out of the SEFD profile coverage.".format(spw_id, band, baseband))

                        except Exception as ex:
                            LOG.warn("Exception: Fail to query SEFD for {!s}. {!s}".format(spw_id, str(ex)))
                            spw_sefd = np.nan

                        if np.isnan(spw_sefd):
                            LOG.warn("The SEFD information of spw {!s} is not avaialble.\
                                    The rms scale caclulation will use a fidudial SEFD of 500Jy, \
                                    equivalent to the assumption of unfirm SEFD within each baseband. ".format(spw_id))
                            spw_sefd = 500.
                        spw_rms_scale[spw_id] = {'rms_scale': spw_sefd/chanwidth_mhz**0.5,
                                                 'sefd_jy': spw_sefd,
                                                 'chanwidth_mhz': chanwidth_mhz}
                        LOG.debug('spw {:>3}   SEFD = {:8.2f} Jy   ChanWidth = {:6.2f} MHz   rms_scale = {:8.2f}'.format(
                            spw_id, spw_sefd, chanwidth_mhz,  spw_rms_scale[spw_id]['rms_scale']))

        return spw_rms_scale


def mssel_valid(vis, field='', spw='', scan='', intent='', correlation='', uvdist=''):
    """Check if the data selection is valid (i.e. not a null selection).

    This method is used as a secondary "null" selection check for flagdata() calls.
    Ideally, the primary "corr_type_string" check should be sufficient.
    """
    with casa_tools.MSReader(vis) as msfile:
        staql = {'field': field, 'spw': spw, 'scan': scan,
                 'scanintent': intent, 'polarization': correlation, 'uvdist': uvdist}
        select_valid = msfile.msselect(staql, onlyparse=True)
    return select_valid


def get_amp_range(vis, field='', spw='', scan='', intent='', datacolumn='corrected',
                  correlation='', uvrange='',
                  useflags=True,
                  timeaverage=True, timebin='1e8s', timespan=''):
    """Get amplitude min/max from a MS after applying data selection and time-averging."""
    amp_range = [0., 0.]

    try:
        if timeaverage:
            vis_averaged = '{}.ms'.format(uuid.uuid4())
            job = casa_tasks.mstransform(vis=vis, outputvis=vis_averaged, field=field, spw=spw, scan=scan, intent=intent,
                                         datacolumn=datacolumn, correlation=correlation, uvrange=uvrange,
                                         timeaverage=timeaverage, timebin=timebin, timespan=timespan,
                                         keepflags=False)
            job.execute(dry_run=False)
            amp_range = _get_amp_range1(vis_averaged, datacolumn='data', useflags=useflags)
            shutil.rmtree(vis_averaged, ignore_errors=True)
        else:
            if correlation == '' and uvrange == '':
                amp_range = _get_amp_range1(vis, field=field, spw=spw, scan=scan,
                                            intent=intent, datacolumn=datacolumn, useflags=useflags)
            else:
                amp_range = _get_amp_range2(vis, field=field, spw=spw, scan=scan, intent=intent, datacolumn=datacolumn,
                                            correlation=correlation, uvrange=uvrange,
                                            useflags=useflags)
    except Exception as ex:
        LOG.warn("Exception: Unable to obtain the range of data amps. {!s}".format(str(ex)))

    return amp_range


def _get_amp_range2(vis, field='', spw='', scan='', intent='', datacolumn='corrected',
                    correlation='', uvrange='',
                    useflags=True):
    """Get amplitude min/max from a MS, with ms.statistic().

    This approach offers additional data selection capaibility (e.g. correlation, uvrange), but lack of pre-averging capability
    - doquantiles=False to improve performance (CASR-550/CAS-13031)
    - ms.statistics(timeaverge=True) doesn't seem to work properly at this moment (likely doing scalar-averging)
    """
    amp_range = [0., 0.]

    try:
        with casa_tools.MSReader(vis) as msfile:
            stats = msfile.statistics(column=datacolumn, complex_value='amp', useweights=False, useflags=useflags,
                                      field=field, scan=scan, intent=intent, spw=spw,
                                      correlation=correlation, uvrange=uvrange,
                                      reportingaxes='', doquantiles=False,
                                      timeaverage=False, timebin='0s', timespan='')
        amp_range = [stats['']['min'], stats['']['max']]
    except Exception as ex:
        LOG.warn("Exception: Unable to obtain the range of data amps. {!s}".format(str(ex)))

    return amp_range


def _get_amp_range1(vis, field='', spw='', scan='', intent='', datacolumn='corrected',
                    useflags=True):
    """Get amplitude min/max from a MS, with ms.range().

    This is the quickest method, but doesn't offer correlation-based selection or pre-averging capability
    """
    amp_range = [0., 0.]

    try:
        with casa_tools.MSReader(vis) as msfile:
            if not (field == '' and spw == '' and scan == '' and intent == ''):
                staql = {'field': field, 'spw': spw, 'scan': scan, 'scanintent': intent}
                r_msselect = msfile.msselect(staql, onlyparse=False)
                # ms.range always works on whole rows in MS, and ms.selectpolarization() won't affect its result.
                # r_msselect = msfile.selectpolarization(['RR','LL']) # doesn't work as expected.
                if not r_msselect:
                    LOG.warn("Null selection from the field/spw/scan combination.")
                    return amp_range
            if datacolumn == 'corrected':
                item = 'corrected_amplitude'
            if datacolumn == 'data':
                item = 'amplitude'
            if datacolumn == 'model':
                item = 'model_amplitude'
            # ms.range (notably val_min) results were seen to be affected by blocksize
            # we increase the blocksize from 10MB (default) to 100MB
            amp_range = msfile.range([item], useflags=useflags, blocksize=100)[item].tolist()
    except Exception as ex:
        LOG.warn("Exception: Unable to obtain the range of data amps. {!s}".format(str(ex)))

    return amp_range


def plotms_get_xyrange(plotms_log):
    """Parse the CASAplotms log to obatin the range of plotted data points."""
    try:
        xyrange = [0, 0, 0, 0]
        idx = 0
        for log_line in plotms_log:
            log_msg = re.split("\t+", log_line)[3]
            if ' to ' in log_msg and '(flagged)' in log_msg and '(unflagged)' in log_msg:
                LOG.debug(log_line.rstrip())
                dd = re.findall(r"[-+]?\d+\.\d+(?:e-?\d+)?", log_msg)
                axis_label = log_msg.split()[0]
                xyrange[idx] = float(dd[0])
                xyrange[idx+1] = float(dd[1])
                idx += 2
    except Exception as ex:
        LOG.warn("Exception: Unable to obtain the plotted data range from CASAplotms log. {!s}".format(str(ex)))
        return [0, 0, 0, 0]

    return xyrange


def plotms_get_autorange(xyrange):
    """Estimate the autoscale plotrange from CASAplotms based on data range.

    The algorithm is translated from casa6/casa5/code/display/QtPlotter:QtDrawSettings::adjustAxis
    """
    autorange = [0, 0, 0, 0]
    for idx in [0, 2]:

        MinTicks = 4
        vmin = xyrange[idx]
        vmax = xyrange[idx+1]

        grossStep = (vmax - vmin) / MinTicks
        step = pow(10, np.floor(np.log10(grossStep)))
        if (5 * step < grossStep):
            step *= 5
        elif (2 * step < grossStep):
            step *= 2

        numTicks = int(np.ceil(vmax / step) - np.floor(vmin / step))
        autorange[idx] = np.floor(vmin / step) * step
        autorange[idx+1] = np.ceil(vmax / step) * step

    LOG.debug('estimated autoscale plotrange from CASAplotms: {!r}'.format(autorange))

    return autorange
