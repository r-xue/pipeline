
import collections
import copy

import matplotlib.pyplot as plt
import numpy as np
import pkg_resources

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api

LOG = infrastructure.get_logger(__name__)


class RflagDevHeuristic(api.Heuristic):
    """Heuristics for Rflag thresholds.
    see PIPE-685/987
    """

    def __init__(self):
        self.vla_sefd = self._get_vla_sefd()

    def calculate(self, ms, rflag_report):

        vlabasebands = ms.get_vla_baseband_spws(science_windows_only=True)

        bbspws = [list(map(int, i.split(','))) for i in vlabasebands]
        rms_scale = self._get_spw_rms_scale(ms, self.vla_sefd)

        if 'rflag' in rflag_report['type'] == 'rflag':
            return self._correct_rflag_ftdev(rflag_report, bbspws, spw_rms_scale=rms_scale)
        else:
            LOG.error("Invalid input rflag report")
            return None

    def _get_vla_sefd(self):
        """Load the VLA SEFD profile"""
        sedf_path = pkg_resources.resource_filename('pipeline', 'hifv/heuristics/sefd')

        bands = ['L', 'S', 'C', 'X', 'Ku', 'K', 'Ka', 'Q']
        sefd = collections.OrderedDict()
        for band in bands:
            sefd[band] = np.loadtxt(sedf_path+'/'+band+'.txt', skiprows=1, comments='#')

        return sefd

    def plot_sefd(self, spws_per_band=None, figfile='vla_sefd.png'):
        """Generate the VLA SEFD summary plot"""

        fig, ax = plt.subplots(figsize=(8, 4))

        for band, sedf_per_band in self.vla_sefd.items():
            ax.plot(sedf_per_band[:, 0], sedf_per_band[:, 1], label=band)

        ax.set_xlabel('Freq. [MHz]')
        ax.set_ylabel('SEFD [Jy]')
        ax.set_xscale('log')
        ax.set_yscale('log')
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        yrange = ax.get_ylim()

        if spws_per_band is not None:
            for band, basebands_per_band in spws_per_band.items():
                for baseband, spws_per_baseband in basebands_per_band.items():
                    for idx, spw_dict in enumerate(spws_per_baseband):
                        for spw_id, spw_freq_range in spw_dict.items():
                            dlog10 = np.log10(yrange[1]/yrange[0])
                            yhline = yrange[0]*10**(dlog10*(0.75+0.01*idx))
                            ax.hlines(yhline, float(spw_freq_range[0].value)/1e6,
                                      float(spw_freq_range[1].value)/1e6, color='k')

        fig.savefig('sedf.png', bbox_inches='tight')
        plt.close(fig)

        return

    def _correct_rflag_ftdev(self, rflag_report, bbspws, spw_rms_scale=None):
        """derive the corrected freqdev/timedev for applying rflag 

        Args:
            rflag_report:  the "rflag" report dictionary in the returne from flagdata(action='calculation',mode='rflag',..)

        - The return report structure of flagdata(action='calculation',mode='rflag',..) from
        the freq-domain and time-domain analysis (see additional details in flagdata documenttaion) is exepcted
        to be:
            report['freqdev'|'timedev']:    (sum_i(nspw_of_field_i), 3) 
            report['*dev'][:, 0]:           FldId
            report['*dev'][:, 1]:           SpwId
            report['freqdev'][:, 2]:        Estimated freqdev/timedev
                                            note: this is not the flagging threshold (which is defined as dev*devscale).
        
        - the median-based clip of spw rms within each baseband/field is summarized in CAS-11598 and PIPE-685/987
        """
        rms_scale_lookup = np.ones(int(np.max(rflag_report['freqdev'][:, 1]))+1)
        if spw_rms_scale is not None:
            for spw_id in range(rms_scale_lookup.size):
                if str(spw_id) in spw_rms_scale:
                    rms_scale_lookup[spw_id] = spw_rms_scale[str(spw_id)]

        new_report = copy.deepcopy(rflag_report)

        freqdev = rflag_report['freqdev']
        timedev = rflag_report['timedev']

        for ftdev in ['freqdev', 'timedev']:

            fields = rflag_report[ftdev][:, 0]
            spws = rflag_report[ftdev][:, 1]
            devs = rflag_report[ftdev][:, 2]
            devs_scale = rms_scale_lookup[rflag_report[ftdev][:, 1].astype(int)]

            ufields = np.unique(fields)
            for ifield in ufields:
                fldmask = np.where(fields == ifield)
                if len(fldmask[0]) == 0:
                    continue  # no data matching field
                # filter spws and threshes whose fields==ifield
                field_spws = spws[fldmask]
                field_devs_scaled = devs[fldmask]/devs_scale[fldmask]

                for ibbspws in bbspws:
                    spwmask = np.where(np.array([ispw in ibbspws for ispw in field_spws]) == True)
                    if len(spwmask[0]) == 0:
                        continue  # no data matching ibbspws
                    # filter threshes whose fields==ifield and spws in ibbspws
                    spw_field_devs_scaled = field_devs_scaled[spwmask]
                    med_devs_scaled = np.median(spw_field_devs_scaled)
                    medmask = np.where(spw_field_devs_scaled > med_devs_scaled)
                    outmask = fldmask[0][spwmask[0][medmask]]
                    new_report[ftdev][:, 2][outmask] = med_devs_scaled*devs_scale[outmask]

        return new_report

    def _get_spw_rms_scale(self, ms, science_windows_only=True):
        """[summary]

        Args:
            ms ([type]): [description]
            sefd_database ([type]): [description]
            science_windows_only (bool, optional): [description]. Defaults to True.

        Returns:
            [type]: [description]
        
        - The abitary rms scaling factor (spw_rms_scale) is calcualted as SEFD/chanwidth_mhz^0.5
        """

        spw_rms_scale = dict()

        for spw in ms.get_spectral_windows(science_windows_only=science_windows_only):
            chanwidth_mhz = spw.channels[0].getWidth().to_units(measures.FrequencyUnits.MEGAHERTZ)
            try:
                band = spw.name.split('#')[0].split('_')[1]
                baseband = spw.name.split('#')[1]
                sedf_per_band = self.vla_sefd[band]
                frequency_mhz = spw.mean_frequency.to_units(measures.FrequencyUnits.MEGAHERTZ)
                spw_sefd = np.interp(float(frequency_mhz), sedf_per_band[:, 0],
                                     sedf_per_band[:, 1], left=np.nan, right=np.nan)
                if frequency_mhz < min(sedf_per_band[:, 0]) or frequency_mhz > max(sedf_per_band[:, 0]):
                    LOG.warn("The mean frequency of spw {!s} is out of the SEFD profile coverage.".format(spw.id))
            except Exception as ex:
                LOG.warn("Exception: Baseband name cannot be parsed.{!s}".format(str(ex)))
                LOG.warn("Exception: Fail to query SEFD for {!s}. {!s}".format(spw.id, str(ex)))
                spw_sefd = np.nan

            if np.isnan(spw_sefd):
                LOG.warn("The SEFD information of spw {!s} is not avaialble.\
                        The rms scale caclulation will assume a fidudial SEFD of 500Jy ")
                spw_sefd = 500.

            spw_rms_scale[str(spw.id)] = spw_sefd/float(chanwidth_mhz)**0.5

        return spw_rms_scale
