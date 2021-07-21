
import collections
import copy

import matplotlib.pyplot as plt
import numpy as np
import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
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

    def _get_vla_sefd(self):
        """Load the VLA SEFD profile.

        Note: P-band (200-500 Mhz)and 4-band (54-86 MHz) are not in the database.
        """
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
                                         'devs': devs,
                                         'bb': bbs,
                                         'sefd_jy': sefd_jy,
                                         'chanwidth_mhz': chanwidth_mhz,
                                         'rms_scale': rms_scale,
                                         'devs_med': devs_med,
                                         'devs_modified': devs_modified}
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
                                                     'chanwidth_mhz': chanwidth_mhz}
                            LOG.debug('spw {:>3}  ChanWidth = {:6.2f} MHz'.format(
                                spw_id, chanwidth_mhz))
                            continue

                        try:
                            sedf_per_band = self.vla_sefd[band]
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
                        LOG.debug('spw {:>3}   SEFD = {:8.2f} Jy   ChanWidth = {:6.2f} MHz'.format(
                            spw_id, spw_sefd, chanwidth_mhz))

        return spw_rms_scale
