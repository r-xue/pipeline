
# ------------------------------------------------------------------------------

# flagdetervla.py

# NB: THESE FlagDeteVLA*() CLASSES INHERIT FlagDeterBase*() CLASSES.  AT
# PRESENT THE FlagDeterVLA*() CLASSES HAVE NO ADDITIONAL INPUT PARAMETERS, SO
# THEY ACT IN EXACTLY THE SAME MANNER AS THE FlagDeterBase*() CLASSES.

# Description:
# ------------
# This file contains the classes to perform VLA deterministic flagging.

# In a nutshell:
# --------------
# * This class performs all of the deterministic flagging types in the
#   FlagDeterBase*() classes.

# To test these classes by themselves without the rest of the pipeline, enter
# these commands:
#
# import pipeline
#
# vis = [ '<MS name>' ]
# context = pipeline.Pipeline( vis ).context
#
# inputs = pipeline.hifv.tasks.flagging.FlagDeterVLA.Inputs( context, vis=vis,
#   output_dir='.', autocorr=True, shadow=True, scan=True, scannumber='4,5,8',
#   intents='*AMPLI*', edgespw=True, fracspw=0.1, fracspwfps=0.1 )
#
# task = pipeline.hifv.tasks.flagging.FlagDeterVLA( inputs )
# jobs = task.analyse()
#
# status = task.execute()
#
# In other words, create a context, create the inputs (which sets the public
# variables to the correct values and creates the temporary flag command file),
# convert the class arguments to arguments to the CASA task tflagdata), create
# the FlatDeterVLA() instance, perform FlatDeterVLA.analyse(), and execute the
# class.

# Classes:
# --------
# FlagDeterVLA        - This class represents the pipeline interface to the
#                        CASA task tflagdata.
# FlagDeterVLAInputs  - This class manages the inputs for the FlagDeterVLA()
#                        class.
# FlagDeterVLAResults - This class manages the results from the FlagDeterVLA()
#                        class.

# Modification history:
# ---------------------
# 2012 May 10 - Nick Elias, NRAO
#               Initial version, identical behavior to FlagDeterBase.py.
# 2012 May 16 - Lindsey Davis, NRAO
#               Changed file name from FlagDeterALMA.py to flagdeteralma.py.
# 2013 May - Brian Kent, NRAO
#             New Deterministic flagging for the VLA

# ------------------------------------------------------------------------------

# Imports
# -------
import math
import os
import string

from casatasks.private import flaghelper

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.h.tasks.flagging import flagdeterbase
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

# ------------------------------------------------------------------------------

# Initialize a logger
# -------------------

LOG = infrastructure.get_logger(__name__)

# ------------------------------------------------------------------------------
# class FlagDeterVLAInputs
# ------------------------------------------------------------------------------

# FlagDeterVLAInputs

# Description:
# ------------
# This class manages the inputs for the FlagDeterVLA() class.

# Inherited classes:
# ------------------
# FlagDeterBaseInputs - This is the base class that handles the inputs for
# deterministic flagging.

# Public member variables:
# ------------------------
# None.

# Public member functions:
# ------------------------
# __init__     - This public member function constructs an instance of the
#                FlagDeterVLAInputs() class.  It is overloaded.
# clone        - This public member function creates a cloned instance of an
#                existing instance.  It is overloaded.
# to_casa_args - This public member function translates the input parameters of
#                this class to task parameters and file-based flag commands
#                required by CASA task tflagdata.  It is overloaded.

# Static public member functions:
# -------------------------------
# create_from_context - This static public member function creates an instance
#                       of this class from a context.  It is overloaded.

# Modification history:
# ---------------------
# 2012 May 10 - Nick Elias, NRAO
#               Initial version created with public member functions __init__(),
#               clone(), and to_casa_args(); and static public member function
#               create_from_context().  All functions are overloaded.

# ------------------------------------------------------------------------------


class FlagDeterVLAInputs(flagdeterbase.FlagDeterBaseInputs):

    # FlagDeterVLAInputs::__init__

    # Description:
    # ------------
    # This public member function constructs an instance of the
    # FlagDeterVLAInputs() class.

    # The primary purpose of this class is to initialize the public member
    # variables.  The defaults for all parameters (except context) are None.

    # NB: This public member function is overloaded.

    # Inherited classes:
    # ------------------
    # FlagDeterBaseInputs - This class manages the inputs for the
    #                       FlagDeterBaseInputs() parent class.

    # Inputs to initialize the FlagDeterBaseInputs() class:
    # -----------------------------------------------------
    # context      - This python dictionary contains the pipeline context (state).
    #                It has no default.
    #
    # vis          - This python string contains the MS name.
    #
    # output_dir   - This python string contains the output directory name.
    #
    # flagbackup   - This python boolean determines whether the existing flags are
    #                backed up before the new flagging begins.
    #
    # autocorr     - This python boolean determines whether autocorrelations are
    #                flagged or not.
    #
    # shadow       - This python boolean determines whether shadowed antennas are
    #                flagged or not.
    #
    # scan         - This python boolean determines whether scan flagging is
    #                performed.
    # scannumber   - This python string contains the comma-delimited scan numbers.
    #                In the task interface, it is a subparameter of the scan
    #                parameter.  Standard data selection syntax is valid.
    # intents      - This python string contains the comma-delimited intents.  In
    #                the task interface, it is a subparameter of the scan parameter.
    #                Wildcards (* character) are allowed.
    #
    # edgespw      - This python boolean determines whether edge channels are
    #                flagged.
    # fracspw      - This python float contains the fraction (between 0.0 and 1.0)
    #                of channels removed from the edge for the ALMA baseline correlator.
    #                In the task interface, it is a subparameter of the edgespw parameter.
    #
    # fracspwfps    - This python float contains the fraction (between 0.0 and 1.0)
    #                of channels removed from the edge for the ACS correlator.  In the
    #                task interface, it it is a subparameter of the edgespw parameter.
    #
    # online       - This python boolean determines whether the online flags are
    #                applied.
    # fileonline   - This python string contains the name of the ASCII file that
    #                has the flagging commands.  It is a subparameter of the
    #                online parameter.
    #
    # template     - This python boolean determines whether flagging templates are
    #                applied.
    # filetemplate - This python string contains the name of the ASCII file that
    #                has the flagging template (for RFI, birdies, telluric lines,
    #                etc.).  It is a subparameter of the template parameter.

    # Inputs:
    # -------
    # None.

    # Outputs:
    # --------
    # None, returned via the function value.

    # Modification history:
    # ---------------------
    # 2012 May 10 - Nick Elias, NRAO
    #               Initial version.

    """
    FlagDeterVLAInputs defines the inputs for the FlagDeterVLA pipeline task.
    """
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    baseband = vdp.VisDependentProperty(default=True)
    clip = vdp.VisDependentProperty(default=True)
    edgespw = vdp.VisDependentProperty(default=True)
    fracspw = vdp.VisDependentProperty(default=0.05)
    # fracspwfps = vdp.VisDependentProperty(default=0.04837)
    template = vdp.VisDependentProperty(default=True)

    @vdp.VisDependentProperty
    def intents(self):
        # return just the unwanted intents that are present in the MS
        # VLA Specific intents that need to be flagged
        intents_to_flag = {'POINTING', 'FOCUS', 'ATMOSPHERE', 'SIDEBAND',
                           'UNKNOWN', 'SYSTEM_CONFIGURATION',
                           'UNSPECIFIED#UNSPECIFIED'}
        return ','.join(self.ms.intents.intersection(intents_to_flag))

    quack = vdp.VisDependentProperty(default=True)

    def __init__(self, context, vis=None, output_dir=None, flagbackup=None, autocorr=None, shadow=None, scan=None,
                 scannumber=None, quack=None, clip=None, baseband=None, intents=None, edgespw=None, fracspw=None,
                 fracspwfps=None, online=None, fileonline=None, template=None, filetemplate=None, hm_tbuff=None,
                 tbuff=None):
        super(FlagDeterVLAInputs, self).__init__(
            context, vis=vis, output_dir=output_dir, flagbackup=flagbackup, autocorr=autocorr, shadow=shadow,
            scan=scan, scannumber=scannumber, intents=intents, edgespw=edgespw, fracspw=fracspw, fracspwfps=fracspwfps,
            online=online, fileonline=fileonline, template=template, filetemplate=filetemplate, hm_tbuff=hm_tbuff,
            tbuff=tbuff)

        # VLA-specific parameters
        self.quack = quack
        self.clip = clip
        self.baseband = baseband

    # ------------------------------------------------------------------------------

    # FlagDeterVLAInputs::to_casa_args

    # Description:
    # ------------
    # This public member function translates the input parameters of this class to
    # task parameters and file-based flag commands required by CASA task tflagdata.

    # NB: This public member function is overloaded.

    # Inputs:
    # -------
    # None.

    # Outputs:
    # --------
    # The python dictionary containing the arguments (and their values) for CASA
    # task tflagdata, returned via the function value.
    #     The the end 5 percent of each spw or minimum of 3 channelsmporary file that
    # contains the flagging commands for the tflagdata task, located in the output
    # directory.

    # Modification history:
    # ---------------------
    # 2012 May 10 - Nick Elias, NRAO
    #               Initial version.

    def to_casa_args(self):

        # Initialize the arguments from the inherited
        # FlagDeterBaseInputs() class
        task_args = super(FlagDeterVLAInputs, self).to_casa_args()

        # Return the tflagdata task arguments
        return task_args

# ------------------------------------------------------------------------------
# class FlagDeterVLAResults
# ------------------------------------------------------------------------------

# FlagDeterVLAResults

# Description:
# ------------
# This class manages the results from the FlagDeterVLA() class.

# Inherited classes:
# ------------------
# FlagDeterBaseResults - This class manages the results from the FlagDeterBase()
#                        class.

# Modification history:
# ---------------------
# 2012 May 10 - Nick Elias, NRAO
#               Initial version created with no new member functions.

# ------------------------------------------------------------------------------


class FlagDeterVLAResults(flagdeterbase.FlagDeterBaseResults):
    pass

# ------------------------------------------------------------------------------
# class FlagDeterVLA
# ------------------------------------------------------------------------------

# FlagDeterVLA

# Description:
# ------------
# This class represents the pipeline interface to the CASA task flagdata.

# Inherited classes:
# ------------------
# FlagDeterBase - This class represents the pipeline interface to the CASA task
#                 flagdata.

# Public member functions:
# ------------------------
# All public member functions from the FlagDeterVLAInputs() class.

# Modification history:
# ---------------------
# 2012 May 10 - Nick Elias, NRAO
#               Initial version.

# ------------------------------------------------------------------------------


@task_registry.set_equivalent_casa_task('hifv_flagdata')
class FlagDeterVLA(flagdeterbase.FlagDeterBase):

    # Make the member functions of the FlagDeterVLAInputs() class member
    # functions of this class

    Inputs = FlagDeterVLAInputs

    def prepare(self):
        """
        Prepare and execute a flagdata flagging job appropriate to the
        task inputs.

        This method generates, overwriting if necessary, an ASCII file
        containing flagdata flagging commands. A flagdata task is then
        executed, using this ASCII file as inputs.
        """
        # create a local alias for inputs, so we're not saying 'self.inputs'
        # everywhere
        inputs = self.inputs

        # get the flagdata command string, ready for the flagdata input file
        flag_cmds = self._get_flag_commands()
        flag_str = '\n'.join(flag_cmds)

        if flag_cmds:
            # Before summary command
            job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary', name='before')
            before_summary_dict = self._executor.execute(job)

        # write the flag commands to the file
        with open(inputs.inpfile, 'w') as stream:
            stream.writelines(flag_str)

        # to save inspecting the file, also log the flag commands
        LOG.debug('Flag commands for %s:\n%s', inputs.vis, flag_str)

        # Map the pipeline inputs to a dictionary of CASA task arguments
        task_args = inputs.to_casa_args()

        # create and execute a flagdata job using these task arguments
        job = casa_tasks.flagdata(**task_args)
        summary_dict_cmds = self._executor.execute(job)

        summary_dict = {}
        if flag_cmds:
            if 'name' in summary_dict_cmds:
                summary_dict['report1'] = summary_dict_cmds
                summary_dict['report0'] = before_summary_dict
            else:
                summary_dict = summary_dict_cmds
                summary_dict['before'] = before_summary_dict

        agent_summaries = dict((v['name'], v) for v in summary_dict.values())

        ordered_agents = ['before', 'anos', 'online', 'shadow', 'intents', 'qa0',  'qa2', 'template', 'autocorr',
                          'edgespw', 'clip', 'quack',
                          'baseband']

        summary_reps = [agent_summaries[agent]
                        for agent in ordered_agents
                        if agent in agent_summaries]

        # return the results object, which will be used for the weblog
        return FlagDeterVLAResults(summary_reps, flag_cmds)

    def _get_flag_commands(self):
        """ Adding quack and clip
        """
        # flag_cmds = super(FlagDeterVLA, self)._get_flag_commands()

        flag_cmds = []

        inputs = self.inputs

        # flag anos?
        if inputs.online:
            if not os.path.exists(inputs.fileonline):
                LOG.warning('Online ANOS flag file \'%s\' was not found. Online ANOS'
                            'flagging for %s disabled.' % (inputs.fileonline, 
                                                           inputs.ms.basename))
            else:
                # ANTENNA_NOT_ON_SOURCE FLAG
                cmdlist = self._read_flagfile(inputs.fileonline)
                flag_cmds.extend([cmd for cmd in cmdlist if ('ANTENNA_NOT_ON_SOURCE' in cmd)])
                flag_cmds.append('mode=\'summary\' name=\'anos\'')

        # Flag online?
        if inputs.online:
            if not os.path.exists(inputs.fileonline):
                LOG.warning('Online flag file \'%s\' was not found. Online '
                            'flagging for %s disabled.' % (inputs.fileonline,
                                                           inputs.ms.basename))
            else:
                cmdlist = self._read_flagfile(inputs.fileonline)
                # All other online flags
                flag_cmds.extend([cmd for cmd in cmdlist if not ('ANTENNA_NOT_ON_SOURCE' in cmd)])
                flag_cmds.append('mode=\'summary\' name=\'online\'')

        # Flag shadowed antennas?
        if inputs.shadow:
            flag_cmds.append('mode=\'shadow\' reason=\'shadow\'')
            flag_cmds.append('mode=\'summary\' name=\'shadow\'')

        # These must be separated due to the way agent flagging works
        if inputs.scan and inputs.intents != '':
            for intent in inputs.intents.split(','):
                if '*' not in intent:
                    intent = '*%s*' % intent
                flag_cmds.append('mode=\'manual\' intent=\'%s\' reason=\'intents\'' % intent)
            flag_cmds.append('mode=\'summary\' name=\'intents\'')

        # flag template?
        if inputs.template:
            if not os.path.exists(inputs.filetemplate):
                LOG.warning('Template flag file \'%s\' was not found. Template '
                            'flagging for %s disabled.' % (inputs.filetemplate,
                                                           inputs.ms.basename))
            else:
                template_cmds = self._read_flagfile(inputs.filetemplate)
                if template_cmds:
                    flag_cmds.extend(template_cmds)
                    flag_cmds.append('mode=\'summary\' name=\'template\'')

        # Flag autocorrelations?
        # if inputs.autocorr:
        #     flag_cmds.append('mode=manual antenna=*&&&')
        #     flag_cmds.append(self._get_autocorr_cmd())

        # Flag autocorrelations?
        if inputs.autocorr:
            flag_cmds.append('mode=\'manual\' autocorr=True reason=\'autocorr\'')
            flag_cmds.append('mode=\'summary\' name=\'autocorr\'')

        # Flag according to scan numbers and intents?
        if inputs.scan and inputs.scannumber != '':
            flag_cmds.append('mode=\'manual\' scan=\'%s\' reason=\'scans\'' % inputs.scannumber)
            flag_cmds.append('mode=\'summary\' name=\'scans\'')

        # Flag end 5 percent of each spw or minimum of 3 channels
        if inputs.edgespw:
            to_flag = self._get_edgespw_cmds()
            if to_flag:
                spw_arg = ','.join(to_flag)
                flag_cmds.append(spw_arg)
                flag_cmds.append('mode=\'summary\' name=\'edgespw\'')

        # VLA specific commands

        # Flag mode clip
        if inputs.clip:
            flag_cmds.append('mode=\'clip\' correlation=\'ABS_ALL\' clipzeros=True reason=\'clip\'')
            flag_cmds.append('mode=\'summary\' name=\'clip\'')

        # Flag quack
        if inputs.quack: 
            flag_cmds.append(self._get_quack_cmds())
            flag_cmds.append('mode=\'summary\' name=\'quack\'')

        # Flag 20MHz of each edge of basebands
        if inputs.baseband:
            to_flag = self._get_baseband_cmds()
            if to_flag:
                flag_cmds.append(to_flag)
                flag_cmds.append('mode=\'summary\' name=\'baseband\'')

        if flag_cmds:
            if flag_cmds[-1] == '':
                flag_cmds = flag_cmds[0:-1]

        # summarise the state before flagging rather than assuming the initial
        # state is unflagged
        # if flag_cmds:
        #    flag_cmds.insert(0, "mode='summary' name='before'")

        return flag_cmds

    def _get_autocorr_cmd(self):
        # return 'mode=manual antenna=*&&&'
        return 'mode=\'manual\' autocorr=True'

    def _get_edgespw_cmds(self):
        """
        Returns flag command to flag edge channels of SPWs.
        The fraction of channels flagged in each edge of SPWs is defined by fracspw.
        At least one channel in each edge of SPW will be flagged when this method is called.
        """

        inputs = self.inputs

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        channels = m.get_vla_numchan()
        numSpws = len(channels)

        SPWtoflag = ''

        for ispw in range(numSpws):
            spwedge_nchan = int(inputs.fracspw * channels[ispw])
            # Minimum number of channels flagged must be one on each end
            if spwedge_nchan < 1:
                spwedge_nchan = 1
            startch1 = 0
            startch2 = spwedge_nchan - 1
            endch1 = channels[ispw] - spwedge_nchan
            endch2 = channels[ispw] - 1

            if ispw < max(range(numSpws)):
                SPWtoflag = SPWtoflag+str(ispw)+':'+str(startch1)+'~'+str(startch2)+';'+str(endch1)+'~'+str(endch2)+','
            else:
                SPWtoflag = SPWtoflag+str(ispw)+':'+str(startch1)+'~'+str(startch2)+';'+str(endch1)+'~'+str(endch2)

        edgespw_cmd = ['mode=\'manual\' spw=\'%s\' reason=\'edgespw\' name=\'edgespw\'' % SPWtoflag]

        return edgespw_cmd

    def _get_quack_cmds(self):
        """
        Return a flagdata flagging command that will quack, ie
        flagdata_list.append("mode='quack' scan=" + quack_scan_string +
        " quackinterval=" + str(1.5*int_time) + " quackmode='beg' " +
        "quackincrement=False")

        :rtype: a string
        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        quack_scan_string = self._get_vla_quackingscans()
        int_time = m.get_vla_max_integration_time()

        quack_mode_cmd = 'mode=\'quack\' scan=\'%s\' quackinterval=%s quackmode=\'beg\' quackincrement=False reason=\'quack\' name=\'quack\'' % (quack_scan_string, str(1.5*int_time))

        return quack_mode_cmd

    def _get_vla_quackingscans(self):
        """Find VLA scans for quacking.  Quack! :)"""

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        vis = m.name
        with casa_tools.MSReader(vis) as ms:
            scan_summary = ms.getscansummary()

        integ_scan_list = []
        for scan in scan_summary:
            integ_scan_list.append(int(scan))
        sorted_scan_list = sorted(integ_scan_list)

        scan_list = [1]
        old_scan = scan_summary[str(sorted_scan_list[0])]['0']

        old_field = old_scan['FieldId']
        old_spws = old_scan['SpwIds']
        for ii in range(1, len(sorted_scan_list)):
            new_scan = scan_summary[str(sorted_scan_list[ii])]['0']
            new_field = new_scan['FieldId']
            new_spws = new_scan['SpwIds']
            if (new_field != old_field) or (set(new_spws) != set(old_spws)):
                scan_list.append(sorted_scan_list[ii])
                old_field = new_field
                old_spws = new_spws
        quack_scan_string = ','.join(["%s" % ii for ii in scan_list])

        return quack_scan_string

    def _get_baseband_cmds(self):
        """
        Returns a flag command which flags 20MHz of each edge of basebands.
        At least one channel in each band edge is flagged when the function is called.
        """

        bottomSPW=''
        topSPW=''

        # Determination of baseband taken from original EVLA scripted pipeline
        # -----MS info script part
        with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW') as table:
            reference_frequencies = table.getcol('REF_FREQUENCY')  # spwobj.ref_frequency
            spw_bandwidths = table.getcol('TOTAL_BANDWIDTH')  # spwobj.bandwidth
            originalBBClist = table.getcol('BBC_NO')  # spwobj.baseband
            channels = table.getcol('NUM_CHAN')  # spwobj.num_channels

        sorted_indices = reference_frequencies.argsort()
        sorted_frequencies = reference_frequencies[sorted_indices]

        spwList = []
        BBC_bandwidths = []
        ii = 0

        while ii < len(sorted_frequencies):
            upper_frequency = sorted_frequencies[ii] + spw_bandwidths[sorted_indices[ii]]
            BBC_bandwidth = spw_bandwidths[sorted_indices[ii]]
            thisSpwList = [sorted_indices[ii]]
            jj = ii + 1
            while jj < len(sorted_frequencies):
                lower_frequency = sorted_frequencies[jj]
                if ((math.fabs(lower_frequency - upper_frequency) < 1.0) and
                        (originalBBClist[sorted_indices[ii]] == originalBBClist[sorted_indices[jj]])):
                    thisSpwList.append(sorted_indices[jj])
                    upper_frequency += spw_bandwidths[sorted_indices[jj]]
                    BBC_bandwidth += spw_bandwidths[sorted_indices[jj]]
                    jj += 1
                    ii += 1
                else:
                    jj = len(sorted_frequencies)
            spwList.append(thisSpwList)
            BBC_bandwidths.append(BBC_bandwidth)
            ii += 1        

        low_spws = []
        high_spws = []

        for ii in range(0, len(BBC_bandwidths)):
            if BBC_bandwidths[ii] > 1.0e9:
                low_spws.append(spwList[ii][0])
                high_spws.append(spwList[ii][len(spwList[ii])-1])

        quanta = casa_tools.quanta
        bandedge_hz = quanta.getvalue(quanta.convert('20MHz', 'Hz'))
        topSPW_list = []
        bottomSPW_list = []
        LOG.info('Generating flag commands for 20MHz Band Edge flagging')
        for ii in range(0, len(low_spws)):
            # lower bandedge
            bspw = low_spws[ii]
            ave_chansep = (spw_bandwidths[bspw]/channels[bspw])
            startch2 = max(int(bandedge_hz/ave_chansep), 1) - 1
            bottomSPW_list.append('%d:0~%d' % (bspw, startch2))
            LOG.debug('Lower band edge spw=%d, average channel separation=%f Hz, edgeChannels = %d' % (bspw, ave_chansep, startch2+1))
            # upper bandedge
            tspw = high_spws[ii]
            ave_chansep = (spw_bandwidths[tspw]/channels[tspw])
            endch1=channels[tspw] - max(int(bandedge_hz/ave_chansep), 1)
            endch2=channels[tspw]-1
            topSPW_list.append('%d:%d~%d' % (tspw, endch1, endch2))
            LOG.debug('Upper band edge spw=%d, average channel separation=%f Hz, edgeChannels = %d' % (tspw, ave_chansep, endch2-endch1+1))
        bottomSPW = str(',').join(bottomSPW_list)
        topSPW = str(',').join(topSPW_list)

        baseband_cmd = ''

        LOG.info('bottomSPW: {!s}'.format(bottomSPW))
        LOG.info('topSPW: {!s}'.format(topSPW))

        if bottomSPW != '':
            SPWtoflag = bottomSPW + ',' + topSPW
            baseband_cmd = 'mode=\'manual\' spw=\'%s\' reason=\'baseband\' name=\'baseband\'' % SPWtoflag

        return baseband_cmd

    def verify_spw(self, spw):
        # override the default verifier, adding an extra test that bypasses
        # flagging of TDM windows
        super(FlagDeterVLA, self).verify_spw(spw)

        # Skip if TDM mode where TDM modes are defined to be modes with 
        # <= 256 channels per correlation
        dd = self.inputs.ms.get_data_description(spw=spw)
        ncorr = len(dd.corr_axis)
        if ncorr*spw.num_channels > 256:
            raise ValueError('Skipping edge flagging for FDM spw %s' % spw.id)            

    def _add_file(self, filename):
        """
        Read and return the contents of a file or list of files.
        """
        # If the input is a list of flagging command file names, call this
        # function recursively.  Otherwise, read in the file and return its
        # contents
        if isinstance(filename, list):
            return ''.join([self._add_file(f) for f in filename])
        else:
            with open(filename) as stream:
                return stream.read().rstrip('\n')

    def _read_flagfile(self, filename):
        if not os.path.exists(filename):
            LOG.warning('%s does not exist' % filename)
            return []

        # strip out comments and empty lines to leave the real commands.
        # This is so we can compare the number of valid commands to the number
        # of commands specified in the file and complain if they differ
        return [cmd for cmd in flaghelper.readFile(filename) 
                if not cmd.strip().startswith('#')
                and not all(c in string.whitespace for c in cmd)]
