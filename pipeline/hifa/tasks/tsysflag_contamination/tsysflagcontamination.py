import dataclasses
import os
import re
from typing import List

import numpy as np

import pipeline.extern.tsys_contamination as extern
import pipeline.h.tasks.tsysflag.tsysflag as tsysflag
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.extern.TsysDataClassFile import TsysData
from pipeline.h.tasks.common import calibrationtableaccess as caltableaccess
from pipeline.h.tasks.tsysflag.resultobjects import TsysflagResults
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.basetask import StandardTaskTemplate
from pipeline.infrastructure.pipelineqa import QAScore, TargetDataSelection

__all__ = ["TsysFlagContamination", "TsysFlagContaminationInputs"]


LOG = infrastructure.get_logger(__name__)


class TsysFlagContaminationInputs(vdp.StandardInputs):
    """
    TsysFlagContaminationInputs defines the inputs for the TsysFlagContamination
    pipeline task.

    Heuristic parameters specific to this task are:

    - remove_n_extreme: defaults to 2
    - relative_detection_factor: defaults to 0.005
    - diagnostic_plots: include diagnostic plots in the weblog. Defaults to
      True.
    """

    @vdp.VisDependentProperty
    def caltable(self):
        caltables = self.context.callibrary.active.get_caltable(caltypes="tsys")

        # return just the tsys table that matches the vis being handled
        result = None
        for name in caltables:
            # Get the tsys table name
            tsystable_vis = caltableaccess.CalibrationTableDataFiller._readvis(name)
            if tsystable_vis in self.vis:
                result = name
                break

        return result

    @vdp.VisDependentProperty
    def filetemplate(self):
        vis_root = os.path.splitext(self.vis)[0]
        return vis_root + ".flag_tsys_contamination.txt"

    @vdp.VisDependentProperty
    def logpath(self):
        vis_root = os.path.splitext(self.vis)[0]
        return vis_root + ".ms_tsys_contamination.txt"

    remove_n_extreme = vdp.VisDependentProperty(default=2)
    relative_detection_factor = vdp.VisDependentProperty(default=0.005)
    diagnostic_plots = vdp.VisDependentProperty(default=True)

    def __init__(
        self,
        context,
        output_dir=None,
        vis=None,
        caltable=None,
        filetemplate=None,
        logpath=None,
        remove_n_extreme=None,
        relative_detection_factor=None,
        diagnostic_plots=None,
    ):
        super(TsysFlagContaminationInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.caltable = caltable

        self.filetemplate = filetemplate
        self.logpath = logpath
        self.diagnostic_plots = diagnostic_plots

        # heuristic parameter arguments
        self.remove_n_extreme = remove_n_extreme
        self.relative_detection_factor = relative_detection_factor


@dataclasses.dataclass
class ExternFunctionArguments:
    """
    Adapter class to adapt TsysflagContaminationInputs task inputs class to
    the function arguments required by the external heuristic.
    """

    vis: str
    diagnostic_plots: bool
    tsystable: str
    remove_n_extreme: float
    relative_detection_factor: float
    logpath: str
    filetemplate: str
    pl_run_dir: str
    plots_path: str

    @staticmethod
    def from_inputs(inputs: TsysFlagContaminationInputs) -> "ExternFunctionArguments":
        context = inputs.context
        weblog_dir = os.path.join(context.report_dir, f"stage{context.task_counter}")
        os.makedirs(weblog_dir, exist_ok=True)

        return ExternFunctionArguments(
            vis=inputs.vis,
            diagnostic_plots=inputs.diagnostic_plots,
            tsystable=inputs.caltable,
            remove_n_extreme=inputs.remove_n_extreme,
            relative_detection_factor=inputs.relative_detection_factor,
            logpath=inputs.logpath,
            filetemplate=inputs.filetemplate,
            pl_run_dir=inputs.context.output_dir,
            plots_path=weblog_dir,
        )


@task_registry.set_equivalent_casa_task("hifa_tsysflagcontamination")
@task_registry.set_casa_commands_comment(
    "Line contamination in the Tsys tables is detected and flagged."
)
class TsysFlagContamination(StandardTaskTemplate):
    """
    Flag line contamination in the Tsys tables.

    This purpose of this class is to call the external flagging heuristic to
    generate flagging commands based on line contamination in Tsys tables, and
    then pass those flagging commands to the standard h_tsysflag child task in
    manual flagging mode.

    The bulk of what you see here comes directly from the extern code,
    sandwiched between a few lines of code to extract input parameters and
    pass them to the heuristic, followed at the end of the method by
    wrapping and adapting the results - a list of flagging commands - into
    a manual flagging request for the existing h_tsysflag task. The results
    of this child task are then captured and adapted so that the QA and
    weblog rendering code can operate on the results of this line
    contamination task.
    """

    Inputs = TsysFlagContaminationInputs

    def prepare(self):
        result = TsysflagResults()
        result.vis = self.inputs.vis
        result.caltable = self.inputs.caltable

        # TODO from PIPE-2009: adding new attributes to the result after
        # instance construction isn't great but we don't have time to
        # rationalise and refactor the base class right now
        result.qascores_from_task = []

        # step 1: do not run the heuristic on data we know it cannot handle
        preflight_qascores = self._assert_heuristic_preconditions()
        result.qascores_from_task.extend(preflight_qascores)
        if preflight_qascores:
            result.task_incomplete_reason = f"Preconditions for line contamination heuristic not met. See QA scores for details."
            result.metric_order = "manual"  # required for renderer
            return result

        # step 2: run extern heuristic
        extern_fn_args = ExternFunctionArguments.from_inputs(self.inputs)
        try:
            plot_wrappers, warnings = self._call_extern_heuristic(extern_fn_args)
        except Exception:
            result.task_incomplete_reason = f"Line contamination heuristic failed while processing {self.inputs.vis}"
            result.metric_order = "manual"  # required for renderer
            return result

        result.plots = plot_wrappers
        result.extern_warnings = warnings

        # Step 3: do not flag data for DSB data
        # Set manual flagging template to that written by the heuristic unless it's a DSB EB.
        filetemplate = None if self._contains_dsb() else self.inputs.filetemplate

        # Always run the child task - even for DSB - as the results are required by the Tsyscalflag renderer
        child_inputs = tsysflag.Tsysflag.Inputs(
            self.inputs.context,
            output_dir=self.inputs.output_dir,
            vis=self.inputs.vis,
            caltable=self.inputs.caltable,
            filetemplate=filetemplate,
            flag_birdies=False,
            flag_derivative=False,
            flag_edgechans=False,
            flag_fieldshape=False,
            flag_nmedian=False,
            flag_toomany=False,
            fnm_byfield=False,
            normalize_tsys=False,
        )
        child_task = tsysflag.Tsysflag(child_inputs)
        child_result = self._executor.execute(child_task)

        result.pool.extend(child_result.pool)
        result.final.extend(child_result.final)
        result.components.update(child_result.components)
        result.summaries = child_result.summaries
        result.error.update(child_result.error)
        result.metric_order = list(child_result.metric_order)

        return result

    def analyse(self, result):
        return result

    def _call_extern_heuristic(self, fn_args: ExternFunctionArguments):
        vis = fn_args.vis
        diagnostic_plots = fn_args.diagnostic_plots
        tsystable = fn_args.tsystable
        remove_n_extreme = fn_args.remove_n_extreme
        relative_detection_factor = fn_args.relative_detection_factor
        logpath = fn_args.logpath
        filetemplate = fn_args.filetemplate
        pl_run_dir = fn_args.pl_run_dir
        plots_path = fn_args.plots_path

        single_polarization = (
            tsystable == "uid___A002_X10ac6bc_Xc408.ms.h_tsyscal.s6_1.tsyscal.tbl"
        )

        tsys = TsysData(
            tsystable=tsystable,
            load_pickle=True,
            single_polarization=single_polarization,
        )

        line_contamination_intervals, warnings_list, plot_wrappers = (
            extern.get_tsys_contaminated_intervals(
                tsys,
                plot=diagnostic_plots,
                remove_n_extreme=remove_n_extreme,
                relative_detection_factor=relative_detection_factor,
                savefigfile=f"{plots_path}/{vis}.tsyscontamination",
            )
        )

        for k, v in line_contamination_intervals.copy().items():
            if np.sum(np.array([len(vv) for vv in v.values()])) == 0:
                del line_contamination_intervals[k]

        [intents, scans, fields] = [
            tsys.tsysdata[tsys.tsysfields.index(f)] for f in ["intent", "scan", "field"]
        ]

        field_intent_dict = dict({(f, i) for f, i in zip(fields, intents)})
        scan_field_dict = dict({(s, f) for s, f in zip(scans, fields)})
        field_scanlist_dict = {}
        for k, v in scan_field_dict.items():
            field_scanlist_dict.setdefault(v, []).append(k)
        # end replace

        all_freqs_mhz = tsys.specdata[tsys.specfields.index("freq_mhz")]

        with open(logpath, "w") as f:
            with open(filetemplate, "a") as ft:
                pl_run_dir = pl_run_dir
                f.write(
                    f"\n# script version {extern.VERSION} {pl_run_dir}\n# {tsystable}\n"
                )

                field_contamination = {}
                for k in line_contamination_intervals:
                    m = re.match(r"(?P<spw>[0-9]+)_(?P<field>[0-9])", k)
                    spw, field = m.group(1, 2)
                    field_contamination.setdefault(np.int64(field), []).append(
                        np.int64(spw)
                    )

                if len(field_contamination) == 0:
                    msg = f"## No tsys contamination identified.\n"
                    f.write(msg)
                    LOG.info(msg)

                # v3.3 large baseline residual
                for w in warnings_list:
                    msg = " ".join(w)
                    f.write(f"# {msg}\n")
                    LOG.info("# %s", msg)

                for field in field_contamination:
                    field = np.int64(field)
                    spw_ranges = []
                    spw_ranges_freq = []

                    for spw in field_contamination[field]:
                        if field_intent_dict[field] == "bandpass":
                            continue
                        key = f"{spw}_{field}"
                        spw = np.int64(spw)
                        freqs_ghz = (
                            all_freqs_mhz[
                                np.nonzero(
                                    tsys.specdata[tsys.specfields.index("spw")] == spw
                                )[0][0]
                            ]
                            / 1000
                        )
                        rs = extern.intervals_to_casa_string(
                            line_contamination_intervals[key]["tsys_contamination"]
                        )
                        rsf = extern.intervals_to_casa_string(
                            line_contamination_intervals[key]["tsys_contamination"],
                            scaled_array=freqs_ghz,
                            unit="GHz",
                            format=".3f",
                        )
                        if rs != "":
                            spw_ranges.append(f"{spw}:{rs}")
                            spw_ranges_freq.append(f"{spw}:{rsf}")

                    if len(spw_ranges) == 0:
                        continue  # v2.2
                    spw_ranges = ",".join(spw_ranges)
                    spw_ranges_freq = ",".join(spw_ranges_freq)
                    contamination_scans = field_scanlist_dict[field]
                    contamination_scans.sort()

                    flagline = f"mode='manual' scan='{','.join([str(sc) for sc in contamination_scans])}' spw='{spw_ranges}' reason='Tsys:tsysflag_tsys_channel'\n"
                    ft.write(flagline)

                    msg = (
                        f"## {tsystable}: field={field}, intent={field_intent_dict[field]}\n"
                        f"# Frequency ranges: '{spw_ranges_freq}' \n"
                        f"{flagline}"
                    )
                    f.write(msg)
                    LOG.info(msg)

        return plot_wrappers, warnings_list

    def _assert_heuristic_preconditions(self) -> List[QAScore]:
        """
        Preflight checks to identify data that the heuristic cannot handle.
        """
        qa_scores = []
        qa_scores.extend(self._assert_not_multisource_multituning())
        qa_scores.extend(self._assert_not_full_polarization())
        qa_scores.extend(self._assert_bandpass_is_present())
        return qa_scores

    def _assert_not_multisource_multituning(self) -> List[QAScore]:
        """
        Returns a list containing an appropriate QAScore if multitunings are
        present, otherwise an empty list is returned.
        """
        ms = self.inputs.ms
        qa_scores = []

        # exclude multi-source, multi-tuning (script fails in EE10).
        science_source_ids = {
            field.source_id for field in ms.get_fields(intent="TARGET")
        }
        if len(science_source_ids) > 1 and len(ms.get_spectral_specs()) > 1:
            s = QAScore(
                score=0.6,
                shortmsg="Multi-source multi-tuning EB",
                longmsg=f"Line contamination heuristic not validated for multi-source multi-tunings present in {ms.basename}.",
                applies_to=TargetDataSelection(vis={ms.basename}),
            )
            qa_scores.append(s)

        return qa_scores

    def _assert_not_full_polarization(self) -> List[QAScore]:
        """
        Returns a list containing an appropriate QAScore if full polarization
        data are present, otherwise an empty list is returned.
        """
        qa_scores = []

        ms = self.inputs.ms
        science_spws = ms.get_spectral_windows(science_windows_only=True)

        # exclude full polarization
        polarizations = {
            ms.get_data_description(spw=spw.id).num_polarizations
            for spw in science_spws
        }
        if any(n > 2 for n in polarizations):
            s = QAScore(
                score=0.6,
                shortmsg="Full polarization data",
                longmsg=f"Line contamination heuristic not validated for full polarization data in {ms.basename}.",
                applies_to=TargetDataSelection(vis={ms.basename}),
            )
            qa_scores.append(s)

        return qa_scores

    def _assert_bandpass_is_present(self) -> List[QAScore]:
        """
        Returns a list containing an appropriate QAScore if BANDPASS data are
        missing, otherwise an empty list is returned.
        """
        qa_scores = []

        ms = self.inputs.ms

        # exclude TP (fails by design; needs bandpass intent scan)
        if "BANDPASS" not in ms.intents:
            s = QAScore(
                score=0.6,
                shortmsg="No BANDPASS data",
                longmsg=f"No bandpass scans in {ms.basename} for the line contamination heuristic to process.",
                applies_to=TargetDataSelection(vis={ms.basename}),
            )
            qa_scores.append(s)

        return qa_scores

    def _contains_dsb(self) -> bool:
        """
        Returns True if any science spectral window uses a DSB receiver.
        """
        ms = self.inputs.ms
        receivers = [
            spw.receiver for spw in ms.get_spectral_windows(science_windows_only=True)
        ]
        return "DSB" in receivers
