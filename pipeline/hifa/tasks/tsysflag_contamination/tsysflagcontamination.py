import os
import re

import pipeline.h.tasks.tsysflag.tsysflag as tsysflag
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.extern.TsysDataClassFile import TsysData
from pipeline.h.tasks.common import calibrationtableaccess as caltableaccess
from pipeline.h.tasks.tsysflag.resultobjects import TsysflagResults
from pipeline.infrastructure import task_registry
import pipeline.extern.tsys_contamination as extern
import numpy as np

__all__ = ["TsysFlagContamination", "TsysFlagContaminationInputs"]

from pipeline.infrastructure.basetask import StandardTaskTemplate

LOG = infrastructure.get_logger(__name__)


class TsysFlagContaminationInputs(vdp.StandardInputs):
    """
    TsysFlagContaminationInputs defines the inputs for the TsysFlagContamination
    pipeline task.
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

        # heuristic parameter arguments
        self.remove_n_extreme = remove_n_extreme
        self.relative_detection_factor = relative_detection_factor


@task_registry.set_equivalent_casa_task("hifa_tsysflagcontamination")
@task_registry.set_casa_commands_comment(
    "Line contamination in the Tsys tables is detected and flagged."
)
class TsysFlagContamination(StandardTaskTemplate):
    Inputs = TsysFlagContaminationInputs

    def prepare(self):
        tsystable = self.inputs.caltable
        context = self.inputs.context
        vis = self.inputs.vis
        weblog_dir = os.path.join(context.report_dir, f'stage{context.task_counter}')
        os.makedirs(weblog_dir, exist_ok=True)

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
                plot=True,
                # spwlist=[np.int(30)],fieldlist=[np.int64(2)],# this selection does not work with the saved spool sample
                remove_n_extreme=self.inputs.remove_n_extreme,
                relative_detection_factor=self.inputs.relative_detection_factor,
                savefigfile=f"{weblog_dir}/{vis}.tsyscontamination",
            )
        )

        for k, v in line_contamination_intervals.copy().items():
            if np.sum(np.array([len(vv) for vv in v.values()])) == 0:
                del line_contamination_intervals[k]

        # start --------------------------------------

        # to replace
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

        with open(self.inputs.logpath, "w") as f:
            with open(self.inputs.filetemplate, "a") as ft:
                pl_run_dir = self.inputs.context.output_dir
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

        child_inputs = tsysflag.Tsysflag.Inputs(
            self.inputs.context,
            output_dir=self.inputs.output_dir,
            vis=self.inputs.vis,
            caltable=self.inputs.caltable,
            filetemplate=self.inputs.filetemplate,
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

        result = TsysflagResults()
        result.vis = child_result.vis
        result.caltable = child_result.caltable
        result.pool.extend(child_result.pool)
        result.final.extend(child_result.final)
        result.components.update(child_result.components)
        result.summaries = child_result.summaries
        result.error.update(child_result.error)
        result.metric_order = list(child_result.metric_order)

        result.plots = plot_wrappers

        return result

    def analyse(self, result):
        return result
