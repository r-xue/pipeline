# Do not evaluate type annotations at definition time.
from __future__ import annotations

import os
import random
import time
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional

import numpy as np

from pipeline import infrastructure
from pipeline.hif.tasks.antpos import antpos
from pipeline.infrastructure import casa_tasks, casa_tools, task_registry, vdp

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context

__all__ = [
    'ALMAAntpos',
    'ALMAAntposInputs'
]

LOG = infrastructure.logging.get_logger(__name__)


def run_with_retry(
    func: Callable[..., Any],
    max_retries: int = 3,
    base_delay: int = 3,
    jitter: int = 2,
    *args,
    **kwargs
) -> Any:
    """
    Executes a function with retry logic and exponential backoff plus random jitter.

    Args:
        func: The function to execute.
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds before retrying.
        jitter: Maximum additional random jitter in seconds added to delay.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        Any: The result of the function call if successful.

    Raises:
        Exception: The last encountered exception if all retries fail.
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            LOG.warning(f"[Attempt {attempt}] Failed with error: {e}")
            if attempt == max_retries:
                LOG.error("Max retries reached. Raising exception.")
                raise e
            time.sleep(base_delay + random.uniform(0, jitter))


class ALMAAntposInputs(antpos.AntposInputs):
    """
    ALMAAntposInputs defines the inputs for the ALMAAntpos pipeline task.
    """
    # These are ALMA specific settings and override the defaults in
    # the base class.

    hm_antpos = vdp.VisDependentProperty(default='online')
    antposfile = vdp.VisDependentProperty(default="antennapos.json")
    threshold = vdp.VisDependentProperty(default=1.0)
    snr = vdp.VisDependentProperty(default=5.0)
    search = vdp.VisDependentProperty(default='both_latest')

    def __init__(
            self,
            context: Context,
            output_dir: Optional[str] = None,
            vis: Optional[list[str]] = None,
            caltable: Optional[list[str]] = None,
            hm_antpos: Optional[Literal['online', 'manual', 'file']] = None,
            antposfile: Optional[str] = None,
            antenna: Optional[str] = None,
            offsets: Optional[list[float]] = None,
            threshold: Optional[float] = None,
            snr: Optional[float] = None,
            search: Optional[Literal['both_latest', 'both_closest']] = None
            ):
        """
        Initializes the pipeline input parameters for antenna position corrections.

        Args:
            context: The pipeline execution context.

            output_dir: Directory where output files will be stored.
                Defaults to the current working directory.

            vis: List of input MeasurementSets.
                Defaults to those specified in the pipeline context.

                Example: `["ngc5921.ms"]`

            caltable: List of output calibration table names.
                Defaults to the standard pipeline naming convention.

                Example: `["ngc5921.gcal"]`

            hm_antpos: 
                Heuristic method for retrieving antenna position corrections.
                - `"online"` (query ALMA database through casa task `getantposalma`)
                - `"manual"` (user-provided corrections)
                - `"file"` (corrections from an external file)

                Example: `"manual"`

            antposfile: 
                Path to a csv file containing antenna position offsets.
                Required if `hm_antpos="file"`. Defaults to "antennapos.json" for `hm_antpos="online"`.

                Example: `"antennapos.csv"`

            antenna: 
                A comma-separated string of antennas whose positions are to be corrected (if `hm_antpos` is "manual" 
                or "online"`).

                Example: `"DV05,DV07"`

            offsets: 
                A flat list of floating-point offsets (X, Y, Z) for all specified antennas.
                The length of the list must be three times the number of antennas.

                Example (for two antennas): `[0.01, 0.02, 0.03, 0.03, 0.02, 0.01]`

            threshold: 
                Threshold value (in wavelengths) above which antenna position offsets are highlighted in the weblog.
                Defaults to `1.0`.

                Example: `1.0`

            snr:
                A float value describing the signal-to-noise threshold used by the getantposalma task. Antennas with 
                snr below the threshold will not be retrieved. Only used with `hm_antpos="online"`. Defaults to `0.0`.

                Example: `5.0`

            search:
                Search algorithm used by the getantposalma task. Supports 'both_latest' and 'both_closest'.
                Only used with `hm_antpos="online"`. Defaults to `both_latest`.

                Example: `both_closest`
        """
        super().__init__(
            context,
            output_dir=output_dir,
            vis=vis,
            caltable=caltable,
            hm_antpos=hm_antpos,
            antposfile=antposfile,
            antenna=antenna,
            offsets=offsets
            )
        self.threshold = threshold
        self.snr = snr
        self.search = search

        if hm_antpos == 'file' and antposfile is None:
            raise ValueError("`antposfile` must be defined for `hm_antpos='file'`.")
        elif hm_antpos == 'manual':
            # Validate `antenna` and `offsets`
            if antenna:
                antenna_list = [a.strip() for a in antenna.split(",") if a.strip()]
                if not antenna_list:
                    raise ValueError("`antenna` must be a non-empty comma-separated string if provided.")

            if offsets:
                if not all(isinstance(x, (int, float)) for x in offsets):
                    raise TypeError("`offsets` must be a list of float values.")

                expected_length = 3 * len(antenna_list)
                if expected_length > 0 and len(offsets) != expected_length:
                    raise ValueError(
                        f"`offsets` must have {expected_length} values (3 per antenna), but got {len(offsets)}."
                    )

    def to_casa_args(self) -> dict[str, str | list[float]]:
        """
        Configure gencal task arguments and return them in dictionary format.

        Returns:
            vis: Name of the input visibility file (MS).
            caltable: Name of the input calibration table.
            infile: Antenna positions file obtained with getantposalma task.
            antenna: Filter data selection based on antenna/baseline.
            parameter: List of calibration values; For this purpose, the offsets for all specified antennas.
        """
        infile = ''
        if self.hm_antpos == 'online':
            infile = os.path.join(self.output_dir, self.antposfile)
        # Get the antenna and offset lists.
        if self.hm_antpos == 'manual':
            antenna = self.antenna
            offsets = self.offsets
        elif self.hm_antpos == 'file':
            filename = os.path.join(self.output_dir, self.antposfile)
            antenna, offsets = self._read_antpos_csvfile(
                filename, os.path.basename(self.vis))
        else:
            antenna = ''
            offsets = []

        return {'vis': self.vis,
                'caltable': self.caltable,
                'infile': infile,
                'antenna': antenna,
                'parameter': offsets}

    def to_antpos_args(self) -> dict[str, str | list[str]]:
        """
        Configure getantposalma task arguments and return them in dictionary format.

        Returns:
            outfile: Name of file to write antenna positions retrieved from DB.
            asdm: The execution block ID (ASDM) of the dataset.
            snr: A float value describing the signal-to-noise threshold. Antennas with snr below the threshold will not be retrieved.
            search: Search algorithm to use. Supports 'both_latest' and 'both_closest'.
        """
        return {'outfile': self.antposfile,
                'asdm': self.context.observing_run.measurement_sets[0].execblock_id,
                'snr': self.snr,
                'search': self.search}

    def __str__(self):
        return (
            f"AlmaAntposInputs:\n"
            f"\tvis: {self.vis}\n"
            f"\tcaltable: {self.caltable}\n"
            f"\thm_antpos: {self.hm_antpos}\n"
            f"\tantposfile: {self.antposfile}\n"
            f"\tantenna: {self.antenna}\n"
            f"\toffsets: {self.offsets}\n"
            f"\tthreshold: {self.threshold}\n"
            f"\tsnr: {self.snr}\n"
            f"\tsearch: {self.search}"
        )


@task_registry.set_equivalent_casa_task('hifa_antpos')
class ALMAAntpos(antpos.Antpos):
    Inputs = ALMAAntposInputs

    def prepare(self):
        inputs = self.inputs
        if inputs.hm_antpos == 'online':
            # PIPE-51: retrieve json file for MS to include in the call to gencal
            antpos_args = inputs.to_antpos_args()
            antpos_job = run_with_retry(casa_tasks.getantposalma, **antpos_args)
            self._executor.execute(antpos_job)

        return super().prepare()

    def analyse(self, result):
        result = super().analyse(result)

        # add the offsets to the result for online query
        if self.inputs.hm_antpos == 'online':
            antennas, offsets = self._get_antenna_offsets(result.final[0].gaintable)
            indices = self._get_antennas_with_significant_offset(offsets)
            if indices:
                result.antenna = ",".join([antennas[i] for i in indices])
                result.offsets = offsets[:, indices].T.flatten().tolist()

        return result

    def _get_antenna_offsets(self, antpos_tbl: str) -> np.ndarray:
        """
        Retrieves the antenna offsets from the antpos table created by gencal and flattens it into a list.

        Args:
            antpos_tbl: file name of the antenna position table.

        Returns:
            A list of coordinate offsets with length 3*X, where X is the number of antennas in the observation.
        """
        with casa_tools.TableReader(antpos_tbl + "/ANTENNA") as tb:
            antennas = tb.getcol('NAME')
            offsets = tb.getcol('OFFSET')

        return antennas, offsets

    def _get_antennas_with_significant_offset(self, offsets: np.ndarray, threshold: float = 1e-9) -> np.ndarray:
        """
        Returns indices of antennas with any coordinate offset exceeding the threshold.

        Args:
            offsets (np.ndarray): A 3 x N array.
            threshold (float): Threshold for significance.

        Returns:
            np.ndarray: Indices of antennas with significant offsets.
        """
        return np.where(np.any(np.abs(offsets) > threshold, axis=0))[0]
