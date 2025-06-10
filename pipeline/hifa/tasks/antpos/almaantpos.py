# Do not evaluate type annotations at definition time.
from __future__ import annotations

import json
import os
import random
import time
from typing import TYPE_CHECKING, Any, Callable, Literal

import numpy as np

from pipeline import infrastructure
from pipeline.hif.tasks.antpos import antpos
from pipeline.infrastructure import casa_tasks, casa_tools, task_registry, utils, vdp

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context

__all__ = [
    'ALMAAntpos',
    'ALMAAntposInputs'
]

LOG = infrastructure.logging.get_logger(__name__)
ORIGIN_ANTPOS = 'https://asa.alma.cl/uncertainties-service/uncertainties/versions/last/measurements/casa/'


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
            if attempt == max_retries - 1:
                LOG.error("Max retries reached. Raising exception.")
                raise e
            delay = base_delay * (2 ** attempt) + random.uniform(0, jitter)
            time.sleep(delay)


class ALMAAntposInputs(antpos.AntposInputs):
    """
    ALMAAntposInputs defines the inputs for the ALMAAntpos pipeline task.
    """
    # These are ALMA specific settings and override the defaults in
    # the base class.

    hm_antpos = vdp.VisDependentProperty(default='online')

    @vdp.VisDependentProperty
    def antposfile(self):
        return "antennapos.json"

    @antposfile.convert
    def antposfile(self, value):
        if not value and self.hm_antpos == 'online':
            value = "antennapos.json"
            LOG.info("Input parameter antposfile cannot be empty when hm_antpos='online', using antposfile=%s instead.", value)
        return value

    threshold = vdp.VisDependentProperty(default=1.0)
    snr = vdp.VisDependentProperty(default=5.0)
    search = vdp.VisDependentProperty(default='both_latest')

    def __init__(
            self,
            context: Context,
            output_dir: str | None = None,
            vis: list[str] | None = None,
            caltable: list[str] | None = None,
            hm_antpos: Literal['online', 'manual', 'file'] | None = None,
            antposfile: str | None = None,
            antenna: str | None = None,
            offsets: list[float] | None = None,
            threshold: float | None = None,
            snr: float | None = None,
            search: Literal['both_latest', 'both_closest'] | None = None,
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
                Path to a csv file containing antenna position offsets for `hm_antpos='file'` (required) or the name
                 of the outfile created by `getantposalma` for `hm_antpos="online"` (defaults to 'antennapos.json').

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
                snr below the threshold will not be retrieved. Only used with `hm_antpos="online"`. Defaults to `5.0`.

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
        if self.hm_antpos == 'file':
            filename = os.path.join(self.output_dir, self.antposfile)
            antenna, offsets = self._read_antpos_csvfile(
                filename, os.path.basename(self.vis))
        else:
            antenna = self.antenna
            offsets = self.offsets

        return {'vis': self.vis,
                'caltable': self.caltable,
                'infile': infile,
                'antenna': antenna,
                'parameter': offsets}

    def to_antpos_args(self) -> dict[str, str | bool | list[str] | Literal['both_latest', 'both_closest'] | None]:
        """
        Configure getantposalma task arguments and return them in dictionary format.

        Returns:
            outfile: Name of file to write antenna positions retrieved from DB.
            overwrite: Tells `getantposalma` whether to overwrite the file if it exists. If False and the file exists,
                it will throw an error.
            asdm: The execution block ID (ASDM) of the dataset.
            snr: A float value describing the signal-to-noise threshold. Antennas with snr below the threshold will 
                not be retrieved.
            search: Search algorithm to use. Supports 'both_latest' and 'both_closest'.
            hosts: Priority-ranked list of hosts to query.
        """
        hosts = utils.get_valid_url('ORIGIN_ANTPOS', ORIGIN_ANTPOS)
        return {'outfile': self.antposfile,
                'overwrite': True,
                'asdm': self.context.observing_run.get_ms(self.vis).execblock_id,
                'snr': self.snr,
                'search': self.search,
                'hosts': [hosts]}

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
            if not os.path.exists(antpos_args['outfile']):
                antpos_job = run_with_retry(casa_tasks.getantposalma, **antpos_args)
                self._executor.execute(antpos_job)
            else:
                LOG.warning('Antenna position file %s exists. Skipping getantposalma task.', antpos_args['outfile'])

        return super().prepare()

    def analyse(self, result):
        result = super().analyse(result)

        # add the offsets to the result for online query
        if self.inputs.hm_antpos == 'online':
            offsets_dict = self._get_antenna_offsets(self.inputs.vis)
            antenna_names, offsets = self._get_antennas_with_significant_offset(offsets_dict)
            if antenna_names:
                result.antenna = ",".join(antenna_names)
                result.offsets = offsets
                LOG.info("Antennas with non-zero corrections applied: %s", result.antenna)

        return result

    def _get_antenna_offsets(self, vis: str) -> dict[np.str_, np.ndarray[float]]:
        """
        Retrieves the antenna names and positions from the vis ANTENNA table and computes the offsets.

        Args:
            vis: The MeasurementSet name.

        Returns:
            Dictionary mapping antenna names to (x, y, z) offset tuples.
        """
        with casa_tools.TableReader(vis + "/ANTENNA") as tb:
            antennas = tb.getcol('NAME')
            tb_positions = tb.getcol('POSITION')

        tb_antpos_dict = dict(zip(antennas, tb_positions.T))

        # Retrieve antenna corrections from antennapos.json
        with open(self.inputs.antposfile, 'r') as f:
            query_dict = json.load(f)
            db_antpos_dict = query_dict['data']

        # calculate offsets
        offsets_dict = {}
        for antenna in antennas:
            offsets_dict[antenna] = db_antpos_dict[antenna] - tb_antpos_dict[antenna]

        return offsets_dict

    def _get_antennas_with_significant_offset(
            self,
            offsets_dict: dict[np.str_, np.ndarray[float]],
            ) -> tuple[list[np.str_], list[np.float64]]:
        """
        Identifies antennas with significant non-zero coordinate offsets.

        Args:
            offsets_dict: Dictionary mapping antenna names to (x, y, z) offset tuples.

        Returns:
            A list of antenna names with significant offsets, and a flattened list of their 
            corresponding offset values.
        """
        names = []
        flattened_offsets = []

        for antenna, offsets in offsets_dict.items():
            if np.any(np.abs(offsets) > 0):
                names.append(antenna)
                flattened_offsets.extend(offsets)

        return names, flattened_offsets
