# Do not evaluate type annotations at definition time.
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Dict, List, Literal, Optional

from pipeline import infrastructure
from pipeline.hif.tasks.antpos import antpos
from pipeline.infrastructure import casa_tasks, task_registry, vdp

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context

__all__ = [
    'ALMAAntpos',
    'ALMAAntposInputs'
]

LOG = infrastructure.logging.get_logger(__name__)


class ALMAAntposInputs(antpos.AntposInputs):
    """
    ALMAAntposInputs defines the inputs for the ALMAAntpos pipeline task.
    """
    # These are ALMA specific settings and override the defaults in
    # the base class.

    hm_antpos = vdp.VisDependentProperty(default='online')
    antposfile = vdp.VisDependentProperty(default="antennapos.json")
    threshold = vdp.VisDependentProperty(default=1.0)

    def __init__(
            self,
            context: Context,
            output_dir: Optional[str] = None,
            vis: Optional[List[str]] = None,
            caltable: Optional[List[str]] = None,
            hm_antpos: Optional[Literal['online', 'manual', 'file']] = None,
            antposfile: Optional[str] = None,
            antenna: Optional[str] = None,
            offsets: Optional[List[float]] = None,
            threshold: Optional[float] = None
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
                A comma-separated string of antennas whose positions are to be corrected (if `hm_antpos="manual"`).

                Example: `"DV05,DV07"`

            offsets: 
                A flat list of floating-point offsets (X, Y, Z) for all specified antennas.
                The length of the list must be three times the number of antennas.

                Example (for two antennas): `[0.01, 0.02, 0.03, 0.03, 0.02, 0.01]`

            threshold: 
                Threshold value (in wavelengths) above which antenna position offsets are highlighted in the weblog.
                Defaults to `1.0`.

                Example: `1.0`

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

    def to_casa_args(self) -> Dict[str, str | List[float]]:
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

    def to_antpos_args(self) -> Dict[str, str | List[str]]:
        """
        Configure getantposalma task arguments and return them in dictionary format.
        TODO: develop heuristics for determining time window (tw)

        Returns:
            outfile: Name of file to write antenna positions retrieved from DB.
            asdm: The execution block ID (ASDM) of the dataset.
            tw: Time window ('start time,end time'; times should be UTC and in YY-MM-DDThh:mm:ss.sss format) in which 
                to consider baseline measurements.
            hosts: Priority-ranked list of URLs used by getantposalma to query. Currently only the production API is known.
        """
        ms = self.context.observing_run.measurement_sets[0]
        retry = 3  # TODO: determine if this retry method works/is effective

        return {'outfile': self.antposfile,
                'asdm': ms.execblock_id,
                'tw': '',
                'hosts': [
                    'https://asa.alma.cl/uncertainties-service/uncertainties/versions/last/measurements/casa'
                    ] * retry}

    def __str__(self):
        s = 'AlmaAntposInputs:\n'
        s += '\tvis: %s\n' % self.vis
        s += '\tcaltable: %s\n' % self.caltable
        s += '\thm_antpos: %s\n' % self.hm_antpos
        s += '\tantposfile: %s\n' % self.antposfile
        s += '\tantenna: %s\n' % self.antenna
        s += '\toffsets: %s\n' % self.offsets
        s += '\tthreshold: %s\n' % self.threshold
        return s


@task_registry.set_equivalent_casa_task('hifa_antpos')
class ALMAAntpos(antpos.Antpos):
    Inputs = ALMAAntposInputs

    def prepare(self):
        inputs = self.inputs
        if inputs.hm_antpos == 'online':
            # PIPE-51: retrieve json file for MS to include in the call to gencal
            antpos_args = inputs.to_antpos_args()
            antpos_job = casa_tasks.getantposalma(**antpos_args)
            self._executor.execute(antpos_job)

        return super().prepare()
