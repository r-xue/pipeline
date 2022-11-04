

# VLASS Selfcal & Restore

* VLASS selfcal workflow

    ```python
    hif_makeimages (tclean,savemodel)
    hifv_selfcal (gaincal,applycal)
    hif_makeimages (tclean)
    ```

* VLASS Selfcal/Restore workflow

    ```python
    hifv_restorpims
        MakeImages
        applycal
        statwt
        ...
    ```

# auto_selfcal prototype

```console
per target per band
    tclean(dirty)
    tclean(initial)
per target per band per spw (optional)
    tclean(dirty)
    tclean(initial)        

per target per band
    solin1->solin2->solin3->...solinX
        tclean:savemodel:priorcal
        per vis
            gaincal
        per vis
            applycal
        tclean:postcal
    solinX-1 (go back when X is worse than X-1)
        per vis
            appplycal

per target per band
    tclean(final)
per target per band per spw (optional)
    tclean(final)

per target 
    per band 
        per vis
            applycal(to originMS): write to a script / or apply on-teh-fly
```

# PipelineTask: `hif_selfcal`

```console
MakeImList(cont): derive clean_target_list

per clean_target (a target/band combination)
    ms_transform()
        pointing table hard linked to the original MS
        output ms per clean_target (so the modelcolumn prediction can happen simultaneously when running tier0)

Creat a temporary context and start using per_clean_target MSs

hif_tclean (per clean_target per spw, data, tier0) dirty/initial (optional)

hif_tclean (per clean_target,tier0) <- selfcal-specific imaging sequence
    tclean(dirty,data)
    tclean(initial,data)
    solin1->solin2->solin3->...solinX
        tclean:savemodel:priorcal
        per vis
            gaincal
        per vis
            applycal
        tclean:postcal
    solinX-1 (go back when X is worse than X-1)
        per vis
            appplycal
        tclean(final,corrected)

hif_tclean (per clean_target per spw, corrected, tier0) final (optional)

per clean_target 
    per vis
        applycal(to originMS)

register the datatype per target per band
```


## MS/Caltable/Image naming convention (tentative):

### MS names
Assuming that original per-EB input MSes are,

```console
eb1_targets.ms
eb2_targets.ms
eb3_targets.ms
```

the "MS working copies" generated and used by `hifv_selfcal` will be

```console 
eb1_targets.04287+1801.spw16_18_20_22_24_26_28_30.selfcal.ms
eb2_targets.04287+1801.spw16_18_20_22_24_26_28_30.selfcal.ms
eb3_targets.04287+1801.spw16_18_20_22_24_26_28_30.selfcal.ms
eb1_targets.04288_1802.spw16_18_20_22_24_26_28_30.selfcal.ms
eb2_targets.04288_1802.spw16_18_20_22_24_26_28_30.selfcal.ms
eb3_targets.04288_1802.spw16_18_20_22_24_26_28_30.selfcal.ms
....
```

Note that the per-EB MSes are split and channel-rebinned into smaller MS quanta, i.e. per-"clean_target" (a target/band combination) MSes.
Each of them is a uvdata container on which the selfcal "ImageSolver-CalSolver" loop can operate.
The MS/data division policy is largely due to a CASA limitation that `casatools` doesn't support parallel-write in MS tables (even though `casacore` does via the parallel storage manager `Adios`):
when their uvdata are stored in separate MSes, multiple selfcal operations of different "clean_targets", notably the modelcolumn prediction/write, can proceed simultaneously; therefore they become tier0-parallelizable.

Diving MS into per_clean_target MSes generally doesn't produce duplications of visibility data (uvw, uvdata, etc.) as they are target-band-specific. This is in contrast to the situation of splitting by spw, where the same uvw gets duplicated for each spw. 
However, a plain group of mstransform() calls will lead to the duplication of the pointing table of the input MS, which can be costly in storage I/O and space.
We manage the issue through an I/O-efficient mstransform wrapper function named `ms_transform`: 
by default, the pointing table of each output MS is a hardlink to the original MS (see `ms_transform` for details).


### Caltable Names

```console 
eb1_targets.04287+1801.spw16_18_20_22_24_26_28_30.selfcal.ms.hif_selfcal.{stage_label}.{solin}.{caltype}.tbl
...
```

### Image Names

For the selfcal image names, we follow a scheme loosely based on the traditional `hif_makeimlist` naming pattern.

```
iter0: dirty
iter1: initial image
iter2: 
    iter2_solin_prior.image
    iter2_solin_post.image 
iter3: final image 
```

Here is a short example:

```
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.I.iter1.image
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.I.iter2.image
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.solint1_prior.I.iter2.image
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.solint1_post.I.iter2.image
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.solint2_prior.I.iter2.image
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.solint2_post.I.iter2.image
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.solint3_prior.I.iter2.image
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.solint3_post.I.iter2.image
oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.I.iter3.image
```



