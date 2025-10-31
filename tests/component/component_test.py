import os

import pytest

from pipeline.infrastructure import casa_tools
from tests.component.component_tester import ComponentTester


@pytest.mark.component
@pytest.mark.importdata
def test_uid___A002_X1181695_X1c6a4_8ant__chan_flagged_import__component():
    """Run test of importdata on small dataset with channels flagged.

    Dataset(s):                 uid___A002_X1181695_X1c6a4_8ant_chans_flagged.ms
    Task(s):                    hifa_importdata
    """
    data_dir = 'pl-componenttest/chan_flagged_import'
    visname = 'uid___A002_X1181695_X1c6a4_8ant_chans_flagged.ms'
    tasks = [
        ('hifa_importdata', {'vis': casa_tools.utils.resolve(os.path.join(data_dir, visname))}),
    ]
    pr = ComponentTester(
        visname=[visname],
        tasks=tasks,
        output_dir='chan_flagged_import',
        expectedoutput_dir=data_dir,
        )

    pr.run()


@pytest.mark.component
@pytest.mark.importdata
@pytest.mark.selfcal
def test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target__selfcal_and_selfcal_restore__component():
    """Run test selfcal and selfcal restoration capabilities.

    Dataset(s):                 uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms
    Task(s):                    hifa_importdata, hif_selfcal
    """
    data_dir = 'pl-unittest'
    visname = 'uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'
    tasks = [
        ('hifa_importdata', {'vis': casa_tools.utils.resolve(os.path.join(data_dir, visname)),
                             'datacolumns': {'data': 'regcal_contline'}}),
        ('hif_selfcal', {}),
        ('hif_selfcal', {'restore_only': True}),
    ]
    pr = ComponentTester(
        visname=['uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms'],
        tasks=tasks,
        output_dir='selfcal_and_selfcal_restore',
        expectedoutput_dir='pl-componenttest/selfcal_and_selfcal_restore',
        )

    pr.run()


@pytest.mark.component
@pytest.mark.importdata
@pytest.mark.makeimages
def test_OphA_X1__missing_spws_first_EB__component():
    """Run tests with two datasets, one with all spws and one missing a single spw.

    test_missing_spws_first_EB: import dataset 1 then dataset 2
    test_missing_spws_second_EB: import dataset 2 then dataset 1

    Dataset(s):                 OphA-X1_spw0_2.ms, OphA-X1_spw0_2_3.ms
    Task(s):                    hifv_importdata, hif_makelist, hif_makeimages
    """
    data_dir = 'pl-componenttest/missing_spws'
    visnames = ['OphA-X1_spw0_2.ms', 'OphA-X1_spw0_2_3.ms']
    vislist = [casa_tools.utils.resolve(os.path.join(data_dir, visname)) for visname in visnames]
    tasks = [
        ('hifv_importdata', {'vis': vislist}),
        ('hif_makeimlist', {'specmode': 'cont'}),
        ('hif_makeimages', {})
    ]
    pr = ComponentTester(
        visname=visnames,
        tasks=tasks,
        output_dir='missing_spws_first_EB',
        expectedoutput_dir=data_dir,
        )

    pr.run()


@pytest.mark.component
@pytest.mark.importdata
@pytest.mark.makeimages
def test_OphA_X1__missing_spws_second_EB__component():
    """Run tests with two datasets, one with all spws and one missing a single spw.

    test_missing_spws_first_EB: import dataset 1 then dataset 2
    test_missing_spws_second_EB: import dataset 2 then dataset 1

    Dataset(s):                 OphA-X1_spw0_2.ms, OphA-X1_spw0_2_3.ms
    Task(s):                    hifv_importdata, hif_makelist, hif_makeimages
    """
    data_dir = 'pl-componenttest/missing_spws'
    visnames = ['OphA-X1_spw0_2_3.ms', 'OphA-X1_spw0_2.ms']
    vislist = [casa_tools.utils.resolve(os.path.join(data_dir, visname)) for visname in visnames]
    tasks = [
        ('hifv_importdata', {'vis': vislist}),
        ('hif_makeimlist', {'specmode': 'cont'}),
        ('hif_makeimages', {})
    ]
    pr = ComponentTester(
        visname=visnames,
        tasks=tasks,
        output_dir='missing_spws_second_EB',
        expectedoutput_dir=data_dir,
        )

    pr.run()
