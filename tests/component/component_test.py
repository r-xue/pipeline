import pytest


@pytest.mark.component
@pytest.mark.importdata
def test_chan_flagged_import():
    """Run test of importdata on small dataset with channels flagged.

    Dataset(s): [TODO]
    Task(s): hif<a,v>_importdata
    """
    pass


@pytest.mark.component
@pytest.mark.importdata
@pytest.mark.selfcal
def test_selfcal_and_selfcal_restore():
    """Run test selfcal and selfcal restoration capabilities.

    Dataset(s): [TODO]
    Task(s): hif<a,v>_importdata, hif_selfcal
    """
    pass


@pytest.mark.component
@pytest.mark.importdata
@pytest.mark.makeimages
def test_missing_spws_between_EBs():
    """Run tests with two datasets, one with all spws and one missing a single spw.

    Test 1: import dataset 1 then dataset 2
    Test 2: import dataset 2 then dataset 1

    Dataset(s): [TODO]
    Task(s): importdata, hif_makeimages, hif_makelist
    """
    pass
