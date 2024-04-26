import pytest


def test_get_metrics():
    pass


@pytest.mark.chk_data
def test_CheckProjectMri(fixt_setup):
    assert "emorep" == fixt_setup.proj_name1


def test_check_emorep_all():
    pass
