import pytest


@pytest.mark.chk_data
def test_CheckProjectMri(fixt_setup):
    assert "emorep" == fixt_setup.proj_name1
