import pytest
from make_reports.resources import survey_download
import helper


class SupplyVars:
    pass


@pytest.fixture(scope="session", autouse=True)
def fixt_setup():
    #
    helper.check_test_env()

    sup_vars = SupplyVars()
    sup_vars.proj_name1 = "emorep"
    sup_vars.proj_name2 = "archival"
    yield sup_vars


@pytest.fixture(scope="session")
def fixt_dl_red(fixt_setup):
    dl_data = SupplyVars()
    dl_data.red_dict = survey_download.dl_redcap(
        [
            "demographics",
            "prescreen",
            "consent_pilot",
            "consent_v1.22",
            "guid",
            "bdi_day2",
            "bdi_day3",
        ]
    )
    yield dl_data


@pytest.fixture(scope="session")
def fixt_dl_qual(fixt_setup):
    dl_data = SupplyVars()
    dl_data.qual_dict = survey_download.dl_qualtrics(
        ["EmoRep_Session_1", "Session 2 & 3 Survey"]
    )
    yield dl_data
