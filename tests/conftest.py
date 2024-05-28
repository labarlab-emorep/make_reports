import pytest
import os
from typing import Iterator
from make_reports.resources import survey_download
from make_reports.resources import survey_clean
import helper


class SupplyVars:
    """Allow each fixture to add respective attrs."""

    pass


@pytest.fixture(scope="session", autouse=True)
def fixt_setup() -> Iterator[SupplyVars]:
    #
    helper.check_test_env()

    #
    test_dir = (
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion"
        + "/code/unit_test/make_reports"
    )
    test_emorep = os.path.join(test_dir, "emorep")
    test_archival = os.path.join(test_dir, "archival")
    for _dir in [test_emorep, test_archival]:
        if not os.path.exists(_dir):
            os.makedirs(_dir)

    sup_vars = SupplyVars()
    sup_vars.proj_name1 = "emorep"
    sup_vars.proj_name2 = "archival"
    sup_vars.test_emorep = test_emorep
    yield sup_vars


# TODO make teardown


@pytest.fixture(scope="session")
def fixt_dl_red(fixt_setup) -> Iterator[SupplyVars]:
    """Download REDCap data."""
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
def fixt_dl_qual(fixt_setup) -> Iterator[SupplyVars]:
    """Download Qualtrics data."""
    dl_data = SupplyVars()
    dl_data.qual_dict = survey_download.dl_qualtrics(
        [
            "EmoRep_Session_1",
            "Session 2 & 3 Survey",
            "FINAL - EmoRep Stimulus Ratings - fMRI Study",
        ]
    )
    yield dl_data


@pytest.fixture(scope="session")
def fixt_cl_qual(fixt_dl_qual) -> Iterator[SupplyVars]:
    """Clean Qualtrics data."""
    cl_data = SupplyVars()
    clean_qual = survey_clean.CleanQualtrics()

    # Clean session 1 surveys
    clean_qual.clean_session_1(fixt_dl_qual.qual_dict["EmoRep_Session_1"][1])
    cl_data.s1_data = clean_qual.data_study

    # Clean session23 surveys
    clean_qual.clean_session_23(
        fixt_dl_qual.qual_dict["Session 2 & 3 Survey"][1]
    )
    cl_data.s23_data = clean_qual.data_study

    # Clean postscan ratings
    clean_qual.clean_postscan_ratings(
        fixt_dl_qual.qual_dict["FINAL - EmoRep Stimulus Ratings - fMRI Study"][
            1
        ]
    )
    cl_data.post_data = clean_qual.data_study

    # Yield
    cl_data.clean_qual = clean_qual
    yield cl_data
