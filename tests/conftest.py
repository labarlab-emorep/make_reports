import pytest
import os
import shutil
import glob
from typing import Iterator
from typing import Type
from make_reports.resources import survey_download
from make_reports.resources import survey_clean
from make_reports.resources import sql_database
from make_reports.resources import manage_data
import helper


class SupplyVars:
    """Allow each fixture to add respective attrs."""

    pass


@pytest.fixture(scope="session", autouse=True)
def fixt_setup() -> Iterator[SupplyVars]:
    #
    helper.check_test_env()
    test_emorep = helper.test_emorep()
    test_archival = helper.test_archival()
    for _dir in [test_emorep, test_archival]:
        if not os.path.exists(_dir):
            os.makedirs(_dir)

    supp_vars = SupplyVars()
    supp_vars.proj_name1 = "emorep"
    supp_vars.proj_name2 = "archival"
    supp_vars.proj_emorep = helper.proj_emorep()
    supp_vars.proj_archival = helper.proj_archival()
    supp_vars.test_emorep = test_emorep
    supp_vars.test_archival = test_archival
    yield supp_vars


def pytest_sessionfinish(session, exitstatus):
    """Teardown if all tests passed."""
    return
    if 0 == exitstatus:
        try:
            shutil.rmtree(helper.test_emorep())
        except FileNotFoundError:
            pass


@pytest.fixture(scope="session")
def fixt_dl_red() -> Iterator[SupplyVars]:
    """Download REDCap data."""
    supp_vars = SupplyVars()
    supp_vars.red_dict = survey_download.dl_redcap(
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
    yield supp_vars


@pytest.fixture(scope="session")
def fixt_cl_red(fixt_dl_red) -> Iterator[SupplyVars]:
    """Clean select REDCap data."""
    clean_red = survey_clean.CleanRedcap()
    supp_vars = SupplyVars()
    clean_red.clean_bdi_day23(fixt_dl_red.red_dict["bdi_day3"][1])
    supp_vars.df_bdi = clean_red.df_study
    clean_red.clean_demographics(helper.simulate_demographics())
    supp_vars.df_demo = clean_red.df_study
    yield supp_vars


@pytest.fixture(scope="session")
def fixt_dl_qual() -> Iterator[SupplyVars]:
    """Download Qualtrics data."""
    supp_vars = SupplyVars()
    supp_vars.qual_dict = survey_download.dl_qualtrics(
        [
            "EmoRep_Session_1",
            "Session 2 & 3 Survey",
            "FINAL - EmoRep Stimulus Ratings - fMRI Study",
        ]
    )
    yield supp_vars


@pytest.fixture(scope="session")
def fixt_cl_qual(fixt_dl_qual) -> Iterator[SupplyVars]:
    """Clean Qualtrics data."""
    supp_vars = SupplyVars()
    clean_qual = survey_clean.CleanQualtrics()

    # Clean session 1 surveys
    clean_qual.clean_session_1(fixt_dl_qual.qual_dict["EmoRep_Session_1"][1])
    supp_vars.s1_data = clean_qual.data_study

    # Clean session23 surveys
    clean_qual.clean_session_23(
        fixt_dl_qual.qual_dict["Session 2 & 3 Survey"][1]
    )
    supp_vars.s23_data = clean_qual.data_study

    # Clean postscan ratings
    clean_qual.clean_postscan_ratings(
        fixt_dl_qual.qual_dict["FINAL - EmoRep Stimulus Ratings - fMRI Study"][
            1
        ]
    )
    supp_vars.post_data = clean_qual.data_study

    # Yield
    supp_vars.clean_qual = clean_qual
    yield supp_vars


@pytest.fixture(scope="session")
def fixt_test_data(fixt_setup) -> Iterator[SupplyVars]:
    # Orienting paths
    proj_raw = os.path.join(
        fixt_setup.proj_emorep, "data_scanner_BIDS/rawdata"
    )
    test_raw = os.path.join(
        fixt_setup.test_emorep, "data_scanner_BIDS/rawdata"
    )

    # Copy rest beh data to test location
    subj = "sub-ER0016"
    sess = "ses-day2"
    subj_beh = os.path.join(proj_raw, subj, sess, "beh")
    test_beh = os.path.join(test_raw, subj, sess, "beh")
    if not os.path.exists(test_beh):
        os.makedirs(test_beh)
    shutil.copytree(subj_beh, test_beh, dirs_exist_ok=True)

    # Copy func events to test location
    subj_func = subj_beh.replace("beh", "func")
    tsv_list = glob.glob(f"{subj_func}/*tsv")
    test_func = test_beh.replace("beh", "func")
    if not os.path.exists(test_func):
        os.makedirs(test_func)
    for tsv_path in tsv_list:
        shutil.copy2(tsv_path, test_func)

    # Clean rest beh
    sess_id = sess.split("-")[-1]
    df_rest = survey_clean.clean_rest_ratings(sess_id, test_raw)

    # Clean task data
    get_task = manage_data.GetTask(fixt_setup.test_emorep)
    get_task.get_task(db_name="db_emorep_unittest")

    # Yield object
    supp_vars = SupplyVars()
    supp_vars.test_raw = test_raw
    supp_vars.df_rest = df_rest
    supp_vars.df_task = get_task.clean_task["study"]["visit_day2"][
        "in_scan_task"
    ]
    yield supp_vars


@pytest.fixture(scope="package")
def fixt_db_connect() -> Type[sql_database.DbConnect]:
    db_con = sql_database.DbConnect(db_name="db_emorep_unittest")
    helper.make_db_connect(db_con)
    yield db_con
    helper.clean_db_connect(db_con)
    db_con.close_con()


@pytest.fixture(scope="class")
def fixt_db_update(
    fixt_cl_red, fixt_cl_qual, fixt_test_data, fixt_db_connect
) -> Iterator[SupplyVars]:
    db_up = sql_database.DbUpdate(db_con=fixt_db_connect)

    # Aggregate data for easy testing, maintenance
    supp_vars = SupplyVars()
    supp_vars.db_up = db_up
    supp_vars.df_aim = fixt_cl_qual.s1_data["visit_day1"]["AIM"]
    supp_vars.df_rrs = fixt_cl_qual.s1_data["visit_day1"]["RRS"]
    supp_vars.df_bdi = fixt_cl_red.df_bdi
    supp_vars.df_demo = fixt_cl_red.df_demo
    supp_vars.df_rest = fixt_test_data.df_rest
    supp_vars.df_task = fixt_test_data.df_task
    yield supp_vars
