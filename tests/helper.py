import os
from typing import Type
from typing import Union
import pandas as pd
import numpy as np
from make_reports.resources import sql_database


def check_test_env():
    """Raise EnvironmentError for improper testing envs."""

    # Check for EmoRep env
    msg_nat = "Please execute pytest in emorep conda env"
    try:
        conda_env = os.environ["CONDA_DEFAULT_ENV"]
        if "emorep" not in conda_env:
            raise EnvironmentError(msg_nat)
    except KeyError:
        raise EnvironmentError(msg_nat)

    # Check for required global vars
    for glob_var in [
        "PAT_REDCAP_EMOREP",
        "PAT_QUALTRICS_EMOREP",
        "SQL_PASS",
        "PROJ_DIR",
    ]:
        try:
            os.environ[glob_var]
        except KeyError:
            raise EnvironmentError(f"Missing global var : {glob_var}")


def status_change_dataframe() -> pd.DataFrame:
    """Return df with status change added.

    Used for testing report_helper.CheckStatus.add_status.

    """
    return pd.DataFrame(
        data={
            "src_subject_id": {
                0: "ER0009",
                1: "ER0017",
                2: "ER0086",
                3: "ER0162",
                4: "ER0962",
            },
            "visit1_status": {
                0: "enrolled",
                1: "enrolled",
                2: "lost",
                3: "enrolled",
                4: "enrolled",
            },
            "visit1_reason": {
                0: np.NaN,
                1: np.NaN,
                2: "Time Conflict",
                3: np.NaN,
                4: np.NaN,
            },
            "visit2_status": {
                0: "enrolled",
                1: "excluded",
                2: np.NaN,
                3: "excluded",
                4: "enrolled",
            },
            "visit2_reason": {
                0: np.NaN,
                1: "BDI",
                2: np.NaN,
                3: "BDI",
                4: np.NaN,
            },
            "visit3_status": {
                0: "enrolled",
                1: np.NaN,
                2: np.NaN,
                3: np.NaN,
                4: "excluded",
            },
            "visit3_reason": {
                0: np.NaN,
                1: np.NaN,
                2: np.NaN,
                3: np.NaN,
                4: "EAE",
            },
        }
    )


def simulate_demographics() -> pd.DataFrame:
    """Return simulated raw demographic data."""
    demo_dict = {
        "firstname": ["a", "b", "c", "d", "e", "f", "g", "h"],
        "middle_name": [
            "aa",
            "?",
            ".",
            "dd.",
            " ",
            "NA",
            "n/a",
            "h-h",
        ],
        "lastname": [
            "aaa",
            "bbb",
            "ccc",
            "ddd",
            "eee",
            "fff",
            "ggg",
            "hhh",
        ],
        "dob": [
            "20000615",
            "06152000",
            "6152000",
            "2000/06/15",
            "06-15-2000",
            "6/15/2000",
            "15/6/2000",
            "October 6 2000",
        ],
        "city": [
            "Denver",
            "Denver CO",
            "Denver, CO",
            "San Jose",
            "San Jose, California",
            "San Jose CA",
            "Sao Paolo",
            "Lagos",
        ],
        "country_birth": [
            "US",
            "USA",
            "United States",
            "United States of America",
            "US",
            "USA",
            "Brazil",
            "Portugal",
        ],
        "age": [20, 21, 22, 23, 24, 25, 26, 27],
        "gender": [1, 1, 1, 1, 2, 2, 2, 2],
        "gender_other": [
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
        ],
        "race___1": [1, 0, 0, 0, 0, 0, 0, 0],
        "race___2": [0, 1, 0, 0, 0, 0, 0, 0],
        "race___3": [0, 0, 1, 0, 0, 0, 0, 0],
        "race___4": [0, 0, 0, 1, 0, 0, 0, 0],
        "race___5": [0, 0, 0, 0, 1, 0, 0, 0],
        "race___6": [0, 0, 0, 0, 0, 1, 0, 0],
        "race___7": [0, 0, 0, 0, 0, 0, 1, 0],
        "race___8": [0, 0, 0, 0, 0, 0, 0, 1],
        "race_other": [
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            "Hispanic",
        ],
        "ethnicity": [1, 1, 1, 1, 1, 1, 2, 2],
        "years_education": [12, 13, 14, 16, 17, 18, 20, "1984"],
        "level_education": [2, 3, 4, 5, 6, 7, 8, 8],
        "handedness": [
            "right",
            "Right",
            "r",
            "R",
            "left",
            "Left",
            "l",
            "L",
        ],
        "demographics_complete": [2, 2, 2, 2, 2, 2, 2, 2],
        "record_id": [1, 2, 3, 4, 5, 6, 7, 8],
    }
    return pd.DataFrame.from_dict(demo_dict)


def simulate_bdi() -> pd.DataFrame:
    """Return simulated BDI data."""
    bdi_dict = {
        "study_id": ["ER01", "ER02", "ER03", "ER01", "ER02", "ER03"],
        "sess_id": ["day2", "day2", "day2", "day3", "day3", "day3"],
        "datetime": [
            "2020-01-15",
            "2020-01-15",
            "2020-01-15",
            "2020-01-16",
            "2020-01-16",
            "2020-01-16",
        ],
        "BDI_1": [1, 0, 0, 2, 0, 0],
        "BDI_2": [0, 1, 0, 0, 2, 0],
        "BDI_3": [0, 0, 1, 0, 0, 2],
    }
    return pd.DataFrame.from_dict(bdi_dict)


def unpack_rows(rows: list) -> dict:
    """Return unpacked rows from SQL query."""
    return {x[0]: x[1] for x in rows}


def _make_refs(db_con: Type[sql_database.DbConnect]):
    """Make ref tables needed for tests, sql_database._PrepPsr."""
    for tbl_name in [
        "ref_emo",
        "ref_task",
    ]:
        with db_con._con_cursor() as cur:
            cur.execute(f"drop table if exists db_emorep_unittest.{tbl_name}")
            db_con.con.commit()
        with db_con._con_cursor() as cur:
            sql_cmd = (
                f"create table db_emorep_unittest.{tbl_name} as "
                + f"select * from db_emorep.{tbl_name}"
            )
            cur.execute(sql_cmd)
            db_con.con.commit()


def _emorep_tbl() -> list:
    """Return list of db_emorep tables."""
    return [
        "ref_sess_task",
        "ref_subj",
        "tbl_aim",
        "tbl_als",
        "tbl_bdi",
        "tbl_demographics",
        "tbl_erq",
        "tbl_in_scan_ratings",
        "tbl_panas",
        "tbl_post_scan_ratings",
        "tbl_pswq",
        "tbl_rest_ratings",
        "tbl_rrs",
        "tbl_stai_state",
        "tbl_stai_trait",
        "tbl_survey_date",
        "tbl_tas",
    ]


def make_db_connect(db_con: Type[sql_database.DbConnect]):
    """Make tables used by tests which reference fixt_db_connect."""
    _make_refs(db_con)

    # Build tables like those in db_emorep
    for tbl_name in _emorep_tbl():
        with db_con._con_cursor() as cur:
            cur.execute(f"drop table if exists db_emorep_unittest.{tbl_name}")
            db_con.con.commit()
        with db_con._con_cursor() as cur:
            sql_cmd = (
                f"create table db_emorep_unittest.{tbl_name} like "
                + f"db_emorep.{tbl_name}"
            )
            cur.execute(sql_cmd)
            db_con.con.commit()


def clean_db_connect(db_con: Type[sql_database.DbConnect]):
    """Clean tables used by tests which reference fixt_db_connect."""
    clean_list = ["ref_emo", "ref_task"] + _emorep_tbl()
    for tbl_name in clean_list:
        with db_con._con_cursor() as cur:
            cur.execute(f"delete from db_emorep_unittest.{tbl_name}")
            db_con.con.commit()


def df_foo() -> pd.DataFrame:
    """Return short foo df."""
    return pd.DataFrame.from_dict(
        {"subj_id": [99, 999], "subj_name": ["FOO99", "FOO999"]}
    )


def proj_emorep() -> Union[str, os.PathLike]:
    """Return path to emorep project directory."""
    return "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion"


def test_emorep() -> Union[str, os.PathLike]:
    """Return path to emorep test directory."""
    return os.path.join(proj_emorep(), "code/unit_test/make_reports")


def proj_archival() -> Union[str, os.PathLike]:
    """Return path to archival project directory."""
    return "/mnt/keoki/experiments2/EmoRep/Exp3_Classify_Archival"


def test_archival() -> Union[str, os.PathLike]:
    """Return path to archival test directory."""
    return os.path.join(proj_archival(), "code/unit_test/make_reports")
