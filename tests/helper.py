import os
import pandas as pd
import numpy as np


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
