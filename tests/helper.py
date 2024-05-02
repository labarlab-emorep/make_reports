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
