"""Functions for downloading survey data from RedCap and Qualtrics.

dl_mri_log : get MRI visit logs
dl_completion_log : get completion log
dl_redcap : download REDCap surveys, demographics, consent,
    guids, and screener
dl_qualtrics : download Qualtrics surveys

"""

import json
import pandas as pd
import importlib.resources as pkg_resources
from make_reports.resources import report_helper
from make_reports import reference_files


def dl_mri_log() -> pd.DataFrame:
    """Download and combine MRI Visit Logs by session.

    Returns a reduced report, containing only a datetime column
    and visit identifier. Used for calculating weekly scan
    attempts.

    """

    def _get_visit_log(rep_key: str, day: str) -> pd.DataFrame:
        """Return dataframe of MRI visit log datetimes."""
        # Manage differing column names
        col_switch = {
            "day2": ("date_mriv3_v2", "session_numberv3_v2"),
            "day3": ("date_mriv3", "session_numberv3"),
        }
        col_date = col_switch[day][0]
        col_value = col_switch[day][1]

        # Download dataframe, clean up
        df_visit = report_helper.pull_redcap_data(rep_key)
        df_visit = df_visit[df_visit[col_value].notna()].reset_index(drop=True)
        df_visit.rename(columns={col_date: "datetime"}, inplace=True)
        df_visit["datetime"] = df_visit["datetime"].astype("datetime64[ns]")

        # Extract values of interest
        df_out = df_visit[["datetime"]].copy()
        df_out["Visit"] = day
        return df_out

    # Access report keys, visit info
    with pkg_resources.open_text(
        reference_files, "log_keys_redcap.json"
    ) as jf:
        report_keys = json.load(jf)
    df2 = _get_visit_log(report_keys["mri_visit2"], "day2")
    df3 = _get_visit_log(report_keys["mri_visit3"], "day3")

    # Combine dataframes, ready for weekly totaling
    df = pd.concat([df2, df3], ignore_index=True)
    df = df.sort_values(by=["datetime"]).reset_index(drop=True)
    return df


def dl_completion_log() -> pd.DataFrame:
    """Return Completion log.

    Only includes participants who have a value in the
    'prescreening_completed' field.

    """
    with pkg_resources.open_text(
        reference_files, "log_keys_redcap.json"
    ) as jf:
        report_keys = json.load(jf)
    df_compl = report_helper.pull_redcap_data(report_keys["completion_log"])
    df_compl = df_compl[
        df_compl["prescreening_completed"].notna()
    ].reset_index(drop=True)
    return df_compl


def _get_ids(database: str) -> dict:
    """Return API survey IDs.

    Parameters
    ----------
    database : str
        {"redcap", "qualtrics"}
        Database name

    """
    if database not in ["redcap", "qualtrics"]:
        raise ValueError(f"Unexpected database name : {database}")

    # Load keys
    with pkg_resources.open_text(
        reference_files, f"report_keys_{database}.json"
    ) as jf:
        report_keys = json.load(jf)
    return report_keys


def dl_redcap(survey_list):
    """Download EmoRep survey data from RedCap.

    Parameters
    ----------
    survey_list : list
        RedCap survey names as found in
        reference_files.report_keys_redcap.json

    Returns
    -------
    dict
        {survey_name: (str|bool, pd.DataFrame)}, e.g.
        {"bdi_day2": ("visit_day2", pd.DataFrame)}
        {"demographics": (False, pd.DataFrame)}

    """
    # Validate survey list
    valid_list = [
        "demographics",
        "prescreen",
        "consent_pilot",
        "consent_v1.22",
        "guid",
        "bdi_day2",
        "bdi_day3",
    ]
    for chk in survey_list:
        if chk not in valid_list:
            raise ValueError(f"Survey name '{chk}' not valid")

    # Determine organization directory names, get survey keys
    org_dict = report_helper.redcap_dict()
    sur_keys = _get_ids("redcap")

    # Download, return data
    out_dict = {}
    for sur_name in survey_list:
        df = report_helper.pull_redcap_data(sur_keys[sur_name])
        out_dict[sur_name] = (org_dict[sur_name], df)
    return out_dict


def dl_qualtrics(survey_list):
    """Download EmoRep survey data from Qualtrics.

    Parameters
    ----------
    survey_name : list
        Qualtrics survey names as found in
        reference_files.report_keys_qualtrics.json

    Returns
    -------
    dict
        {survey_name: (visit, pd.DataFrame)}, e.g.
        {"Session 2 & 3 Survey_latest": ("visit_day23", pd.DataFrame)}

    """
    # Validate survey list
    valid_list = [
        "EmoRep_Session_1",
        "Session 2 & 3 Survey",
        "FINAL - EmoRep Stimulus Ratings - fMRI Study",
    ]
    for chk in survey_list:
        if chk not in valid_list:
            raise ValueError(f"Survey name '{chk}' not valid")

    # Determine organization directory names, get survey keys
    org_dict = report_helper.qualtrics_dict()
    sur_keys = _get_ids("qualtrics")

    # Get survey names, keys, directory mapping
    out_dict = {}
    for sur_name in survey_list:
        # Determine post_label status
        post_labels = (
            True
            if sur_name == "FINAL - EmoRep Stimulus Ratings - fMRI Study"
            else False
        )

        # Get data
        df = report_helper.pull_qualtrics_data(
            sur_name,
            sur_keys[sur_name],
            sur_keys["datacenter_ID"],
            post_labels,
        )

        # Build dict value
        if isinstance(org_dict[sur_name], list):
            out_dict[sur_name] = ("visit_day23", df)
        else:
            out_dict[sur_name] = (org_dict[sur_name], df)
    return out_dict
