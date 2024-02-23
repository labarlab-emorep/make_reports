"""Functions for downloading survey data from RedCap and Qualtrics.

dl_mri_log : get MRI visit logs
dl_completion_log : get completion log
dl_prescreening : get prescreening responses
dl_redcap : download REDCap surveys
download qualtrics : download Qualtrics surveys

"""

import json
import pandas as pd
import importlib.resources as pkg_resources
from make_reports.resources import report_helper
from make_reports import reference_files


def dl_mri_log():
    """Download and combine MRI Visit Logs by session.

    Returns a reduced report, containing only a datetime column
    and visit identifier. Used for calculating weekly scan
    attempts.

    Returns
    -------
    pd.DataFrame

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


def dl_completion_log():
    """Return Completion log.

    Only includes participants who have a value in the
    'prescreening_completed' field.

    Returns
    -------
    pd.DataFrame

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


def dl_prescreening():
    """Return reduced prescreening survey.

    Returns
    -------
    pd.DataFrame

    """
    with pkg_resources.open_text(
        reference_files, "report_keys_redcap.json"
    ) as jf:
        report_keys = json.load(jf)
    df_pre = report_helper.pull_redcap_data(report_keys["prescreen"])
    df_pre = df_pre[
        (df_pre["permission"] == 1)
        & (df_pre["prescreening_survey_complete"] == 2)
    ].reset_index(drop=True)
    df_out = df_pre[
        ["record_id", "permission", "prescreening_survey_complete"]
    ].copy()
    return df_out


def _dl_info(database, survey_name):
    """Gather API survey IDs and organization mapping.

    Parameters
    ----------
    database : str
        [redcap | qualtrics]
    survey_name : str
        Individual survey name from database

    Returns
    -------
    tuple
        [0] dict : key = survey name, value = organization map
        [1] dict : key = survey name, value = survey ID

    Raises
    ------
    ValueError
        When a survey_name is specified that does not exist in the
        reference methods and files.

    """
    # Load keys
    with pkg_resources.open_text(
        reference_files, f"report_keys_{database}.json"
    ) as jf:
        report_keys = json.load(jf)

    # Use proper report_helper method
    h_meth = getattr(report_helper, f"{database}_dict")
    h_map = h_meth()

    # Check if survey_name is valid
    if survey_name not in h_map.keys():
        raise ValueError(
            f"Survey name {survey_name} was not found in "
            + f"make_reports.report_helper.{database}_dict."
        )

    # Determine which surveys to download based on user input
    report_org = {survey_name: h_map[survey_name]}

    # Check that surveys have a key
    for h_key in report_org:
        if h_key not in report_keys.keys():
            raise ValueError(
                f"Missing key pair for survey : {h_key}, check "
                + f"make_reports.reference_files.report_keys_{database}.json"
            )
    return (report_org, report_keys)


def dl_redcap(proj_dir, survey_list):
    """Download EmoRep survey data from RedCap.

    Parameters
    ----------
    proj_dir : path
        Location of parent directory for project
    survey_list : list
        RedCap survey names

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
        "consent_pilot",
        "consent_v1.22",
        "guid",
        "bdi_day2",
        "bdi_day3",
    ]
    for chk in survey_list:
        if chk not in valid_list:
            raise ValueError(f"Survey name '{chk}' not valid")

    # Download, return data
    out_dict = {}
    for sur_name in survey_list:
        rep_org, rep_key = _dl_info("redcap", sur_name)
        df = report_helper.pull_redcap_data(rep_key[sur_name])
        out_dict[sur_name] = (rep_org[sur_name], df)
    return out_dict


def dl_qualtrics(proj_dir, survey_list):
    """Download EmoRep survey data from Qualtrics.

    Parameters
    ----------
    proj_dir : path
        Location of parent directory for project
    survey_name : list
        Qualtrics survey names

    Returns
    -------
    dict
        {survey_name: (visit, pd.DataFrame)}, e.g.
        {"Session 2 & 3 Survey_latest": ("visit_day23", pd.DataFrame)}

    """
    print("\nPulling Qualtrics surveys ...")
    valid_list = [
        "EmoRep_Session_1",
        "Session 2 & 3 Survey",
        "FINAL - EmoRep Stimulus Ratings - fMRI Study",
    ]
    for chk in survey_list:
        if chk not in valid_list:
            raise ValueError(f"Survey name '{chk}' not valid")

    # Get survey names, keys, directory mapping
    out_dict = {}
    for sur_name in survey_list:
        # Setup for data pull
        report_org, report_keys = _dl_info("qualtrics", sur_name)
        datacenter_id = report_keys["datacenter_ID"]
        dir_name = report_org[sur_name]
        post_labels = (
            True
            if sur_name == "FINAL - EmoRep Stimulus Ratings - fMRI Study"
            else False
        )
        survey_id = report_keys[sur_name]

        # Get data
        df = report_helper.pull_qualtrics_data(
            sur_name,
            survey_id,
            datacenter_id,
            post_labels,
        )
        if isinstance(dir_name, list):
            out_dict[sur_name] = ("visit_day23", df)
        else:
            out_dict[sur_name] = (dir_name, df)
    return out_dict
