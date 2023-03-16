"""Functions for downloading survey data from RedCap and Qualtrics."""
import os
import json
import importlib.resources as pkg_resources
from make_reports.resources import report_helper
from make_reports import reference_files


def _download_info(database, survey_name=None):
    """Gather API survey IDs and organiation mapping.

    Parameters
    ----------
    database : str
        [redcap | qualtrics]
    survey_name : str
        Individual survey name from database (optional)

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
    if survey_name and survey_name not in h_map.keys():
        raise ValueError(
            f"Survey name {survey_name} was not found in "
            + f"make_reports.report_helper.{database}_dict."
        )

    # Determine which surveys to download based on user input
    report_org = {survey_name: h_map[survey_name]} if survey_name else h_map

    # Check that surveys have a key
    for h_key in report_org:
        if h_key not in report_keys.keys():
            raise ValueError(
                f"Missing key pair for survey : {h_key}, check "
                + f"make_reports.reference_files.report_keys_{database}.json"
            )
    return (report_org, report_keys)


def download_redcap(proj_dir, redcap_token, survey_name=None):
    """Download EmoRep survey data from RedCap.

    Parameters
    ----------
    proj_dir : path
        Location of parent directory for project
    redcap_token : str
        API token for RedCap
    survey_name : str
        RedCap survey name (optional)

    Returns
    -------
    dict
        key : survey name
        value : pd.DataFrame

    Notes
    -----
    Study data written to:
        <proj_dir>/data_survey/<visit>/data_raw

    """
    print("\nPulling RedCap surveys ...")

    # Get survey names, keys, directory mapping
    report_org, report_keys = _download_info("redcap", survey_name)

    # Download and write desired RedCap surveys
    out_dict = {}
    for sur_name, dir_name in report_org.items():
        print(f"\t Downloading RedCap survey : {sur_name}")
        report_id = report_keys[sur_name]
        df = report_helper.pull_redcap_data(redcap_token, report_id)

        # Setup output name, location
        out_file = os.path.join(
            proj_dir,
            "data_survey",
            dir_name,
            "data_raw",
            f"df_{sur_name}_latest.csv",
        )
        out_dir = os.path.dirname(out_file)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # Write, update dictionary
        df.to_csv(out_file, index=False, na_rep="")
        print(f"\t Wrote : {out_file}")
        out_dict[sur_name] = df
    return out_dict


def download_qualtrics(proj_dir, qualtrics_token, survey_name=None):
    """Download EmoRep survey data from Qualtrics.

    Parameters
    ----------
    proj_dir : path
        Location of parent directory for project
    qualtrics_token : str
        API token for Qualtrics
    survey_name : str
        Qualtrics survey name (optional)

    Returns
    -------
    dict
        {day: {survey_name: pd.DataFrame}}

    Notes
    -----
    Study data written to:
        <proj_dir>/data_survey/<visit>/data_raw

    """
    print("\nPulling Qualtrics surveys ...")

    # Get survey names, keys, directory mapping
    report_org, report_keys = _download_info("qualtrics", survey_name)
    datacenter_id = report_keys["datacenter_ID"]

    # Download and write desired Qualtrics surveys
    out_dict = {}
    for sur_name, dir_name in report_org.items():
        post_labels = (
            True
            if sur_name == "FINAL - EmoRep Stimulus Ratings - fMRI Study"
            else False
        )
        survey_id = report_keys[sur_name]
        df = report_helper.pull_qualtrics_data(
            sur_name,
            survey_id,
            datacenter_id,
            qualtrics_token,
            post_labels,
        )

        # Account for visit/directory identifier
        if type(dir_name) == list:

            # Write same file to visit_day2 and visit_day3 since
            # the survey has info for both sessions.
            for day in dir_name:

                # Setup output location, file
                out_file = os.path.join(
                    proj_dir,
                    "data_survey",
                    day,
                    "data_raw",
                    f"{sur_name}_latest.csv",
                )
                out_dir = os.path.dirname(out_file)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)

                # Write out and update return dict
                df.to_csv(out_file, index=False, na_rep="")
                print(f"\tWrote : {out_file}")
                out_dict[day] = df
        else:
            out_file = os.path.join(
                proj_dir,
                "data_survey",
                dir_name,
                "data_raw",
                f"{sur_name}_latest.csv",
            )
            out_dir = os.path.dirname(out_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            df.to_csv(out_file, index=False, na_rep="")
            print(f"\tWrote : {out_file}")
            out_dict[sur_name] = df
    return out_dict
