"""Functions for downloading survey data from RedCap and Qualtrics."""
import os
import json
import importlib.resources as pkg_resources
from make_reports import report_helper
from make_reports import reference_files


def download_redcap(proj_dir, redcap_token, survey_name=None):
    """Download EmoRep survey data from RedCap.

    Download surveys and write original/raw dataframes to <proj_dir>.

    Parameters
    ----------
    proj_dir : path
        Location of parent directory for project
    redcap_token : str
        API token for RedCap

    Returns
    -------

    """
    print("\nPulling RedCap surveys ...")

    # Load redcap info for api pull data
    with pkg_resources.open_text(
        reference_files, "report_keys_redcap.json"
    ) as jf:
        report_keys_redcap = json.load(jf)

    redcap_dict = report_helper.redcap_dict()

    # TODO validate survey list in report_keys_redcap
    # TODO validate survey_name in redcap_dict
    iter_dict = (
        {survey_name: redcap_dict[survey_name]} if survey_name else redcap_dict
    )

    out_dict = {}
    for sur_name, dir_name in iter_dict.items():
        print(f"\t Downloading RedCap survey : {sur_name}")
        report_id = report_keys_redcap[sur_name]
        df = report_helper.pull_redcap_data(redcap_token, report_id)
        out_file = os.path.join(
            proj_dir,
            "data_survey",
            dir_name,
            "data_raw",
            f"df_{sur_name}_latest.csv",
        )
        df.to_csv(out_file, index=False, na_rep="")
        print(f"\t Wrote : {out_file}")
        out_dict[sur_name] = df
    return out_dict


def download_qualtrics(proj_dir, qualtrics_token, survey_name=None):
    """Title.

    Desc.

    Parameters
    ----------
    proj_dir
    qualtrics_token

    Returns
    -------

    """
    print("\nPulling Qualtrics surveys ...")
    with pkg_resources.open_text(
        reference_files, "report_keys_qualtrics.json"
    ) as jf:
        report_keys_qualtrics = json.load(jf)
    datacenter_id = report_keys_qualtrics["datacenter_ID"]

    # qualtrics_dict = {
    #     "EmoRep_Session_1": "visit_day1",
    #     "FINAL - EmoRep Stimulus Ratings - fMRI Study": "post_scan_ratings",
    #     "Session 2 & 3 Survey": "visit_day23",
    # }
    qualtrics_dict = report_helper.qualtrics_dict()

    # TODO validate survey list in report_keys_redcap
    iter_dict = (
        {survey_name: qualtrics_dict[survey_name]}
        if survey_name
        else qualtrics_dict
    )

    out_dict = {}
    for sur_name, dir_name in iter_dict.items():
        post_labels = True if dir_name == "post_scan_ratings" else False
        survey_id = report_keys_qualtrics[sur_name]
        df = report_helper.pull_qualtrics_data(
            sur_name,
            survey_id,
            datacenter_id,
            qualtrics_token,
            post_labels,
        )
        if dir_name == "visit_day23":
            for day in ["visit_day2", "visit_day3"]:
                out_file = os.path.join(
                    proj_dir,
                    "data_survey",
                    day,
                    "data_raw",
                    f"{sur_name}_latest.csv",
                )
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
            df.to_csv(out_file, index=False, na_rep="")
            print(f"\tWrote : {out_file}")
            out_dict[sur_name] = df
    return out_dict
