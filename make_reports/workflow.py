"""Setup workflows for specific types of reports."""
# %%
import os
from datetime import datetime
from make_reports import survey_download, build_reports


# %%
def make_manager_reports(manager_reports, query_date, proj_dir, redcap_token):
    """Make reports for the lab manager.

    Coordinate the use of build_reports.ManagerRegular to generate
    desired nih12, nih4, or duke3 report. Write dataframes to
    <proj_dir>/documents/manager_reports.

    Parameters
    ----------
    manager_reports : list
        Desired report names e.g. ["nih4", "nih12"]
    query_date : str, datetime
        Date for finding report range
    proj_dir : path
        Project's experiment directory
    redcap_token : str
        RedCap API token

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If redcap api token not supplied
        If report requested is not found in valid_mr_args
        If query_date occures before 2022-03-31

    """
    # Validate redcap token
    if not redcap_token:
        raise ValueError(
            "RedCap API token required for --manager-reports."
            + " Please specify --redcap-token."
        )

    # Validate manager_reports arguments
    valid_mr_args = ["nih12", "nih4", "duke3"]
    for report in manager_reports:
        if report not in valid_mr_args:
            raise ValueError(
                "--manager-reports contained inappropriate "
                + f"argument : {report}"
            )

    # Validate query date
    if isinstance(query_date, str):
        query_date = datetime.strptime(query_date, "%Y-%m-%d").date()
    if query_date < datetime.strptime("2022-03-31", "%Y-%m-%d").date():
        raise ValueError(f"Query date {query_date} precedes 2022-03-31.")

    # Setup output location
    manager_dir = os.path.join(proj_dir, "documents/manager_reports")
    if not os.path.exists(manager_dir):
        os.makedirs(manager_dir)

    # Query RedCap demographic info
    redcap_demo = survey_download.GetRedcapDemographic(redcap_token)

    # Generate reports
    for report in manager_reports:
        mr = build_reports.ManagerRegular(
            query_date, redcap_demo.final_demo, report
        )

        # Setup file name, write csv
        start_date = mr.range_start.strftime("%Y-%m-%d")
        end_date = mr.range_end.strftime("%Y-%m-%d")
        out_file = os.path.join(
            manager_dir, f"report_{report}_{start_date}_{end_date}.csv"
        )
        print(f"\tWriting : {out_file}")
        mr.df_report.to_csv(out_file, index=False, na_rep="")
        del mr


def make_survey_reports(proj_dir, post_labels, qualtrics_token, redcap_token):
    """Make raw and clean dataframes from RedCap, Qualtrics data.

    Download data from Qualtrics and RedCap, organize according to
    visit/session type, and write both original (raw) and cleaned
    dataframes.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    post_labels : bool
        Whether to use labels in post_scan_ratings pull
    qualtrics_token : str
        Qualtrics API token
    redcap_token : str
        RedCap API token

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If both redcap and qualtrics api tokens are not provided

    Notes
    -----
    EmoRep survey data file structure is hardcoded.

    """
    # Validate redcap and qualtrics tokens
    if not redcap_token and not qualtrics_token:
        raise ValueError(
            "RedCap and Qualtrics API token required for --pull-surveys."
            + " Please specify --redcap-token and --qualtrics-token."
        )

    # Set output parent directory
    survey_par = os.path.join(proj_dir, "data_survey")

    # Make raw and clean dataframes from qualtrics surveys
    qualtrics_data = survey_download.GetQualtricsSurveys(
        qualtrics_token, post_labels
    )
    for visit in [
        "visit_day1",
        "visit_day2",
        "visit_day3",
        "post_scan_ratings",
    ]:
        # Write raw dataframes
        survey_name, df_raw = qualtrics_data.make_raw_reports(visit)
        out_file = (
            f"{survey_name}_labels_latest.csv"
            if post_labels
            else f"{survey_name}_latest.csv"
        )
        out_raw = os.path.join(survey_par, visit, "data_raw", out_file)
        print(f"\nWriting raw {visit} survey data : \n\t{out_raw}")
        df_raw.to_csv(out_raw, index=False, na_rep="")

        # TODO clean post_scan_ratings
        if visit == "post_scan_ratings":
            continue

        # Write cleaned dataframes
        print(f"\nMaking clean day for visit : {visit}")
        qualtrics_data.make_clean_reports(visit)
        for sur_name, sur_df in qualtrics_data.clean_visit.items():
            out_clean = os.path.join(
                survey_par, visit, "data_clean", f"df_{sur_name}.csv"
            )
            print(f"\tWriting clean survey data : \n\t{out_clean}")
            sur_df.to_csv(out_clean, index=False, na_rep="")

    # Make raw and clean dataframes from redcap
    redcap_demo = survey_download.GetRedcapDemographic(redcap_token)
    redcap_data = survey_download.GetRedcapSurveys(redcap_token)
    for visit in ["visit_day2", "visit_day3"]:

        # Write raw dataframes
        df_bdi_raw = redcap_data.make_raw_reports(visit)
        out_raw = os.path.join(survey_par, visit, "data_raw/df_BDI_latest.csv")
        print(f"Writing raw {visit} BDI data : \n\t {out_raw}")
        df_bdi_raw.to_csv(out_raw, index=False, na_rep="")

        # Write cleaned dataframes
        redcap_data.make_clean_reports(visit, redcap_demo.subj_consent)
        out_clean = os.path.join(survey_par, visit, "data_clean/df_BDI.csv")
        print(f"Writing clean {visit} BDI data : \n\t {out_clean}")
        redcap_data.df_clean_bdi.to_csv(out_clean, index=False, na_rep="")


def make_nda_reports(
    nda_reports, proj_dir, post_labels, qualtrics_token, redcap_token
):
    """Make reports and organize data for NDAR upload.

    Generate requested NDAR reports and organize data (if required) for the
    biannual upload.

    Parameters
    ----------
    nda_reports : list
        Names of desired NDA reports e.g. ["demo_info01", "affim01"]
    proj_dir : path
        Project's experiment directory
    post_labels : bool
        Whether to use labels in post_scan_ratings pull
    qualtrics_token : str
        Qualtrics API token
    redcap_token : str
        RedCap API token

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If both redcap and qualtrics api tokens are not provided
        If report name requested is not found in nda_switch

    """
    # Validate redcap and qualtrics tokens
    if not redcap_token and not qualtrics_token:
        raise ValueError(
            "RedCap and Qualtrics API token required for --nda-reports."
            + " Please specify --redcap-token and --qualtrics-token."
        )

    # Set switch to find appropriate class: key = user-specified
    # report name, value = relevant class.
    mod_build = "make_reports.build_reports"
    nda_switch = {
        "demo_info01": f"{mod_build}.NdarDemoInfo01",
        "affim01": f"{mod_build}.NdarAffim01",
        "als01": f"{mod_build}.NdarAls01",
    }

    # Validate nda_reports arguments
    for report in nda_reports:
        if report not in nda_switch.keys():
            raise ValueError(
                f"Inappropriate --nda-reports argument : {report}"
            )

    # Setup output directories
    report_dir = os.path.join(proj_dir, "ndar_upload/reports")
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    # Get redcap and qualtrics survey info
    redcap_demo = survey_download.GetRedcapDemographic(redcap_token)
    redcap_data = survey_download.GetRedcapSurveys(redcap_token)
    qualtrics_data = survey_download.GetQualtricsSurveys(
        qualtrics_token, post_labels
    )

    # Make requested reports
    for report in nda_reports:

        # Get appropriate class for report
        h_pkg, h_mod, h_class = nda_switch[report].split(".")
        mod = __import__(f"{h_pkg}.{h_mod}", fromlist=[h_class])
        rep_class = getattr(mod, h_class)

        # Supply appropriate dataset to report
        if report == "demo_info01":
            rep_obj = rep_class(redcap_demo)
        elif report == "bdi01":
            rep_obj = rep_class(redcap_data, redcap_demo)
        else:
            rep_obj = rep_class(qualtrics_data, redcap_demo)

        # Write out report
        out_file = os.path.join(report_dir, f"{report}_dataset.csv")
        print(f"\tWriting : {out_file}")
        rep_obj.df_report.to_csv(out_file, index=False, na_rep="")

        # Preprend header
        dummy_file = f"{out_file}.bak"
        with open(out_file, "r") as read_obj, open(
            dummy_file, "w"
        ) as write_obj:
            write_obj.write(f"{','.join(rep_obj.nda_label)}\n")
            for line in read_obj:
                write_obj.write(line)
        os.remove(out_file)
        os.rename(dummy_file, out_file)
        del rep_obj


# %%
