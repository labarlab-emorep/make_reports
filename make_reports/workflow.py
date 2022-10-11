"""Setup workflows for specific types of reports."""
# %%
import os
from datetime import datetime
from make_reports import survey_download, survey_clean
from make_reports import build_reports
from make_reports import report_helper


# %%
def download_surveys(
    proj_dir,
    redcap_token=None,
    qualtrics_token=None,
    get_redcap=False,
    get_qualtrics=False,
):
    """Title.

    Desc.

    Parameters
    ----------
    proj_dir
    redcap_token
    qualtrics_token
    get_redcap
    get_qualtrics

    Raises
    ------
    ValueError

    """
    print("\nStarting survey download ...")
    if get_redcap:
        if not redcap_token:
            raise ValueError("Expected --redcap-token with --get-redcap.")
        _ = survey_download.download_redcap(proj_dir, redcap_token)

    if get_qualtrics:
        if not qualtrics_token:
            raise ValueError(
                "Expected --qualtrics-token with --get-qualtrics."
            )
        _ = survey_download.download_qualtrics(proj_dir, qualtrics_token)
    print("\nDone with survey download!")


# %%
def clean_surveys(proj_dir, clean_redcap=False, clean_qualtrics=False):
    """Title.

    Desc.

    Parameters
    ----------
    proj_dir
    clean_redcap : bool
    clean_qualtrics : bool

    """

    def _write_clean_redcap(data_clean, data_pilot, dir_name, sur_name):
        """Title.

        Desc.

        """
        clean_file = os.path.join(
            proj_dir,
            "data_survey",
            dir_name,
            "data_clean",
            f"df_{sur_name}.csv",
        )
        data_clean.to_csv(clean_file, index=False, na_rep="")

        pilot_file = os.path.join(
            proj_dir,
            "data_pilot/data_survey",
            dir_name,
            "data_clean",
            f"df_{sur_name}.csv",
        )
        if not os.path.exists(os.path.dirname(pilot_file)):
            os.makedirs(os.path.dirname(pilot_file))
        data_pilot.to_csv(pilot_file, index=False, na_rep="")

    def _write_clean_qualtrics(clean_dict, pilot_dict, dir_name):
        """Title.

        Desc.

        """
        for h_name, h_df in clean_dict.items():
            out_file = os.path.join(
                proj_dir,
                "data_survey",
                dir_name,
                "data_clean",
                f"df_{h_name}.csv",
            )
            print(f"\tWriting : {out_file}")
            h_df.to_csv(out_file, index=False, na_rep="")

        for h_name, h_df in pilot_dict.items():
            out_file = os.path.join(
                proj_dir,
                "data_pilot/data_survey",
                dir_name,
                "data_clean",
                f"df_{h_name}.csv",
            )
            print(f"\tWriting : {out_file}")
            h_df.to_csv(out_file, index=False, na_rep="")

    if clean_redcap:
        redcap_dict = report_helper.redcap_dict()
        clean_redcap = survey_clean.CleanRedcap(proj_dir)
        for sur_name, dir_name in redcap_dict.items():
            clean_redcap.clean_surveys(sur_name)
            _write_clean_redcap(
                clean_redcap.df_clean,
                clean_redcap.df_pilot,
                dir_name,
                sur_name,
            )

    if clean_qualtrics:
        qualtrics_dict = report_helper.qualtrics_dict()
        clean_qualtrics = survey_clean.CleanQualtrics(proj_dir)
        for sur_name, dir_name in qualtrics_dict.items():
            if dir_name == "post_scan_ratings":
                continue
            clean_qualtrics.clean_surveys(sur_name)

            if dir_name == "visit_day1":
                _write_clean_qualtrics(
                    clean_qualtrics.data_clean,
                    clean_qualtrics.data_pilot,
                    dir_name,
                )
            elif dir_name == "visit_day23":
                for vis_name in ["visit_day2", "visit_day3"]:
                    _write_clean_qualtrics(
                        clean_qualtrics.data_clean[vis_name],
                        clean_qualtrics.data_pilot[vis_name],
                        vis_name,
                    )


# %%
def make_manager_reports(manager_reports, query_date, proj_dir):
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
    # if not redcap_token:
    #     raise ValueError(
    #         "RedCap API token required for --manager-reports."
    #         + " Please specify --redcap-token."
    #     )

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
    # redcap_demo = gather_surveys.GetRedcapDemographic(redcap_token)
    redcap_demo = build_reports.DemoAll(proj_dir)

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
    qualtrics_data = gather_surveys.GetQualtricsSurveys(
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
    redcap_demo = gather_surveys.GetRedcapDemographic(redcap_token)
    redcap_data = gather_surveys.GetRedcapSurveys(redcap_token)
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
        "bdi01": f"{mod_build}.NdarBdi01",
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
    redcap_demo = gather_surveys.GetRedcapDemographic(redcap_token)
    redcap_data = gather_surveys.GetRedcapSurveys(redcap_token)
    qualtrics_data = gather_surveys.GetQualtricsSurveys(
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
