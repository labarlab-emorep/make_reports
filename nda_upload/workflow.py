"""Setup workflows for specific types of reports."""
# %%
import os
import json
from datetime import datetime
from nda_upload import survey_download, build_reports


def make_manager_reports(manager_reports, query_date, proj_dir, redcap_token):
    """Make reports for the lab manager.

    Coordinate the use of reports.MakeRegularReports to generate
    desired nih12, nih4, or duke3 report. Write dataframes to
    <proj_dir>/documents/manager_reports.

    Parameters
    ----------
    manager_reports : list
        Desired reports
        e.g. nih4, nih12
    query_date : str, datetime
        Date for finding report range
    proj_dir : path
        Project's experiment directory

    Returns
    -------
    None

    """
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


# %%
def make_qualtrics_reports(survey_par, qualtrics_token):
    """Title.

    Desc.
    """
    # Get qualtrics reports
    rep_qualtrics = pull_qualtrics.MakeQualtrics(qualtrics_token, survey_par)

    # Write raw data
    for visit in [
        "visit_day1",
        "visit_day2",
        "visit_day3",
        "post_scan_ratings",
    ]:
        rep_qualtrics.write_raw_reports(visit)

    # Clean data
    rep_qualtrics.write_clean_reports("visit_day1")

    # Test - day2
    df_raw_visit23 = rep_qualtrics.df_raw_visit23
    day = "day2"
    subj_col = ["SubID"]
    df_raw_visit23.rename(
        {"RecipientLastName": subj_col[0]}, axis=1, inplace=True
    )
    col_names = df_raw_visit23.columns

    visit23_surveys = ["PANAS", "STAI_State"]


def make_nda_reports(nda_reports, final_demo, proj_dir):
    """Title.

    Desc.
    """
    # Setup output directories
    report_dir = os.path.join(proj_dir, "ndar_upload/reports")
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    # Set switch to find appropriate class: key = user-specified
    # argument, value = relevant class.
    nda_switch = {"demo_info01": "nda_upload.reports.DemoInfo"}

    # Make requested reports
    for report in nda_reports:

        # Validate nda_reports arguments
        if report not in nda_switch.keys():
            raise ValueError(
                f"Inappropriate --nda-reports argument : {report}"
            )

        # Get appropriate class for report
        h_pkg, h_mod, h_class = nda_switch[report].split(".")
        mod = __import__(f"{h_pkg}.{h_mod}", fromlist=[h_class])
        rep_class = getattr(mod, h_class)

        # Make report, write out
        rep_obj = rep_class(final_demo)
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
