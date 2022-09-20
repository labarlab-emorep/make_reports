"""Setup workflows for specific types of reports."""
import os
from datetime import datetime
from nda_upload import reports


def make_manager_reports(manager_reports, final_demo, query_date, proj_dir):
    """Make reports for the lab manager.

    Coordinate the use of reports.MakeRegularReports to generate
    desired nih12, nih4, or duke3 report. Write dataframes to
    <proj_dir>/documents/manager_reports.

    Parameters
    ----------
    manager_reports : list
        Desired reports
        e.g. nih4, nih12
    final_demo : pd.DataFrame
        Compiled demographic information, attribute of
        by general_info.MakeDemographic
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

    # Generate reports
    for report in manager_reports:
        mr = reports.RegularReports(query_date, final_demo, report)

        # Setup file name, write csv
        start_date = mr.range_start.strftime("%Y-%m-%d")
        end_date = mr.range_end.strftime("%Y-%m-%d")
        out_file = os.path.join(
            manager_dir, f"report_{report}_{start_date}_{end_date}.csv"
        )
        print(f"\tWriting : {out_file}")
        mr.df_report.to_csv(out_file, index=False, na_rep="")
        del mr


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
