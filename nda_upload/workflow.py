"""Title.

Desc.
"""
import os
from datetime import datetime
from nda_upload import reports


def make_manager_reports(manager_reports, final_demo, query_date, proj_dir):
    """Title.

    Desc.
    """
    # Validate manager_reports arguments
    valid_mr_args = ["nih12", "nih4", "duke3"]
    for report in manager_reports:
        if report not in valid_mr_args:
            raise ValueError(
                "--manager-reports contained inappropriate "
                + f"argument : {report}"
            )

    # TODO validate query date
    if isinstance(query_date, str):
        query_date = datetime.strptime(query_date, "%Y-%m-%d").date()
    if query_date < datetime.strptime("2022-04-01", "%Y-%m-%d").date():
        raise ValueError(f"Query date {query_date} precedes 2022-04-01.")

    # Setup output location
    manager_dir = os.path.join(proj_dir, "derivatives/manager_reports")
    if not os.path.exists(manager_dir):
        os.makedirs(manager_dir)

    # Generate reports
    for report in manager_reports:
        mr = reports.MakeRegularReports(query_date, final_demo, report)

        # Setup file name, write csv
        start_date = mr.range_start.strftime("%Y-%m-%d")
        end_date = mr.range_end.strftime("%Y-%m-%d")
        out_file = os.path.join(
            manager_dir, f"report_{report}_{start_date}_{end_date}.csv"
        )
        print(f"\tWriting : {out_file}")
        mr.df_report.to_csv(out_file, index=False, na_rep="NaN")
        del mr
