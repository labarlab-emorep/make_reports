r"""Title.

Desc.

Example
-------
cli.py \
    -a $PAT_REDCAP_EMOREP
"""

# %%
import os
import sys
import textwrap
from datetime import datetime, date
from argparse import ArgumentParser, RawTextHelpFormatter
from nda_upload import general_info, reports


# %%
def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--manager-reports",
        type=str,
        nargs="+",
        help=textwrap.dedent(
            """\
            [nih4 | nih12 | duke3]

            List of lab manager reports to generate. Acceptable
            args are "nih4", "nih12", and "duke3" for the reports
            submitted to the NIH every 4 months, NIH every 12 months,
            and Duke every 3 months, respectively.

            e.g. "--manager-reports nih4 duke3"
            """
        ),
    )
    parser.add_argument(
        "--proj-dir",
        type=str,
        default="/mnt/keoki/experiments2/EmoRep/Emorep_BIDS",
        help=textwrap.dedent(
            """\
            Path to BIDS-formatted project directory
            (default : %(default)s)
            """
        ),
    )
    parser.add_argument(
        "--query-date",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help=textwrap.dedent(
            """\
            Required if report options are used.
            A Y-m-d formatted date used to find a submission window
            e.g. 2021-06-06 would find all data between 2021-01-01
            and 2021-12-31 when doing an annual report.
            (default : today's date (%(default)s))
            """
        ),
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-a",
        "--api-redcap",
        type=str,
        required=True,
        help="API Token for RedCap project",
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    "Title."

    # For testing
    proj_dir = "/mnt/keoki/experiments2/EmoRep/Emorep_BIDS"
    query_date = datetime.strptime("2022-07-29", "%Y-%m-%d").date()
    manager_reports = ["nih12", "nih4", "duke3"]
    report = manager_reports[2]

    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    api_token = args.api_redcap
    query_date = args.query_date
    manager_reports = args.manager_reports

    # Setup output directories
    deriv_dir = os.path.join(proj_dir, "derivatives/nda_upload")
    if not os.path.exists(deriv_dir):
        os.makedirs(deriv_dir)

    info_demographic = general_info.MakeDemo(api_token)
    # print(info_demographic.final_demo)

    # TODO validate manager_reports args

    test = reports.MakeRegularReports(
        query_date, info_demographic.final_demo, "duke3"
    )
    df_range = test.df_range
    df_hold = df_range[["src_subject_id", "sex", "ethnicity", "race"]]
    df_hold["comb"] = (
        df_hold["sex"] + "," + df_hold["ethnicity"] + "," + df_hold["race"]
    )
    df_report = df_hold["comb"].value_counts()

    if manager_reports:
        manager_dir = os.path.join(proj_dir, "derivatives/manager_reports")
        if not os.path.exists(manager_dir):
            os.makedirs(manager_dir)
        for report in manager_reports:
            mr = reports.MakeRegularReports(
                query_date, info_demographic.final_demo, report
            )
            start_date = mr.range_start.strftime("%Y-%m-%d")
            end_date = mr.range_end.strftime("%Y-%m-%d")
            out_file = os.path.join(
                manager_dir, f"report_{report}_{start_date}_{end_date}.csv"
            )
            mr.df_report.to_csv(out_file, index=False, na_rep="NaN")
            del mr


if __name__ == "__main__":
    main()

# %%
