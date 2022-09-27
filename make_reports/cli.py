r"""Make reports for EmoRep project.

This package has three uses: (a) to make reports the lab manager submits
on a regular basis, (b) download and clean survey data from RedCap and
Qualtrics, and (c) generate reports for the NDAR submission.

(a) is triggered by specifying --manager-reports and a date and will
generate a report containing the relevenat information from the
appropriate date range. Reports are written to
    <proj-dir>/documents/manager_reports/report_<name>_<start>_<end>.csv

(b) is triggered by --pull-surveys, and will make raw and clean
dataframes. RedCap and Qualtrics dataframes will be written to
    <proj-dir>/data_survey/<visit_type>/data_[clean|raw]
where visit_type is visit_day[1|2|3] or post_scan_ratings

(c) is triggered by --nda-reports or --nda-reports-all, and will make
the requested NDAR report as well as organize data for uploads. Reports
and written to <proj-dir>/make_reports/reports and data will be organized
in <proj-dir>/make_reports/data.

Examples
--------
make_reports \
    --redcap-token $PAT_REDCAP_EMOREP \
    --manager-reports nih4 nih12 duke3 \
    --query-date 2022-06-29

make_reports \
    --redcap-token $PAT_REDCAP_EMOREP \
    --qualtrics-token $PAT_QUALTRICS_EMOREP \
    --pull-surveys

make_reports \
    --redcap-token $PAT_REDCAP_EMOREP \
    --qualtrics-token $PAT_QUALTRICS_EMOREP \
    --nda-reports demo_info01 affim01

make_reports \
    --redcap-token $PAT_REDCAP_EMOREP \
    --qualtrics-token $PAT_QUALTRICS_EMOREP \
    --nda-reports-all

"""
# %%
import sys
import textwrap
from datetime import date
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports import workflow


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
            [nih4 | nih12 | duke3], requires --redcap-token.
            List of lab manager reports to generate. Acceptable
            args are "nih4", "nih12", and "duke3" for the reports
            submitted to the NIH every 4 months, NIH every 12 months,
            and Duke every 3 months, respectively.
            e.g. --manager-reports nih4 duke3
            """
        ),
    )
    parser.add_argument(
        "--nda-reports",
        type=str,
        nargs="+",
        help=textwrap.dedent(
            """\
            [demo_info01 | affim01],
            requires --redcap-token and --qualtrics-token.
            Make specific NDA reports by name.
            e.g. --nda-reports demo_info01 affim01
            """
        ),
    )
    parser.add_argument(
        "--nda-reports-all",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --redcap-token and --qualtrics-token.
            Make all planned NDA reports.
            True if "--nda-reports-all" else False.
            """
        ),
    )
    parser.add_argument(
        "--proj-dir",
        type=str,
        default="/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion",
        help=textwrap.dedent(
            """\
            Path to project's experiment directory
            (default : %(default)s)
            """
        ),
    )
    parser.add_argument(
        "--pull-surveys",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --qualtrics-token and --redcap-token.
            Pull Qualtrics & RedCap surveys, write raw & clean versions.
            True if "--pull-surveys" else False.
            """
        ),
    )
    parser.add_argument(
        "--qualtrics-token",
        type=str,
        default=None,
        help="API token for Qualtrics project",
    )
    parser.add_argument(
        "--query-date",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help=textwrap.dedent(
            """\
            Used with --manager-reports.
            A Y-m-d formatted date AFTER 2022-04-01 used to find a
            submission window e.g. 2022-06-06 would find all data
            between 2022-01-01 and 2022-12-31 when doing an annual
            report.
            (default : today's date (%(default)s))
            """
        ),
    )
    parser.add_argument(
        "--redcap-token",
        type=str,
        default=None,
        help="API token for RedCap project",
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    "Coordinate resources according to user input."
    args = _get_args().parse_args()
    manager_reports = args.manager_reports
    nda_reports = args.nda_reports
    nda_reports_all = args.nda_reports_all
    proj_dir = args.proj_dir
    pull_surveys = args.pull_surveys
    qualtrics_token = args.qualtrics_token
    query_date = args.query_date
    redcap_token = args.redcap_token

    # Generate lab manager reports
    if manager_reports:
        workflow.make_manager_reports(
            manager_reports, query_date, proj_dir, redcap_token
        )

    # Get survey data, make raw and cleaned dataframes
    if pull_surveys:
        workflow.make_survey_reports(proj_dir, qualtrics_token, redcap_token)

    # Generate NDA reports
    if nda_reports_all:
        nda_reports = ["demo_info01", "affim01"]
    if nda_reports:
        workflow.make_nda_reports(
            nda_reports, proj_dir, qualtrics_token, redcap_token
        )


if __name__ == "__main__":
    main()

# %%
