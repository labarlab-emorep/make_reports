r"""Generate regular reports.

Mine RedCap demographic information to construct reports regularly
submitted to the NIH or Duke.

Notes
-----
- Requires global variable 'PAT_REDCAP_EMOREP' in user env, which
    holds the personal access token to the emorep REDCap database.
- Previous submissions can also be generated via --query-date.
- Reports are written to <proj_dir>/documents/regular_reports

Examples
--------
rep_regular --names nih4 nih12 duke12
rep_regular --names nih4 --query-date 2022-06-29

"""

import sys
import textwrap
from datetime import date
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports.workflows import required_reports
from make_reports.resources import report_helper


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
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
        "--query-date",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help=textwrap.dedent(
            """\
            YYYY-MM-DD format, after 2022-04-01.
            Used to find a submission window e.g. 2022-06-06
            would find all data between 2022-01-01 and 2022-12-31
            when doing an annual report.
            (default : today's date (%(default)s))
            """
        ),
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "--names",
        nargs="+",
        required=True,
        type=str,
        choices=["nih4", "nih12", "duke3", "duke12"],
        help="List of desired reports to generate",
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    regular_reports = args.names
    proj_dir = args.proj_dir
    query_date = args.query_date

    report_helper.check_redcap_pat()
    required_reports.make_regular_reports(
        regular_reports, query_date, proj_dir
    )


if __name__ == "__main__":
    main()
