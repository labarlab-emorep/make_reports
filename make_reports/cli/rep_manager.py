r"""Generate regular reports.

Mine RedCap demographic information to construct reports regularly
submitted to the NIH or Duke.

Reports are written to:
    <proj_dir>/documents/manager_reports

Previous submissions can also be generated via --query-date.

Examples
--------
rep_manager \
    --report-names nih4 nih12 duke3

rep_manager \
    --report-names nih4 \
    --query-date 2022-06-29

"""
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
            Used with --manager-reports.
            A Y-m-d formatted date AFTER 2022-04-01 used to find a
            submission window e.g. 2022-06-06 would find all data
            between 2022-01-01 and 2022-12-31 when doing an annual
            report.
            (default : today's date (%(default)s))
            """
        ),
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "--report-names",
        nargs="+",
        required=True,
        type=str,
        help=textwrap.dedent(
            """\
            [nih4 | nih12 | duke3]
            List of lab manager reports to generate. Acceptable
            args are "nih4", "nih12", and "duke3" for the reports
            submitted to the NIH every 4 months, NIH every 12 months,
            and Duke every 3 months, respectively.
            e.g. --manager-reports nih4 duke3
            """
        ),
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    manager_reports = args.report_names
    proj_dir = args.proj_dir
    query_date = args.query_date

    workflow.make_manager_reports(manager_reports, query_date, proj_dir)


if __name__ == "__main__":
    main()
