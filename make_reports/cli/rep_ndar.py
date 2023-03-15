r"""Generate NDAR reports for EmoRep project.

Organize project data and generate reports for regular
NDAR submissions. List all reports available for generation
via "--report-avail" option.

Reports are written to:
    <proj_dir>/ndar_upload/cycle_<close_date>

Required data (e.g. image03) are copied to:
    <proj_dir>/ndar_upload/data_<foo>

Examples
--------
rep_ndar --report-avail

rep_ndar \
    --report-names demo_info01 affim01 \
    --close-date 2022-12-01

rep_ndar \
    --report-all \
    --close-date 2022-12-01

"""
import sys
import textwrap
from datetime import datetime
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports.workflows import required_reports


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--close-date",
        type=str,
        default=None,
        help=textwrap.dedent(
            """\
            YYYY-MM-DD format.
            Close date for NDAR submission cycle, e.g.
            "--close-date 2022-12-01" for 2023-01-15
            submission. Used to submit data from
            participants in the correct cycle.
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
        "--report-all",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --close-date.
            Make all planned NDA reports.
            True if "--report-all" else False.
            """
        ),
    )
    parser.add_argument(
        "--report-avail",
        action="store_true",
        help=textwrap.dedent(
            """\
            Print list of NDAR reports available for generating.
            True if "--available-reports" else False.
            """
        ),
    )
    parser.add_argument(
        "--report-names",
        type=str,
        nargs="+",
        help=textwrap.dedent(
            """\
            Requires --close-date.
            Make specific NDA reports by name.
            e.g. --report-names affim01 als01
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
    print_avail = args.report_avail
    ndar_reports = args.report_names
    ndar_reports_all = args.report_all
    proj_dir = args.proj_dir

    # Set supported reports
    rep_avail = [
        "affim01",
        "als01",
        "bdi01",
        "brd01",
        "demo_info01",
        "emrq01",
        "image03",
        "panas01",
        "pswq01",
        "restsurv01",
        "rrs01",
        "stai01",
        "tas01",
    ]
    if print_avail:
        print(f"Available reports for generation : \n\t{rep_avail}")
        sys.exit(0)

    # Check close date
    if args.close_date:
        close_date = datetime.strptime(args.close_date, "%Y-%m-%d").date()
    else:
        raise ValueError(
            "--close-date required by --report-all and --report-names"
        )

    # Generate requested reports
    if ndar_reports_all:
        ndar_reports = rep_avail
    required_reports.make_ndar_reports(ndar_reports, proj_dir, close_date)


if __name__ == "__main__":
    main()
