r"""Generate NDAR reports for EmoRep project.

Organize project data and generate reports for regular
NDAR submissions. Reports are written to:
    <proj_dir>/ndar_upload/cycle_<close_date>

Required data (e.g. image03) are copied to:
    <proj_dir>/ndar_upload/data_<foo>

Notes
-----
* Available reports:
    affim01, als01, bdi01, brd01, demo_info01, emrq01,
    image03, panas01, pswq01, restsurv01, rrs01,
    stai01, tas01
* Requires global variables 'PAT_REDCAP_EMOREP' and
    'PAT_QUALTRICS_EMOREP' in user env, which hold the
    personal access tokens to the emorep REDCap and
    Qualtrics databases, respectively.

Examples
--------
rep_ndar -c 2022-12-01 --report-names demo_info01 affim01
rep_ndar -c 2022-12-01 --report-all

"""
import sys
import textwrap
from datetime import datetime
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
        "--report-all",
        action="store_true",
        help=textwrap.dedent(
            """\
            Make all planned NDA reports.
            True if "--report-all" else False.
            """
        ),
    )
    parser.add_argument(
        "--report-names",
        type=str,
        nargs="+",
        help=textwrap.dedent(
            """\
            Make specific NDA reports by name.
            e.g. --report-names affim01 als01
            """
        ),
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-c",
        "--close-date",
        type=str,
        help=textwrap.dedent(
            """\
            YYYY-MM-DD format.
            Close date for NDAR submission cycle, e.g.
            "--close-date 2022-12-01" for 2023-01-15
            submission. Used to submit data from
            participants in the correct cycle.
            """
        ),
        required=True,
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    ndar_reports = args.report_names
    ndar_reports_all = args.report_all
    proj_dir = args.proj_dir
    close_date = datetime.strptime(args.close_date, "%Y-%m-%d").date()

    # Chek for pats
    report_helper.check_qualtrics_pat()
    report_helper.check_redcap_pat()

    # Set, validate report names
    valid_reports = [
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
    if ndar_reports_all:
        ndar_reports = valid_reports
    for chk_rep in ndar_reports:
        if chk_rep not in valid_reports:
            raise ValueError(f"Unexpected report name : {chk_rep}")

    # Generate requested reports
    make_ndar = required_reports.MakeNdarReports(proj_dir, close_date)
    make_ndar.make_report(ndar_reports)


if __name__ == "__main__":
    main()
