r"""Generate NDAR reports for EmoRep project.

Organize project data and generate reports for regular
NDAR submissions. List all reports available for generation
via "--report-avail" option.

Reports are written to:
    <proj_dir>/ndar_upload/reports

Examples
--------
rep_ndar \
    --report-names demo_info01 affim01

rep_ndar \
    --report-all

"""
import sys
import textwrap
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
    nda_reports = args.report_names
    nda_reports_all = args.report_all
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
        "psychophys_subj_exp01",
        "restsurv01",
        "rrs01",
        "stai01",
        "tas01",
    ]
    if print_avail:
        print(f"Available reports for generation : \n\t{rep_avail}")
        sys.exit(0)

    # Generate requested reports
    if nda_reports_all:
        nda_reports = rep_avail
    workflow.make_nda_reports(nda_reports, proj_dir)


if __name__ == "__main__":
    main()
