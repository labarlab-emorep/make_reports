r"""Generate NDAR reports for EmoRep project.

Organize project data and generate reports for regular
NDAR submissions.

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
        "--report-names",
        type=str,
        nargs="+",
        help=textwrap.dedent(
            """\
            [affim01 | als01 | bdi01 | demo_info01 | emrq01 | image03 |
                panas01 | pswq01]
            Make specific NDA reports by name.
            e.g. --report-names affim01 als01
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

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    nda_reports = args.report_names
    nda_reports_all = args.report_all
    proj_dir = args.proj_dir

    if nda_reports_all:
        nda_reports = [
            "affim01",
            "als01",
            "bdi01",
            "demo_info01",
            "emrq01",
            "image03",
            "panas01",
            "pswq01",
        ]
    workflow.make_nda_reports(nda_reports, proj_dir)


if __name__ == "__main__":
    main()
