"""Generate descriptive metrics from the data.

Make plots and reports to give snapshots of the data:
    -   recruit-demo : Mine REDCap demographics to compare the
            actual enrolled demographic profile versus the
            proposed to help curtail under-representation.
    -   scan-pace : Quantify and plot the number of scans
            attempted each week, to help understand recruitment
            pace and adjustments.

Plots and reports are written to:
    <proj-dir>/analyses/metrics_recruit

Examples
--------
rep_metrics --recruit-demo
rep_metrics --scan-pace --redcap-token $PAT_REDCAP_EMOREP

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
        "--recruit-demo",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to calculated recruitement demographics,
            True if "--recruit-demo" else False.
            """
        ),
    )
    parser.add_argument(
        "--redcap-token",
        type=str,
        default=None,
        help="API token for RedCap project",
    )
    parser.add_argument(
        "--scan-pace",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --redcap-token.
            Plot weekly scanning pace.
            True if "--scan-pace" else False.
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
    proj_dir = args.proj_dir
    recruit_demo = args.recruit_demo
    redcap_token = args.redcap_token
    scan_pace = args.scan_pace

    if not redcap_token and scan_pace:
        raise ValueError("Option --scan-pace requires --redcap-token.")

    workflow.get_metrics(proj_dir, recruit_demo, scan_pace, redcap_token)


if __name__ == "__main__":
    main()
