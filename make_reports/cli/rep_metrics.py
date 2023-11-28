"""Generate descriptive metrics about recruitment.

Make plots and reports to give snapshots of the data:
    -   recruit-demo : Mine REDCap demographics to compare the
            actual enrolled demographic profile versus the
            proposed to help curtail under-representation.
    -   scan-pace : Quantify and plot the number of scans
            attempted each week, to help understand recruitment
            pace and adjustments.
    -   participant-flow : Draw PRISMA flowchart of participant
            flow, exclusion, lost-to-follow-up, and withdrawal
            for Experiment2
    -   prop-motion : Calculate the proportion of volumes that
            exceed framewise displacement thresholds.

Plots and reports are written to:
    <proj-dir>/analyses/metrics_recruit

Notes
-----
Options --recruit-demo, --participant-flow, --scan-pace require
global variable 'PAT_REDCAP_EMOREP' in user env, which holds the
personal access token to the emorep REDCap database.

Examples
--------
rep_metrics --prop-motion
rep_metrics --recruit-demo
rep_metrics --participant-flow
rep_metrics --scan-pace

"""
import sys
import textwrap
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports.workflows import data_metrics
from make_reports.resources import report_helper


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--participant-flow",
        action="store_true",
        help="Draw participant PRISMA flowchart",
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
        "--prop-motion",
        action="store_true",
        help="Calculate proportion of volumes that exceed FD threshold",
    )
    parser.add_argument(
        "--recruit-demo",
        action="store_true",
        help="Calculate recruitment demographics",
    )
    parser.add_argument(
        "--scan-pace",
        action="store_true",
        help="Plot weekly scanning pace",
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
    prop_motion = args.prop_motion
    participant_flow = args.participant_flow
    scan_pace = args.scan_pace

    if scan_pace or participant_flow or recruit_demo:
        report_helper.check_redcap_pat()

    data_metrics.get_metrics(
        proj_dir,
        recruit_demo,
        prop_motion,
        scan_pace,
        participant_flow,
    )


if __name__ == "__main__":
    main()
