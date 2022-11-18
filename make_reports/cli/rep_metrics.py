r"""Title.

Desc.

Example
-------
rep_metrics --recruit-demo
rep_metrics --pending-scans --redcap-token $PAT_REDCAP_EMOREP

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
        "--pending-scans",
        action="store_true",
        help=textwrap.dedent(
            """\
            Determine which participants need a second scan, requires
            --redcap-token.
            True if "--pending-scans" else False.
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

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    recruit_demo = args.recruit_demo
    pending_scans = args.pending_scans
    redcap_token = args.redcap_token

    workflow.calc_metrics(proj_dir, recruit_demo, pending_scans, redcap_token)


if __name__ == "__main__":
    main()
