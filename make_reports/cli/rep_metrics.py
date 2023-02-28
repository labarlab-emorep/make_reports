r"""Title.

Desc.

Example
-------
rep_metrics --recruit-demo
rep_metrics --scan-pace --redcap-token $PAT_REDCAP_EMOREP
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
            Requires --redcap-token.
            Determine which participants need a second scan,
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
    pending_scans = args.pending_scans
    redcap_token = args.redcap_token
    scan_pace = args.scan_pace

    if not redcap_token:
        if pending_scans:
            raise ValueError("Option --pending-scans requires --redcap_token.")
        if scan_pace:
            raise ValueError("Option --scan-pace requires --redcap_token.")

    workflow.get_metrics(
        proj_dir, recruit_demo, pending_scans, scan_pace, redcap_token
    )


if __name__ == "__main__":
    main()
