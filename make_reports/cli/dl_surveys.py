r"""Download surveys, write to keoki.

Examples
--------
rep_dl \
    --get-redcap \
    --redcap-token $PAT_REDCAP_EMOREP \
    --get-qualtrics \
    --qualtrics-token $PAT_QUALTRICS_EMOREP

"""
# %%
import sys
import textwrap
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports import workflow


# %%
def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--get-redcap",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --redcap-token.
            Whether to download RedCap surveys,
            True if "--get-redcap" else False.
            """
        ),
    )
    parser.add_argument(
        "--get-qualtrics",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --qualtrics-token.
            Whether to download Qualtrics surveys,
            True if "--get-qualtrics" else False.
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
        "--qualtrics-token",
        type=str,
        default=None,
        help="API token for Qualtrics project",
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


# %%
def main():
    "Coordinate resources according to user input."
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    qualtrics_token = args.qualtrics_token
    redcap_token = args.redcap_token
    get_redcap = args.get_redcap
    get_qualtrics = args.get_qualtrics

    # TODO Validate args
    workflow.download_surveys(
        proj_dir,
        redcap_token=redcap_token,
        qualtrics_token=qualtrics_token,
        get_redcap=get_redcap,
        get_qualtrics=get_qualtrics,
    )


if __name__ == "__main__":
    main()
