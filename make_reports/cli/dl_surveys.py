r"""Download EmoRep survey data.

Pull RedCap and Qualtrics surveys for EmoRep and write out original/raw
dataframes to Keoki. Pilot and study data are separated.

Study data written to:
    <proj_dir>/data_survey/<visit>/data_raw

Pilot data written to:
    <proj_dir>/data_pilot/data_survey/<visit>/data_raw

Example
-------
rep_dl \
    --get-redcap \
    --redcap-token $PAT_REDCAP_EMOREP \
    --get-qualtrics \
    --qualtrics-token $PAT_QUALTRICS_EMOREP

"""
import os
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


def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    qualtrics_token = args.qualtrics_token
    redcap_token = args.redcap_token
    get_redcap = args.get_redcap
    get_qualtrics = args.get_qualtrics

    # Validate usage
    if get_redcap and not redcap_token:
        raise ValueError("Expected --redcap-token with --get-redcap.")
    if get_qualtrics and not qualtrics_token:
        raise ValueError("Expected --qualtrics-token with --get-qualtrics.")

    # Check, setup proj_dir organization
    for h_dir in ["data_survey", "data_pilot/data_survey"]:
        sur_dir = os.path.join(proj_dir, h_dir)
        if not os.path.exists(sur_dir):
            os.path.makedirs(sur_dir)

    workflow.download_surveys(
        proj_dir,
        redcap_token=redcap_token,
        qualtrics_token=qualtrics_token,
        get_redcap=get_redcap,
        get_qualtrics=get_qualtrics,
    )


if __name__ == "__main__":
    main()
