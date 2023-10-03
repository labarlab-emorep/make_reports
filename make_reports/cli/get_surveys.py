r"""Download and clean survey data.

Download RedCap and Qualtrics data, and aggregate all rest-rating
responses. Clean dataframes, and write raw and clean dataframes to
<proj-dir>/data_survey according to visit.

Example
-------
rep_get \
    --get-redcap --redcap-token $PAT_REDCAP_EMOREP \
    --get-qualtrics --qualtrics-token $PAT_QUALTRICS_EMOREP \
    --get-rest

"""
import sys
import textwrap
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports.resources import manage_data


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--get-redcap",
        action="store_true",
        help="Requires --redcap-token, download and clean RedCap surveys",
    )
    parser.add_argument(
        "--get-qualtrics",
        action="store_true",
        help="Requires --qualtrics-token, download and clean Qualtrics surveys",  # noqa: E501
    )
    parser.add_argument(
        "--get-rest",
        action="store_true",
        help="Clean and aggregate resting state ratings",
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
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    qualtrics_token = args.qualtrics_token
    redcap_token = args.redcap_token
    manage_redcap = args.get_redcap
    manage_qualtrics = args.get_qualtrics
    manage_rest = args.get_rest

    if manage_redcap and not redcap_token:
        raise ValueError("--get-redcap requires --redcap-token")
    if manage_qualtrics and not qualtrics_token:
        raise ValueError("--get-qualtrics requires --qualtrics-token")

    if manage_rest:
        dl_clean__rest = manage_data.GetRest(proj_dir)
        dl_clean__rest.get_rest()

    if manage_redcap:
        dl_clean__redcap = manage_data.GetRedcap(proj_dir, redcap_token)
        dl_clean__redcap.get_redcap()

    if manage_qualtrics:
        dl_clean__qualtrics = manage_data.GetQualtrics(
            proj_dir, qualtrics_token
        )
        dl_clean__qualtrics.get_qualtrics()


if __name__ == "__main__":
    main()
