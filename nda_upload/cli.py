r"""Title.

Desc.

Example
-------
cli.py \
    -a $PAT_REDCAP_EMOREP
"""

# %%
import os
import sys
import textwrap

# import importlib.resources as pkg_resources
# import pandas as pd
from argparse import ArgumentParser, RawTextHelpFormatter
from nda_upload import general_info

# from nda_upload import request_redcap
# from nda_upload import dataset_definitions

# %%
def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--proj-dir",
        type=str,
        default="/mnt/keoki/experiments2/EmoRep/Emorep_BIDS",
        help=textwrap.dedent(
            """\
            Path to BIDS-formatted project directory
            (default : %(default)s)
            """
        ),
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-a",
        "--api_redcap",
        type=str,
        required=True,
        help="API Token for RedCap project",
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    "Title."

    # For testing
    proj_dir = "/mnt/keoki/experiments2/EmoRep/Emorep_BIDS"

    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    api_token = args.api_redcap

    # Setup output directories
    deriv_dir = os.path.join(proj_dir, "derivatives/nda_upload")
    if not os.path.exists(deriv_dir):
        os.makedirs(deriv_dir)

    # Setup report references
    report_keys = {
        "demographics": "48944",
        "guid": "48959",
        "consent_new": "48960",
    }


if __name__ == "__main__":
    main()
