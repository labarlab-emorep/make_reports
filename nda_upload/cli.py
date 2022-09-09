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
from datetime import date
from argparse import ArgumentParser, RawTextHelpFormatter
from nda_upload import general_info


# %%
def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--report-duke-3mo",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to generate Duke figures submitted every 3 months.
            True if "--report-duke-3mo" else False.
            """
        ),
    )
    parser.add_argument(
        "--report-nih-4mo",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to generate NIH figures submitted every 4 months.
            True if "--report-nih-4mo" else False.
            """
        ),
    )
    parser.add_argument(
        "--report-nih-12mo",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to generate NIH figures submitted every 12 months.
            True if "--report-nih-12mo" else False.
            """
        ),
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
    parser.add_argument(
        "--query-date",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help=textwrap.dedent(
            """\
            Required if report options are used.
            A Y-m-d formatted date used to find a submission window
            e.g. 2021-06-06 would find all data between 2021-01-01
            and 2021-12-31 when doing an annual report.
            (default : today's date (%(default)s))
            """
        ),
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-a",
        "--api-redcap",
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

    info_general = general_info.MakeDemo(api_token)
    info_general.make_complete()
    print(info_general.final_demo)


if __name__ == "__main__":
    main()
