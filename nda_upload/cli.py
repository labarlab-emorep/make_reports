r"""Title.

Desc.

Examples
--------
nda_upload \
    -a $PAT_REDCAP_EMOREP \
    --manager-reports nih4 nih12 duke3 \
    --query-date 2022-06-29

nda_upload \
    -a $PAT_REDCAP_EMOREP \
    --nda-reports demo_info01
"""

# %%
import os
import sys
import textwrap
from datetime import date
from argparse import ArgumentParser, RawTextHelpFormatter
from nda_upload import pull_redcap, workflow


# %%
def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--manager-reports",
        type=str,
        nargs="+",
        help=textwrap.dedent(
            """\
            [nih4 | nih12 | duke3]
            List of lab manager reports to generate. Acceptable
            args are "nih4", "nih12", and "duke3" for the reports
            submitted to the NIH every 4 months, NIH every 12 months,
            and Duke every 3 months, respectively.
            e.g. --manager-reports nih4 duke3
            """
        ),
    )
    parser.add_argument(
        "--nda-reports",
        type=str,
        nargs="+",
        help=textwrap.dedent(
            """\
            [demo_info01]

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
            A Y-m-d formatted date AFTER 2022-04-01 used to find a
            submission window e.g. 2022-06-06 would find all data
            between 2022-01-01 and 2022-12-31 when doing an annual
            report.
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

    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    api_token = args.api_redcap
    query_date = args.query_date
    manager_reports = args.manager_reports
    nda_reports = args.nda_reports

    # Get demographic info for consented subjs
    info_demographic = pull_redcap.MakeDemographic(api_token)

    # Generate lab manager reports
    if manager_reports:
        workflow.make_manager_reports(
            manager_reports, info_demographic.final_demo, query_date, proj_dir
        )

    # Generate NDA reports
    if nda_reports:
        workflow.make_nda_reports(
            nda_reports, info_demographic.final_demo, proj_dir
        )


if __name__ == "__main__":
    main()

# %%
