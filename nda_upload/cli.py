r"""Title.

Desc.

Examples
--------
nda_upload \
    -r $PAT_REDCAP_EMOREP \
    --manager-reports nih4 nih12 duke3 \
    --query-date 2022-06-29

nda_upload \
    -r $PAT_REDCAP_EMOREP \
    --nda-reports demo_info01
"""

# %%
import os
import sys
import textwrap
from datetime import date
from argparse import ArgumentParser, RawTextHelpFormatter
from nda_upload import workflow


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
        default="/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion",
        help=textwrap.dedent(
            """\
            Path to project's experiment directory
            (default : %(default)s)
            """
        ),
    )
    parser.add_argument(
        "--pull-surveys",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --qualtrics-token and --redcap-token.
            Pull Qualtrics, RedCap surveys and write
            raw and clean versions.
            True if "--pull-surveys" else False.
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
    parser.add_argument(
        "--qualtrics-token",
        type=str,
        default=None,
        help="API token for Qualtrics project",
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-r",
        "--redcap-token",
        type=str,
        required=True,
        help="API token for RedCap project",
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    "Title."

    # For testing
    proj_dir = "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion"

    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    redcap_token = args.redcap_token
    pull_surveys = args.pull_surveys
    qualtrics_token = args.qualtrics_token
    query_date = args.query_date
    manager_reports = args.manager_reports
    nda_reports = args.nda_reports

    # Set paths
    survey_par = os.path.join(proj_dir, "data_survey")

    # Generate lab manager reports
    if manager_reports:
        workflow.make_manager_reports(
            manager_reports, query_date, proj_dir, redcap_token
        )

    # Get survey data, make raw and cleaned dataframes
    if pull_surveys:
        workflow.make_survey_reports(survey_par, qualtrics_token, redcap_token)

    # Generate NDA reports
    if nda_reports:
        workflow.make_nda_reports(nda_reports, proj_dir, redcap_token)


if __name__ == "__main__":
    main()

# %%
