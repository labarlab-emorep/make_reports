r"""Download and clean survey data.

Download RedCap and Qualtrics data, and aggregate all rest-rating
responses. Clean dataframes, and write raw and clean dataframes to
<proj-dir>/data_survey according to visit.

Notes
-----
* requires global variable 'SQL_PASS' in user environment, which holds
    user password to mysql db_emorep database.
* --get-redcap requires global variable 'PAT_REDCAP_EMOREP' in user
    env, which holds the personal access token to the emorep REDCap database.
* --get-qulatrics requires global variable 'PAT_QUALTRICS_EMOREP' in
    user env, which holds the personal access token to the emorep
    Qualtrics database.

Example
-------
rep_get \
    --get-redcap \
    --get-qualtrics \
    --get-rest \
    --get-task

"""
import sys
import textwrap
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports.resources import manage_data
from make_reports.resources import report_helper


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--get-redcap",
        action="store_true",
        help="Download and clean RedCap surveys",
    )
    parser.add_argument(
        "--get-qualtrics",
        action="store_true",
        help="Download and clean Qualtrics surveys",
    )
    parser.add_argument(
        "--get-rest",
        action="store_true",
        help="Clean and aggregate resting state ratings",
    )
    parser.add_argument(
        "--get-task",
        action="store_true",
        help="Clean and aggregate task ratings",
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

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    manage_redcap = args.get_redcap
    manage_qualtrics = args.get_qualtrics
    manage_rest = args.get_rest
    manage_task = args.get_task

    # Check for required tokens
    report_helper.check_sql_pass()
    if manage_redcap:
        report_helper.check_redcap_pat()
    if manage_qualtrics:
        report_helper.check_qualtrics_pat()

    if manage_rest:
        dl_clean_rest = manage_data.GetRest(proj_dir)
        dl_clean_rest.get_rest()

    if manage_redcap:
        dl_clean_redcap = manage_data.GetRedcap(proj_dir)
        dl_clean_redcap.get_redcap()

    if manage_qualtrics:
        dl_clean_qualtrics = manage_data.GetQualtrics(proj_dir)
        dl_clean_qualtrics.get_qualtrics()

    if manage_task:
        dl_clean_task = manage_data.GetTask(proj_dir)
        dl_clean_task.get_task()


if __name__ == "__main__":
    main()
