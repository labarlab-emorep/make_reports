"""Generate GUIDs for EmoRep.

Utilize RedCap demographic information to generate a batch
of GUIDs using the NDA's guid-tool for linux. Compare generated
GUIDs to those in RedCap survey.

Generated GUIDs are written to:
    <proj_dir>/data_survey/redcap/output_guid_*.txt

Notes
-----
Requires global variable 'PAT_REDCAP_EMOREP' in user env, which
holds the personal access token to the emorep REDCap database.

Examples
--------
gen_guids -n nmuncy
gen_guids -n nmuncy --find-mismatch

"""

import sys
import textwrap
from getpass import getpass
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports.workflows import required_reports
from make_reports.resources import report_helper


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--find-mismatch",
        action="store_true",
        help=textwrap.dedent(
            """\
            Check for mismatches between generated
            GUIDs and those in the RedCap survey.
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

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-n", "--user-name", type=str, required=True, help="NDA user name"
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflows."""
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    user_name = args.user_name
    find_mismatch = args.find_mismatch

    # Get, check for password
    report_helper.check_redcap_pat()
    user_pass = getpass(
        """
        Please provide NDA password used with the GUID tool
        (this may differ from the NIMH Data Archive password).

        Note -- after 5 failed attempts your NDA account will be locked!

        NDA Password: """
    )
    if len(user_pass) == 0:
        print("\nNo password provided, exiting.")
        sys.exit(0)

    # Start workflows
    print("\nStarting workflow ...")
    required_reports.generate_guids(
        proj_dir, user_name, user_pass, find_mismatch
    )


if __name__ == "__main__":
    main()
