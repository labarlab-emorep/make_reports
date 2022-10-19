r"""Generate GUIDs for EmoRep.

Utilize RedCap demographic information to generate a batch
of GUIDs using the NDA's guid-tool for linux.

Generated GUIDs are written to:
    <proj_dir>/data_survey/redcap_demographics/data_clean/output_guid_*.txt

Example
-------
gen_guids --user-name nmuncy

"""
import sys
import textwrap
from getpass import getpass
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports import workflow


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
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
        "--user-name", type=str, required=True, help="NDA user name"
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    user_name = args.user_name

    # Get, check for password
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

    # Start workflow
    print("\nStarting workflow ...")
    workflow.generate_guids(proj_dir, user_name, user_pass)


if __name__ == "__main__":
    main()
