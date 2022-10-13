r"""Clean EmoRep survey data.

Clean RedCap and Qualtrics survey data downloaded by rep_dl.

Study data written to:
    <proj_dir>/data_survey/<visit>/data_clean

Pilot data written to:
    <proj_dir>/data_pilot/data_survey/<visit>/data_clean

Example
-------
rep_cl \
    --clean-redcap \
    --clean-qualtrics

"""
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
        "--clean-redcap",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to clean RedCap surveys,
            True if "--clean-redcap" else False.
            """
        ),
    )
    parser.add_argument(
        "--clean-qualtrics",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to clean RedCap surveys,
            True if "--clean-qualtrics" else False.
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

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    clean_redcap = args.clean_redcap
    clean_qualtrics = args.clean_qualtrics

    workflow.clean_surveys(
        proj_dir,
        clean_redcap=clean_redcap,
        clean_qualtrics=clean_qualtrics,
    )


if __name__ == "__main__":
    main()
