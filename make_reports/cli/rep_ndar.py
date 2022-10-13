r"""Make NDAR reports for EmoRep project.


Examples
--------
rep_ndar \
    --nda-reports demo_info01 affim01

rep_ndar \
    --nda-reports-all

"""
# %%
import sys
import textwrap
from datetime import date
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports import workflow


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--nda-reports",
        type=str,
        nargs="+",
        help=textwrap.dedent(
            """\
            [affim01 | als01 | bdi01 | demo_info01]
            requires --redcap-token and --qualtrics-token.
            Make specific NDA reports by name.
            e.g. --nda-reports affim01 als01
            """
        ),
    )
    parser.add_argument(
        "--nda-reports-all",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --redcap-token and --qualtrics-token.
            Make all planned NDA reports.
            True if "--nda-reports-all" else False.
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
    "Coordinate resources according to user input."
    args = _get_args().parse_args()
    nda_reports = args.nda_reports
    nda_reports_all = args.nda_reports_all
    proj_dir = args.proj_dir

    # Generate NDA reports
    if nda_reports_all:
        nda_reports = ["affim01", "als01", "bdi01", "demo_info01"]
    workflow.make_nda_reports(nda_reports, proj_dir)


if __name__ == "__main__":
    main()

# %%