r"""Conduct data checking for EmoRep and Archival data.

Compare detected files against known lists to identify
participants with missing data (survey, scanner) or need
processing of MRI data.

Examples
--------
check_data --process emorep
check_data --process emorep --complete
check_data --process archival

Notes
-----
- Written to be executed on the local VM labarserv2
- Assumes EmoRep data structure

"""
# %%
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports.workflows import data_metrics


# %%
def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )

    parser.add_argument(
        "--complete",
        action="store_true",
        help="Check for expected EmoRep survey and scanner files",
    )
    parser.add_argument(
        "--process",
        type=str,
        choices=["emorep", "archival"],
        default=None,
        help="""Name of project to check for MRI processing output""",
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    """Trigger workflow."""
    args = _get_args().parse_args()
    process = args.process
    complete = args.complete

    if process:
        do_chk = data_metrics.CheckProjectMri()
        do_chk.run_check(process)
    if complete:
        print("Complete is currently deprecated")
        sys.exit(0)
        data_metrics.check_emorep_all()


if __name__ == "__main__":
    # Require proj env
    env_found = [x for x in sys.path if "emorep" in x]
    if not env_found:
        print("\nERROR: missing required project environment 'emorep'.")
        print("\tHint: $labar_env emorep\n")
        sys.exit(1)
    main()
