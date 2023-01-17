r"""Title.

Desc.

Example
-------
sur_stats --survey-avail
sur_stats --survey-all
sur_stats --survey-name AIM ALS

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
        "--survey-all",
        action="store_true",
        help=textwrap.dedent(
            """\
            Generate descriptive statistics and draw violin plots
            for all surveys. See --survey-avail for list.
            """
        ),
    )
    parser.add_argument(
        "--survey-avail",
        action="store_true",
        help=textwrap.dedent(
            """\
            Print list of surveys availble for descriptive statistics.
            True if "--available-reports" else False.
            """
        ),
    )
    parser.add_argument(
        "--survey-name",
        nargs="+",
        type=str,
        help=textwrap.dedent(
            """\
            List of surveys, for generating descriptive statistics
            and drawing violin plots. See --survey-avail for list.
            """
        ),
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflow."""
    args = _get_args().parse_args()
    proj_dir = args.proj_dir
    survey_list = args.survey_name
    survey_all = args.survey_all
    survey_avail = args.survey_avail

    #
    sur_rc_qual = [
        "AIM",
        "ALS",
        "BDI",
        "ERQ",
        "PANAS",
        "PSWQ",
        "RRS",
        "STAI_Trait",
        "STAI_State",
        "TAS",
    ]
    sur_scan = ["rest", "stim"]
    sur_all = sur_rc_qual + sur_scan
    if survey_avail:
        print(f"Available surveys : \n\t{sur_all}")
        sys.exit(0)

    #
    if survey_all:
        survey_list = sur_all

    # Validate survey names
    for sur in survey_list:
        if sur not in sur_rc_qual and sur not in sur_scan:
            raise ValueError(
                f"Unexpected survey requested : {sur}, see --survey-avail."
            )

    #
    sur_online = [x for x in survey_list if x in sur_rc_qual]
    sur_scan = [x for x in survey_list if x in sur_scan]

    if sur_online:
        sur_stat = workflow.CalcRedcapQualtricsStats(proj_dir, sur_online)
        sur_stat.coord_visits()

    if sur_scan:
        workflow.survey_scan(proj_dir, sur_scan)


if __name__ == "__main__":
    main()
