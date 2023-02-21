r"""Generate descriptives for survey data.

Calculate descriptive statistics and draw plots for
REDCap, Qualtrics, rest-ratings, and stim-ratings
surveys.

Output files are written to:
    <proj_dir>/analyses/surveys_stats_descriptive

Examples
--------
sur_stats --survey-avail
sur_stats --survey-all
sur_stats --survey-names AIM ALS

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
        "--survey-names",
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
    survey_list = args.survey_names
    survey_all = args.survey_all
    survey_avail = args.survey_avail

    # Set redcap/qualtircs and scan lists
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
    sur_scan = ["rest", "stim", "task"]
    sur_all = sur_rc_qual + sur_scan

    # Manage avail, all options
    if survey_avail:
        print(f"Available surveys : \n\t{sur_all}")
        sys.exit(0)
    if survey_all:
        survey_list = sur_all

    # Validate survey names
    for sur in survey_list:
        if sur not in sur_all:
            raise ValueError(
                f"Unexpected survey requested : {sur}, see --survey-avail."
            )

    # Sort requested survey names, trigger appropriate workflows
    sur_online = [x for x in survey_list if x in sur_rc_qual]
    sur_scanner = [x for x in survey_list if x in sur_scan]

    if sur_online:
        sur_stat = workflow.CalcRedcapQualtricsStats(proj_dir, sur_online)
        sur_stat.match_survey_visits()

    if sur_scanner:
        workflow.calc_task_stats(proj_dir, sur_scanner)


if __name__ == "__main__":
    main()
