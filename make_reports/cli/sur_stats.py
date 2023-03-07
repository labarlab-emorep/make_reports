r"""Generate descriptives for survey data.

Calculate descriptive statistics and draw plots for
REDCap, Qualtrics, rest-ratings, and stim-ratings
surveys.

Output files are written to:
    <proj_dir>/analyses/metrics_surveys

Examples
--------
sur_stats --survey-avail
sur_stats --survey-all --draw-plots
sur_stats --survey-names AIM ALS BDI --write-json
sur_stast --make-tables --draw-plots

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
        "--draw-plots",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether figures should be generated.
            True if "--draw-plot" else False.
            """
        ),
    )
    parser.add_argument(
        "--make-tables",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to compile generated dataframes into tables. Uses
            data from all surveys available (similar to --survey-all).
            Replaces --survey-list.
            True if "--make-table" else False.
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
        "--survey-all",
        action="store_true",
        help=textwrap.dedent(
            """\
            Generate descriptive statistics and draw plots
            for all surveys. Replaces --survey-list.
            See --survey-avail for list.
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
            Requres --draw-plots or --write-json.
            List of surveys, for generating descriptive statistics
            and drawing figures. See --survey-avail for list.
            """
        ),
    )
    parser.add_argument(
        "--write-json",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether write Qualtrics and RedCap descriptive
            stats out to JSON file.
            True if "--write-json" else False.
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
    draw_plot = args.draw_plots
    write_json = args.write_json
    make_tables = args.make_tables
    proj_dir = args.proj_dir
    survey_list = args.survey_names
    survey_all = args.survey_all
    survey_avail = args.survey_avail

    # Validate input
    if survey_list and (not draw_plot and not write_json):
        raise ValueError(
            "Option --survey-names requires --draw-plot or --write-json."
        )

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

    # Check user-specified survey names
    if survey_list:
        for sur in survey_list:
            if sur not in sur_all:
                raise ValueError(
                    "Unexpected survey requested : "
                    + f"{sur}, see --survey-avail."
                )

    # Manage avail, all options
    if survey_avail:
        print(f"Available surveys : \n\t{sur_all}")
        sys.exit(0)
    if survey_all or make_tables:
        survey_list = sur_all

    # Sort requested survey names, trigger appropriate workflows
    sur_online = [x for x in survey_list if x in sur_rc_qual]
    sur_scanner = [x for x in survey_list if x in sur_scan]

    # Trigger workflows based on user input
    if make_tables:
        workflow.make_survey_table(proj_dir, sur_online, sur_scanner)
        sys.exit(0)

    if sur_online:
        sur_stat = workflow.CalcRedcapQualtricsStats(
            proj_dir, sur_online, draw_plot, write_json
        )
        sur_stat.match_survey_visits()

    if sur_scanner:
        _ = workflow.calc_task_stats(proj_dir, sur_scanner, draw_plot)


if __name__ == "__main__":
    main()
