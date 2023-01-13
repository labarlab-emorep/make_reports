r"""Title.

Desc.

Example
-------
rep_metrics --survey-avail
rep_metrics --survey-all
rep_metrics --survey-name AIM ALS
rep_metrics --recruit-demo
rep_metrics --pending-scans --redcap-token $PAT_REDCAP_EMOREP

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
        "--pending-scans",
        action="store_true",
        help=textwrap.dedent(
            """\
            Requires --redcap-token.
            Determine which participants need a second scan,
            True if "--pending-scans" else False.
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
        "--recruit-demo",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to calculated recruitement demographics,
            True if "--recruit-demo" else False.
            """
        ),
    )
    parser.add_argument(
        "--redcap-token",
        type=str,
        default=None,
        help="API token for RedCap project",
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
    recruit_demo = args.recruit_demo
    pending_scans = args.pending_scans
    redcap_token = args.redcap_token
    survey_name = args.survey_name
    survey_all = args.survey_all
    survey_avail = args.survey_avail

    if pending_scans and not redcap_token:
        raise ValueError("Option --pending-scans requires --redcap_token.")

    if recruit_demo or pending_scans:
        workflow.calc_metrics(
            proj_dir, recruit_demo, pending_scans, redcap_token
        )

    #
    sur_avail = [
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
    if survey_avail:
        print(f"Available surveys : \n\t{sur_avail}")
        sys.exit(0)

    if survey_all:
        survey_name = sur_avail

    #
    if survey_name:
        for sur in survey_name:
            if sur not in sur_avail:
                raise ValueError(
                    f"Unexpected survey requested : {sur}, see --survey-avail."
                )
        sur_stat = workflow.CalcSurveyStats(proj_dir, survey_name)
        sur_stat.coord_visits()


if __name__ == "__main__":
    main()
