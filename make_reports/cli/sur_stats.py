r"""Generate descriptives for survey data.

Download, clean, and calculate descriptive statistics and plots
for REDCap, Qualtrics, rest-ratings, and stim-ratings surveys.

Output files are written to:
    <proj_dir>/analyses/metrics_surveys

Notes
-----
* Appropriate database tokens required, defined in user env via
    'PAT_REDCAP_EMOREP' and/or 'PAT_QUALTRICS_EMOREP'.

* General descriptive stats, all data are included.

* Available survey names:
    BDI, AIM, ALS, ERQ, PANAS, PSWQ, RRS, STAI_Trait, STAI_State,
    TAS, rest (post-rest ratings), stim (post-scan stim ratings),
    task (in-scan task responses)

Examples
--------
sur_stats --all
sur_stats --names AIM ALS stim
sur_stats --names rest task --write-json
sur_stats --make-tables

"""

import sys
import textwrap
from argparse import ArgumentParser, RawTextHelpFormatter
from make_reports.workflows import behavioral_reports
from make_reports.resources import report_helper


def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--make-tables",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to compile generated dataframes into tables. Uses
            data from all surveys available (similar to --all).
            Replaces --names.
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
        "--all",
        action="store_true",
        help=textwrap.dedent(
            """\
            Generate descriptive statistics and draw plots
            for all surveys. Replaces --names.
            """
        ),
    )
    parser.add_argument(
        "--names",
        nargs="+",
        type=str,
        help=textwrap.dedent(
            """\
            List of surveys, for generating descriptive statistics
            and drawing figures.
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
            """
        ),
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def main():
    """Capture arguments and trigger workflows."""
    args = _get_args().parse_args()
    write_json = args.write_json
    make_tables = args.make_tables
    proj_dir = args.proj_dir
    survey_list = args.names
    survey_all = args.all

    # Set redcap/qualtircs and scan lists
    sur_rc = ["BDI"]
    sur_qual = [
        "AIM",
        "ALS",
        "ERQ",
        "PANAS",
        "PSWQ",
        "RRS",
        "STAI_Trait",
        "STAI_State",
        "TAS",
    ]
    sur_rc_qual = sur_rc + sur_qual
    sur_scan = ["rest", "stim", "task"]
    sur_all = sur_rc_qual + sur_scan

    # Manage all option
    if survey_all or make_tables:
        survey_list = sur_all

    # Validate survey names (in case user specified)
    for sur in survey_list:
        if sur not in sur_all:
            raise ValueError(f"Unexpected survey requested : {sur}")

    # Check tokens
    for chk_sur in survey_list:
        if chk_sur in sur_qual + ["stim"]:
            report_helper.check_qualtrics_pat()
        if chk_sur == "BDI":
            report_helper.check_redcap_pat()

    # Sort requested survey names, trigger appropriate workflows
    sur_online = [x for x in survey_list if x in sur_rc_qual]
    sur_scanner = [x for x in survey_list if x in sur_scan]

    # Trigger workflows based on user input
    if make_tables:
        behavioral_reports.make_survey_table(proj_dir, sur_online, sur_scanner)
        sys.exit(0)

    if sur_online:
        sur_stat = behavioral_reports.CalcRedcapQualtricsStats(proj_dir)
        sur_stat.gen_stats_plots(sur_online, write_json)

    if sur_scanner:
        _ = behavioral_reports.calc_task_stats(proj_dir, sur_scanner)


if __name__ == "__main__":
    main()
