"""Methods for generating metrics about the project.

Supports tracking scanning pace, participant demographics,
and amount of motion in EPI data.

"""
# %%
import os
import glob
from graphviz import Digraph
from make_reports.resources import build_reports, calc_metrics
from make_reports.resources import survey_download, report_helper
from make_reports.workflows import manage_data


# %%
def get_metrics(proj_dir, recruit_demo, prop_motion, scan_pace, redcap_token):
    """Generate descriptive metrics about the data.

    Miscellaneous methods to aid in guiding data collection
    and maintenance.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    recruit_demo : bool
        Compare enrolled demographics versus proposed
    prop_motion : bool
        Calculate proportion of volumes that exceed FD threshold
    scan_pace : bool
        Plot number of attempted scans by week
    redcap_token : str
        API token for RedCap project

    """

    def _get_surveys():
        """Check for survey data and attempt cleaning if needed."""
        redcap_clean = glob.glob(
            f"{proj_dir}/data_survey/redcap_demographics/data_clean/*.csv"
        )
        visit_clean = glob.glob(
            f"{proj_dir}/data_survey/visit*/data_clean/*.csv"
        )
        if len(redcap_clean) != 4 or len(visit_clean) != 17:
            print("Missing RedCap, Qualtrics clean data. Cleaning ...")
            cl_data = manage_data.CleanSurveys(proj_dir)
            cl_data.clean_redcap()
            cl_data.clean_qualtrics()
            print("\tDone.")

    out_dir = os.path.join(proj_dir, "analyses/metrics_recruit")
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Compare planned versus actual recruitment demographics
    if recruit_demo:
        _get_surveys()
        redcap_demo = build_reports.DemoAll(proj_dir)
        redcap_demo.remove_withdrawn()
        print("\nComparing planned vs. actual recruitment demographics ...")
        _ = calc_metrics.demographics(proj_dir, redcap_demo.final_demo)

    # Plot number of attempted scans per week
    if scan_pace:
        print("Calculate number of attempted scans per week ...\n")
        _ = calc_metrics.scan_pace(redcap_token, proj_dir)

    if prop_motion:
        _ = calc_metrics.censored_volumes(proj_dir)


# %%
def prisma_flow(proj_dir, redcap_token):
    """Title."""
    # 0) Recruit
    #       from prescreening
    # 1) Enrolled -- consent + demo + guid
    #       from build_reports.DemoAll.final_demo
    # 2) Visit 1
    #       Complete surveys
    # 3) Visit 2
    #       BDI
    #       MRI
    # 4) Visit 3
    #       BDI
    #       MRI

    # Recruit
    df_pre = survey_download.download_prescreening(redcap_token)
    num_recruit = df_pre.shape[0]

    # Visit1
    redcap_demo = build_reports.DemoAll(proj_dir)
    df_compl = survey_download.download_completion_log(redcap_token)

    num_enroll = redcap_demo.final_demo.shape[0]
    num_v1_sur = len(
        df_compl.index[
            (df_compl["day_1_fully_completed"] == 1.0)
            | (df_compl["emotion_quest_completed"] == 1.0)
        ].tolist()
    )
    num_v1_x = len(report_helper.Excluded().visit1)
    num_v1_w = len(report_helper.Withdrew().visit1)
    num_v1_l = len(report_helper.Lost().visit1)

    #
    flo = Digraph("participant_flow")
    flo.attr(label="Participant Flow", labelloc="t", fontsize="18")
    flo.node("a", f"Recruited individuals: {num_recruit}", shape="box")
    with flo.subgraph() as c:
        c.attr(rank="same")
        c.node(
            "b",
            "Visit1\n"
            + f"Enrolled: {num_enroll}\l"  # noqa: W605
            + f"Surveys: {num_v1_sur}\l",  # noqa: W605
            shape="box",
        )
        c.node(
            "c",
            f"Excluded: {num_v1_x}\l"  # noqa: W605
            + f"Lost: {num_v1_l}\l"  # noqa: W605
            + f"Withdrawn: {num_v1_w}\l",  # noqa: W605
            shape="box",
        )
    with flo.subgraph() as c:
        c.attr(rank="same")
        c.node("d", "Visit2\nSurveys: TODO\lMRI: TODO\l", shape="box")
        c.node(
            "e", "Excluded: TODO\lLost: TODO\lWithdrawn: TODO\l", shape="box"
        )
    with flo.subgraph() as c:
        c.attr(rank="same")
        c.node("f", "Visit3\nSurveys: TODO\lMRI: TODO\l", shape="box")
        c.node(
            "g", "Excluded: TODO\lLost: TODO\lWithdrawn: TODO\l", shape="box"
        )
    flo.node("i", "Final: TODO", shape="box")
    flo.edges(["ab", "bc", "bd", "de", "df", "fg", "fi"])

    flo.format = "png"
    out_plot = os.path.join(
        proj_dir, "analyses/metrics_recruit", "plot_flow-participant"
    )
    flo.render(out_plot)
