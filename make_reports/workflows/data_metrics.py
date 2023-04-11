"""Methods for generating metrics about the project.

Supports tracking scanning pace, participant demographics,
and amount of motion in EPI data.

"""
# %%
import os
import glob
from make_reports.resources import build_reports, calc_metrics
from make_reports.workflows import manage_data


# %%
def get_metrics(
    proj_dir,
    recruit_demo,
    prop_motion,
    scan_pace,
    participant_flow,
    redcap_token,
):
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
    participant_flow : bool
        Draw participant PRISMA flowchart
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

    # Plot proportion of volumes censored
    if prop_motion:
        _ = calc_metrics.censored_volumes(proj_dir)

    # Draw PRSIMA flow
    if participant_flow:
        part_flo = calc_metrics.ParticipantFlow(proj_dir, redcap_token)
        part_flo.draw_prisma()
