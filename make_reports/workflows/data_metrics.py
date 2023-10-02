"""Methods for generating metrics about the project.

get_metrics : generate metrics, figures for recruitment
                demographics, scanning pace, EPI motion,
                and participant flow PRISMA

"""
# %%
import os
from make_reports.resources import calc_metrics


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
    out_dir = os.path.join(proj_dir, "analyses/metrics_recruit")
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Compare planned versus actual recruitment demographics
    if recruit_demo:
        _ = calc_metrics.demographics(proj_dir, redcap_token)

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
