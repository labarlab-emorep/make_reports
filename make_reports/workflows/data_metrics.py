"""Methods for generating metrics about the project.

get_metrics : generate metrics, figures for recruitment
                demographics, scanning pace, EPI motion,
                and participant flow PRISMA
CheckProjectMri : check for expected rawdata and derivatives
check_emorep_all : check for expected EmoRep survey and rawdata files


"""
# %%
import os
from fnmatch import fnmatch
from typing import Union
from make_reports.resources import calc_metrics
from make_reports.resources import check_data


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


# %%
class CheckProjectMri:
    """Check projects for expected pipeline files.

    Check emorep or archival projects for planned expected files
    to identify missing data and coordinate data processing.
    Output file is named:
        [emorep | archival]_pipeline_progress.csv

    Methods
    -------
    run_check(project)
        Conduct the check for either archival or emorep

    Example
    -------
    do_chk = CheckProject()
    do_chk.run_check("emorep")

    """

    def run_check(self, proj_name):
        """Conduct check of expected files for specified project.

        Parameters
        ----------
        proj_name : str
            [emorep | archival]
            Desired project to check

        """
        # Check args and setup
        if proj_name not in ["emorep", "archival"]:
            raise ValueError(
                f"Unexpected argument for --proj_name : {proj_name}"
            )
        self._setup(proj_name)

        # Conduct planned checks
        print(f"\tStarting checks for {proj_name}")
        chk_data = check_data.CheckMri(
            self._subj_list, self._sess_list, self._raw_dir, self._deriv_dir
        )
        if proj_name == "emorep":
            chk_data.check_emorep()
        else:
            chk_data.check_archival()

        # Write dataframe, push
        out_name = f"{proj_name}_pipeline_progress.csv"
        out_dir = os.path.join(self._deriv_dir, "track_data")
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        out_file = os.path.join(out_dir, out_name)
        print(f"\tGenerating {out_file} ...")
        chk_data.df_mri.to_csv(out_file, index=False, na_rep="")

    def _setup(self, project: str):
        """Set project-specific paths, lists as attributes."""
        # Set paths, lists
        proj_dict = {
            "emorep": {
                "proj_path": "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/data_scanner_BIDS",  # noqa: E501
                "sess": ["day2", "day3"],
            },
            "archival": {
                "proj_path": "/mnt/keoki/experiments2/EmoRep/Exp3_Classify_Archival/data_mri_BIDS",  # noqa: E501
                "sess": ["BAS1"],
            },
        }
        self._proj_dir = proj_dict[project]["proj_path"]
        self._raw_dir = os.path.join(self._proj_dir, "rawdata")
        self._deriv_dir = os.path.join(self._proj_dir, "derivatives")
        self._sess_list = proj_dict[project]["sess"]

        # Find subjects
        if project == "emorep":
            self._subj_list = self._find_subj(
                os.path.join(self._proj_dir, "sourcedata"), "ER"
            )
        else:
            _list = self._find_subj(self._raw_dir, "sub-")
            self._subj_list = [x.split("-")[1] for x in _list]

    def _find_subj(
        self, search_path: Union[str, os.PathLike], search_str: str
    ) -> list:
        """Return list of files matching search_str at search_path."""
        return [
            x for x in os.listdir(search_path) if fnmatch(x, f"{search_str}*")
        ]


def check_emorep_all():
    """Check EmoRep for missing data.

    Compare encountered EmoRep data with expected, generate
    a dataframe to identify what participants are missing
    which files, and whether the missing data is
    expected.

    """
    # Conduct check
    proj_dir = "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion"
    cec = check_data.CheckEmorepComplete(proj_dir)
    cec.check_data()

    # Write dataframe
    out_dir = os.path.join(proj_dir, "derivatives/track_data")
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    out_file = os.path.join(out_dir, "emorep_complete_check.csv")
    print(f"\tGenerating {out_file} ...")
    cec.df_check.to_csv(out_file, index=False, na_rep="")


# %%
