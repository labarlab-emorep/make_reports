"""Methods for gathering and organizing survey, task responses.

Download survey data from REDCap and Qualtrics, and find EmoRep
task data from the scanner. Split omnibus dataframes into
survey-specific dataframes, clean, and organize according
to the EmoRep scheme.

download_surveys : download survey responses from REDCap, Qualtrics
CleanSurveys : organize, split, and clean survey responses into
                separate dataframes

"""
# %%
import os
import glob
from make_reports.resources import survey_download, survey_clean
from make_reports.resources import report_helper


# %%
def download_surveys(
    proj_dir,
    redcap_token=None,
    qualtrics_token=None,
    get_redcap=False,
    get_qualtrics=False,
):
    """Coordinate survey download resources.

    Parameters
    ----------
    proj_dir : path
        Location of parent directory for project
    redcap_token : str
        API token for RedCap
    qualtrics_token : str
        API token for Qualtrics
    get_redcap : bool
        Whether to download RedCap surveys
    get_qualtrics : bool
        Whether to download Qualtrics surveys

    Returns
    -------
    None

    """
    print("\nStarting survey download ...")
    if get_redcap:
        _ = survey_download.download_redcap(proj_dir, redcap_token)
    if get_qualtrics:
        _ = survey_download.download_qualtrics(proj_dir, qualtrics_token)
    print("\nDone with survey download!")


class CleanSurveys:
    """Coordinate cleaning of survey data.

    Each class method will read in raw data from their respective
    sources and then corrdinate cleaning methods from survey_clean.

    Cleaned dataframes are written to:
        <proj_dir>/data_survey/<visit>/data_clean

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory

    Methods
    -------
    clean_redcap()
        Clean all RedCap surveys
    clean_rest()
        Clean ratings of resting state experiences
    clean_qualtrics()
        Clean all Qualtrics surveys

    Example
    -------
    cl = CleanSurveys("/path/to/proj/dir")
    cl.clean_redcap()
    cl.clean_qualtrics()
    cl.clean_rest()

    """

    def __init__(self, proj_dir):
        """Initialize."""
        print("Initializing CleanSurveys")
        self._proj_dir = proj_dir

    def clean_redcap(self):
        """Coordinate cleaning of RedCap surveys.

        Clean each survey specified in report_helper.redcap_dict and
        write out the cleaned dataframe.

        Raises
        ------
        FileNotFoundError
            Unexpected number of files in:
                <proj_dir>/data_survey/redcap_demographics/data_raw

        """
        # Check for data
        redcap_raw = glob.glob(
            f"{self._proj_dir}/data_survey/redcap*/data_raw/*latest.csv"
        )
        if len(redcap_raw) != 4:
            raise FileNotFoundError(
                "Missing raw survey data in redcap directories,"
                + " please download raw data via rep_dl."
            )

        # Get cleaning obj
        print("\tRunning CleanSurveys.clean_redcap")
        redcap_dict = report_helper.redcap_dict()
        clean_redcap = survey_clean.CleanRedcap(self._proj_dir)

        # Clean each planned survey, write out
        for sur_name, dir_name in redcap_dict.items():
            clean_redcap.clean_surveys(sur_name)

            # Write study data
            out_name = "BDI" if "bdi" in sur_name else sur_name
            clean_file = os.path.join(
                self._proj_dir,
                "data_survey",
                dir_name,
                "data_clean",
                f"df_{out_name}.csv",
            )
            out_dir = os.path.dirname(clean_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            clean_redcap.df_clean.to_csv(clean_file, index=False, na_rep="")

            # Write pilot data
            pilot_file = os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                dir_name,
                "data_clean",
                f"df_{out_name}.csv",
            )
            out_dir = os.path.dirname(pilot_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            clean_redcap.df_pilot.to_csv(pilot_file, index=False, na_rep="")

    def clean_qualtrics(self):
        """Coordinate cleaning of Qualtrics surveys.

        Clean each survey specified in report_helper.qualtrics_dict and
        write out the cleaned dataframe.

        Raises
        ------
        FileNotFoundError
            Unexpected number of files in:
                <proj_dir>/data_survey/visit_day*/data_raw

        """

        def _write_clean_qualtrics(
            clean_dict: dict, pilot_dict: dict, dir_name: str
        ) -> None:
            """Write cleaned dataframes for RedCap surveys."""
            # Unpack study clean data
            for h_name, h_df in clean_dict.items():
                out_file = os.path.join(
                    self._proj_dir,
                    "data_survey",
                    dir_name,
                    "data_clean",
                    f"df_{h_name}.csv",
                )
                out_dir = os.path.dirname(out_file)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                print(f"\tWriting : {out_file}")
                h_df.to_csv(out_file, index=False, na_rep="")

            # Unpack pilot clean data
            for h_name, h_df in pilot_dict.items():
                out_file = os.path.join(
                    self._proj_dir,
                    "data_pilot/data_survey",
                    dir_name,
                    "data_clean",
                    f"df_{h_name}.csv",
                )
                out_dir = os.path.dirname(out_file)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                print(f"\tWriting : {out_file}")
                h_df.to_csv(out_file, index=False, na_rep="")

        # Check for data
        visit_raw = glob.glob(
            f"{self._proj_dir}/data_survey/visit*/data_raw/*latest.csv"
        )
        if len(visit_raw) != 7:
            raise FileNotFoundError(
                "Missing raw survey data in visit directories,"
                + " please download raw data via rep_dl."
            )

        # Get cleaning obj
        print("\tRunning CleanSurveys.clean_qualtrics")
        qualtrics_dict = report_helper.qualtrics_dict()
        clean_qualtrics = survey_clean.CleanQualtrics(self._proj_dir)

        # Clean each planned survey and write out
        for sur_name, dir_name in qualtrics_dict.items():
            clean_qualtrics.clean_surveys(sur_name)

            # Account for visit type, survey name/report organization
            if type(dir_name) == list:
                for vis_name in dir_name:
                    _write_clean_qualtrics(
                        clean_qualtrics.data_clean[vis_name],
                        clean_qualtrics.data_pilot[vis_name],
                        vis_name,
                    )
            elif dir_name == "visit_day1":
                _write_clean_qualtrics(
                    clean_qualtrics.data_clean,
                    clean_qualtrics.data_pilot,
                    dir_name,
                )

    def clean_rest(self):
        """Coordinate cleaning of rest ratings survey.

        Raises
        ------
        FileNotFoundError
            *rest-ratings*.tsv are not found in BIDS location

        """
        print("Cleaning survey : rest ratings")

        # Check for data
        raw_path = os.path.join(
            self._proj_dir,
            "data_scanner_BIDS",
            "rawdata",
        )
        rest_list = glob.glob(f"{raw_path}/sub-*/ses-*/beh/*rest-ratings*.tsv")
        if not rest_list:
            raise FileNotFoundError(
                "Missing rest ratings, try running dcm_conversion."
            )

        # Aggregate rest ratings, for each session day
        print("\tRunning CleanSurveys.clean_rest")
        agg_rest = survey_clean.CombineRestRatings(self._proj_dir)
        for day in ["day2", "day3"]:

            # Get, write out study data
            agg_rest.get_rest_ratings(day, raw_path)
            out_file = os.path.join(
                self._proj_dir,
                "data_survey",
                f"visit_{day}",
                "data_clean",
                "df_rest-ratings.csv",
            )
            out_dir = os.path.dirname(out_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            print(f"\tWriting : {out_file}")
            agg_rest.df_sess.to_csv(out_file, index=False, na_rep="")

            # Get, write out pilot data
            rawdata_pilot = os.path.join(
                self._proj_dir, "data_pilot/data_scanner_BIDS", "rawdata"
            )
            agg_rest.get_rest_ratings(day, rawdata_pilot)
            out_file = os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                f"visit_{day}",
                "data_clean",
                "df_rest-ratings.csv",
            )
            out_dir = os.path.dirname(out_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            print(f"\tWriting : {out_file}")
            agg_rest.df_sess.to_csv(out_file, index=False, na_rep="")
