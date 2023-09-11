"""Methods for gathering and organizing survey, task responses.

Download survey data from REDCap and Qualtrics, and find EmoRep
resting task data from the scanner. Split omnibus dataframes into
survey-specific dataframes, clean, and organize according
to the EmoRep scheme.

TODO
GetSurveys : organize, split, and clean survey responses into
                separate dataframes

"""
# %%
import os
from typing import Union, Tuple
import pandas as pd
from make_reports.resources import survey_download
from make_reports.resources import survey_clean
from make_reports.resources import report_helper


# %%
def _write_dfs(df: pd.DataFrame, out_file: Union[str, os.PathLike]):
    """Make output dir and write out_file from df."""
    out_dir = os.path.dirname(out_file)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    df.to_csv(out_file, index=False, na_rep="")
    print(f"\tWrote : {out_file}")


class GetRedcap(survey_clean.CleanRedcap):
    """Download and clean RedCap surveys.

    Inherits survey_clean.CleanRedcap. Intended to be inherited
    by GetSurveys, references attrs set in child.

    Download RedCap data and coordinate cleaning methods. Write both
    raw and cleaned dataframes to disk, except for reports containing PHI.

    Methods
    -------
    manage_redcap()
        Coordinate download and cleaning,
        builds attr clean_redcap

    """

    def __init__(self, proj_dir, redcap_token):
        """Initialize."""
        self._proj_dir = proj_dir
        self._pilot_list = report_helper.pilot_list()
        self._redcap_token = redcap_token

    def _download_redcap(self, survey_list) -> dict:
        """Get, write, and return RedCap survey info.

        Returns
        -------
        dict
            {survey_name: (str|bool, pd.DataFrame)}, e.g.
            {"bdi_day2": ("visit_day2", pd.DataFrame)}
            {"demographics": (False, pd.DataFrame)}

        """
        raw_redcap = survey_download.dl_redcap(
            self._proj_dir, self._redcap_token, survey_list
        )

        # Write rawdata to csv, skip writing PHI
        for sur_name in raw_redcap:
            dir_name, df = raw_redcap[sur_name]
            if not dir_name:
                continue
            out_file = os.path.join(
                self._proj_dir,
                "data_survey",
                dir_name,
                f"raw_{sur_name}.csv",
            )
            _write_dfs(df, out_file)
        return raw_redcap

    def get_redcap(self, survey_list=None):
        """Get and clean RedCap survey info.

        Coordinate RedCap survey download, then match survey to cleaning
        method. Write certain raw and cleaned dataframes to disk.

        Parameters
        ----------

        Attributes
        ----------
        clean_redcap : dict
            {pilot|study: {visit: {survey_name: pd.DataFrame}}}

        """
        # Align survey name with survey_clean.CleanRedcap method, visit
        clean_map = {
            "demographics": ["clean_demographics", "visit_day1"],
            "consent_pilot": ["clean_consent", "visit_day1"],
            "consent_v1.22": ["clean_consent", "visit_day1"],
            "guid": ["clean_guid", "visit_day0"],
            "bdi_day2": ["clean_bdi_day23", "visit_day2"],
            "bdi_day3": ["clean_bdi_day23", "visit_day3"],
        }

        # Download and write raw data
        sur_list = list(clean_map.keys()) if not survey_list else survey_list
        raw_redcap = self._download_redcap(sur_list)

        # Clean each survey and build clean_redcap attr
        self.clean_redcap = {"pilot": {}, "study": {}}
        for sur_name in raw_redcap:
            visit = clean_map[sur_name][1]
            dir_name, self._df_raw = raw_redcap[sur_name]

            # Find and execute appropriate clean method
            print(f"\tCleaning RedCap survey : {sur_name}")
            clean_method = getattr(self, clean_map[sur_name][0])
            clean_method()

            # Update clean_redcap
            if visit not in self.clean_redcap["study"].keys():
                self.clean_redcap["pilot"][visit] = {sur_name: self.df_pilot}
                self.clean_redcap["study"][visit] = {sur_name: self.df_study}
            else:
                self.clean_redcap["pilot"][visit][sur_name] = self.df_pilot
                self.clean_redcap["study"][visit][sur_name] = self.df_study

            # Avoid writing PHI to disk
            if dir_name:
                self._write_redcap(self.df_study, sur_name, dir_name, False)
                self._write_redcap(self.df_pilot, sur_name, dir_name, True)

    def _write_redcap(
        self, df: pd.DataFrame, sur_name: str, dir_name: str, is_pilot: bool
    ):
        """Determine output path and write dataframe."""
        out_name = "BDI" if "bdi" in sur_name else sur_name
        out_dir = "data_pilot/data_survey" if is_pilot else "data_survey"
        out_file = os.path.join(
            self._proj_dir,
            out_dir,
            dir_name,
            f"df_{out_name}.csv",
        )
        _write_dfs(df, out_file)


class GetQualtrics(survey_clean.CleanQualtrics):
    """Download and clean Qualtrics surveys.

    Inherits survey_clean.CleanQualtrics.

    Download Qualtrics data and coordinate cleaning methods. Write both
    raw and cleaned dataframes to disk.

    Methods
    -------
    manage_qualtrics()
        Coordinate download and cleaning,
        builds attr clean_qualtrics

    """

    def __init__(self, proj_dir, qualtrics_token):
        """Initialize."""
        self._proj_dir = proj_dir
        self._pilot_list = report_helper.pilot_list()
        self._qualtrics_token = qualtrics_token
        part_comp = report_helper.ParticipantComplete()
        part_comp.status_change("withdrew")
        self._withdrew_list = part_comp.all

    def _download_qualtrics(self):
        """Get, write, and return Qualtrics survey info.

        Returns
        -------
        dict
            {survey_name: (visit, pd.DataFrame)}, e.g.
            {"Session 2 & 3 Survey_latest": ("visit_day23", pd.DataFrame)}

        """
        raw_qualtrics = survey_download.dl_qualtrics(
            self._proj_dir, self._qualtrics_token
        )

        # Coordinate writing to disk -- write session2&3 surveys to
        # visit_day2 to avoid duplication.
        for sur_name in raw_qualtrics:
            dir_name, df = raw_qualtrics[sur_name]
            visit = "visit_day2" if dir_name == "visit_day23" else dir_name
            out_file = os.path.join(
                self._proj_dir, "data_survey", visit, f"raw_{sur_name}.csv"
            )
            _write_dfs(df, out_file)
        return raw_qualtrics

    def get_qualtrics(self):
        """Get and clean Qualtrics survey info.

        Coordinate Qualtrics survey download, then match survey to cleaning
        method. Write raw and cleaned dataframes to disk.

        Attributes
        ----------
        clean_qualtrics : dict
            {pilot|study: {visit: {survey_name: pd.DataFrame}}}

        """
        # Download and write raw survey data
        raw_qualtrics = self._download_qualtrics()

        # Map survey name to survey_clean.CleanQualtrics method
        clean_map = {
            "EmoRep_Session_1": "clean_session_1",
            "Session 2 & 3 Survey": "clean_session_23",
            "FINAL - EmoRep Stimulus Ratings - "
            + "fMRI Study": "clean_postscan_ratings",
        }

        # Clean surveys and build clean_qualtrics attr
        self.clean_qualtrics = {"pilot": {}, "study": {}}
        for omni_name in raw_qualtrics:
            _, self._df_raw = raw_qualtrics[omni_name]

            # Trigger relevant cleaning method
            clean_method = getattr(self, clean_map[omni_name])
            clean_method()
            self._unpack_qualtrics()

    def _unpack_qualtrics(self):
        """Organize cleaned qualtrics data, trigger writing."""
        for data_type in self.clean_qualtrics.keys():
            is_pilot = True if data_type == "pilot" else False
            data_dict = getattr(self, f"data_{data_type}")
            for visit, sur_dict in data_dict.items():
                for sur_name, df in sur_dict.items():
                    if visit not in self.clean_qualtrics[data_type].keys():
                        self.clean_qualtrics[data_type][visit] = {sur_name: df}
                    else:
                        self.clean_qualtrics[data_type][visit][sur_name] = df
                    self._write_qualtrics(df, sur_name, visit, is_pilot)

    def _write_qualtrics(
        self, df: pd.DataFrame, sur_name: str, visit: str, is_pilot: bool
    ):
        """Determine output path and write dataframe."""
        out_dir = "data_pilot/data_survey" if is_pilot else "data_survey"
        out_file = os.path.join(
            self._proj_dir,
            out_dir,
            visit,
            f"df_{sur_name}.csv",
        )
        _write_dfs(df, out_file)


class GetRest:
    """Aggregate rest-rating survey responses.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project parent directory

    Attributes
    ----------
    clean_rest : dict
        All cleaned rest ratings
        {pilot|study: {visit: {"rest_ratings": pd.DataFrame}}}

    Methods
    -------
    get_rest()
        Get and clean all rest-rating responses

    Example
    -------


    """

    def __init__(self, proj_dir):
        """Initialize."""
        print("Initializing CleanSurveys")
        self._proj_dir = proj_dir

    def get_rest(self):
        """Coordinate cleaning of rest ratings survey.

        Cleaned surveys are written to disk and are available
        in clean_rest attr.

        Attributes
        ----------
        clean_rest : dict
            {pilot|study: {visit: {"rest_ratings": pd.DataFrame}}}

        """
        print("Cleaning survey : rest ratings")

        # Aggregate rest ratings, for each session day
        self.clean_rest = {"pilot": {}, "study": {}}
        for data_type in self.clean_rest.keys():
            raw_dir, out_dir = self._rest_paths(data_type)
            for day in ["day2", "day3"]:
                df_sess = survey_clean.clean_rest_ratings(day, raw_dir)

                # Build clean_rest attr, write out df
                self.clean_rest[data_type][f"visit_{day}"] = {
                    "rest_ratings": df_sess
                }
                out_file = os.path.join(
                    out_dir,
                    f"visit_{day}/df_rest-ratings.csv",
                )
                _write_dfs(df_sess, out_file)

    def _rest_paths(self, data_type: str) -> Tuple:
        """Return paths to rawdata, output directory."""
        if data_type == "pilot":
            raw_dir = os.path.join(
                self._proj_dir, "data_pilot/data_scanner_BIDS/rawdata"
            )
            out_dir = os.path.join(self._proj_dir, "data_pilot/data_survey")
        else:
            raw_dir = os.path.join(self._proj_dir, "data_scanner_BIDS/rawdata")
            out_dir = os.path.join(self._proj_dir, "data_survey")
        return (raw_dir, out_dir)
