"""Methods for gathering and organizing survey, task responses.

Download survey data from REDCap and Qualtrics, and find EmoRep
resting task data from the scanner. Split omnibus dataframes into
survey-specific dataframes, clean, and organize according
to the EmoRep scheme.

CleanSurveys : organize, split, and clean survey responses into
                separate dataframes

"""
# %%
import os
from typing import Union
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


class _GetRedcap(survey_clean.CleanRedcap):
    """Title.

    Inherits survey_clean.CleanRedcap. Intended to be inherited
    by GetSurveys, references attrs set in child.

    """

    def _download_redcap(self) -> dict:
        """Get, write, and return RedCap survey info."""
        raw_redcap = survey_download.dl_redcap(
            self._proj_dir, self._redcap_token
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

    def manage_redcap(self):
        """Get and clean RedCap survey info.

        Coordinate RedCap survey download, then match survey to cleaning
        method. Write certain raw and cleaned dataframes to disk.

        Attributes
        ----------
        clean_redcap : dict
            {pilot|study: {visit: {survey_name: pd.DataFrame}}}

        """
        #
        raw_redcap = self._download_redcap()

        # Align survey name with survey_clean.CleanRedcap method, visit
        clean_map = {
            "demographics": ["clean_demographics", "visit_day1"],
            "consent_pilot": ["clean_consent", "visit_day1"],
            "consent_v1.22": ["clean_consent", "visit_day1"],
            "guid": ["clean_guid", "visit_day0"],
            "bdi_day2": ["clean_bdi_day23", "visit_day2"],
            "bdi_day3": ["clean_bdi_day23", "visit_day3"],
        }

        #
        self.clean_redcap = {"pilot": {}, "study": {}}
        for sur_name in raw_redcap:
            visit = clean_map[sur_name][1]
            dir_name, self._df_raw = raw_redcap[sur_name]

            #
            clean_method = getattr(self, clean_map[sur_name][0])
            clean_method()
            self.clean_redcap["pilot"][visit] = {sur_name: self.df_pilot}
            self.clean_redcap["study"][visit] = {sur_name: self.df_study}

            #
            if dir_name:
                self._write_redcap(self.df_study, sur_name, dir_name, False)
                self._write_redcap(self.df_pilot, sur_name, dir_name, True)

    def _write_redcap(
        self, df: pd.DataFrame, sur_name: str, dir_name: str, is_pilot: bool
    ):
        """Title."""
        out_name = "BDI" if "bdi" in sur_name else sur_name
        out_dir = "data_pilot/data_survey" if is_pilot else "data_survey"
        out_file = os.path.join(
            self._proj_dir,
            out_dir,
            dir_name,
            f"df_{out_name}.csv",
        )
        _write_dfs(df, out_file)


class _GetQualtrics(survey_clean.CleanQualtrics):
    """Title.

    Inherits survey_clean.CleanQualtrics. Intended to be inherited
    by GetSurveys, references attrs set in child.

    """

    def _download_qualtrics(self):
        """Title."""
        raw_qualtrics = survey_download.dl_qualtrics(
            self._proj_dir, self._qualtrics_token
        )

        #
        for sur_name in raw_qualtrics:
            dir_name, df = raw_qualtrics[sur_name]
            visit = "visit_day2" if dir_name == "visit_day23" else dir_name
            out_file = os.path.join(
                self._proj_dir, "data_survey", visit, f"raw_{sur_name}.csv"
            )
            _write_dfs(df, out_file)
        return raw_qualtrics

    def manage_qualtrics(self):
        """Title.

        Attributes
        ----------
        clean_qualtrics : dict
            {pilot|study: {visit: {survey_name: pd.DataFrame}}}

        """
        raw_qualtrics = self._download_qualtrics()

        clean_map = {
            "EmoRep_Session_1": "clean_session_1",
            "Session 2 & 3 Survey": "clean_session_23",
            "FINAL - EmoRep Stimulus Ratings - "
            + "fMRI Study": "clean_postscan_ratings",
        }

        #
        self.clean_qualtrics = {"pilot": {}, "study": {}}
        for omni_name in raw_qualtrics:
            self._dir_name, self._df_raw = raw_qualtrics[omni_name]

            #
            clean_method = getattr(self, clean_map[omni_name])
            clean_method()
            self._unpack_qualtrics()

    def _unpack_qualtrics(self):
        """Organized cleaned qualtrics data, trigger writing."""
        for data_type in self.clean_qualtrics.keys():
            is_pilot = True if data_type == "pilot" else False
            data_dict = getattr(self, f"data_{data_type}")
            for visit, sur_dict in data_dict.items():
                for sur_name, df in sur_dict.items():
                    self.clean_qualtrics[data_type][visit] = {sur_name: df}
                    self._write_qualtrics(df, sur_name, is_pilot)

    def _write_qualtrics(
        self, df: pd.DataFrame, sur_name: str, is_pilot: bool
    ):
        """Title."""
        out_dir = "data_pilot/data_survey" if is_pilot else "data_survey"
        out_file = os.path.join(
            self._proj_dir,
            out_dir,
            self._dir_name,
            f"df_{sur_name}.csv",
        )
        _write_dfs(df, out_file)


class GetSurveys(_GetRedcap, _GetQualtrics):
    """Title

    Inherits _GetRedcap and _GetQualtrics.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project parent directory

    Attributes
    ----------
    clean_redcap : dict
        All cleaned RedCap survey data
    clean_qualtrics : dict
        All cleaned Qualtrics survey data
    clean_rest : dict
        All cleaned rest ratings

    Methods
    -------
    get_redcap(redcap_token)
        Download and clean all RedCap surveys, write certain to disk,
        generates clean_redcap.
    get_qualtrics(qualtrics_token)
        Download, clean, and write Qualtrics surveys to disk, generates
        clean_qualtrics.
    get_rest()
        Aggregate and clean rest ratings, write to disk, generates
        clean_rest.

    Example
    -------
    get_sur = GetSurveys("/path/to/project/dir")
    get_sur.get_redcap("token")
    get_sur.get_qualtrics("token")
    get_sur.get_rest()

    data_qualtrics = get_sur.clean_qualtrics
    data_redcap = get_sur.clean_redcap
    data_rest = get_sur.clean_rest

    """

    def __init__(self, proj_dir):
        """Initialize."""
        print("Initializing CleanSurveys")
        self._proj_dir = proj_dir
        self._pilot_list = report_helper.pilot_list()
        part_comp = report_helper.ParticipantComplete()
        part_comp.status_change("withdrew")
        self._withdrew_list = part_comp.all

    def get_redcap(self, redcap_token):
        """Title.

        Parameters
        ----------
        redcap_token : str
            Personal access token for RedCap

        Attributes
        ----------
        clean_redcap : dict
            {pilot|study: {visit: {survey_name: pd.DataFrame}}}

        """
        self._redcap_token = redcap_token
        self.manage_redcap()

    def get_qualtrics(self, qualtrics_token):
        """Title.

        Parameters
        ----------
        qualtrics_token : str
            Personal access token for Qualtrics

        Attributes
        ----------
        clean_qualtrics : dict
            {pilot|study: {visit: {survey_name: pd.DataFrame}}}

        """
        self._qualtrics_token = qualtrics_token
        self.manage_qualtrics()

    def get_rest(self):
        """Coordinate cleaning of rest ratings survey.

        Attributes
        ----------
        clean_qualtrics : dict
            {pilot|study: {visit: {"rest_ratings": pd.DataFrame}}}

        """
        print("Cleaning survey : rest ratings")

        # Aggregate rest ratings, for each session day
        self.clean_rest = {"pilot": {}, "study": {}}
        for data_type in self.clean_rest.keys():
            if data_type == "pilot":
                raw_dir = os.path.join(
                    self._proj_dir, "data_pilot/data_scanner_BIDS/rawdata"
                )
                out_dir = os.path.join(
                    self._proj_dir, "data_pilot/data_survey"
                )
            else:
                raw_dir = os.path.join(
                    self._proj_dir, "data_scanner_BIDS/rawdata"
                )
                out_dir = os.path.join(self._proj_dir, "data_survey")

            for day in ["day2", "day3"]:
                df_sess = survey_clean.clean_rest_ratings(day, raw_dir)
                self.clean_rest[data_type][f"visit_{day}"] = {
                    "rest_ratings": df_sess
                }
                out_file = os.path.join(
                    out_dir,
                    f"visit_{day}/df_rest-ratings.csv",
                )
                _write_dfs(df_sess, out_file)
