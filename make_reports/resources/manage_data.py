"""Methods for gathering and organizing survey, task responses.

Download survey data from REDCap and Qualtrics, and find EmoRep
resting task data from the scanner. Split omnibus dataframes into
survey-specific dataframes, clean, and organize according
to the EmoRep scheme.

Mysql database db_emorep is also updated with participant responses.

GetRedcap : download, clean, and write RedCap report data
GetQualtrics : download, clean, and write Qualtrics survey data
GetRest : aggregate, clean, and write rest rating responses (rest_ratings)
GetTask : aggregate and write emorep task responses (in_scan_ratings)

"""
# %%
import os
import glob
from typing import Union, Tuple
import pandas as pd
import numpy as np
from multiprocessing import Pool
from make_reports.resources import survey_download
from make_reports.resources import survey_clean
from make_reports.resources import report_helper
from make_reports.resources import sql_database


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

    Inherits survey_clean.CleanRedcap.

    Download RedCap data and coordinate cleaning methods. Write both
    raw and cleaned dataframes to disk, except for reports containing PHI.
    Update mysql db_emorep.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project parent directory

    Attributes
    ----------
    clean_redcap : dict
        All cleaned RedCap reports, organized by pilot/study
        participant and visit.
        {pilot|study: {visit: {survey_name: pd.DataFrame}}}

    Methods
    -------
    get_redcap()
        Coordinate download and cleaning,
        builds attr clean_redcap

    Example
    -------
    gr = GetRedcap(*args)
    gr.get_redcap()
    rc_dict = gr.clean_redcap

    gr.get_redcap(survey_list=["bdi_day2", "bdi_day3"])
    rc_bdi_dict = gr.clean_redcap

    """

    def __init__(self, proj_dir):
        """Initialize."""
        self._proj_dir = proj_dir
        pilot_list = report_helper.pilot_list()
        super().__init__(self._proj_dir, pilot_list)

        # Start mysql connection
        db_con = sql_database.DbConnect()
        self._up_mysql = sql_database.MysqlUpdate(db_con)

    def _download_redcap(self, survey_list: list) -> dict:
        """Get, write, and return RedCap survey info.

        Returns
        -------
        dict
            {survey_name: (str|bool, pd.DataFrame)}, e.g.
            {"bdi_day2": ("visit_day2", pd.DataFrame)}
            {"demographics": (False, pd.DataFrame)}

        """
        raw_redcap = survey_download.dl_redcap(self._proj_dir, survey_list)

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
        Update mysql db_emorep.

        Parameters
        ----------
        survey_list : list, optional
            If None, pull all reports. Available report names:
            demographics, consent_pilot, consent_v1.22, guid,
            bdi_day2, bdi_day3

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

        # Download and write raw data, account for user input
        if survey_list:
            for chk_sur in survey_list:
                if chk_sur not in clean_map.keys():
                    raise ValueError(
                        f"Unexpected RedCap report name : {chk_sur}"
                    )
        else:
            survey_list = list(clean_map.keys())
        raw_redcap = self._download_redcap(survey_list)

        # Clean each survey and build clean_redcap attr
        self.clean_redcap = {"pilot": {}, "study": {}}
        for sur_name in raw_redcap:
            visit = clean_map[sur_name][1]
            dir_name, df_raw = raw_redcap[sur_name]

            # Find and execute appropriate clean method
            print(f"\tCleaning RedCap survey : {sur_name}")
            clean_method = getattr(self, clean_map[sur_name][0])
            clean_method(df_raw)

            # Update clean_redcap attr
            key_name = (
                "BDI" if sur_name in ["bdi_day2", "bdi_day3"] else sur_name
            )
            if visit not in self.clean_redcap["study"].keys():
                self.clean_redcap["pilot"][visit] = {key_name: self.df_pilot}
                self.clean_redcap["study"][visit] = {key_name: self.df_study}
            else:
                self.clean_redcap["pilot"][visit][key_name] = self.df_pilot
                self.clean_redcap["study"][visit][key_name] = self.df_study

            # Avoid writing PHI to disk
            if dir_name:
                self._write_redcap(self.df_study, sur_name, dir_name, False)
                self._write_redcap(self.df_pilot, sur_name, dir_name, True)

            # Update mysql db_emorep.tbl_bdi
            if key_name == "BDI":
                self._up_mysql.update_db(
                    self.df_study.copy(), key_name, int(visit[-1]), "redcap"
                )

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
    raw and cleaned dataframes to disk. Update mysql db_emorep.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project parent directory

    Attributes
    ----------
    clean_qualtrics : dict
        All cleaned Qualtrics reports, organized by pilot/study
        participant and visit.
        {pilot|study: {visit: {survey_name: pd.DataFrame}}}

    Methods
    -------
    get_qualtrics()
        Coordinate download and cleaning,
        builds attr clean_qualtrics

    Example
    -------
    gq = GetQualtrics(*args)
    gq.get_qualtrics()
    q_dict = gq.clean_qualtrics

    gq.get_qualtrics(survey_list=["EmoRep_Session_1"])
    q_s1_dict = gq.clean_qualtrics

    """

    def __init__(self, proj_dir):
        """Initialize."""
        self._proj_dir = proj_dir
        pilot_list = report_helper.pilot_list()
        part_comp = report_helper.CheckStatus()
        part_comp.status_change("withdrew")
        withdrew_list = [x for x in part_comp.all.keys()]
        super().__init__(self._proj_dir, pilot_list, withdrew_list)

        # Start mysql server connection
        db_con = sql_database.DbConnect()
        self._up_mysql = sql_database.MysqlUpdate(db_con)

    def _download_qualtrics(self, survey_list: list) -> dict:
        """Get, write, and return Qualtrics survey info.

        Returns
        -------
        dict
            {survey_name: (visit, pd.DataFrame)}, e.g.
            {"Session 2 & 3 Survey_latest": ("visit_day23", pd.DataFrame)}

        """
        raw_qualtrics = survey_download.dl_qualtrics(
            self._proj_dir, survey_list
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

    def get_qualtrics(self, survey_list=None):
        """Get and clean Qualtrics survey info.

        Coordinate Qualtrics survey download, then match survey to cleaning
        method. Write raw and cleaned dataframes to disk. Update mysql
        db_emorep.

        Parameters
        ----------
        survey_list : list, optional
            Qualtrics report names, available names = EmoRep_Session_1,
            Session 2 & 3 Survey, FINAL - EmoRep Stimulus Ratings - fMRI Study

        Attributes
        ----------
        clean_qualtrics : dict
            {pilot|study: {visit: {survey_name: pd.DataFrame}}}

        """
        # Map survey name to survey_clean.CleanQualtrics method
        clean_map = {
            "EmoRep_Session_1": "clean_session_1",
            "Session 2 & 3 Survey": "clean_session_23",
            "FINAL - EmoRep Stimulus Ratings - "
            + "fMRI Study": "clean_postscan_ratings",
        }

        # Download and write raw survey data
        if survey_list:
            for chk_sur in survey_list:
                if chk_sur not in clean_map.keys():
                    raise ValueError(
                        f"Unexpected Qualtrics report name : {chk_sur}"
                    )
        else:
            survey_list = list(clean_map.keys())
        raw_qualtrics = self._download_qualtrics(survey_list)

        # Clean surveys and build clean_qualtrics attr
        self.clean_qualtrics = {"pilot": {}, "study": {}}
        for omni_name in raw_qualtrics:
            _, df_raw = raw_qualtrics[omni_name]

            # Trigger relevant cleaning method
            clean_method = getattr(self, clean_map[omni_name])
            clean_method(df_raw)
            self._unpack_qualtrics()

    def _unpack_qualtrics(self):
        """Organize cleaned qualtrics data, trigger writing."""
        for data_type in self.clean_qualtrics.keys():
            # Get attr data_study|pilot
            is_pilot = True if data_type == "pilot" else False
            data_dict = getattr(self, f"data_{data_type}")

            # Unpack data_dict
            for visit, sur_dict in data_dict.items():
                for sur_name, df in sur_dict.items():
                    # Build attr clean_qualtrics, write
                    if visit not in self.clean_qualtrics[data_type].keys():
                        self.clean_qualtrics[data_type][visit] = {sur_name: df}
                    else:
                        self.clean_qualtrics[data_type][visit][sur_name] = df
                    self._write_qualtrics(df, sur_name, visit, is_pilot)

                    # Update mysql db_emorep with study (not pilot) data
                    if is_pilot:
                        continue
                    self._up_mysql.update_db(
                        df.copy(), sur_name, int(visit[-1]), "qualtrics"
                    )

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

    Writes cleaned dataframe to disk. Updates mysql db_emorep.

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
    gr = GetRest(*args)
    gr.get_rest()
    rest_dict = gr.clean_rest

    """

    def __init__(self, proj_dir):
        """Initialize."""
        self._proj_dir = proj_dir
        db_con = sql_database.DbConnect()
        self._up_mysql = sql_database.MysqlUpdate(db_con)

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

                # Update mysql db_emorep.tbl_rest_ratings with study data
                if data_type == "pilot":
                    continue
                self._up_mysql.update_db(
                    df_sess.copy(),
                    "rest_ratings",
                    int(day[-1]),
                    "rest_ratings",
                )

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


# %%
class GetTask:
    """Aggregate in-scanner task responses from BIDS events files.

    Writes cleaned dataframe to disk. Updates mysql db_emorep.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project parent directory

    Attributes
    ----------
    clean_task : dict
        All cleaned task responses
        {study: visit: {"in_scan_task": pd.DataFrame}}

    Methods
    -------
    get_task()
        Get and clean in-scanner task responses

    Example
    -------
    gt = GetTask(*args)
    gt.get_task()
    task_dict = gt.clean_task

    """

    def __init__(self, proj_dir):
        """Initialize."""
        self._proj_dir = proj_dir

    def get_task(self):
        """Coordinate finding and cleaning task responses.

        Data are written to disk, used to update mysql db_emorep,
        and available on clean_task attr.

        Attributes
        ----------
        clean_task : dict
            {study: visit: {"in_scan_task": pd.DataFrame}}

        """
        # Start DbConnect here to avoid pickle issue
        db_con = sql_database.DbConnect()
        up_mysql = sql_database.MysqlUpdate(db_con)

        # Aggregate data
        print("Aggregating in-scanner task responses ...")
        self.clean_task = {"study": {}}
        self._build_df()

        # Build clean_task attr
        for sess in ["day2", "day3"]:
            df_sess = self._df_all[self._df_all["visit"] == sess]
            self.clean_task["study"][f"visit_{sess}"] = {
                "in_scan_task": df_sess
            }

            # Write out and update database
            out_path = os.path.join(
                self._proj_dir,
                f"data_survey/visit_{sess}",
                "df_in_scan_ratings.csv",
            )
            _write_dfs(df_sess, out_path)
            up_mysql.update_db(
                df_sess.copy(),
                "in_scan_ratings",
                int(sess[-1]),
                "in_scan_ratings",
            )

    def _build_df(self):
        """Build attr df_all from rawdata events files."""
        # Find all events files
        mri_rawdata = os.path.join(
            self._proj_dir, "data_scanner_BIDS", "rawdata"
        )
        events_all = sorted(
            glob.glob(f"{mri_rawdata}/sub-*/ses-*/func/*_events.tsv")
        )
        if not events_all:
            raise ValueError(
                f"Expected to find BIDS events files in : {mri_rawdata}"
            )

        # Load (in parallel) all dfs, build df_all, trigger cleaning
        events_dfs = Pool().starmap(
            self._load_event, [(event_path,) for event_path in events_all]
        )
        self._df_all = pd.concat(events_dfs, axis=0, ignore_index=True)
        self._clean_df()

    def _load_event(self, event_path: Union[str, os.PathLike]) -> pd.DataFrame:
        """Return organized pd.DataFrame of events file."""
        print(f"\tLoading {os.path.basename(event_path)} ...")
        subj, sess, task, run, _ = os.path.basename(event_path).split("_")
        df = pd.read_csv(event_path, sep="\t")

        # Add columns for keeping subj, sess straight
        df["subj"] = subj.split("-")[-1]
        df["sess"] = sess.split("-")[-1]
        df["task"] = task.split("-")[-1]
        df["run"] = int(run[-1])
        return df

    def _clean_df(self):
        """Tidy-format attr df_all."""
        print("\tCleaning dataframe ...")
        # Get participant responses
        self._df_all = self._df_all.loc[
            self._df_all["trial_type"].isin(
                ["movie", "scenario", "emotion", "intensity"]
            )
        ].reset_index(drop=True)

        # Organize dataframe
        self._df_all["emotion"] = self._df_all["emotion"].fillna(
            method="ffill"
        )
        self._df_all = self._df_all.loc[
            ~self._df_all["trial_type"].isin(["movie", "scenario"])
        ].reset_index(drop=True)
        self._df_all = self._df_all.drop(
            ["onset", "duration", "accuracy", "stim_info"], axis=1
        )
        self._df_all = self._df_all.drop("response_time", axis=1)

        # Tidy format with resp_emotion, resp_intensity cols
        self._df_all = self._df_all.rename(columns={"emotion": "block"})
        self._df_all = self._df_all.pivot(
            index=[
                "subj",
                "sess",
                "task",
                "run",
                "block",
            ],
            columns=["trial_type"],
            values="response",
        ).reset_index()
        self._df_all = self._df_all.rename(
            columns={
                "emotion": "resp_emotion",
                "intensity": "resp_intensity",
            }
        )

        # Update column names, types
        self._df_all = self._df_all.rename(
            columns={"subj": "study_id", "sess": "visit"}
        )
        self._df_all["resp_emotion"] = self._df_all["resp_emotion"].str.lower()
        self._df_all["resp_intensity"] = self._df_all[
            "resp_intensity"
        ].replace("NONE", np.nan)
        self._df_all["resp_intensity"] = self._df_all["resp_intensity"].astype(
            "Int64"
        )
