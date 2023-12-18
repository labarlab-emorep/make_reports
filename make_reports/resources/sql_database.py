"""Methods for interacting with mysql db_emorep.

DbConnect : connect to and interact with mysql server
MysqlUpdate : update db_emorep tables

"""
# %%
import os
import pandas as pd
import numpy as np
from typing import Type
import mysql.connector
from contextlib import contextmanager


# %%
class DbConnect:
    """Connect to mysql server and update db_emorep.

    Methods
    -------
    exec_many()
        Update mysql db_emorep.tbl_* with multiple values

    Notes
    -----
    Requires global var 'SQL_PASS' to contain user password
    for mysql db_emorep.

    Example
    -------
    db_con = sql_database.DbConnect()
    sql_cmd = (
        "insert ignore into ref_subj (subj_id, subj_name) values (%s, %s)"
    )
    tbl_input = [(9, "ER0009"), (16, "ER0016")]
    db_con.exec_many(sql_cmd, tbl_input)

    """

    def __init__(self):
        """Set db_con attr as mysql connection."""
        # Check for user password
        try:
            os.environ["SQL_PASS"]
        except KeyError as e:
            raise Exception(
                "No global variable 'SQL_PASS' defined in user env"
            ) from e

        # Connect to server
        self.db_con = mysql.connector.connect(
            host="localhost",
            user=os.environ["USER"],
            password=os.environ["SQL_PASS"],
            database="db_emorep",
        )

    @contextmanager
    def connect(self):
        """Yield cursor."""
        db_cursor = self.db_con.cursor()
        try:
            yield db_cursor
        finally:
            db_cursor.close()

    def exec_many(self, sql_cmd: str, value_list: list):
        """Update db_emorep via executemany."""
        with self.connect() as con:
            con.executemany(sql_cmd, value_list)
            self.db_con.commit()


# %%
class _DfManip:
    """Methods for manipulating pd.DataFrames."""

    def subj_col(self, df: pd.DataFrame, subj_col: str):
        """Make subj_id column for db_emorep."""
        df["subj_id"] = df[subj_col].str[2:].astype(int)
        return df

    def convert_wide_long(
        self, df: pd.DataFrame, sur_name: str, item_type: object = int
    ) -> pd.DataFrame:
        """Return long-formatted dataframe."""
        df["id"] = df.index
        df_long = pd.wide_to_long(
            df,
            stubnames=f"{sur_name}",
            sep="_",
            suffix=".*",
            i=["subj_id", "sess_id"],
            j="item",
        ).reset_index()
        df_long = df_long.drop(["id"], axis=1)
        df_long["item"] = df_long["item"].astype(item_type)
        df_long = df_long.rename(columns={sur_name: "resp"})
        df_long["resp"] = df_long["resp"].astype(int)
        return df_long


class _DbUpdateRecipes:
    """SQL recipes for updating db_emorep tables.

    Insert commands hardcoded for each type of table.

    """

    def __init__(self, db_con: Type[DbConnect]):
        """Initialize."""
        self._db_con = db_con

    def update_ref_subj(
        self,
        df: pd.DataFrame,
        subj_col: str = "study_id",
    ):
        """Update mysql db_emorep.ref_subj."""
        print("\tUpdating db_emorep.ref_subj ...")
        tbl_input = list(
            df[["subj_id", subj_col]].itertuples(index=False, name=None)
        )
        self._db_con.exec_many(
            "insert ignore into ref_subj (subj_id, subj_name) values (%s, %s)",
            tbl_input,
        )

    def update_survey_date(
        self,
        df: pd.DataFrame,
        sur_low: str,
        date_col: str = "datetime",
    ):
        """Update mysql db_emorep.tbl_survey_date."""
        print(f"\tUpdating db_emorep.tbl_survey_date for {sur_low} ...")

        # Prep df, manage NAN
        df = df.copy()
        df["sur_name"] = sur_low
        df = df.where(pd.notnull(df), None)

        # Update table
        tbl_input = list(
            df[["subj_id", "sess_id", "sur_name", date_col]].itertuples(
                index=False, name=None
            )
        )
        self._db_con.exec_many(
            "insert ignore into tbl_survey_date "
            + "(subj_id, sess_id, sur_name, sur_date) "
            + "values (%s, %s, %s, %s)",
            tbl_input,
        )

    def update_basic_tbl(self, df: pd.DataFrame, sur_low: str):
        """Update mysql db_emorep for common (REDCap, Qualtrics) tables."""
        print(f"\tUpdating db_emorep.tbl_{sur_low} ...")
        tbl_input = list(
            df[["subj_id", "sess_id", "item", "resp"]].itertuples(
                index=False, name=None
            )
        )
        self._db_con.exec_many(
            f"insert ignore into tbl_{sur_low} "
            + f"(subj_id, sess_id, item_{sur_low}, resp_{sur_low}) "
            + "values (%s, %s, %s, %s)",
            tbl_input,
        )

    def _print_tbl_out(self, sur_low: str):
        """Print table update."""
        print(f"\tUpdating db_emorep.tbl_{sur_low} ...")

    def update_psr(self, df: pd.DataFrame, sur_low: str):
        """Update mysql db_emorep.tbl_post_scan_ratings."""
        self._print_tbl_out(sur_low)
        tbl_input = list(
            df[
                [
                    "subj_id",
                    "sess_id",
                    "task_id",
                    "emo_id",
                    "stim_name",
                    "resp_arousal",
                    "resp_endorse",
                    "resp_valence",
                ]
            ].itertuples(index=False, name=None)
        )
        self._db_con.exec_many(
            f"insert ignore into tbl_{sur_low} "
            + "(subj_id, sess_id, task_id, emo_id, stim_name,"
            + " resp_arousal, resp_endorse, resp_valence)"
            + " values (%s, %s, %s, %s, %s, %s, %s, %s)",
            tbl_input,
        )

    def update_rest_ratings(self, df: pd.DataFrame, sur_low: str):
        """Update mysql db_emorep.tbl_rest_ratings."""
        self._print_tbl_out(sur_low)
        tbl_input = list(
            df[
                [
                    "subj_id",
                    "sess_id",
                    "task_id",
                    "emo_id",
                    "resp_int",
                    "resp_alpha",
                ]
            ].itertuples(index=False, name=None)
        )
        self._db_con.exec_many(
            f"insert ignore into tbl_{sur_low} "
            + "(subj_id, sess_id, task_id, emo_id, resp_int, resp_alpha) "
            + "values (%s, %s, %s, %s, %s, %s)",
            tbl_input,
        )

    def update_in_scan_ratings(self, df: pd.DataFrame, sur_low: str):
        """Update mysql db_emorep.tbl_in_scan_ratings."""
        self._print_tbl_out(sur_low)
        tbl_input = list(
            df[
                [
                    "subj_id",
                    "sess_id",
                    "task_id",
                    "run",
                    "block_id",
                    "resp_emo_id",
                    "resp_intensity",
                ]
            ].itertuples(index=False, name=None)
        )
        self._db_con.exec_many(
            f"insert ignore into tbl_{sur_low} "
            + "(subj_id, sess_id, task_id, run, block_id, "
            + "resp_emo_id, resp_intensity) "
            + "values (%s, %s, %s, %s, %s, %s, %s)",
            tbl_input,
        )


# %%
class _TaskMaps:
    """Supply mappings to SQL reference table values."""

    @property
    def emo_map(self):
        return {
            "amusement": 1,
            "anger": 2,
            "anxiety": 3,
            "awe": 4,
            "calmness": 5,
            "craving": 6,
            "disgust": 7,
            "excitement": 8,
            "fear": 9,
            "horror": 10,
            "joy": 11,
            "neutral": 12,
            "romance": 13,
            "sadness": 14,
            "surprise": 15,
        }

    @property
    def task_map(self):
        return {
            "movies": 1,
            "scenarios": 2,
        }

    def task_label(self, row, row_name) -> int:
        """Return task ID given task name."""
        for task_name, task_id in self.task_map.items():
            if row[row_name] == task_name:
                return task_id

    def emo_label(self, row, row_name) -> int:
        """Return emotion ID given emotion name."""
        for emo_name, emo_id in self.emo_map.items():
            if row[row_name] == emo_name:
                return emo_id


class _PrepPsr(_TaskMaps):
    """Make df_tidy, df_date for post_scan_ratings."""

    def __init__(
        self, df: pd.DataFrame, sess_id: int, subj_col: str = "study_id"
    ):
        """Initialize."""
        self._df = df
        self._sess_id = sess_id
        self._subj_col = subj_col

    def prep_dfs(self):
        """Make attrs df_tidy, df_date."""
        self._prep_tidy()
        self._prep_date()

    def _prep_tidy(self):
        """Convert long to tidy format, make attr df_tidy."""
        # Convert for sql compat
        self._df["type"] = self._df["type"].str.lower()
        self._df["prompt"] = self._df["prompt"].str.lower()
        self._df["task_id"] = self._df.apply(
            lambda x: self.task_label(x, "type"), axis=1
        )
        self._df["emo_id"] = self._df.apply(
            lambda x: self.emo_label(x, "emotion"), axis=1
        )

        # Make tidy format
        self.df_tidy = self._df.pivot(
            index=[
                "study_id",
                "subj_id",
                "sess_id",
                "datetime",
                "session",
                "type",
                "task_id",
                "emotion",
                "emo_id",
                "stimulus",
            ],
            columns=[
                "prompt",
            ],
            values="response",
        ).reset_index()
        self.df_tidy = self.df_tidy.rename(
            columns={
                "stimulus": "stim_name",
                "arousal": "resp_arousal",
                "endorsement": "resp_endorse",
                "valence": "resp_valence",
            }
        )

        # Manage col types
        char_list = ["stim_name", "resp_endorse"]
        self.df_tidy[char_list] = self.df_tidy[char_list].astype(str)
        int_list = [
            "subj_id",
            "sess_id",
            "task_id",
            "emo_id",
            "resp_arousal",
            "resp_valence",
        ]
        self.df_tidy[int_list] = self.df_tidy[int_list].astype(int)

    def _prep_date(self):
        """Make attr df_date for db_emorep.tbl_survey_date."""
        idx = list(np.unique(self.df_tidy["subj_id"], return_index=True)[1])
        self.df_date = self.df_tidy.loc[
            idx, ["subj_id", "sess_id", "datetime"]
        ]


# %%
class MysqlUpdate(_DbUpdateRecipes, _DfManip, _TaskMaps):
    """Update mysql db_emorep tables.

    Inherits _DbUpdateRecipes, _DfManip, _TaskMaps.

    Methods
    -------
    update_db(*args)
        Update appropriate table given args

    Example
    -------
    db_con = sql_database.DbConnect()
    up_mysql = sql_database.MysqlUpdate(db_con)
    up_mysql.update_db(*args)

    """

    def __init__(self, db_con: Type[DbConnect]):
        """Initialize."""
        super().__init__(db_con)

    def update_db(
        self, df, sur_name, sess_id, data_source, subj_col="study_id"
    ):
        """Identify and update appropriate mysql db_emorep table.

        Coordinates internal methods to wrap _DbUpdateRecipes with
        appropriate input.

        Parameters
        ----------
        df : pd.DataFrame
            Survey data
        sur_name : str
            Name of survey
        sess_id : int
            Session ID
        data_source : str
            {"qualtrics", "redcap", "rest_ratings", "in_scan_ratings"}
            Survey or task source of data
        subj_col : str, optional
            Name of column holding subject identifiers

        """
        # Check input parameters
        if not isinstance(sess_id, int):
            raise TypeError("Expected type int for sess_id parameter")
        if data_source not in [
            "qualtrics",
            "redcap",
            "rest_ratings",
            "in_scan_ratings",
        ]:
            raise ValueError("Unexpected data_source parameter")
        if subj_col not in df.columns:
            raise KeyError(f"Column '{subj_col}' not found in df")

        # Setup
        self._df = df
        self._sur_name = sur_name
        self._sur_low = sur_name.lower()
        self._sess_id = sess_id
        self._subj_col = subj_col
        self._basic_prep()

        # Find appropriate method and run
        up_meth = getattr(self, f"_update_{data_source}")
        up_meth()

    def _basic_prep(self):
        """Add subj_id and sess_id to df."""
        self._df["sess_id"] = self._sess_id
        self._df = self.subj_col(self._df, self._subj_col)

    def _update_qualtrics(self):
        """Update db_emorep tables with qualtrics data."""
        # Treat post_scan_ratings specifically
        if self._sur_name == "post_scan_ratings":
            prep_psr = _PrepPsr(self._df.copy(), self._sess_id)
            prep_psr.prep_dfs()
            self.update_psr(prep_psr.df_tidy, self._sur_low)
            self.update_survey_date(prep_psr.df_date, self._sur_low)
            return

        # Update db_emorep.tbl_survey_date and other table
        self.update_survey_date(self._df, self._sur_low)
        df_long = self.convert_wide_long(self._df, self._sur_name)
        self.update_basic_tbl(df_long, self._sur_low)

    def _update_redcap(self):
        """Update db_emorep tables with redcap data."""
        self.update_survey_date(self._df, self._sur_low)
        df_long = self.convert_wide_long(
            self._df, self._sur_name, item_type=str
        )
        self.update_basic_tbl(df_long, self._sur_low)

    def _update_rest_ratings(self):
        """Update db_emorep.tbl_rest_ratings and tbl_survey_date."""
        # Update db_emorep.tbl_survey_date
        df_date = self._df.loc[self._df["resp_type"] == "resp_int"]
        self.update_survey_date(df_date, self._sur_low)
        del df_date

        # Prep certain cols for tbl_rest_ratings
        self._df["task_id"] = self._df.apply(
            lambda x: self.task_label(x, "task"), axis=1
        )
        df = self._df.drop(
            [self._subj_col, "visit", "datetime", "task"], axis=1
        )
        for col_name in df.columns:
            if col_name.lower() in self.emo_map.keys():
                df = df.rename(columns={col_name: f"rsp_{col_name.lower()}"})

        # Format into tidy format
        df["id"] = df.index
        df_long = pd.wide_to_long(
            df,
            stubnames="rsp",
            sep="_",
            suffix=".*",
            i="id",
            j="emo_name",
        ).reset_index()
        df_long.drop(["id"], axis=1, inplace=True)
        df_tidy = df_long.pivot(
            index=["emo_name", "task_id", "sess_id", "subj_id"],
            columns=["resp_type"],
            values="rsp",
        ).reset_index()
        del df_long

        # Finish formatting for tbl_rest_ratings, update
        df_tidy["emo_id"] = df_tidy.apply(
            lambda x: self.emo_label(x, "emo_name"), axis=1
        )
        int_list = ["subj_id", "sess_id", "task_id", "emo_id", "resp_int"]
        for col_name in int_list:
            df_tidy[col_name] = df_tidy[col_name].astype(int)
        df_tidy["resp_alpha"] = df_tidy["resp_alpha"].astype(str)
        self.update_rest_ratings(df_tidy, self._sur_low)

    def _update_in_scan_ratings(self):
        """Update db_emorep.tbl_in_scan_ratings.

        db_emorep.tbl_survey_dates not updated due to BIDS
        format of events files.

        """
        # Add id columns
        df = self._df.copy()
        df["task_id"] = df.apply(lambda x: self.task_label(x, "task"), axis=1)
        df["block_id"] = df.apply(lambda x: self.emo_label(x, "block"), axis=1)
        df["resp_emo_id"] = df.apply(
            lambda x: self.emo_label(x, "resp_emotion"), axis=1
        )

        # Manage column values and types
        df["resp_emo_id"] = df["resp_emo_id"].replace(np.nan, "")
        df["resp_intensity"] = df["resp_intensity"].astype(str)
        df["resp_intensity"] = df["resp_intensity"].replace("<NA>", "")
        self.update_in_scan_ratings(df, self._sur_low)


# %%
