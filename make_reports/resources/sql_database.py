"""Methods for interacting with mysql db_emorep.

DbConnect : connect to and interact with mysql server
DbUpdate : update db_emorep tables

"""

# %%
import os
import pandas as pd
import numpy as np
from typing import Type
import mysql.connector
from contextlib import contextmanager
from make_reports.resources import report_helper


# %%
class DbConnect:
    """Connect to mysql server and update db_emorep.

    Parameters
    ----------
    db_name : str, optional
        {"db_emorep", "db_emorep_unittest"}
        Name of MySQL database

    Attributes
    ----------
    con : mysql.connector.connection_cext.CMySQLConnection
        Connection object to database

    Methods
    -------
    close_con()
        Close database connection
    connect()
        Yield cursor
    exec_many()
        Update mysql db_emorep.tbl_* with multiple values
    fetch_rows()
        Return fetchall rows

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
    subj_rows = db_con.fetch_rows("select * from ref_subj")
    db_con.close_con()

    """

    def __init__(self, db_name="db_emorep"):
        """Set db_con attr as mysql connection."""
        report_helper.check_sql_pass()
        if db_name not in ["db_emorep", "db_emorep_unittest"]:
            raise ValueError("Unexpected db_name")
        self.con = mysql.connector.connect(
            host="localhost",
            user=os.environ["USER"],
            password=os.environ["SQL_PASS"],
            database=db_name,
        )

    @contextmanager
    def _con_cursor(self):
        """Yield cursor."""
        db_cursor = self.con.cursor()
        try:
            yield db_cursor
        finally:
            db_cursor.close()

    def exec_many(self, sql_cmd: str, value_list: list):
        """Update db_emorep via executemany."""
        with self._con_cursor() as cur:
            cur.executemany(sql_cmd, value_list)
            self.con.commit()

    def fetch_rows(self, sql_cmd: str) -> list:
        """Return rows from query output."""
        with self._con_cursor() as cur:
            cur.execute(sql_cmd)
            rows = cur.fetchall()
        return rows

    def close_con(self):
        """Close database connection."""
        self.con.close()


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


class _Recipes:
    """SQL recipes for updating db_emorep tables.

    Insert commands hardcoded for each type of table.

    """

    def __init__(self, db_con: Type[DbConnect]):
        """Initialize."""
        self._db_con = db_con

    def insert_ref_subj(
        self,
        df: pd.DataFrame,
        subj_col: str = "study_id",
    ):
        """Update mysql db_emorep.ref_subj."""
        print("\tUpdating db_emorep.ref_subj ...")
        col_list = ["subj_id", subj_col]
        self._validate_cols(df, col_list)
        tbl_input = list(df[col_list].itertuples(index=False, name=None))
        self._db_con.exec_many(
            "insert ignore into ref_subj (subj_id, subj_name) values (%s, %s)",
            tbl_input,
        )

    def _validate_cols(self, df: pd.DataFrame, col_list: list):
        """Raise error is missing column."""
        for col_name in col_list:
            if col_name not in df.columns:
                raise KeyError(
                    f"Expected col '{col_name}' in df, found : {df.columns}"
                )

    def insert_survey_date(
        self,
        df: pd.DataFrame,
        sur_low: str,
        date_col: str = "datetime",
    ):
        """Update mysql db_emorep.tbl_survey_date."""
        print(f"\tUpdating db_emorep.tbl_survey_date for {sur_low} ...")

        # Validate df
        col_list = ["subj_id", "sess_id", date_col]
        self._validate_cols(df, col_list)

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

    def insert_basic_tbl(self, df: pd.DataFrame, sur_low: str):
        """Update mysql db_emorep for common (REDCap, Qualtrics) tables."""
        print(f"\tUpdating db_emorep.tbl_{sur_low} ...")
        col_list = ["subj_id", "sess_id", "item", "resp"]
        self._validate_cols(df, col_list)
        tbl_input = list(df[col_list].itertuples(index=False, name=None))
        self._db_con.exec_many(
            f"insert ignore into tbl_{sur_low} "
            + f"(subj_id, sess_id, item_{sur_low}, resp_{sur_low}) "
            + "values (%s, %s, %s, %s)",
            tbl_input,
        )

    def _print_tbl_out(self, sur_low: str):
        """Print table update."""
        print(f"\tUpdating db_emorep.tbl_{sur_low} ...")

    def insert_demographics(self, df: pd.DataFrame):
        """Update db_emorep.tbl_demographics."""
        self._print_tbl_out("demographics")
        col_list = [
            "subj_id",
            "sess_id",
            "age",
            "interview_age",
            "years_education",
            "sex",
            "handedness",
            "race",
            "is_hispanic",
            "is_minority",
        ]
        self._validate_cols(df, col_list)
        tbl_input = list(df[col_list].itertuples(index=False, name=None))
        self._db_con.exec_many(
            "insert ignore into tbl_demographics "
            + "(subj_id, sess_id, age_yrs, age_mos, edu_yrs, "
            + "sex, hand, race, is_hispanic, is_minority) "
            + "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            tbl_input,
        )

    def _build_cols(self, col_list):
        return "(" + ", ".join(col_list) + ")"

    def insert_psr(self, df: pd.DataFrame, sur_low: str):
        """Update mysql db_emorep.tbl_post_scan_ratings."""
        self._print_tbl_out(sur_low)
        col_list = [
            "subj_id",
            "sess_id",
            "task_id",
            "emo_id",
            "stim_name",
            "resp_arousal",
            "resp_endorse",
            "resp_valence",
        ]
        self._validate_cols(df, col_list)
        tbl_input = list(df[col_list].itertuples(index=False, name=None))
        self._db_con.exec_many(
            f"insert ignore into tbl_{sur_low} {self._build_cols(col_list)}"
            + " values (%s, %s, %s, %s, %s, %s, %s, %s)",
            tbl_input,
        )

    def insert_rest_ratings(self, df: pd.DataFrame, sur_low: str):
        """Update mysql db_emorep.tbl_rest_ratings."""
        self._print_tbl_out(sur_low)
        col_list = [
            "subj_id",
            "sess_id",
            "task_id",
            "emo_id",
            "resp_int",
            "resp_alpha",
        ]
        self._validate_cols(df, col_list)
        tbl_input = list(df[col_list].itertuples(index=False, name=None))
        self._db_con.exec_many(
            f"insert ignore into tbl_{sur_low} {self._build_cols(col_list)}"
            + " values (%s, %s, %s, %s, %s, %s)",
            tbl_input,
        )

    def insert_in_scan_ratings(self, df: pd.DataFrame, sur_low: str):
        """Update mysql db_emorep.tbl_in_scan_ratings."""
        self._print_tbl_out(sur_low)
        col_list = [
            "subj_id",
            "sess_id",
            "task_id",
            "run",
            "block_id",
            "resp_emo_id",
            "resp_intensity",
        ]
        self._validate_cols(df, col_list)
        tbl_input = list(df[col_list].itertuples(index=False, name=None))
        self._db_con.exec_many(
            f"insert ignore into tbl_{sur_low} {self._build_cols(col_list)}"
            + " values (%s, %s, %s, %s, %s, %s, %s)",
            tbl_input,
        )

    def insert_ref_sess_task(self, df: pd.DataFrame):
        """Update mysql db_emorep.ref_sess_task."""
        col_list = ["subj_id", "sess_id", "task_id"]
        self._validate_cols(df, col_list)
        tbl_input = list(df[col_list].itertuples(index=False, name=None))
        self._db_con.exec_many(
            f"insert ignore into ref_sess_task {self._build_cols(col_list)}"
            + " values (%s, %s, %s)",
            tbl_input,
        )


# %%
class _TaskMaps:
    """Supply mappings to SQL reference table values."""

    def __init__(self, db_con: Type[DbConnect]):
        """Initialize."""
        self._db_con = db_con
        self._load_refs()

    def _load_refs(self):
        """Supply mappings in format {name: id}."""
        self._ref_task = {
            x[1]: x[0]
            for x in self._db_con.fetch_rows("select * from ref_task")
        }
        self._ref_emo = {
            x[1]: x[0]
            for x in self._db_con.fetch_rows("select * from ref_emo")
        }

    def task_label(self, row, row_name) -> int:
        """Return task ID given task name."""
        for task_name, task_id in self._ref_task.items():
            if row[row_name] == task_name:
                return task_id

    def emo_label(self, row, row_name) -> int:
        """Return emotion ID given emotion name."""
        for emo_name, emo_id in self._ref_emo.items():
            if row[row_name] == emo_name:
                return emo_id


class _PrepPsr(_TaskMaps):
    """Make df_tidy, df_date for post_scan_ratings."""

    def __init__(
        self,
        df: pd.DataFrame,
        sess_id: int,
        db_con: Type[DbConnect],
        subj_col: str = "study_id",
    ):
        """Initialize."""
        self._df = df
        self._sess_id = sess_id
        self._subj_col = subj_col
        super().__init__(db_con)

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
class DbUpdate(_Recipes, _DfManip, _TaskMaps):
    """Update mysql db_emorep tables.

    Format dataframes to match db_emorep tables and then coordinate inserts.

    Inherits _Recipes, _DfManip, _TaskMaps.

    Methods
    -------
    update_db(*args)
        Update appropriate table given args
    close_db()
        Close connection with mysql server

    Example
    -------
    up_db_emorep = sql_database.DbUpdate()
    up_db_emorep.update_db(*args)
    up_db_emorep.close_db()

    """

    def __init__(self):
        """Initialize."""
        self._db_con = DbConnect()
        _Recipes.__init__(self, self._db_con)
        _TaskMaps.__init__(self, self._db_con)

    def update_db(
        self, df, sur_name, sess_id, data_source, subj_col="study_id"
    ):
        """Identify and update appropriate mysql db_emorep table.

        Coordinates internal methods to wrap _Recipes with
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
            {
                "qualtrics",
                "redcap",
                "rest_ratings",
                "in_scan_ratings",
                "demographics"
            }
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
            "demographics",
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

    def close_db(self):
        """Close database connection."""
        self._db_con.close_con()

    def _basic_prep(self):
        """Add subj_id and sess_id to df."""
        self._df["sess_id"] = self._sess_id
        self._df = self.subj_col(self._df, self._subj_col)

    def _update_qualtrics(self):
        """Update db_emorep tables with qualtrics data."""
        # Treat post_scan_ratings specifically
        if self._sur_name == "post_scan_ratings":
            prep_psr = _PrepPsr(self._df.copy(), self._sess_id, self._db_con)
            prep_psr.prep_dfs()
            self.insert_psr(prep_psr.df_tidy, self._sur_low)
            self.insert_survey_date(prep_psr.df_date, self._sur_low)
            return

        # Update db_emorep.tbl_survey_date and other table
        self.insert_survey_date(self._df, self._sur_low)
        df_long = self.convert_wide_long(self._df, self._sur_name)
        self.insert_basic_tbl(df_long, self._sur_low)

    def _update_redcap(self):
        """Update db_emorep tables with redcap data."""
        self.insert_survey_date(self._df, self._sur_low)
        df_long = self.convert_wide_long(
            self._df, self._sur_name, item_type=str
        )
        self.insert_basic_tbl(df_long, self._sur_low)

    def _update_demographics(self):
        """Update db_emorep.tbl_demographics."""
        # Manage casing, type, nan
        self._df["age"] = self._df["age"].astype(int)
        self._df["sex"] = self._df["sex"].str.lower()
        self._df["handedness"] = self._df["handedness"].replace(np.nan, "")

        # Manage is hispanic
        idx_hisp = self._df.index[
            self._df["ethnicity"] == "Hispanic or Latino"
        ].to_list()
        self._df["is_hispanic"] = "no"
        self._df.loc[idx_hisp, "is_hispanic"] = "yes"

        # Manage is minority
        self._df = self._df.rename(
            {"is_minority": "minority_status"}, axis="columns"
        )
        idx_minor = self._df.index[
            self._df["minority_status"] == "Minority"
        ].to_list()
        self._df["is_minority"] = "no"
        self._df.loc[idx_minor, "is_minority"] = "yes"
        self.insert_demographics(self._df)

    def _update_rest_ratings(self):
        """Update db_emorep.tbl_rest_ratings and tbl_survey_date."""
        # Update db_emorep.tbl_survey_date
        df_date = self._df.loc[self._df["resp_type"] == "resp_int"]
        self.insert_survey_date(df_date, self._sur_low)
        del df_date

        # Prep certain cols for tbl_rest_ratings
        self._df["task_id"] = self._df.apply(
            lambda x: self.task_label(x, "task"), axis=1
        )
        df = self._df.drop(
            [self._subj_col, "visit", "datetime", "task"], axis=1
        )
        for col_name in df.columns:
            if col_name.lower() in self._ref_emo.keys():
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
        self.insert_rest_ratings(df_tidy, self._sur_low)

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

        # Update reference and task tables
        self.insert_in_scan_ratings(df, self._sur_low)
        self.insert_ref_sess_task(
            df.drop_duplicates(subset=["subj_id", "sess_id", "task_id"]),
        )


# %%
