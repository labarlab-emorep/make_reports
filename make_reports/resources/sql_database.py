"""Title.

DbConnect :
UpdateQualtrics :

"""
# %%
import os
import pandas as pd
import numpy as np
import mysql.connector
from contextlib import contextmanager


# %%
class DbConnect:
    """Title."""

    def __init__(self):
        """Set db_con attr as mysql connection."""
        #
        try:
            os.environ["PAS_SQL"]
        except KeyError as e:
            raise Exception(
                "No global variable 'PAS_SQL' defined in user env"
            ) from e

        #
        self.db_con = mysql.connector.connect(
            host="localhost",
            user=os.environ["USER"],
            password=os.environ["PAS_SQL"],
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
        """Update db via executemany."""
        with self.connect() as con:
            con.executemany(sql_cmd, value_list)
            self.db_con.commit()


# %%
class _DfManip:
    def subj_col(self, df, subj_col):
        """Title."""
        df["subj_id"] = df[subj_col].str[2:].astype(int)
        return df

    def convert_wide_long(self, df, sur_name, item_type=int):
        """Title."""
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
    """Title."""

    def __init__(self, db_con):
        self._db_con = db_con

    def update_ref_subj(
        self,
        df: pd.DataFrame,
        subj_col: str = "study_id",
    ):
        """Update mysql db_emorep.ref_subj."""
        #
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

    def update_basic_tbl(self, df, sur_low):
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

    def update_psr(self, df, sur_low):
        #
        print(f"\tUpdating db_emorep.tbl_{sur_low} ...")
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


# %%
def _emo_map():
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


def _task_map():
    return {
        "movies": 1,
        "scenarios": 2,
    }


class _PrepPsr:
    """Title."""

    def __init__(self, df, sess_id, subj_col="study_id"):
        self._df = df.copy()
        self._sess_id = sess_id
        self._sur_name = "post_scan_ratings"
        self._subj_col = subj_col

    def prep_dfs(self):
        """Make attrs df_tidy, df_date."""
        self._prep_tidy()
        self._prep_date()

    def _prep_tidy(self):
        """Title."""
        # convert for sql compat
        self._df["type"] = self._df["type"].str.lower()
        self._df["prompt"] = self._df["prompt"].str.lower()
        self._df["task_id"] = self._df.apply(self._task_label, axis=1)
        self._df["emo_id"] = self._df.apply(self._emo_label, axis=1)

        #
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

        #
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
        """Title."""
        idx = list(np.unique(self.df_tidy["subj_id"], return_index=True)[1])
        self.df_date = self.df_tidy.loc[
            idx, ["subj_id", "sess_id", "datetime"]
        ]
        self.df_date["sur_name"] = self._sur_name

    def _task_label(self, row):
        """Title."""
        task_map = _task_map()
        for task_name, task_id in task_map.items():
            if row["type"] == task_name:
                return task_id

    def _emo_label(self, row):
        """Title."""
        emo_map = _emo_map()
        for emo_name, emo_id in emo_map.items():
            if row["emotion"] == emo_name:
                return emo_id


def update_qualtrics(db_con, df, sur_name, sess_id, subj_col="study_id"):
    """Title."""
    #
    sur_low = sur_name.lower()
    df = df.copy()
    df["sess_id"] = sess_id

    #
    rsc_up = _DbUpdateRecipes(db_con)
    rsc_df = _DfManip()
    df = rsc_df.subj_col(df, subj_col)

    #
    if sur_name == "post_scan_ratings":
        prep_psr = _PrepPsr(df, sess_id)
        prep_psr.prep_dfs()
        rsc_up.update_psr(prep_psr.df_tidy, sur_low)
        rsc_up.update_survey_date(prep_psr.df_date, sur_low)
        return

    #
    df_date = df.copy()
    df_date["sur_name"] = sur_low
    rsc_up.update_survey_date(df_date, sur_low)
    del df_date

    #
    df_long = rsc_df.convert_wide_long(df, sur_name)
    rsc_up.update_basic_tbl(df_long, sur_low)


# %%
def update_redcap(db_con, df, sur_name, sess_id, subj_col="study_id"):
    """Title."""
    #
    sur_low = sur_name.lower()
    df = df.copy()
    df["sess_id"] = sess_id

    #
    rsc_df = _DfManip()
    rsc_up = _DbUpdateRecipes(db_con)

    #
    df = rsc_df.subj_col(df, subj_col)
    df = df.where(pd.notnull(df), None)
    df_date = df.copy()
    df_date["sur_name"] = sur_low
    rsc_up.update_survey_date(df_date, sur_low)
    del df_date

    #
    df_long = rsc_df.convert_wide_long(df, sur_name, item_type=str)
    rsc_up.update_basic_tbl(df_long, sur_low)


class UpdateRest:
    """Title."""

    def __init__(self):
        pass


class UpdateTask:
    """Title."""

    def __init__(self):
        pass


# %%
