"""Title.

DbConnect :
update_qualtrics :
update_redcap :

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

        #
        df = df.copy()
        df["sur_name"] = sur_low
        df = df.where(pd.notnull(df), None)

        #
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

    def update_rest_ratings(self, df, sur_low):
        """Title."""
        #
        print(f"\tUpdating db_emorep.tbl_{sur_low} ...")
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


# %%
class _TaskMaps:
    """Title."""

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

    def task_label(self, row, row_name):
        """Title."""
        for task_name, task_id in self.task_map.items():
            if row[row_name] == task_name:
                return task_id

    def emo_label(self, row, row_name):
        """Title."""
        for emo_name, emo_id in self.emo_map.items():
            if row[row_name] == emo_name:
                return emo_id


class _PrepPsr(_TaskMaps):
    """Title."""

    def __init__(self, df, sess_id, subj_col="study_id"):
        self._df = df.copy()
        self._sess_id = sess_id
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
        self._df["task_id"] = self._df.apply(
            lambda x: self.task_label(x, "type"), axis=1
        )
        self._df["emo_id"] = self._df.apply(
            lambda x: self.emo_label(x, "emotion"), axis=1
        )

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


# %%
class MysqlUpdate(_DbUpdateRecipes, _DfManip, _TaskMaps):
    """Title."""

    def __init__(self, db_con):
        super().__init__(db_con)

    def update_db(
        self, df, sur_name, sess_id, data_source, subj_col="study_id"
    ):
        """Title."""
        if not isinstance(sess_id, int):
            raise TypeError("Expected type int for sess_id parameter")
        if data_source not in ["qualtrics", "redcap", "rest_ratings"]:
            raise ValueError("Unexpected data_source parameter")
        if subj_col not in df.columns:
            raise KeyError(f"Column '{subj_col}' not found in df")

        self._df = df
        self._sur_name = sur_name
        self._sur_low = sur_name.lower()
        self._sess_id = sess_id
        self._subj_col = subj_col
        self._basic_prep()

        #
        up_meth = getattr(self, f"_update_{data_source}")
        up_meth()

    def _basic_prep(self):
        """Title."""
        self._df = self._df.copy()
        self._df["sess_id"] = self._sess_id
        self._df = self.subj_col(self._df, self._subj_col)

    def _update_qualtrics(self):
        """Title."""
        #
        if self._sur_name == "post_scan_ratings":
            prep_psr = _PrepPsr(self._df, self._sess_id)
            prep_psr.prep_dfs()
            self.update_psr(prep_psr.df_tidy, self._sur_low)
            self.update_survey_date(prep_psr.df_date, self._sur_low)
            return

        #
        self.update_survey_date(self._df.copy(), self._sur_low)
        df_long = self.convert_wide_long(self._df, self._sur_name)
        self.update_basic_tbl(df_long, self._sur_low)

    def _update_redcap(self):
        """Title."""
        self.update_survey_date(self._df.copy(), self._sur_low)
        df_long = self.convert_wide_long(
            self._df, self._sur_name, item_type=str
        )
        self.update_basic_tbl(df_long, self._sur_low)

    def _update_rest_ratings(self):
        """Title."""
        #
        df_date = self._df.loc[self._df["resp_type"] == "resp_int"]
        self.update_survey_date(df_date, self._sur_low)
        del df_date

        #
        self._df["task_id"] = self._df.apply(
            lambda x: self.task_label(x, "task"), axis=1
        )
        df = self._df.drop(
            [self._subj_col, "visit", "datetime", "task"], axis=1
        )
        for col_name in df.columns:
            if col_name.lower() in self.emo_map.keys():
                df = df.rename(columns={col_name: f"rsp_{col_name.lower()}"})

        #
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

        #
        df_tidy["emo_id"] = df_tidy.apply(
            lambda x: self.emo_label(x, "emo_name"), axis=1
        )
        int_list = ["subj_id", "sess_id", "task_id", "emo_id", "resp_int"]
        for col_name in int_list:
            df_tidy[col_name] = df_tidy[col_name].astype(int)
        df_tidy["resp_alpha"] = df_tidy["resp_alpha"].astype(str)
        self.update_rest_ratings(df_tidy, self._sur_low)


class UpdateTask:
    """Title."""

    def __init__(self):
        pass


# %%
