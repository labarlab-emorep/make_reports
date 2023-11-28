"""Title.

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
def _subj_col(df: pd.DataFrame, subj_col: str) -> pd.DataFrame:
    """Return df containing db_emorep subj_id col."""
    df["subj_id"] = df[subj_col].str[2:].astype(int)
    return df


# %%
def update_ref_subj(
    db_con: DbConnect,
    df: pd.DataFrame,
    subj_col: str = "study_id",
):
    """Update mysql db_emorep.ref_subj."""
    df = _subj_col(df, subj_col)
    tbl_input = list(
        df[["subj_id", subj_col]].itertuples(index=False, name=None)
    )
    db_con.exec_many(
        "insert ignore into ref_subj (subj_id, subj_name) values (%s, %s)",
        tbl_input,
    )


# %%
def _update_survey_date(
    db_con: DbConnect,
    df: pd.DataFrame,
    sur_name: str,
    date_col: str = "datetime",
):
    """Update mysql db_emorep.tbl_survey_date."""
    df["sur_name"] = sur_name.lower()
    tbl_input = list(
        df[["subj_id", "sess_id", "sur_name", date_col]].itertuples(
            index=False, name=None
        )
    )
    db_con.exec_many(
        "insert ignore into tbl_survey_date "
        + "(subj_id, sess_id, sur_name, sur_date) "
        + "values (%s, %s, %s, %s)",
        tbl_input,
    )


# %%
def _convert_wide_long(df, sur_name):
    """Title."""
    df["id"] = df.index
    df_long = pd.wide_to_long(
        df,
        stubnames=f"{sur_name}",
        sep="_",
        i=["subj_id", "sess_id"],
        j="item",
    ).reset_index()
    df_long = df_long.drop(["id"], axis=1)
    df_long["item"] = df_long["item"].astype(int)
    df_long = df_long.rename(columns={sur_name: "resp"})
    df_long["resp"] = df_long["resp"].astype(int)
    return df_long


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


class _UpdatePsr:
    """Title."""

    def __init__(self, db_con, df, sess_id, subj_col="study_id"):
        self._db_con = db_con
        self._df = df
        self._sess_id = sess_id
        self._sur_name = "post_scan_ratings"
        self._subj_col = subj_col

    def _update_psr(self):
        """Title."""
        self._prep_tidy()
        self._prep_date()
        _update_survey_date(self._db_con, self._df_date, self._sur_name)

        #
        tbl_input = list(
            self._df_tidy[
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
            f"insert ignore into tbl_{self._sur_name.lower()} "
            + "(subj_id, sess_id, task_id, emo_id, stim_name,"
            + " resp_arousal, resp_endorse, resp_valence)"
            + " values (%s, %s, %s, %s, %s, %s, %s, %s)",
            tbl_input,
        )

    def _prep_tidy(self):
        """Title."""
        # convert for sql compat
        self._df["type"] = self._df["type"].str.lower()
        self._df["prompt"] = self._df["prompt"].str.lower()
        self._df["task_id"] = self._df.apply(self._task_label, axis=1)
        self._df["emo_id"] = self._df.apply(self._emo_label, axis=1)
        self._df_tidy = self._df.pivot(
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

        #
        self._df_tidy = self._df_tidy.rename(
            columns={
                "stimulus": "stim_name",
                "arousal": "resp_arousal",
                "endorsement": "resp_endorse",
                "valence": "resp_valence",
            }
        )
        char_list = ["stim_name", "resp_endorse"]
        self._df_tidy[char_list] = self._df_tidy[char_list].astype(str)
        int_list = [
            "subj_id",
            "sess_id",
            "task_id",
            "emo_id",
            "resp_arousal",
            "resp_valence",
        ]
        self._df_tidy[int_list] = self._df_tidy[int_list].astype(int)

    def _prep_date(self):
        """Title."""
        idx = list(np.unique(self._df_tidy["subj_id"], return_index=True)[1])
        self._df_date = self._df_tidy.loc[
            idx, ["subj_id", "sess_id", "datetime"]
        ]

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


class UpdateQualtrics:
    """Title."""

    def __init__(self, db_con, df, sur_name, sess_id, subj_col="study_id"):
        self._db_con = db_con
        self._df = df
        self._sess_id = sess_id
        self._sur_name = sur_name
        self._subj_col = subj_col

    def db_update(self):
        """Title."""
        self._df = _subj_col(self._df, self._subj_col)
        self._df["sess_id"] = self._sess_id

        if self._sur_name == "post_scan_ratings":
            up_psr = _UpdatePsr(self._db_con, self._df, self._sess_id)
            up_psr._update_psr()
        else:
            self._update_reg()

    def _update_reg(self):
        _update_survey_date(self._db_con, self._df, self._sur_name.lower())
        self._df_long = _convert_wide_long(self._df, self._sur_name)
        tbl_input = list(
            self._df_long[["subj_id", "sess_id", "item", "resp"]].itertuples(
                index=False, name=None
            )
        )
        self._db_con.exec_many(
            f"insert ignore into tbl_{self._sur_name.lower()} "
            + "(subj_id, sess_id, item, resp) "
            + "values (%s, %s, %s, %s)",
            tbl_input,
        )
