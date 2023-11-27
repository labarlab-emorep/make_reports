"""Title.

"""
# %%
import os
import pandas as pd
import mysql.connector
from contextlib import contextmanager


# %%
class DbConnect:
    """Title."""

    def __init__(self, sql_pass: str):
        """Set db_con attr as mysql connection."""
        self.db_con = mysql.connector.connect(
            host="localhost",
            user=os.environ["USER"],
            password=sql_pass,
            database="db_emorep",
        )

    @contextmanager
    def connect(self):
        """Yield cursor."""
        db_cursor = self.db_con.cursor()
        try:
            yield db_cursor
        finally:
            db_cursor.closedf = _subj_col(df, subj_col)

    df["sess_id"] = sess_id()

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
    df_long["item"] = df_long["item"].astype(str)
    df_long["item"] = f"{sur_name}_" + df_long["item"]
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


class UpdateQualtrics:
    """Title."""

    def __init__(self, db_con, df, sur_name, sess_id, subj_col="study_id"):
        self._db_con = db_con
        self._df = df
        self._sur_name = sur_name
        self._sess_id = sess_id
        self._subj_col = subj_col

    def db_update(self):
        """Title."""
        self._df = _subj_col(self._df, self._subj_col)
        self._df["sess_id"] = self._sess_id

        if self._sur_name == "post_scan_ratings":
            self._update_psr()
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

    def _update_psr(self):
        """Title."""
        emo_map = _emo_map()
        task_map = _task_map()


def update_qualtrics(db_con, df, sur_name, sess_id, subj_col="study_id"):
    """Title."""
    #
    df = _subj_col(df, subj_col)
    df["sess_id"] = sess_id
    _update_survey_date(db_con, df, sur_name.lower())

    #
    if sur_name == "post_scan_ratings":
        df = _convert_post_scan()
    else:
        df = _convert_wide_long(df, sur_name)

    #
    tbl_input = list(
        df[["subj_id", "sess_id", "item", "resp"]].itertuples(
            index=False, name=None
        )
    )
    db_con.exec_many(
        f"insert ignore into tbl_{sur_name.lower()} "
        + "(subj_id, sess_id, item, resp) "
        + "values (%s, %s, %s, %s)",
        tbl_input,
    )


# udpate tbl_*
