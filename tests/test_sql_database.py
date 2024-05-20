import pytest
import pandas as pd
from make_reports.resources import sql_database
import helper


def clean_test_db(db_con):
    # Clean for TestDbConnect
    with db_con._con_cursor() as cur:
        cur.execute("delete from ref_emo where emo_name like 'foo%'")
        db_con.con.commit()

    # Clean for _Recipes
    with db_con._con_cursor() as cur:
        cur.execute("delete from ref_subj where subj_name like 'FOO%'")
        db_con.con.commit()

    for tbl_name in [
        "tbl_survey_date",
        "tbl_als",
        "tbl_demographics",
        "tbl_post_scan_ratings",
        "tbl_rest_ratings",
        "tbl_in_scan_ratings",
        "ref_sess_task",
    ]:
        with db_con._con_cursor() as cur:
            cur.execute(f"delete from {tbl_name} where subj_id=99")
            db_con.con.commit()


@pytest.fixture(scope="module")
def fixt_database():
    db_con = sql_database.DbConnect(db_name="db_emorep_unittest")
    yield db_con
    clean_test_db(db_con)
    db_con.close_con()


def unpack_rows(rows: list) -> dict:
    return {x[0]: x[1] for x in rows}


@pytest.mark.rep_get
class TestDbConnect:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_database):
        self.db_con = fixt_database

    @pytest.mark.dependency()
    def test_con_cursor(self):
        with self.db_con._con_cursor() as cur:
            cur.execute("select * from ref_emo where emo_id=1")
            rows = cur.fetchall()
        assert (1, "amusement") == rows[0]

    @pytest.mark.dependency(depends=["TestDbConnect::test_con_cursor"])
    def test_fetch_rows(self):
        rows = self.db_con.fetch_rows("select * from ref_emo")
        assert isinstance(rows, list)
        assert isinstance(rows[0], tuple)

        emo_dict = unpack_rows(rows)
        assert emo_dict[3] == "anxiety"
        assert emo_dict[15] == "surprise"

    @pytest.mark.dependency(depends=["TestDbConnect::test_fetch_rows"])
    def test_exec_many(self):
        # Add records
        sql_cmd = (
            "insert ignore into ref_emo (emo_id, emo_name) values (%s, %s)"
        )
        value_list = [(16, "foo"), (17, "foobar")]
        self.db_con.exec_many(sql_cmd, value_list)

        # Pull data
        sql_cmd = "select * from ref_emo where emo_name like 'foo%'"
        rows = self.db_con.fetch_rows(sql_cmd)
        emo_dict = unpack_rows(rows)
        assert emo_dict[16] == "foo"
        assert emo_dict[17] == "foobar"


class Test_DfManip:

    @pytest.fixture(autouse=True)
    def _setup(self):
        df_bdi = helper.simulate_bdi()
        df_manip = sql_database._DfManip()
        self.df_subj = df_manip.subj_col(df_bdi, "study_id")
        self.df_long = df_manip.convert_wide_long(self.df_subj.copy(), "BDI")

    def test_subj_col(self):
        assert "subj_id" in list(self.df_subj.columns)
        assert 1 == self.df_subj.loc[0, "subj_id"]

    def test_convert_wide_long(self):
        assert (18, 6) == self.df_long.shape
        for chk_col in ["item", "resp"]:
            assert chk_col in list(self.df_long.columns)
        assert 1 == self.df_long.loc[4, "resp"]
        assert 2 == self.df_long.loc[17, "resp"]


class Test_Recipes:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_database):
        self.db_con = fixt_database
        self.recipe = sql_database._Recipes(self.db_con)

    def test_build_cols(self):
        assert "(foo, bar, foobar)" == self.recipe._build_cols(
            ["foo", "bar", "foobar"]
        )

    def test_validate_cols(self):
        df = pd.DataFrame.from_dict(
            {"subj_id": [99, 999], "subj_name": ["FOO99", "FOO999"]}
        )
        col_list = ["subj_id", "subj_name", "foobar"]
        with pytest.raises(KeyError):
            self.recipe._validate_cols(df, col_list)

    def test_insert_ref_subj(self):
        # Test insert
        df = pd.DataFrame.from_dict(
            {"subj_id": [99, 999], "subj_name": ["FOO99", "FOO999"]}
        )
        self.recipe.insert_ref_subj(df, subj_col="subj_name")

        # Pull data
        sql_cmd = "select * from ref_subj where subj_name like 'FOO%'"
        rows = self.db_con.fetch_rows(sql_cmd)
        subj_dict = unpack_rows(rows)
        assert subj_dict[99] == "FOO99"
        assert subj_dict[999] == "FOO999"

    def test_insert_survey_date(self):
        # Test insert
        df = pd.DataFrame.from_dict(
            {
                "subj_id": [99],
                "sess_id": [9],
                "sur_date": ["2020-06-01"],
            }
        )
        self.recipe.insert_survey_date(df, "bdi", date_col="sur_date")

        # Pull data
        data_chk = self.db_con.fetch_rows(
            "select * from tbl_survey_date where sur_name='bdi'"
        )
        assert 99 == data_chk[0][0]
        assert 9 == data_chk[0][1]
        assert "bdi" == data_chk[0][2]

    def test_insert_basic_tbl(self):
        # Test insert
        df = pd.DataFrame.from_dict(
            {
                "subj_id": [99],
                "sess_id": [9],
                "item": [100],
                "resp": [999],
            }
        )
        self.recipe.insert_basic_tbl(df, "als")

        # Pull data
        data_chk = self.db_con.fetch_rows(
            "select * from tbl_als where subj_id=99"
        )
        assert 99 == data_chk[0][0]
        assert 9 == data_chk[0][1]
        assert 100 == data_chk[0][2]
        assert 999 == data_chk[0][3]

    def test_insert_demographics(self):
        # Test insert
        df = pd.DataFrame.from_dict(
            {
                "subj_id": [99],
                "sess_id": [9],
                "age": [20],
                "interview_age": [240],
                "years_education": [16],
                "sex": ["male"],
                "handedness": ["left"],
                "race": ["foo"],
                "is_hispanic": ["no"],
                "is_minority": ["no"],
            }
        )
        self.recipe.insert_demographics(df)

        # Pull data
        data_chk = self.db_con.fetch_rows(
            "select * from tbl_demographics where subj_id=99"
        )
        assert 99 == data_chk[0][0]
        assert 9 == data_chk[0][1]
        assert 20 == data_chk[0][2]
        assert 240 == data_chk[0][3]
        assert 16 == data_chk[0][4]
        assert "male" == data_chk[0][5]
        assert "left" == data_chk[0][6]
        assert "foo" == data_chk[0][7]
        assert "no" == data_chk[0][8]
        assert "no" == data_chk[0][9]

    def test_insert_psr(self):
        # Test insert
        df = pd.DataFrame.from_dict(
            {
                "subj_id": [99],
                "sess_id": [9],
                "task_id": [4],
                "emo_id": [1],
                "stim_name": ["foo1.mp4"],
                "resp_arousal": [1],
                "resp_endorse": ["foo"],
                "resp_valence": [3],
            }
        )
        self.recipe.insert_psr(df, "post_scan_ratings")

        # Pull data
        data_chk = self.db_con.fetch_rows(
            "select * from tbl_post_scan_ratings where subj_id=99"
        )
        assert 99 == data_chk[0][0]
        assert 9 == data_chk[0][1]
        assert 4 == data_chk[0][2]
        assert 1 == data_chk[0][3]
        assert "foo1.mp4" == data_chk[0][4]
        assert 1 == data_chk[0][5]
        assert "foo" == data_chk[0][6]
        assert 3 == data_chk[0][7]

    def test_rest_ratings(self):
        # Test insert
        df = pd.DataFrame.from_dict(
            {
                "subj_id": [99],
                "sess_id": [9],
                "task_id": [4],
                "emo_id": [1],
                "resp_int": [1],
                "resp_alpha": ["foo"],
            }
        )
        self.recipe.insert_rest_ratings(df, "rest_ratings")

        # Pull data
        data_chk = self.db_con.fetch_rows(
            "select * from tbl_rest_ratings where subj_id=99"
        )
        assert 99 == data_chk[0][0]
        assert 9 == data_chk[0][1]
        assert 4 == data_chk[0][2]
        assert 1 == data_chk[0][3]
        assert 1 == data_chk[0][4]
        assert "foo" == data_chk[0][5]

    def test_in_scan_ratings(self):
        # Test insert
        df = pd.DataFrame.from_dict(
            {
                "subj_id": [99],
                "sess_id": [9],
                "task_id": [4],
                "run": [1],
                "block_id": [4],
                "resp_emo_id": [12],
                "resp_intensity": [22],
            }
        )
        self.recipe.insert_in_scan_ratings(df, "in_scan_ratings")

        # Pull data
        data_chk = self.db_con.fetch_rows(
            "select * from tbl_in_scan_ratings where subj_id=99"
        )
        assert 99 == data_chk[0][0]
        assert 9 == data_chk[0][1]
        assert 4 == data_chk[0][2]
        assert 1 == data_chk[0][3]
        assert 4 == data_chk[0][4]
        assert 12 == data_chk[0][5]
        assert 22 == data_chk[0][6]

    def test_ref_sess_task(self):
        # Test insert
        df = pd.DataFrame.from_dict(
            {
                "subj_id": [99],
                "sess_id": [9],
                "task_id": [4],
            }
        )
        self.recipe.insert_ref_sess_task(df)

        # Pull data
        data_chk = self.db_con.fetch_rows(
            "select * from ref_sess_task where subj_id=99"
        )
        assert 99 == data_chk[0][0]
        assert 9 == data_chk[0][1]
        assert 4 == data_chk[0][2]


def test_TaskMaps():
    pass


def test_PrepPsr():
    pass


def test_DbUpdate():
    pass
