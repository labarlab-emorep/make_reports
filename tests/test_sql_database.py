import pytest
import datetime
import pandas as pd
import pandas.api.types as ptypes
from make_reports.resources import sql_database
import helper


@pytest.mark.rep_get
class TestDbConnect:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_db_connect):
        self.db_con = fixt_db_connect

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

        emo_dict = helper.unpack_rows(rows)
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
        emo_dict = helper.unpack_rows(rows)
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
    def _setup(self, fixt_db_connect):
        self.db_con = fixt_db_connect
        self.recipe = sql_database._Recipes(self.db_con)

    def test_validate_cols(self):
        df = helper.df_foo()
        col_list = ["subj_id", "subj_name", "foobar"]
        with pytest.raises(KeyError):
            self.recipe._validate_cols(df, col_list)

    def test_insert_ref_subj(self):
        # Test insert
        df = helper.df_foo()
        self.recipe.insert_ref_subj(df, subj_col="subj_name")

        # Pull data
        sql_cmd = "select * from ref_subj where subj_name like 'FOO%'"
        rows = self.db_con.fetch_rows(sql_cmd)
        subj_dict = helper.unpack_rows(rows)
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

    def test_build_cols(self):
        assert "(foo, bar, foobar)" == self.recipe._build_cols(
            ["foo", "bar", "foobar"]
        )

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


class Test_TaskMaps:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_db_connect):
        self.task_map = sql_database._TaskMaps(fixt_db_connect)

    def test_init(self):
        assert hasattr(self.task_map, "_ref_task")
        assert hasattr(self.task_map, "_ref_emo")

    def test_load_refs(self):
        ref_task = {"movies": 1, "scenarios": 2}
        assert ref_task == self.task_map._ref_task
        assert "amusement" in list(self.task_map._ref_emo.keys())
        assert 1 == self.task_map._ref_emo["amusement"]

    def test_task_label(self):
        ref_dict = {"stim-mov": "movies", "stim-sce": "scenarios"}
        assert 1 == self.task_map.task_label(ref_dict, "stim-mov")
        assert 2 == self.task_map.task_label(ref_dict, "stim-sce")

    def test_emo_label(self):
        ref_emo = {"emo-amu": "amusement", "emo-ang": "anger"}
        assert 1 == self.task_map.emo_label(ref_emo, "emo-amu")
        assert 2 == self.task_map.emo_label(ref_emo, "emo-ang")


@pytest.mark.rep_get
@pytest.mark.long
class Test_PrepPsr:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_db_connect, fixt_cl_qual):
        df = fixt_cl_qual.post_data["visit_day2"]["post_scan_ratings"].copy()
        df["subj_id"] = df["study_id"].str[2:].astype(int)
        df["sess_id"] = 2
        self.prep_psr = sql_database._PrepPsr(df, fixt_db_connect)
        self.prep_psr.prep_dfs()

    @pytest.mark.dependency()
    def test_prep_dfs(self):
        assert hasattr(self.prep_psr, "df_tidy")
        assert hasattr(self.prep_psr, "df_date")

    @pytest.mark.dependency(depends=["Test_PrepPsr::test_prep_dfs"])
    def test_prep_tidy_shape(self):
        # Test shape, column names needed by sql table
        print(self.prep_psr.df_tidy.info())
        assert 13 == self.prep_psr.df_tidy.shape[1]

        col_int = [
            "subj_id",
            "sess_id",
            "task_id",
            "emo_id",
            "resp_arousal",
            "resp_valence",
        ]
        col_str = ["stim_name", "resp_endorse"]
        col_all = col_int + col_str
        for chk_col in col_all:
            assert chk_col in list(self.prep_psr.df_tidy.columns)

        # Test column types
        assert all(
            ptypes.is_numeric_dtype(self.prep_psr.df_tidy[col])
            for col in col_int
        )
        assert all(
            ptypes.is_string_dtype(self.prep_psr.df_tidy[col])
            for col in col_str
        )

    @pytest.mark.dependency(
        depends=[
            "Test_PrepPsr::test_prep_dfs",
            "Test_PrepPsr::test_prep_tidy_shape",
        ]
    )
    def test_prep_tidy_data(self):
        assert 9 == self.prep_psr.df_tidy.loc[0, "subj_id"]
        assert 2 == self.prep_psr.df_tidy.loc[0, "sess_id"]
        assert 1 == self.prep_psr.df_tidy.loc[0, "task_id"]
        assert 1 == self.prep_psr.df_tidy.loc[0, "emo_id"]
        assert "00001.mp4" == self.prep_psr.df_tidy.loc[0, "stim_name"]
        assert "Amusement" == self.prep_psr.df_tidy.loc[0, "resp_endorse"]
        assert 2 == self.prep_psr.df_tidy.loc[0, "resp_arousal"]
        assert 6 == self.prep_psr.df_tidy.loc[0, "resp_valence"]

    @pytest.mark.dependency(depends=["Test_PrepPsr::test_prep_dfs"])
    def test_prep_date_shape(self):
        assert 3 == self.prep_psr.df_date.shape[1]
        assert ["subj_id", "sess_id", "datetime"] == list(
            self.prep_psr.df_date.columns
        )

    @pytest.mark.dependency(
        depends=[
            "Test_PrepPsr::test_prep_dfs",
            "Test_PrepPsr::test_prep_date_shape",
        ]
    )
    def test_prep_date_data(self):
        assert 9 == self.prep_psr.df_date.loc[0, "subj_id"]
        assert 2 == self.prep_psr.df_date.loc[0, "sess_id"]
        assert "2022-04-22" == self.prep_psr.df_date.loc[0, "datetime"]


@pytest.mark.long
class TestDbUpdate:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_db_update):
        self.fixt_up = fixt_db_update

    def _pull_five(self, tbl_name: str, col_list: list) -> pd.DataFrame:
        """Pull first 5 rows from table."""
        with self.fixt_up.db_up._db_con._con_cursor() as cur:
            cur.execute(f"select * from {tbl_name} limit 5")
            results = cur.fetchall()
        return pd.DataFrame(results, columns=col_list)

    def _pull_one(self, subj_id: int, sur_name: str) -> tuple:
        """Pull row from tbl_survey_date for subj_id and sur_name."""
        with self.fixt_up.db_up._db_con._con_cursor() as cur:
            cur.execute(
                f"select * from tbl_survey_date where subj_id={subj_id} "
                + f"and sur_name='{sur_name}' limit 1"
            )
            results = cur.fetchall()
        return results

    def test_update_db(self):
        df = self.fixt_up.df_rrs.copy()
        with pytest.raises(TypeError):
            self.fixt_up.db_up.update_db(df, "RRS", "2", "qualtrics")
        with pytest.raises(ValueError):
            self.fixt_up.db_up.update_db(df, "RRS", 2, "foobar")
        with pytest.raises(KeyError):
            self.fixt_up.db_up.update_db(
                df, "RRS", 2, "qualtrics", subj_col="foo"
            )

        #
        self.fixt_up.db_up.update_db(df, "RRS", 1, "qualtrics")

        #
        df_pull = self._pull_five(
            "tbl_rrs", ["subj_id", "sess_id", "item_rrs", "resp_rrs"]
        )
        assert (5, 4) == df_pull.shape
        assert 9 == df_pull.loc[0, "subj_id"]
        assert 1 == df_pull.loc[0, "item_rrs"]
        assert 3 == df_pull.loc[0, "resp_rrs"]

    def test_basic_prep(self):
        self.fixt_up.db_up._df = pd.DataFrame.from_dict(
            data={"study_id": ["ER1", "ER2"], "foo": ["a", "b"]}
        )
        self.fixt_up.db_up._sess_id = 2
        self.fixt_up.db_up._subj_col = "study_id"
        self.fixt_up.db_up._basic_prep()

        assert "sess_id" in list(self.fixt_up.db_up._df.columns)
        assert "subj_id" in list(self.fixt_up.db_up._df.columns)

    def test_update_qualtrics(self):
        # Run method
        df = self.fixt_up.df_aim.copy()
        self.fixt_up.db_up.update_db(df, "AIM", 1, "qualtrics")

        # Pull data, test struct, data
        df_pull = self._pull_five(
            "tbl_aim", ["subj_id", "sess_id", "item_aim", "resp_aim"]
        )
        assert (5, 4) == df_pull.shape
        assert 9 == df_pull.loc[0, "subj_id"]
        assert 1 == df_pull.loc[0, "item_aim"]
        assert 6 == df_pull.loc[0, "resp_aim"]

        # Check survey_date
        subj_id, _, sur_name, sur_date = self._pull_one(9, "aim")[0]
        assert 9 == subj_id
        assert "aim" == sur_name
        assert datetime.date(2022, 4, 19) == sur_date

    def test_update_redcap(self):
        df = self.fixt_up.df_bdi.copy()
        self.fixt_up.db_up.update_db(df, "BDI", 3, "redcap")

        #
        df_pull = self._pull_five(
            "tbl_bdi", ["subj_id", "sess_id", "item_bdi", "resp_bdi"]
        )
        assert (5, 4) == df_pull.shape
        assert 9 == df_pull.loc[0, "subj_id"]
        assert "10" == df_pull.loc[1, "item_bdi"]
        assert 0 == df_pull.loc[0, "resp_bdi"]

        #
        subj_id, _, sur_name, sur_date = self._pull_one(9, "bdi")[0]
        assert 9 == subj_id
        assert "bdi" == sur_name
        assert datetime.date(2022, 4, 28) == sur_date

    def test_update_demographics(self):
        # TODO test with DemoAll.final_demo
        pass

    def test_update_rest_ratings(self):
        df = self.fixt_up.df_rest.copy()
        self.fixt_up.db_up.update_db(df, "rest_ratings", 2, "rest_ratings")

        #
        df_pull = self._pull_five(
            "tbl_rest_ratings",
            [
                "subj_id",
                "sess_id",
                "task_id",
                "emo_id",
                "resp_int",
                "resp_alpha",
            ],
        )
        assert (5, 6) == df_pull.shape
        assert 16 == df_pull.loc[1, "subj_id"]
        assert 2 == df_pull.loc[1, "sess_id"]
        assert 2 == df_pull.loc[1, "task_id"]
        assert 1 == df_pull.loc[0, "emo_id"]
        assert 2 == df_pull.loc[0, "resp_int"]
        assert "Slightly" == df_pull.loc[0, "resp_alpha"]

        subj_id, _, sur_name, sur_date = self._pull_one(16, "rest_ratings")[0]
        assert 16 == subj_id
        assert "rest_ratings" == sur_name
        assert datetime.date(2022, 5, 5) == sur_date

    def test_update_in_scan_ratings(self):
        df = self.fixt_up.df_task.copy()
        self.fixt_up.db_up.update_db(
            df, "in_scan_ratings", 2, "in_scan_ratings"
        )

        #
        df_pull = self._pull_five(
            "tbl_in_scan_ratings",
            [
                "subj_id",
                "sess_id",
                "task_id",
                "run",
                "block_id",
                "resp_emo_id",
                "resp_intensity",
            ],
        )
        assert (5, 7) == df_pull.shape
        assert 16 == df_pull.loc[1, "subj_id"]
        assert 2 == df_pull.loc[1, "sess_id"]
        assert 2 == df_pull.loc[1, "task_id"]
        assert 1 == df_pull.loc[0, "run"]
        assert 2 == df_pull.loc[0, "block_id"]
        assert 2 == df_pull.loc[0, "resp_emo_id"]
        assert 7 == df_pull.loc[0, "resp_intensity"]
