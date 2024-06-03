import pytest
import os
import glob
import pandas as pd
from make_reports.resources import manage_data
from make_reports.resources import sql_database
import helper


@pytest.mark.rep_get
class TestGetRedcap:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_setup):
        self.proj_dir = fixt_setup.test_emorep
        self.get_red = manage_data.GetRedcap(self.proj_dir)

    @pytest.mark.dependency()
    def test_clean_map(self):
        ref_dict = {
            "prescreen": ["clean_prescreen", "visit_day0"],
            "demographics": ["clean_demographics", "visit_day1"],
            "consent_pilot": ["clean_consent", "visit_day1"],
            "consent_v1.22": ["clean_consent", "visit_day1"],
            "guid": ["clean_guid", "visit_day0"],
            "bdi_day2": ["clean_bdi_day23", "visit_day2"],
            "bdi_day3": ["clean_bdi_day23", "visit_day3"],
        }
        assert ref_dict == self.get_red._clean_map

    @pytest.mark.dependency(depends=["TestGetRedcap::test_clean_map"])
    def test_download_redcap(self):
        sur_name = "bdi_day2"
        raw_dict = self.get_red._download_redcap([sur_name])

        # Check return object
        assert sur_name in list(raw_dict.keys())
        assert "visit_day2" == raw_dict[sur_name][0]
        assert isinstance(raw_dict[sur_name][1], pd.DataFrame)

        # Check file written
        chk_raw = os.path.join(
            self.proj_dir, "data_survey", "visit_day2", f"raw_{sur_name}.csv"
        )
        assert os.path.exists(chk_raw)

    @pytest.mark.dependency(depends=["TestGetRedcap::test_download_redcap"])
    def test_write_redcap(self):
        df = helper.df_foo()
        file_path = self.get_red._write_redcap(df, "foo", "bar", False)
        assert os.path.exists(file_path)
        assert "df_foo.csv" == os.path.basename(file_path)
        assert os.path.join(
            self.proj_dir, "data_survey/bar"
        ) == os.path.dirname(file_path)

        file_path = self.get_red._write_redcap(df, "foo", "bar", True)
        assert os.path.join(
            self.proj_dir, "data_pilot/data_survey/bar"
        ) == os.path.dirname(file_path)

    @pytest.mark.dependency(depends=["TestGetRedcap::test_write_redcap"])
    def test_get_redcap(self):
        with pytest.raises(ValueError):
            self.get_red.get_redcap(survey_list=["foo"])

        #
        write_list = self.get_red.get_redcap(
            survey_list=["bdi_day2"], db_name="db_emorep_unittest"
        )
        assert 2 == len(write_list)
        assert os.path.exists(write_list[1])
        assert "df_BDI.csv" == os.path.basename(write_list[0])
        assert os.path.join(
            self.proj_dir, "data_survey/visit_day2"
        ) == os.path.dirname(write_list[0])


@pytest.mark.rep_get
class TestGetQualtrics:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_setup, fixt_db_connect):
        # Initiailze obj
        self.proj_dir = fixt_setup.test_emorep
        self.get_qual = manage_data.GetQualtrics(self.proj_dir)

        # Setup for test_download_qualtrics
        self.sur_name = "EmoRep_Session_1"
        self.raw_dict = self.get_qual._download_qualtrics([self.sur_name])
        self.get_qual.clean_session_1(self.raw_dict[self.sur_name][1])

        # Setup for test_unpack_qualtrics, test_write_qualtrics
        self.get_qual._test_sur = None
        df = self.get_qual.data_study["visit_day1"]["ALS"]
        self.get_qual.data_study = {}
        self.get_qual.data_study = {"visit_day1": {"ALS": df}}
        self.get_qual.clean_qualtrics = {"study": {}}
        up_mysql = sql_database.DbUpdate(db_con=fixt_db_connect)
        self.get_qual._unpack_qualtrics(up_mysql=up_mysql)

    @pytest.mark.dependency()
    def test_clean_map(self):
        ref_dict = {
            "EmoRep_Session_1": "clean_session_1",
            "Session 2 & 3 Survey": "clean_session_23",
            "FINAL - EmoRep Stimulus Ratings - "
            + "fMRI Study": "clean_postscan_ratings",
        }
        assert ref_dict == self.get_qual._clean_map

    @pytest.mark.dependency(depends=["TestGetQualtrics::test_clean_map"])
    def test_download_qualtrics(self):
        # Check return object
        assert self.sur_name in list(self.raw_dict.keys())
        assert "visit_day1" == self.raw_dict[self.sur_name][0]
        assert isinstance(self.raw_dict[self.sur_name][1], pd.DataFrame)

        # Check file written
        chk_raw = os.path.join(
            self.proj_dir,
            "data_survey",
            "visit_day1",
            f"raw_{self.sur_name}.csv",
        )
        assert os.path.exists(chk_raw)

    @pytest.mark.dependency(
        depends=["TestGetQualtrics::test_download_qualtrics"]
    )
    def test_unpack_qualtrics(
        self,
    ):
        assert "visit_day1" in list(
            self.get_qual.clean_qualtrics["study"].keys()
        )
        assert "ALS" in list(
            self.get_qual.clean_qualtrics["study"]["visit_day1"].keys()
        )
        assert isinstance(
            self.get_qual.clean_qualtrics["study"]["visit_day1"]["ALS"],
            pd.DataFrame,
        )

    @pytest.mark.dependency(
        depends=["TestGetQualtrics::test_unpack_qualtrics"]
    )
    def test_write_qualtrics(self):
        chk_path = os.path.join(
            self.proj_dir, "data_survey/visit_day1", "df_als.csv"
        )
        assert os.path.exists(chk_path)

    def test_get_qualtrics(self):
        with pytest.raises(ValueError):
            self.get_qual.get_qualtrics(survey_list=["foo"])

        self.get_qual.get_qualtrics(
            db_name="db_emorep_unittest", test_sur="PSWQ"
        )

        assert isinstance(
            self.get_qual.clean_qualtrics["study"]["visit_day1"]["PSWQ"],
            pd.DataFrame,
        )
        chk_path = os.path.join(
            self.proj_dir, "data_survey/visit_day1", "df_pswq.csv"
        )
        assert os.path.exists(chk_path)


class TestGetRest:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_setup, fixt_db_connect, fixt_test_data):
        self.proj_dir = fixt_setup.test_emorep
        self.get_rest = manage_data.GetRest(self.proj_dir)
        self.get_rest.get_rest(db_name="db_emorep_unittest")

    def test_rest_paths(self):
        raw_dir, out_dir = self.get_rest._rest_paths("pilot")
        assert raw_dir == os.path.join(
            self.proj_dir, "data_pilot", "data_scanner_BIDS", "rawdata"
        )
        assert out_dir == os.path.join(
            self.proj_dir, "data_pilot", "data_survey"
        )

        raw_dir, out_dir = self.get_rest._rest_paths("study")
        assert raw_dir == os.path.join(
            self.proj_dir, "data_scanner_BIDS", "rawdata"
        )
        assert out_dir == os.path.join(self.proj_dir, "data_survey")

    def test_get_rest(self):
        assert hasattr(self.get_rest, "clean_rest")
        assert isinstance(self.get_rest.clean_rest, dict)
        assert "study" in list(self.get_rest.clean_rest.keys())
        assert "visit_day2" in list(self.get_rest.clean_rest["study"].keys())
        assert "rest_ratings" in list(
            self.get_rest.clean_rest["study"]["visit_day2"]
        )
        assert isinstance(
            self.get_rest.clean_rest["study"]["visit_day2"]["rest_ratings"],
            pd.DataFrame,
        )

        #
        chk_path = os.path.join(
            self.proj_dir, "data_survey/visit_day2", "df_rest_ratings.csv"
        )
        assert os.path.exists(chk_path)


class TestGetTask:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_setup, fixt_db_connect, fixt_test_data):
        self.proj_dir = fixt_setup.test_emorep
        self.test_func = os.path.join(fixt_test_data.test_func)
        self.get_task = manage_data.GetTask(self.proj_dir)
        self.event_path = sorted(glob.glob(f"{self.test_func}/*events.tsv"))[0]

    @pytest.mark.dependency()
    def test_load_event(self):
        df = self.get_task._load_event(self.event_path)
        for chk_col in ["subj", "sess", "task", "run"]:
            assert chk_col in list(df.columns)
        assert (94, 12) == df.shape
        assert "ER0016" == df.loc[0, "subj"]
        assert "day2" == df.loc[0, "sess"]
        assert "scenarios" == df.loc[0, "task"]
        assert 1 == df.loc[0, "run"]

    @pytest.mark.dependency(depends=["TestGetTask::test_load_event"])
    def test_clean_df(self):
        self.get_task._df_all = self.get_task._load_event(self.event_path)
        self.get_task._clean_df()
        for chk_col in [
            "study_id",
            "visit",
            "task",
            "run",
            "block",
            "resp_emotion",
            "resp_intensity",
        ]:
            assert chk_col in list(self.get_task._df_all.columns)

        assert (4, 7) == self.get_task._df_all.shape
        assert "ER0016" == self.get_task._df_all.loc[0, "study_id"]
        assert "day2" == self.get_task._df_all.loc[0, "visit"]
        assert 1 == self.get_task._df_all.loc[0, "run"]
        assert "anger" == self.get_task._df_all.loc[0, "block"]
        assert "anger" == self.get_task._df_all.loc[0, "resp_emotion"]
        assert 7 == self.get_task._df_all.loc[0, "resp_intensity"]

    @pytest.mark.dependency(depends=["TestGetTask::test_clean_df"])
    def test_build_df(self):
        self.get_task._build_df()
        assert hasattr(self.get_task, "_df_all")
        assert (30, 7) == self.get_task._df_all.shape
        assert 8 == self.get_task._df_all.loc[29, "run"]
        assert "neutral" == self.get_task._df_all.loc[29, "block"]
        assert "neutral" == self.get_task._df_all.loc[29, "resp_emotion"]
        assert 5 == self.get_task._df_all.loc[29, "resp_intensity"]

    @pytest.mark.dependency(depends=["TestGetTask::test_build_df"])
    def test_get_task(self):
        self.get_task.get_task(db_name="db_emorep_unittest")
        assert hasattr(self.get_task, "clean_task")
        assert isinstance(self.get_task.clean_task, dict)
        assert "study" in list(self.get_task.clean_task.keys())
        assert "visit_day2" in list(self.get_task.clean_task["study"].keys())
        assert "in_scan_task" in list(
            self.get_task.clean_task["study"]["visit_day2"].keys()
        )
        assert isinstance(
            self.get_task.clean_task["study"]["visit_day2"]["in_scan_task"],
            pd.DataFrame,
        )
        chk_path = os.path.join(
            self.proj_dir, "data_survey/visit_day2", "df_in_scan_ratings.csv"
        )
        assert os.path.exists(chk_path)
