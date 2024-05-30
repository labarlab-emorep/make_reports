import pytest
import numpy as np
import pandas as pd
from make_reports.resources import survey_clean
import helper


@pytest.mark.rep_get
class TestCleanRedcap_demographics:

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.clean_red = survey_clean.CleanRedcap()
        self.clean_red.clean_demographics(helper.simulate_demographics())

    def test_dob_convert(self):
        dob_str = [
            x.strftime("%Y-%m-%d")
            for x in self.clean_red._df_raw["dob"].tolist()
        ]
        chk_list = [
            "2000-06-15",
            "2000-06-15",
            "2000-06-15",
            "2000-06-15",
            "2000-06-15",
            "2000-06-15",
            "2000-06-15",
            "2000-10-06",
        ]
        assert chk_list == dob_str

    def test_get_educ_years(self):
        assert [12, 13, 14, 16, 17, 18, 20, 20] == self.clean_red._df_raw[
            "years_education"
        ].tolist()

    def test_clean_city_state(self):
        assert [
            "Denver",
            "Denver",
            "Denver",
            "San Jose",
            "San Jose",
            "San Jose",
            "Sao Paolo",
            "Lagos",
        ] == self.clean_red._df_raw["city"].tolist()
        assert [
            np.nan,
            "CO",
            "CO",
            np.nan,
            "California",
            "CA",
            np.nan,
            np.nan,
        ] == self.clean_red._df_raw["state"].tolist()

    def test_clean_middle_name(self):
        assert [
            "aa",
            np.nan,
            np.nan,
            "dd",
            np.nan,
            np.nan,
            np.nan,
            "h-h",
        ] == self.clean_red._df_raw["middle_name"].tolist()

    def test_clean_demographics(self):
        # Test final attributes
        assert hasattr(self.clean_red, "df_study")
        assert hasattr(self.clean_red, "df_pilot")
        assert 6 == self.clean_red.df_study.loc[0, "record_id"]
        assert (3, 25) == self.clean_red.df_study.shape
        assert 1 == self.clean_red.df_pilot.loc[0, "record_id"]
        assert (5, 25) == self.clean_red.df_pilot.shape


@pytest.mark.rep_get
class TestCleanRedcap_remaining:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_dl_red):
        self.fixt_dl = fixt_dl_red
        self.clean_red = survey_clean.CleanRedcap()

    def test_clean_consent_pilot(self):
        self.clean_red.clean_consent(self.fixt_dl.red_dict["consent_pilot"][1])
        assert hasattr(self.clean_red, "df_study")
        assert hasattr(self.clean_red, "df_pilot")
        assert 9 == self.clean_red.df_study.shape[1]
        assert 25 == self.clean_red.df_study.loc[0, "record_id"]
        assert "10:14" == self.clean_red.df_study.loc[0, "time"]
        assert "2022-07-20" == self.clean_red.df_study.loc[0, "date"]

    def test_clean_consent_v1(self):
        self.clean_red.clean_consent(self.fixt_dl.red_dict["consent_v1.22"][1])
        assert hasattr(self.clean_red, "df_study")
        assert hasattr(self.clean_red, "df_pilot")
        assert 9 == self.clean_red.df_study.shape[1]
        assert 16 == self.clean_red.df_study.loc[1, "record_id"]
        assert "13:09" == self.clean_red.df_study.loc[1, "time_v2"]
        assert "2022-05-04" == self.clean_red.df_study.loc[1, "date_v2"]

    def test_prescreen(self):
        self.clean_red.clean_prescreen(self.fixt_dl.red_dict["prescreen"][1])
        assert hasattr(self.clean_red, "df_study")
        assert hasattr(self.clean_red, "df_pilot")
        assert 28 == self.clean_red.df_study.shape[1]
        assert 6 == self.clean_red.df_study.loc[0, "record_id"]

    def test_clean_guid(self):
        self.clean_red.clean_guid(self.fixt_dl.red_dict["guid"][1])
        assert hasattr(self.clean_red, "df_study")
        assert hasattr(self.clean_red, "df_pilot")
        for chk_col in ["record_id", "study_id", "guid", "guid_complete"]:
            assert chk_col in self.clean_red.df_study.columns
        assert "ER0009" == self.clean_red.df_study.loc[0, "study_id"]

    def test_clean_bdi_day23(self):
        self.clean_red.clean_bdi_day23(self.fixt_dl.red_dict["bdi_day3"][1])
        assert hasattr(self.clean_red, "df_study")
        assert hasattr(self.clean_red, "df_pilot")
        for chk_col in [
            "record_id",
            "study_id",
            "datetime",
            "BDI_1",
            "BDI_19b",
        ]:
            assert chk_col in self.clean_red.df_study.columns
        assert 9 == self.clean_red.df_study.loc[0, "record_id"]
        assert "ER0009" == self.clean_red.df_study.loc[0, "study_id"]
        assert "2022-04-28" == self.clean_red.df_study.loc[0, "datetime"]
        assert 1.0 == self.clean_red.df_study.loc[1, "BDI_19"]


@pytest.mark.long
@pytest.mark.rep_get
class TestCleanQualtrics:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_cl_qual):
        self.fixt_cl = fixt_cl_qual

    def test_init(self):
        assert hasattr(self.fixt_cl.clean_qual, "_pilot_list")
        assert hasattr(self.fixt_cl.clean_qual, "_withdrew_list")
        assert "ER0003" in self.fixt_cl.clean_qual._pilot_list

    def test_clean_session_1_attr(self):
        # Check attribute organization
        assert "visit_day1" in self.fixt_cl.s1_data.keys()
        for chk_key in [
            "ALS",
            "AIM",
            "ERQ",
            "PSWQ",
            "RRS",
            "STAI_Trait",
            "TAS",
        ]:
            assert chk_key in self.fixt_cl.s1_data["visit_day1"].keys()
        assert isinstance(
            self.fixt_cl.s1_data["visit_day1"]["ALS"], pd.DataFrame
        )

    def test_clean_session_1_data(self):
        # Check dataframe organization
        df = self.fixt_cl.s1_data["visit_day1"]["ALS"]
        for chk_col in ["study_id", "datetime"]:
            assert chk_col in df.columns

        # Check dataframe content
        assert 20 == df.shape[1]
        assert "ER0009" == df.loc[0, "study_id"]
        assert "2022-04-19" == df.loc[0, "datetime"]

    def test_clean_session_23_attr(self):
        # Check attribute organization
        assert [
            "visit_day2",
            "visit_day3",
        ] == list(self.fixt_cl.s23_data.keys())
        for chk_key in ["PANAS", "STAI_State"]:
            assert chk_key in self.fixt_cl.s23_data["visit_day2"].keys()
        assert isinstance(
            self.fixt_cl.s23_data["visit_day2"]["PANAS"], pd.DataFrame
        )

    def test_clean_session_23_data(self):
        # Check dataframe organization
        df = self.fixt_cl.s23_data["visit_day2"]["PANAS"]
        for chk_col in ["study_id", "datetime", "visit"]:
            assert chk_col in df.columns

        # Check dataframe content
        assert 23 == df.shape[1]
        assert "ER0009" == df.loc[0, "study_id"]
        assert "day2" == df.loc[0, "visit"]
        assert "2022-04-22" == df.loc[0, "datetime"]

    def test_clean_postscan_ratings_attr(self):
        # Check attribute organization
        assert [
            "visit_day2",
            "visit_day3",
        ] == list(self.fixt_cl.post_data.keys())
        assert (
            "post_scan_ratings" in self.fixt_cl.post_data["visit_day2"].keys()
        )
        assert isinstance(
            self.fixt_cl.post_data["visit_day2"]["post_scan_ratings"],
            pd.DataFrame,
        )

    def test_clean_postscan_ratings_data(self):
        # Check dataframe organization
        df = self.fixt_cl.post_data["visit_day2"]["post_scan_ratings"]
        for chk_col in [
            "study_id",
            "datetime",
            "session",
            "type",
            "emotion",
            "stimulus",
            "prompt",
            "response",
        ]:
            assert chk_col in df.columns

        # Check dataframe content
        assert 8 == df.shape[1]
        assert "ER0009" == df.loc[0, "study_id"]
        assert "day2" == df.loc[0, "session"]
        assert "2022-04-22" == df.loc[0, "datetime"]
        assert "Movies" == df.loc[0, "type"]
        assert "amusement" == df.loc[0, "emotion"]
        assert "00001.mp4" == df.loc[0, "stimulus"]
        assert "Arousal" == df.loc[0, "prompt"]
        assert "2" == df.loc[0, "response"]
        assert "Endorsement" == df.loc[1, "prompt"]
        assert "Amusement" == df.loc[1, "response"]


def test_clean_rest_ratings(fixt_test_data):
    # Check errors raised
    with pytest.raises(ValueError):
        _ = survey_clean.clean_rest_ratings("foo", "bar")
    with pytest.raises(FileNotFoundError):
        _ = survey_clean.clean_rest_ratings("day2", "bar")

    # Get dataframe, test structure
    df = fixt_test_data.df_rest
    assert (2, 20) == df.shape
    col_names = [
        "study_id",
        "visit",
        "task",
        "datetime",
        "resp_type",
        "AMUSEMENT",
        "ANGER",
        "ANXIETY",
        "AWE",
        "CALMNESS",
        "CRAVING",
        "DISGUST",
        "EXCITEMENT",
        "FEAR",
        "HORROR",
        "JOY",
        "NEUTRAL",
        "ROMANCE",
        "SADNESS",
        "SURPRISE",
    ]
    assert col_names == list(df.columns)

    # Check some data
    assert "ER0016" == df.loc[0, "study_id"]
    assert "day2" == df.loc[0, "visit"]
    assert "scenarios" == df.loc[0, "task"]
    assert 1 == df.loc[0, "SURPRISE"]
    assert "Not At All" == df.loc[1, "SURPRISE"]
