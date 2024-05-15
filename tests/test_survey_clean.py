import pytest
import numpy as np
from make_reports.resources import survey_clean
import helper


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


class TestCleanRedcap_remaining:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_dl_red):
        self.fixt_dl = fixt_dl_red


def test_CleanRedcap():
    pass


def test_CleanQualtrics():
    pass


def test_clean_rest_ratings():
    pass
