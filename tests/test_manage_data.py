import pytest
import os
import pandas as pd
from make_reports.resources import manage_data


@pytest.mark.rep_get
class TestGetRedcap:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_setup):
        self.proj_dir = fixt_setup.test_emorep
        self.get_red = manage_data.GetRedcap(self.proj_dir)

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

    def test_get_redcap(self):
        with pytest.raises(ValueError):
            self.get_red.get_redcap(survey_list=["foo"])


def test_GetQualtrics():
    pass


def test_GetRest():
    pass


def test_GetTask():
    pass
