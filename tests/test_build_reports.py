import pytest
import pandas as pd


class TestDemoAll:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_demo_all):
        self.demo_all = fixt_demo_all

    def test_init(self):
        assert hasattr(self.demo_all, "final_demo")
        assert isinstance(self.demo_all.final_demo, pd.DataFrame)

    def test_get_demo(self):
        assert hasattr(self.demo_all, "_df_merge")
        print(self.demo_all._df_merge.columns)
        print(self.demo_all._df_merge.head())
        assert 36 == self.demo_all._df_merge.shape[1]
        chk_col = [
            "record_id",
            "consent",
            "first_name",
            "last_name",
            "email",
            "signature",
            "date",
            "time",
            "study_id",
            "guid",
            "guid_complete",
            "demographics_complete",
            "firstname",
            "middle_name",
            "lastname",
            "dob",
            "city",
            "state",
            "country_birth",
            "age",
            "gender",
            "gender_other",
            "race___1",
            "race___2",
            "race___3",
            "race___4",
            "race___5",
            "race___6",
            "race___7",
            "race___8",
            "race_other",
            "ethnicity",
            "years_education",
            "level_education",
            "handedness",
            "datetime",
        ]
        assert chk_col == list(self.demo_all._df_merge.columns)
        assert False

    def test_get_pilot_study(self):
        pass

    def test_get_race(self):
        pass

    def test_get_ethnic_minority(self):
        pass

    def test_get_hand(self):
        pass

    def test_make_complete(self):
        pass

    def test_remove_withdrawn(self):
        pass

    def test_submission_cycle(self):
        pass


def test_ManagerRegular():
    pass


def test_GenerateGuids():
    pass
