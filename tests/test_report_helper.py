import pytest
import json
from datetime import datetime
import importlib.resources as pkg_resources
import pandas as pd
from make_reports.resources import report_helper
from make_reports import reference_files
import helper


def test_drop_participant():
    df = pd.DataFrame(
        data={"subjid": ["ER01", "ER02", "ER03"], "value": [1, 2, 3]}
    )
    df = report_helper.drop_participant("ER03", df, "subjid")
    df_ref = pd.DataFrame(data={"subjid": ["ER01", "ER02"], "value": [1, 2]})
    pd.testing.assert_frame_equal(df_ref, df)

    with pytest.raises(ValueError):
        df = report_helper.drop_participant("ER02", df, "Subject")


def test_check_redcap_pat():
    try:
        report_helper.check_redcap_pat()
    except KeyError:
        pytest.fail("Unexpected KeyError")


def test_qualtrics_pat():
    try:
        report_helper.check_qualtrics_pat()
    except KeyError:
        pytest.fail("Unexpected KeyError")


def test_check_sql_pass():
    try:
        report_helper.check_sql_pass()
    except KeyError:
        pytest.fail("Unexpected KeyError")


def test_pull_redcap_data():
    with pkg_resources.open_text(
        reference_files, "report_keys_redcap.json"
    ) as jf:
        report_keys = json.load(jf)
    df = report_helper.pull_redcap_data(report_keys["bdi_day2"])
    assert 27 == df.shape[1]
    assert 1 == df.iloc[0, 0]
    assert "ER0001" == df.loc[0, "study_id"]
    assert 1.0 == df.loc[0, "q_5"]


def test_pull_qualtrics_data():
    with pkg_resources.open_text(
        reference_files, "report_keys_qualtrics.json"
    ) as jf:
        report_keys = json.load(jf)
    sur_name = "EmoRep_Session_1"
    df = report_helper.pull_qualtrics_data(
        sur_name, report_keys[sur_name], report_keys["datacenter_ID"]
    )
    assert 163 == df.shape[1]
    assert "1" == df.loc[2, "Status"]
    assert "839" == df.loc[2, "Duration (in seconds)"]
    assert "5" == df.loc[2, "TAS_11"]
    assert "ER0559" == df.loc[158, "RecipientLastName"]


def test_mine_template():
    nda_label, nda_cols = report_helper.mine_template("affim01_template.csv")
    assert ["affim", "01"] == nda_label
    assert "subjectkey" == nda_cols[0]
    assert "src_subject_id" == nda_cols[1]
    assert "interview_date" == nda_cols[2]
    assert "interview_age" == nda_cols[3]
    assert "sex" == nda_cols[4]
    assert "aim_1" == nda_cols[5]

    with pytest.raises(ValueError):
        _, _ = report_helper.mine_template("foo_template.csv")
    with pytest.raises(ValueError):
        _, _ = report_helper.mine_template("affim01")


def test_load_dataframes_status():
    with pytest.raises(ValueError):
        _ = report_helper.load_dataframes("foo")

    df = report_helper.load_dataframes("status")
    assert 6 == df.shape[1]
    assert "ER0017" == df.loc[0, "subj"]
    assert "excl" == df.loc[0, "visit2"]
    assert "BDI" == df.loc[0, "notes"]


def test_load_dataframes_incomplete():
    df = report_helper.load_dataframes("incomplete")
    assert 15 == df.shape[1]
    assert "ER0009" == df.loc[0, "subj"]
    assert "MB_YD" == df.loc[0, "visit2_RA"]
    assert "incomplete" == df.loc[0, "visit2_task"]


def test_calc_age_mo():
    dob_dos_age = {
        "1990-10-01": ["2000-12-01", 122],
        "1990-10-16": ["2000-12-01", 122],
        "1990-10-17": ["2000-12-01", 121],
        "1990-11-01": ["2000-11-03", 120],
        "1990-11-02": ["2000-10-30", 120],
        "1990-11-03": ["2000-10-01", 119],
        "1990-11-04": ["2000-11-03", 120],
        "1990-01-01": ["2000-06-01", 125],
        "1990-01-02": ["2000-06-20", 126],
    }
    subj_dob = []
    subj_dos = []
    real_age = []
    for dob, dos_age in dob_dos_age.items():
        subj_dob.append(datetime.strptime(dob, "%Y-%m-%d"))
        subj_dos.append(datetime.strptime(dos_age[0], "%Y-%m-%d"))
        real_age.append(dos_age[1])
    age_mo = report_helper.calc_age_mo(subj_dob, subj_dos)
    assert real_age == age_mo


def test_get_survey_age():
    df_survey = pd.DataFrame(
        data={
            "src_subject_id": ["ER1"],
            "datetime": ["2000-06-20"],
            "resp_1": [1],
        }
    )
    df_demo = pd.DataFrame(
        data={"src_subject_id": ["ER1"], "dob": ["1990-01-02"]}
    )
    df_survey = report_helper.get_survey_age(df_survey, df_demo)
    assert "interview_age" in df_survey.columns
    assert "interview_date" in df_survey.columns
    assert 126 == df_survey.loc[0, "interview_age"]
    assert "06/20/2000" == df_survey.loc[0, "interview_date"]


def test_pilot_list():
    pilot_list = report_helper.pilot_list()
    assert ["ER0001", "ER0002", "ER0003", "ER0004", "ER0005"] == pilot_list


def test_redcap_dict():
    rc_dict = report_helper.redcap_dict()
    assert not rc_dict["demographics"]
    assert not rc_dict["prescreen"]
    assert not rc_dict["consent_pilot"]
    assert not rc_dict["consent_v1.22"]
    assert "redcap" == rc_dict["guid"]
    assert "visit_day2" == rc_dict["bdi_day2"]
    assert "visit_day3" == rc_dict["bdi_day3"]


def test_qualtrics_dict():
    ql_dict = report_helper.qualtrics_dict()
    assert "visit_day1" == ql_dict["EmoRep_Session_1"]
    assert (
        "visit_day2"
        == ql_dict["FINAL - EmoRep Stimulus Ratings - fMRI Study"][0]
    )
    assert "visit_day3" == ql_dict["Session 2 & 3 Survey"][1]


def test_CheckIncomplete():
    # TODO
    pass


class Test_RedCapComplete:

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.rc_compl = report_helper._RedCapComplete()
        self.chk_subj = ["ER0001", "ER0060", "ER0679", "ER1175", "ER1211"]

    def test_init(self):
        assert hasattr(self.rc_compl, "df_compl")
        assert 27 == self.rc_compl.df_compl.shape[1]
        assert "ER0001" == self.rc_compl.df_compl.loc[0, "record_id"]
        assert 1.0 == self.rc_compl.df_compl.loc[0, "day_1_fully_completed"]
        assert 2 == self.rc_compl.df_compl.loc[0, "completion_log_complete"]

    def test_v1_start(self):
        v1_list = self.rc_compl.v1_start()
        for subj in self.chk_subj:
            assert subj in v1_list

    def test_v2_start(self):
        v2_list = self.rc_compl.v23_start(2)
        for subj in self.chk_subj:
            assert subj in v2_list

    def test_v3_start(self):
        v3_list = self.rc_compl.v23_start(3)
        for subj in self.chk_subj:
            assert subj in v3_list


class TestCheckStatus:

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.chk_stat = report_helper.CheckStatus()

    def test_class_attr(self):
        assert hasattr(self.chk_stat, "df_status")
        assert 6 == self.chk_stat.df_status.shape[1]

    def test_status_change_lost(self):
        # Check error raising
        with pytest.raises(ValueError):
            self.chk_stat.status_change("foo")

        # Check attr types
        self.chk_stat.status_change("lost")
        assert isinstance(self.chk_stat.all, dict)
        assert isinstance(self.chk_stat.visit1, dict)
        assert isinstance(self.chk_stat.visit2, dict)
        assert isinstance(self.chk_stat.visit3, dict)

        # Check attr values
        assert "Time Conflict" == self.chk_stat.all["ER0086"]
        assert "Unknown" == self.chk_stat.visit1["ER0190"]
        assert "Personal Preference" == self.chk_stat.visit2["ER0400"]
        assert "Unknown" == self.chk_stat.visit3["ER1137"]

    def test_status_change_excluded(self):
        self.chk_stat.status_change("excluded")
        assert "MRI Compatibility" == self.chk_stat.all["ER0411"]
        assert "Eligibility" == self.chk_stat.visit1["ER0620"]
        assert "BDI" == self.chk_stat.visit2["ER0162"]
        assert "EAE" == self.chk_stat.visit3["ER0103"]

    def test_status_change_withdrew(self):
        self.chk_stat.status_change("withdrew")
        assert isinstance(self.chk_stat.all, dict)

    def test_build_all(self):
        self.chk_stat.status_change("excluded")
        assert all(
            (
                self.chk_stat.all.get(key) == val
                for key, val in self.chk_stat.visit1.items()
            )
        )
        assert all(
            (
                self.chk_stat.all.get(key) == val
                for key, val in self.chk_stat.visit2.items()
            )
        )
        assert all(
            (
                self.chk_stat.all.get(key) == val
                for key, val in self.chk_stat.visit3.items()
            )
        )

    def test_add_status(self):
        # Check error raising
        df_test_valid = pd.DataFrame(data={"subj": [1]})
        with pytest.raises(KeyError):
            self.chk_stat.add_status(df_test_valid)
        with pytest.raises(ValueError):
            self.chk_stat.add_status(
                df_test_valid, subj_col="subj", status_list=["foo"]
            )

        # Check dataframe updating
        df = pd.DataFrame(
            data={
                "src_subject_id": [
                    "ER0009",
                    "ER0017",
                    "ER0086",
                    "ER0162",
                    "ER0962",
                ]
            }
        )
        df = self.chk_stat.add_status(df)
        df_chk = helper.status_change_dataframe()
        pd.testing.assert_frame_equal(df_chk, df)
