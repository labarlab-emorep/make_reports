import pytest
import pandas as pd
from make_reports.resources import report_helper


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
    pass


def test_qualtrics_pat():
    pass


def test_check_sql_pass():
    pass


def test_pull_redcap_data():
    pass


def test_pull_qualtrics_data():
    pass


def test_mine_template():
    pass


def test_load_dataframes():
    pass


def test_calc_age_mo():
    pass


def test_get_survey_age():
    pass


def test_pilot_list():
    pass


def test_redcap_dict():
    pass


def test_qualtrics_dict():
    pass


def test_CheckIncomplete():
    pass


def test_RedCapComplete():
    pass


def test_CheckStatus():
    pass


def test_ParticipantComplete():
    pass


def test_AddStatus():
    pass
