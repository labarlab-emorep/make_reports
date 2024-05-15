import pytest
import pandas as pd
from make_reports.resources import survey_download


@pytest.mark.rep_get
def test_dl_mri_log():
    df = survey_download.dl_mri_log()
    assert 2 == df.shape[1]
    assert ["datetime", "Visit"] == df.columns.tolist()
    assert ["day2", "day3"] == df.Visit.unique().tolist()


@pytest.mark.rep_get
def test_dl_completion_log():
    df = survey_download.dl_completion_log()
    assert 27 == df.shape[1]
    assert 1.0 == df.loc[4, "prescreening_completed"]
    assert 1.0 == df.loc[4, "payment_form_processed"]
    assert 2 == df.loc[4, "completion_log_complete"]


@pytest.mark.rep_get
def test_get_ids_redcap():
    with pytest.raises(ValueError):
        _ = survey_download._get_ids("foobar")

    rc_dict = survey_download._get_ids("redcap")
    assert "48980" == rc_dict["prescreen"]
    assert "48959" == rc_dict["guid"]
    assert "48985" == rc_dict["bdi_day3"]


@pytest.mark.rep_get
def test_get_ids_qualtrics():
    ql_dict = survey_download._get_ids("qualtrics")
    assert "duke" == ql_dict["organization_ID"]
    assert "ca1" == ql_dict["datacenter_ID"]
    assert "SV_emJra2UBm4Ulhqu" == ql_dict["EmoRep_Session_1"]


@pytest.mark.rep_get
def test_dl_redcap(fixt_dl_red):
    with pytest.raises(ValueError):
        _ = survey_download.dl_redcap(["foobar"])

    assert not fixt_dl_red.red_dict["demographics"][0]
    assert isinstance(fixt_dl_red.red_dict["demographics"][1], pd.DataFrame)
    assert "visit_day2" == fixt_dl_red.red_dict["bdi_day2"][0]
    assert isinstance(fixt_dl_red.red_dict["bdi_day2"][1], pd.DataFrame)


@pytest.mark.rep_get
def test_dl_qualtrics(fixt_dl_qual):
    with pytest.raises(ValueError):
        _ = survey_download.dl_qualtrics(["foobar"])

    assert "visit_day1" == fixt_dl_qual.qual_dict["EmoRep_Session_1"][0]
    assert "visit_day23" == fixt_dl_qual.qual_dict["Session 2 & 3 Survey"][0]
    assert isinstance(
        fixt_dl_qual.qual_dict["EmoRep_Session_1"][1], pd.DataFrame
    )
