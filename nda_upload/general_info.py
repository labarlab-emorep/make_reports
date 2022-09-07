"""Title.

Desc.
"""
# %%
import pandas as pd
import numpy as np
from nda_upload import request_redcap


# class DetermineSubjs:
#     """Title.

#     Desc.
#     """

# def __init__(self, report_keys, api_token):
#     """Title.

#     Desc.
#     """
#     self.api_token = api_token
#     self.report_keys = report_keys

# %%
def _get_basic():
    """Title.

    Desc.
    """
    df_consent = request_redcap.pull_data(
        api_token, report_keys["consent_new"]
    )
    df_guid = request_redcap.pull_data(api_token, report_keys["guid"])
    df_demo = request_redcap.pull_data(api_token, report_keys["demographics"])

    # For testing
    return (df_consent, df_guid, df_demo)


# %%
def _get_race(df_demo, idx_demo):
    """Title.

    Desc.
    """
    # Get race
    race_switch = {
        1: "American Indian or Alaska Native",
        2: "Asian",
        3: "Black or African-American",
        4: "White",
        5: "Native Hawaiian or Other Pacific Islander",
        6: "More than one race",
        7: "Unknown or not reported",
    }
    get_race_resp = [
        (df_demo["race___1"] == 1),
        (df_demo["race___2"] == 1),
        (df_demo["race___3"] == 1),
        (df_demo["race___4"] == 1),
        (df_demo["race___5"] == 1),
        (df_demo["race___6"] == 1),
        (df_demo["race___7"] == 1),
    ]
    set_race_str = [
        race_switch[1],
        race_switch[2],
        race_switch[3],
        race_switch[4],
        race_switch[5],
        race_switch[6],
        race_switch[7],
    ]
    df_demo["race_str"] = np.select(get_race_resp, set_race_str)
    idx_other = df_demo.index[df_demo["race___8"] == 1].tolist()
    race_other = [
        f"Other - {x}" for x in df_demo.loc[idx_other, "race_other"].tolist()
    ]
    df_demo.loc[idx_other, "race_str"] = race_other
    subj_race = df_demo.loc[idx_demo, "race_str"].tolist()
    return subj_race


# %%
def _get_dob(df_demo, idx_demo):
    """Title.

    Desc.
    """
    dob_switch = {"October 6 2000": "2000-10-06"}
    dob_orig = df_demo.loc[idx_demo, "dob"].tolist()
    subj_dob = []
    for dob in dob_orig:
        if "/" in dob or "-" in dob:
            subj_dob.append(
                pd.to_datetime(dob, infer_datetime_format=True).date()
            )
        elif dob.isnumeric():
            subj_dob.append(
                pd.to_datetime(f"{dob[:2]}-{dob[2:4]}-{dob[4:]}").date()
            )
        elif dob in dob_switch:
            subj_dob.append(pd.to_datetime(dob_switch[dob]).date())
        else:
            raise TypeError(
                f"Unrecognized datetime str: {dob}. "
                + "Check general_info.make_complete."
            )
    return subj_dob


# %%
def _get_age_mo(subj_dob, subj_consent_date):
    """Title.

    Desc.
    """
    subj_age_mo = []
    for dob, doc in zip(subj_dob, subj_consent_date):
        num_years = doc.year - dob.year
        num_months = doc.month - dob.month
        if doc.day < dob.day:
            num_months -= 1
        while num_months < 0:
            num_months += 12
            num_years -= 1
        total_months = (12 * num_years) + num_months

        # Use John's day method
        diff_days = doc.day - dob.day
        if diff_days < 0:
            total_months -= 1
        subj_age_mo.append(total_months)
    return subj_age_mo


# %%
def make_complete():
    """Title.

    Desc.
    """
    # self._get_basic()
    df_consent, df_guid, df_demo = _get_basic()

    # Determine who consented
    idx_consent = df_consent.index[df_consent["consent_v2"] == 1.0].tolist()
    subj_consent = df_consent.loc[idx_consent, "record_id"].tolist()

    # TODO Something here for subjs who withdraw consent

    # Get consent date
    df_consent["datetime"] = pd.to_datetime(df_consent["date_v2"])
    df_consent["datetime"] = df_consent["datetime"].dt.date
    subj_consent_date = df_consent.loc[idx_consent, "datetime"].tolist()

    # Get GUIDs, study ID for consented
    idx_guid = df_guid[df_guid["record_id"].isin(subj_consent)].index.tolist()
    subj_guid = df_guid.loc[idx_guid, "guid"].tolist()
    subj_study = df_guid.loc[idx_guid, "study_id"].tolist()

    # Get age
    idx_demo = df_demo[df_demo["record_id"].isin(subj_consent)].index.tolist()
    subj_age = df_demo.loc[idx_demo, "age"].tolist()

    # Get sex
    h_sex = df_demo.loc[idx_demo, "gender"].tolist()
    sex_switch = {1.0: "male", 2.0: "female", 3.0: "neither"}
    subj_sex = [sex_switch[x] for x in h_sex]

    # Get DOB, age in months, race
    subj_dob = _get_dob(df_demo, idx_demo)
    subj_age_mo = _get_age_mo(subj_dob, subj_consent_date)
    subj_race = _get_race(df_demo, idx_demo)

    # Get ethnicity
    h_ethnic = df_demo.loc[idx_demo, "ethnicity"].tolist()
    ethnic_switch = {1.0: "Hispanic or Latino", 2.0: "Not Hispanic or Latino"}
    subj_ethnic = [ethnic_switch[x] for x in h_ethnic]

    # Determine if minority - not white or hispanic
    subj_minor = []
    for race, ethnic in zip(subj_race, subj_ethnic):
        if race != "White" and ethnic == "Not Hispanic or Latino":
            subj_minor.append("Minority")
        else:
            subj_minor.append("Not Minority")

    # Get education
    # TODO enforce int
    subj_educate = df_demo.loc[idx_demo, "years_education"].tolist()

    # Write dataframe
    out_dict = {
        "subjectkey": subj_guid,
        "src_subject_id": subj_study,
        "interview_date": subj_consent_date,
        "interview_age": subj_age_mo,
        "sex": subj_sex,
        "age": subj_age,
        "dob": subj_dob,
        "ethnicity": subj_ethnic,
        "race": subj_race,
        "is_minority": subj_minor,
        "years_education": subj_educate,
    }
    df_out = pd.DataFrame(out_dict, columns=out_dict.keys())
    return df_out
