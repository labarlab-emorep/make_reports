"""Title.

Desc.
"""
# %%
import pandas as pd
import numpy as np
import json
import importlib.resources as pkg_resources
from nda_upload import request_redcap
from nda_upload import reference_files


class MakeDemo:
    """Title.

    Desc.
    """

    def __init__(self, api_token):
        """Title.

        Desc.
        """
        # self.api_token = api_token
        with pkg_resources.open_text(
            reference_files, "report_keys.json"
        ) as jf:
            self.report_keys = json.load(jf)

        # Get consent dataframe, index, subjects
        self.df_consent = request_redcap.pull_data(
            api_token, self.report_keys["consent_new"]
        )
        self.idx_consent = self.df_consent.index[
            self.df_consent["consent_v2"] == 1.0
        ].tolist()
        self.subj_consent = self.df_consent.loc[
            self.idx_consent, "record_id"
        ].tolist()

        # Get guid dataframe, index of consented
        self.df_guid = request_redcap.pull_data(
            api_token, self.report_keys["guid"]
        )
        self.idx_guid = self.df_guid[
            self.df_guid["record_id"].isin(self.subj_consent)
        ].index.tolist()

        # Get demographic dataframe, index of consented
        self.df_demo = request_redcap.pull_data(
            api_token, self.report_keys["demographics"]
        )
        self.idx_demo = self.df_demo[
            self.df_demo["record_id"].isin(self.subj_consent)
        ].index.tolist()

    def _get_dob(self):
        """Title.

        Desc.
        """
        dob_switch = {"October 6 2000": "2000-10-06"}
        dob_orig = self.df_demo.loc[self.idx_demo, "dob"].tolist()
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

    def _get_age_mo(self, subj_dob, subj_consent_date):
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

    def _get_educate(self):
        """Title.

        Desc.
        """
        educate_switch = {2: 12, 4: 14, 5: 16, 7: 18, 8: 20}
        edu_year = self.df_demo.loc[self.idx_demo, "years_education"].tolist()
        edu_level = self.df_demo.loc[self.idx_demo, "level_education"].tolist()
        subj_educate = []
        for h_year, h_level in zip(edu_year, edu_level):
            if h_year.isnumeric():
                subj_educate.append(int(h_year))
            else:
                subj_educate.append(educate_switch[h_level])
        return subj_educate

    def _get_race(self):
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
            (self.df_demo["race___1"] == 1),
            (self.df_demo["race___2"] == 1),
            (self.df_demo["race___3"] == 1),
            (self.df_demo["race___4"] == 1),
            (self.df_demo["race___5"] == 1),
            (self.df_demo["race___6"] == 1),
            (self.df_demo["race___7"] == 1),
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
        self.df_demo["race_str"] = np.select(get_race_resp, set_race_str)
        idx_other = self.df_demo.index[self.df_demo["race___8"] == 1].tolist()
        race_other = [
            f"Other - {x}"
            for x in self.df_demo.loc[idx_other, "race_other"].tolist()
        ]
        self.df_demo.loc[idx_other, "race_str"] = race_other
        return self.df_demo.loc[self.idx_demo, "race_str"].tolist()

    def _get_ethnic_minority(self, subj_race):
        """Title.

        Desc.
        """
        # Get ethnicity
        h_ethnic = self.df_demo.loc[self.idx_demo, "ethnicity"].tolist()
        ethnic_switch = {
            1.0: "Hispanic or Latino",
            2.0: "Not Hispanic or Latino",
        }
        subj_ethnic = [ethnic_switch[x] for x in h_ethnic]

        # Determine if minority - not white or hispanic
        subj_minor = []
        for race, ethnic in zip(subj_race, subj_ethnic):
            if race != "White" and ethnic == "Not Hispanic or Latino":
                subj_minor.append("Minority")
            else:
                subj_minor.append("Not Minority")
        return (subj_ethnic, subj_minor)

    def make_complete(self):
        """Title.

        Desc.
        """
        # TODO Something here for subjs who withdraw consent

        # Get consent date
        self.df_consent["datetime"] = pd.to_datetime(
            self.df_consent["date_v2"]
        )
        self.df_consent["datetime"] = self.df_consent["datetime"].dt.date
        subj_consent_date = self.df_consent.loc[
            self.idx_consent, "datetime"
        ].tolist()

        # Get GUIDs, study ID for consented
        subj_guid = self.df_guid.loc[self.idx_guid, "guid"].tolist()
        subj_study = self.df_guid.loc[self.idx_guid, "study_id"].tolist()

        # Get age, sex
        subj_age = self.df_demo.loc[self.idx_demo, "age"].tolist()
        h_sex = self.df_demo.loc[self.idx_demo, "gender"].tolist()
        sex_switch = {1.0: "male", 2.0: "female", 3.0: "neither"}
        subj_sex = [sex_switch[x] for x in h_sex]

        # Get DOB, age in months, education
        subj_dob = self._get_dob()
        subj_age_mo = self._get_age_mo(subj_dob, subj_consent_date)
        subj_educate = self._get_educate()

        # Get race, ethnicity, minority status
        subj_race = self._get_race()
        subj_ethnic, subj_minor = self._get_ethnic_minority(subj_race)

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
        self.final_demo = pd.DataFrame(out_dict, columns=out_dict.keys())
