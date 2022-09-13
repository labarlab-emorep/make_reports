"""Get general participant information."""
import pandas as pd
import numpy as np
import json
import importlib.resources as pkg_resources
from nda_upload import report_helper
from nda_upload import reference_files


class MakeDemographic:
    """Make a dataframe of demographic info.

    Gather information from consent, guid, and demographic reports,
    combine relevant info for NDA submission and NIH/Duke reports.

    Parameters
    ----------
    api_token : str
        RedCap API token

    Attributes
    ----------
    df_consent : pd.DataFrame
        Merged dataframe of original and new consent report
    df_demo : pd.DataFrame
        Participant demographic information
    df_guid : pd.DataFrame
        Participant GUID information
    idx_consent : list
        Indices of consent responses in df_consent
    idx_demo : list
        Indices of consented subjects in df_demo
    idx_guid : list
        Indices of consented subjects in df_guid
    subj_consent : list
        Subjects who consented
    final_demo : pd.DataFrame
        Complete report containing demographic info for NDA submission

    """

    def __init__(self, api_token):
        """Get RedCap reports, combine.

        Uses report_keys.json from reference_files to match
        report name to report ID. Pull consent, GUID, and
        demographic reports. Find consented subjects.
        Trigger make_complete method.

        Parameters
        ----------
        api_token : str
            RedCap API token

        Attributes
        ----------
        df_consent : pd.DataFrame
            Merged dataframe of original and new consent report
        df_demo : pd.DataFrame
            Participant demographic information
        df_guid : pd.DataFrame
            Participant GUID information
        idx_consent : list
            Indices of consent responses in df_consent
        idx_demo : list
            Indices of consented subjects in df_demo
        idx_guid : list
            Indices of consented subjects in df_guid
        subj_consent : list
            Subjects who consented

        """
        # Communicate
        print("Starting demographic, guid, consent pull ...")

        # Load report keys
        with pkg_resources.open_text(
            reference_files, "report_keys.json"
        ) as jf:
            report_keys = json.load(jf)

        # Get original & new consent dataframes
        df_consent_orig = report_helper.pull_data(
            api_token, report_keys["consent_orig"]
        )
        df_consent_new = report_helper.pull_data(
            api_token, report_keys["consent_new"]
        )

        # Update consent_new column names, merge
        cols_new = df_consent_new.columns.tolist()
        cols_orig = df_consent_orig.columns.tolist()
        cols_replace = {}
        for h_new, h_orig in zip(cols_new, cols_orig):
            cols_replace[h_new] = h_orig
        df_consent_new = df_consent_new.rename(columns=cols_replace)
        self.df_consent = df_consent_new.combine_first(df_consent_orig)

        # Get index, subjects that consented
        self.idx_consent = self.df_consent.index[
            self.df_consent["consent"] == 1.0
        ].tolist()
        self.subj_consent = self.df_consent.loc[
            self.idx_consent, "record_id"
        ].tolist()

        # Get guid dataframe, index of consented
        self.df_guid = report_helper.pull_data(api_token, report_keys["guid"])
        self.idx_guid = self.df_guid[
            self.df_guid["record_id"].isin(self.subj_consent)
        ].index.tolist()

        # Get demographic dataframe, index of consented
        self.df_demo = report_helper.pull_data(
            api_token, report_keys["demographics"]
        )
        self.idx_demo = self.df_demo[
            self.df_demo["record_id"].isin(self.subj_consent)
        ].index.tolist()

        # Run methods
        self.make_complete()

    def _get_dob(self):
        """Get participants' date of birth.

        Get date-of-birth info from df_demo column "dob", and
        convert to datetime. Account for forward slash, dash,
        only numeric, and alpha numeric response methods.

        Returns
        -------
        list
            Subject dob datetimes

        Raises
        ------
        TypeError
            If dob value is not slash, dash, numeric, or in dob_switch
        """
        # Set switch for special cases
        dob_switch = {"October 6 2000": "2000-10-06"}

        # Get column values
        dob_orig = self.df_demo.loc[self.idx_demo, "dob"].tolist()

        # Solve column values, append list
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
                    + "Check general_info._get_dob."
                )
        return subj_dob

    def _get_age_mo(self, subj_dob, subj_consent_date):
        """Calculate age in months.

        Convert each participant's age at consent into
        age in months. Use the John day method for dealing
        with number of days, for consistency with previous
        submissions.

        Parameters
        ----------
        subj_dob : list
            Subjects' date-of-birth datetimes
        subj_consent_date : list
            Subjects' date-of-consent datetimes

        Returns
        -------
        list
            Participant ages in months (int)
        """
        subj_age_mo = []
        for dob, doc in zip(subj_dob, subj_consent_date):

            # Calculate years and months
            num_years = doc.year - dob.year
            num_months = doc.month - dob.month

            # Avoid including current partial month
            if doc.day < dob.day:
                num_months -= 1

            # Adjust months, years to account for partial years
            while num_months < 0:
                num_months += 12
                num_years -= 1

            # Convert all to months
            total_months = (12 * num_years) + num_months

            # Use John's day method to deal with half months
            diff_days = doc.day - dob.day
            if diff_days < 0:
                total_months -= 1
            subj_age_mo.append(total_months)
        return subj_age_mo

    def _get_educate(self):
        """Get participant education level.

        Use info from years_education column of df_demo when they are numeric,
        otherwise use the educate_switch to convert from level of education
        to number of years.

        Returns
        -------
        list
            Number of years completed of education (int)
        """
        # Convert education level to years
        educate_switch = {2: 12, 4: 14, 5: 16, 7: 18, 8: 20}

        # Get education level, and self-report of years educated
        edu_year = self.df_demo.loc[self.idx_demo, "years_education"].tolist()
        edu_level = self.df_demo.loc[self.idx_demo, "level_education"].tolist()

        # Convert into years (deal with self-reports)
        subj_educate = []
        for h_year, h_level in zip(edu_year, edu_level):
            # Patch for 1984 education issue
            if h_year == "1984":
                subj_educate.append(educate_switch[8])
            elif h_year.isnumeric():
                subj_educate.append(int(h_year))
            else:
                subj_educate.append(educate_switch[h_level])
        return subj_educate

    def _get_race(self):
        """Get participant race response.

        Account for single response, single response of
        multiple, multiple responses (which may not include
        the multiple option), and "other" responses.

        Returns
        -------
        list
            Participant responses to race question
        """
        # Get race response - deal with "More than one" (6) and
        # "Other" (8) separately.
        race_switch = {
            1: "American Indian or Alaska Native",
            2: "Asian",
            3: "Black or African-American",
            4: "White",
            5: "Native Hawaiian or Other Pacific Islander",
            7: "Unknown or not reported",
        }
        get_race_resp = [
            (self.df_demo["race___1"] == 1),
            (self.df_demo["race___2"] == 1),
            (self.df_demo["race___3"] == 1),
            (self.df_demo["race___4"] == 1),
            (self.df_demo["race___5"] == 1),
            (self.df_demo["race___7"] == 1),
        ]
        set_race_str = [
            race_switch[1],
            race_switch[2],
            race_switch[3],
            race_switch[4],
            race_switch[5],
            race_switch[7],
        ]

        # Get race responses, set to new column
        self.df_demo["race_resp"] = np.select(get_race_resp, set_race_str)

        # Capture "Other" responses, stitch response together
        idx_other = self.df_demo.index[self.df_demo["race___8"] == 1].tolist()
        race_other = [
            f"Other - {x}"
            for x in self.df_demo.loc[idx_other, "race_other"].tolist()
        ]
        self.df_demo.loc[idx_other, "race_resp"] = race_other

        # Capture "More than one race" responses, write to df_demo["race_more"]
        idx_more = self.df_demo.index[self.df_demo["race___6"] == 1].tolist()
        self.df_demo["race_more"] = np.nan
        self.df_demo.loc[idx_more, "race_more"] = "More"

        # Capture multiple race responses (in case option 6 not selected)
        col_list = [
            "race___1",
            "race___2",
            "race___3",
            "race___4",
            "race___5",
            "race___7",
            "race___8",
        ]
        self.df_demo["race_sum"] = self.df_demo[col_list].sum(axis=1)
        idx_mult = self.df_demo.index[self.df_demo["race_sum"] > 1].tolist()
        self.df_demo.loc[idx_mult, "race_more"] = "More"

        # Update race_resp col with responses in df_demo["race_more"]
        idx_more = self.df_demo.index[
            self.df_demo["race_more"] == "More"
        ].tolist()
        self.df_demo.loc[idx_more, "race_resp"] = "More than one race"
        race_resp = self.df_demo.loc[self.idx_demo, "race_resp"].tolist()
        return race_resp

    def _get_ethnic_minority(self, subj_race):
        """Determine if participant is considered a minority.

        Parameters
        ----------
        subj_race : list
            Participant race responses

        Returns
        -------
        tuple
            [0] list of participants' ethnicity status
            [1] list of whether participants' are minority
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
        """Make a demographic dataframe with all needed information.

        Pull relevant data from consent, GUID, and demographic reports
        to compile data for all participants in RedCap who have consented.

        Attributes
        ----------
        final_demo : pd.DataFrame
            Complete report containing demographic info for NDA submission
        """
        # TODO Something here for subjs who withdraw consent

        # Get consent date - solve for multiple consent forms
        self.df_consent["datetime"] = pd.to_datetime(self.df_consent["date"])
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
        sex_switch = {1.0: "Male", 2.0: "Female", 3.0: "Unknown"}
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
