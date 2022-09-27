"""Download and clean survey data."""
import sys
import json
import io
import requests
import zipfile
import pandas as pd
import numpy as np
import importlib.resources as pkg_resources
from make_reports import report_helper
from make_reports import reference_files


class GetRedcapSurveys:
    """Download and clean RedCap surveys.

    Currently querries only the BDI reports from each scanning session.

    Parameters
    ----------
    redcap_token : str
        RedCap API token

    Attributes
    ----------
    df_clean_bdi = pd.DataFrame
        Cleaned BDI dataframe of specified visit/session
    df_raw_bdi2 = pd.DataFrame
        Raw BDI responses for session day2
    df_raw_bdi3 = pd.DataFrame
        Raw BDI responses for session day3
    report_keys_redcap : dict
        Survey name - key mappings

    Notes
    -----
    Requires reference_files.report_keys_redcap.json

    """

    def __init__(self, redcap_token):
        """Download RedCap BDI survey data.

        Parameters
        ----------
        redcap_token : str
            RedCap API token

        Attributes
        ----------
        df_raw_bdi2 = pd.DataFrame
            Raw BDI responses for session day2
        df_raw_bdi3 = pd.DataFrame
            Raw BDI responses for session day3

        """
        # Communicate
        print("\nPulling RedCap surveys ...")

        # Get BDI dataframes
        self.df_raw_bdi2 = report_helper.pull_redcap_data(
            redcap_token, self.report_keys_redcap["bdi_day2"]
        )
        self.df_raw_bdi3 = report_helper.pull_redcap_data(
            redcap_token, self.report_keys_redcap["bdi_day3"]
        )

    # Make self.report_keys_redcap immutable
    @property
    def report_keys_redcap(self):
        with pkg_resources.open_text(
            reference_files, "report_keys_redcap.json"
        ) as jf:
            return json.load(jf)

    def make_raw_reports(self, visit_name):
        """Find specified raw dataframe.

        Parameters
        ----------
        visit_name : str
            Name of session visit, e.g. "visit_day1"

        Returns
        -------
        pd.DataFrame
            Raw BDI dataframe of visit/session

        Raises
        ------
        ValueError
            If visit_name is not key in visit_dict

        """
        # Setup for finding raw dataframe
        visit_dict = {
            "visit_day2": "df_raw_bdi2",
            "visit_day3": "df_raw_bdi3",
        }
        if visit_name not in visit_dict.keys():
            raise ValueError(
                f"Inappropriate visit_name specified : {visit_name}"
            )

        # Get, return requested dataframe
        return getattr(self, visit_dict[visit_name])

    def make_clean_reports(self, visit_name, subj_consent):
        """Clean dataframes and organize.

        Dataframe reflects participants who have consented.

        Parameters
        ----------
        visit_name : str
            Name of session visit, e.g. "visit_day1"
        subj_consent : list
            List of participant IDs who have consented, see
            survey_download.GetRedcapDemographic.subj_consent

        Attributes
        ----------
        df_clean : pd.DataFrame
            Final dataframe of consented BDI data

        """
        # Get requested raw dataframe
        visit_dict = {
            "visit_day2": "df_raw_bdi2",
            "visit_day3": "df_raw_bdi3",
        }
        df_raw = getattr(self, visit_dict[visit_name])

        # Keep data for consented participants
        idx_consent = df_raw[
            df_raw["record_id"].isin(subj_consent)
        ].index.tolist()
        df_raw = df_raw.loc[idx_consent]

        # Reorganize columns and drop missing subject IDs
        df_raw = df_raw.drop("record_id", axis=1)
        col_names = df_raw.columns.tolist()
        col_reorder = col_names[-1:] + col_names[-2:-1] + col_names[:-2]
        df_raw = df_raw[col_reorder]
        df_raw = df_raw[df_raw["study_id"].notna()]
        self.df_clean_bdi = df_raw


class GetRedcapDemographic:
    """Make a dataframe of demographic info.

    Gather information from consent, guid, and demographic reports,
    combine relevant info for NDA submission and NIH/Duke reports.

    Parameters
    ----------
    redcap_token : str
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

    def __init__(self, redcap_token):
        """Get RedCap reports, combine.

        Uses report_keys_redcap.json from reference_files to match
        report name to report ID. Pull consent, GUID, and
        demographic reports. Find consented subjects.
        Trigger make_complete method.

        Parameters
        ----------
        redcap_token : str
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
        print("\nPulling RedCap demographic, guid, consent reports ...")

        # Load report keys
        with pkg_resources.open_text(
            reference_files, "report_keys_redcap.json"
        ) as jf:
            report_keys_redcap = json.load(jf)

        # Get original & new consent dataframes
        df_consent_orig = report_helper.pull_redcap_data(
            redcap_token, report_keys_redcap["consent_orig"]
        )
        df_consent_new = report_helper.pull_redcap_data(
            redcap_token, report_keys_redcap["consent_new"]
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
        self.df_guid = report_helper.pull_redcap_data(
            redcap_token, report_keys_redcap["guid"]
        )
        self.idx_guid = self.df_guid[
            self.df_guid["record_id"].isin(self.subj_consent)
        ].index.tolist()

        # Purge subjects who do not have GUID from idx, subj lists
        h_subj_guid = self.df_guid.loc[self.idx_guid, "guid"].tolist()
        idx_nan = np.where(pd.isnull(h_subj_guid))[0].tolist()
        if idx_nan:
            self.idx_guid = [
                x for idx, x in enumerate(self.idx_guid) if idx not in idx_nan
            ]
            self.idx_consent = [
                x
                for idx, x in enumerate(self.idx_consent)
                if idx not in idx_nan
            ]
            self.subj_consent = [
                x
                for idx, x in enumerate(self.subj_consent)
                if idx not in idx_nan
            ]

        # Get demographic dataframe, index of consented
        self.df_demo = report_helper.pull_redcap_data(
            redcap_token, report_keys_redcap["demographics"]
        )
        self.idx_demo = self.df_demo[
            self.df_demo["record_id"].isin(self.subj_consent)
        ].index.tolist()

        # Run methods
        print("Compiling needed demographic info ...")
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

            # Calculate years, months, and days
            num_years = doc.year - dob.year
            num_months = doc.month - dob.month
            num_days = doc.day - dob.day

            # Adjust for day-month wrap around
            if num_days < 0:
                num_days += 30

            # Avoid including current partial month
            if doc.day < dob.day:
                num_months -= 1

            # Adjust including current partial year
            while num_months < 0:
                num_months += 12
                num_years -= 1

            # Add month if participant is older than num_months
            # plus 15 days.
            if num_days >= 15:
                num_months += 1

            # Convert all to months, add to list
            total_months = (12 * num_years) + num_months
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
        sex_switch = {1.0: "Male", 2.0: "Female", 3.0: "Neither"}
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


class GetQualtricsSurveys:
    """Download and clean Qualtrics surveys.

    Download, organize, and clean Qualtrics surveys for each visit. Write
    original (raw) session dataframes, and cleaned dataframes for each
    individual survey.

    Parameters
    ----------
    qualtrics_token : str
        Qualtrics API token

    Attributes
    ----------
    clean_visit : dict
        Keys = name of survey
        Values = survey dataframe
    df_raw_visit1 : pd.DataFrame
        Original dataframe for session/visit 1
    df_raw_visit23 : pd.DataFrame
        Original dataframe for sessions/visits 2 & 3
    df_raw_post : pd.DataFrame
        Original dataframe for post-scan
    surveys_visit1 : list
        Survey names for session/visit 1
    surveys_visit23 : list
        Survey names for sessions/visits 2 & 3
    qualtrics_token : str
        Qualtrics API token

    """

    def __init__(self, qualtrics_token):
        """Download Qualtrics surveys.

        Parameters
        ----------
        qualtrics_token : str
            Qualtrics API token

        Attributes
        ----------
        df_raw_visit1 : pd.DataFrame
            Original dataframe for session/visit 1
        df_raw_visit23 : pd.DataFrame
            Original dataframe for sessions/visits 2 & 3
        df_raw_post : pd.DataFrame
            Original dataframe for post-scan
        surveys_visit1 : list
            Survey names for session/visit 1
        surveys_visit23 : list
            Survey names for sessions/visits 2 & 3
        qualtrics_token : str
            Qualtrics API token

        """
        self.qualtrics_token = qualtrics_token

        # Specify visit survey keys
        self.surveys_visit1 = [
            "ALS",
            "AIM",
            "ERQ",
            "PSWQ",
            "RRS",
            "STAI",
            "TAS",
        ]
        self.surveys_visit23 = ["PANAS", "STAI_State"]

        # Get visit dataframes
        self.df_raw_visit1 = self._pull_qualtrics_data(self.name_visit1)
        self.df_raw_visit23 = self._pull_qualtrics_data(self.name_visit23)
        self.df_raw_post = self._pull_qualtrics_data(self.name_post)

        # Setup mapping to find attributes by visit name
        self.visit_map = {
            "visit_day1": ["df_raw_visit1", "name_visit1"],
            "visit_day2": ["df_raw_visit23", "name_visit23"],
            "visit_day3": ["df_raw_visit23", "name_visit23"],
            "post_scan_ratings": ["df_raw_post", "name_post"],
        }

    # Make survey names, report keys immutable
    @property
    def name_visit1(self):
        return "EmoRep_Session_1"

    @property
    def name_visit23(self):
        return "Session 2 & 3 Survey"

    @property
    def name_post(self):
        return "FINAL - EmoRep Stimulus Ratings - fMRI Study"

    @property
    def report_keys_qualtrics(self):
        with pkg_resources.open_text(
            reference_files, "report_keys_qualtrics.json"
        ) as jf:
            return json.load(jf)

    def _pull_qualtrics_data(
        self,
        survey_name,
    ):
        """Pull a Qualtrics report and make a pandas dataframe.

        References guide at
            https://api.qualtrics.com/ZG9jOjg3NzY3Nw-new-survey-response-export-guide

        Parameters
        ----------
        survey_name : str
            Qualtrics survey name

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        TimeoutError
            If response export progress takes too long

        """
        # Get ids
        survey_id = self.report_keys_qualtrics[survey_name]
        dc_id = self.report_keys_qualtrics["datacenter_ID"]

        # Setting static parameters
        request_check_progress = 0.0
        progress_status = "inProgress"
        url = (
            f"https://{dc_id}.qualtrics.com/API/v3/surveys/{survey_id}"
            + "/export-responses/"
        )
        headers = {
            "content-type": "application/json",
            "x-api-token": self.qualtrics_token,
        }

        # Create data export
        #   "useLabels": True
        #   "seenUnansweredRecode": 999
        data = {"format": "csv"}
        download_request_response = requests.request(
            "POST", url, json=data, headers=headers
        )
        # print(download_request_response.json())
        try:
            progressId = download_request_response.json()["result"][
                "progressId"
            ]
        except KeyError:
            print(download_request_response.json())
            sys.exit(2)

        # Check on data export progress and wait until export is ready.
        # TODO beautify print out
        is_file = None
        while (
            progress_status != "complete"
            and progress_status != "failed"
            and is_file is None
        ):
            if not is_file:
                print(f"Progress status for {survey_name} : {progress_status}")

            # Check progress
            request_check_url = url + progressId
            request_check_response = requests.request(
                "GET", request_check_url, headers=headers
            )
            try:
                is_file = request_check_response.json()["result"]["fileId"]
            except KeyError:
                pass

            # Print out progress, update progress_status
            # print(request_check_response.json())
            request_check_progress = request_check_response.json()["result"][
                "percentComplete"
            ]
            print(f"Download is {str(request_check_progress)} complete")
            progress_status = request_check_response.json()["result"]["status"]

        # Check for error
        if progress_status == "failed":
            raise Exception("export failed")
        file_id = request_check_response.json()["result"]["fileId"]

        # Download requested survey file
        request_download_url = url + file_id + "/file"
        request_download = requests.request(
            "GET", request_download_url, headers=headers, stream=True
        )

        # Extract compressed file
        req_file_zipped = io.BytesIO(request_download.content)
        with zipfile.ZipFile(req_file_zipped) as req_file:
            with req_file.open(f"{survey_name}.csv") as f:
                df = pd.read_csv(f)
        print(f"\n\t Successfully downloaded : {survey_name}.csv\n")
        return df

    def make_raw_reports(self, visit_name):
        """Access the requested raw dataframe.

        Parameters
        ----------
        visit_name : str
            [visit_day1 | visit_day2 | visit_day3 | post_scan_ratings]
            Session/day of visit corresponds to EmoRep data_survey
            directory organization.

        Returns
        -------
        tuple
            [0] = name of master session survey
            [1] = pd.DataFrame

        Raises
        ------
        ValueError
            If visit_name is not a key in visit_dict

        """
        # Setup switch for accessing appropriate attribute given visit_name.
        if visit_name not in self.visit_map.keys():
            raise ValueError(f"Inappropriate visit name : {visit_name}")

        # Get appropriate survey name, dataframe
        visit_dict = self.visit_map
        survey_name = getattr(self, visit_dict[visit_name][1])
        df_out = getattr(self, visit_dict[visit_name][0])
        return (survey_name, df_out)

    def _clean_visit_day1(self):
        """Make clean dataframes of visit 1 surveys.

        Split master session dataframe into individual surveys, clean up
        and setup dictionary of dataframes.

        Attributes
        ----------
        clean_visit : dict
            Keys = name of survey, from surveys_visit1 attribute
            Values = survey dataframe

        """
        # Setup and identify column names
        print(f"\nCleaning raw survey data : {self.name_visit1}")
        subj_cols = ["SubID"]
        self.df_raw_visit1.rename(
            {"RecipientLastName": subj_cols[0]}, axis=1, inplace=True
        )
        col_names = self.df_raw_visit1.columns

        # Subset dataframe by survey key
        data_clean = {}
        for sur_key in self.surveys_visit1:
            print(f"\tCleaning survey data : day1, {sur_key}")
            sur_cols = [x for x in col_names if sur_key in x]
            ext_cols = subj_cols + sur_cols
            df_sub = self.df_raw_visit1[ext_cols]
            df_sub = df_sub.fillna("NaN")

            # Clean subset dataframe, writeout
            df_sub = df_sub[df_sub[subj_cols[0]].str.contains("ER")]
            df_sub = df_sub.sort_values(by=[subj_cols[0]])
            data_clean[sur_key] = df_sub
            del df_sub
        self.clean_visit = data_clean

    def _clean_visit_day23(self, visit_name):
        """Make clean dataframes of visits 2 & 3 surveys.

        Split master session dataframe into individual surveys, clean up
        and setup dictionary of dataframes.

        Parameters
        ----------
        visit_name : str
            [visit_day2 | visit_day3]
            Session/day of visit corresponds to EmoRep data_survey
            directory organization.

        Attributes
        ----------
        clean_visit : dict
            Keys = name of survey, from surveys_visit23 attribute
            Values = survey dataframe

        Raises
        ------
        ValueError
            If visit_name is not visit_day2 or visit_day3

        """
        # Identify session, how session is coded in raw data
        day_dict = {"day2": "1", "day3": "2"}
        day_str = visit_name.split("_")[1]
        if day_str not in day_dict.keys():
            raise ValueError(f"Inapproproiate visit_name : {visit_name}")
        print(f"Cleaning raw survey data : {day_str}")
        day_code = day_dict[day_str]

        # Get dataframe and setup output column names
        subj_cols = ["SubID", "Session_Num"]
        df_raw_visit23 = self.df_raw_visit23
        col_names = df_raw_visit23.columns

        # Get relevant info from dataframe
        data_clean = {}
        for sur_key in self.surveys_visit23:
            print(f"\tCleaning survey data : {day_str}, {sur_key}")
            sur_cols = [x for x in col_names if sur_key in x]
            ext_cols = subj_cols + sur_cols
            df_sub = df_raw_visit23[ext_cols]
            df_sub = df_sub.fillna("NaN")

            # Organize rows and columns, get visit-specific responses
            df_sub = df_sub[df_sub[subj_cols[0]].str.contains("ER")]
            df_sub[subj_cols[1]] = np.where(
                df_sub[subj_cols[1]] == day_code, day_str, df_sub[subj_cols[1]]
            )
            df_sub = df_sub[df_sub[subj_cols[1]].str.contains(day_str)]
            df_sub = df_sub.sort_values(by=[subj_cols[0]])
            data_clean[sur_key] = df_sub
            del df_sub
        del df_raw_visit23
        self.clean_visit = data_clean

    def _clean_post_scan_ratings(self, visit_name):
        """Title

        Desc.

        Parameters
        ----------
        visit_name : str
            [post_scan_ratings]
            Session/day of visit corresponds to EmoRep data_survey
            directory organization.

        Attributes
        ----------
        clean_visit : dict
            Keys = name of survey
            Values = survey dataframe

        """
        pass

    def make_clean_reports(self, visit_name):
        """Clean survey data for visit.

        Identify and use appropriate cleaning method for visit name.
        Private cleaning methods construct clean_data attribute.

        Parameters
        ----------
        visit_name : str
            [visit_day1 | visit_day2 | visit_day3 | post_scan_ratings]
            Session/day of visit corresponds to EmoRep data_survey
            directory organization.

        """
        # Check visit name
        if visit_name not in self.visit_map.keys():
            raise ValueError(f"Inapproproiate visit_name : {visit_name}")

        # Use appropriate cleaning method
        if visit_name == "visit_day2" or visit_name == "visit_day3":
            self._clean_visit_day23(visit_name)
        else:
            clean_method = getattr(self, f"_clean_{visit_name}")
            clean_method()
