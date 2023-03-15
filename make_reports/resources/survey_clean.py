"""Clean survey data from RedCap and Qualtrics."""
import os
import string
import glob
import re
import pandas as pd
import numpy as np
from string import punctuation
from make_reports import report_helper
import importlib.resources as pkg_resources
from make_reports import reference_files


class CleanRedcap:
    """Clean RedCap surveys.

    Find downloaded original/raw RedCap survey responses, and
    convert values into usable dataframe types and formats. Participants
    who have withdraw consent are included in the cleaned dataframes
    for NIH/Duke reporting purposes.

    Attributes
    ----------
    df_clean : pd.DataFrame
        Cleaned survey responses for study participants
    df_pilot : pd.DataFrame
        Cleaned survey responses for pilot participants

    Methods
    -------
    clean_surveys(survey_name)
        Load original survey data and coordinate cleaning methods.

    """

    def __init__(self, proj_dir):
        """Set helper attributes.

        Parameters
        ----------
        proj_dir : path
            Location of parent directory for project

        Attributes
        ----------
        _pilot_list : make_reports.report_helper.pilot_list
            Pilot participants
        _proj_dir : path
            Location of parent directory for project
        _redcap_dict : make_reports.report_helper.redcap_dict
            Mapping of survey name to directory organization

        """
        self._proj_dir = proj_dir
        self._redcap_dict = report_helper.redcap_dict()
        self._pilot_list = report_helper.pilot_list()

    def clean_surveys(self, survey_name):
        """Clean original RedCap survey data.

        Wrapper method, find data_raw csv dataframes and coordinate
        appropriate cleaning methods for survey.

        Parameters
        ----------
        survey_name : str, make_reports.report_helper.redcap_dict key
            Name of RedCap survey

        Attributes
        ----------
        _df_raw : pd.DataFrame
            Original RedCap survey data

        Raises
        ------
        FileNotFoundError
            When dataframe not encountered in data_raw

        """
        # Load raw data
        raw_file = os.path.join(
            self._proj_dir,
            "data_survey",
            self._redcap_dict[survey_name],
            "data_raw",
            f"df_{survey_name}_latest.csv",
        )
        if not os.path.exists(raw_file):
            raise FileNotFoundError(f"Failed to find {raw_file}.")
        self._df_raw = pd.read_csv(raw_file)

        # Get and run cleaning method
        print(f"Cleaning survey : {survey_name}")
        if survey_name == "bdi_day2" or survey_name == "bdi_day3":
            self._clean_bdi_day23()
        elif survey_name == "consent_v1.22" or survey_name == "consent_pilot":
            self._clean_consent()
        else:
            clean_method = getattr(self, f"_clean_{survey_name}")
            clean_method()

    def _dob_convert(self, dob_list):
        """Helper method for _clean_demographics.

        Resolves participant free date-of-birth responses by
        interpreting and converting them into datetime objects.

        Parameters
        ----------
        dob_list : list
            Participant date-of-birth responses

        Returns
        -------
        list
            Participant date-of-birth datetime obj

        Raises
        ------
        ValueError
            When numeric dob response does not match expected formats
        TypeError
            When dob response does not match expected formats

        """
        # Manage nesting nightmare with inner function
        def _num_convert(dob):
            """Interpret numeric dates."""
            # Attempt to parse date string, account for formats
            # 20000606, 06062000, and 6062000.
            if dob[:2] == "19" or dob[:2] == "20":
                date_c, date_b, date_a = dob[:4], dob[4:6], dob[6:]
            elif len(dob) == 8:
                date_a, date_b, date_c = dob[:2], dob[2:4], dob[4:]
            elif len(dob) == 7:
                date_a, date_b, date_c = dob[:1], dob[1:3], dob[3:]
            else:
                raise ValueError(f"Unrecognized format date response: {dob}.")

            # Convert parsed dates, account for formats
            # 06152000 and 15062000.
            if int(date_a) < 13:
                return pd.to_datetime(f"{date_a}-{date_b}-{date_c}").date()
            else:
                return pd.to_datetime(f"{date_b}-{date_a}-{date_c}").date()

        # Set switch for extra special cases
        dob_switch = {"October 6 2000": "2000-10-06"}

        # Convert each dob free response or redcap datetime
        dob_clean = []
        for dob in dob_list:
            if "/" in dob or "-" in dob:
                dob_clean.append(
                    pd.to_datetime(dob, infer_datetime_format=True).date()
                )
            elif dob.isnumeric():
                dob_clean.append(_num_convert(dob))
            elif dob in dob_switch:
                dob_clean.append(pd.to_datetime(dob_switch[dob]).date())
            else:
                raise TypeError(f"Unrecognized datetime str: {dob}.")
        return dob_clean

    def _get_educ_years(self):
        """Get participant education level.

        Use info from years_education column when they are numeric,
        otherwise use educate_switch to convert from level_education
        to number of years.

        Returns
        -------
        list
            Years of participant education (int)

        """
        # Convert education level to years
        educate_switch = {2: 12, 3: 13, 4: 14, 5: 16, 6: 17, 7: 18, 8: 20}

        # Get education level, and self-report of years educated
        edu_year = self._df_raw["years_education"].tolist()
        edu_level = self._df_raw["level_education"].tolist()
        record_id = self._df_raw["record_id"].tolist()

        # Convert into years (deal with self-reports)
        subj_educate = []
        for h_year, h_level, h_id in zip(edu_year, edu_level, record_id):
            # Patch for 1984 education issue
            if h_year == "1984":
                subj_educate.append(educate_switch[8])
            else:
                try:
                    subj_educate.append(int(h_year))
                except ValueError:
                    subj_educate.append(educate_switch[h_level])
        return subj_educate

    def _clean_city_state(self):
        """Fix city-of-birth free response.

        Account for formats:
            1. San Jose
            2. San Jose, California
            4. San Jose CA

        """
        self._df_raw = self._df_raw.rename(columns={"city": "city-state"})
        city_state = self._df_raw["city-state"].tolist()
        city_list = []
        state_list = []
        for cs in city_state:
            cs = cs.strip()
            if "," in cs:
                h_city, h_st = cs.split(",")
                h_state = h_st.lstrip()
            elif " " in cs:
                h_split = cs.rsplit(" ", 1)[-1]
                if h_split.isupper():
                    h_state = h_split
                    h_city = cs.rsplit(" ", 1)[0]
                else:
                    h_city = cs
                    h_state = np.nan
            else:
                h_city = cs
                h_state = np.nan
            city_list.append(h_city)
            state_list.append(h_state)

        self._df_raw["city"] = city_list
        self._df_raw["state"] = state_list

        self._df_raw = self._df_raw.drop("city-state", axis=1)
        col_reorder = [
            "record_id",
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
        ]
        self._df_raw = self._df_raw[col_reorder]

    def _clean_demographics(self):
        """Cleaning method for RedCap demographics survey.

        Convert RedCap report values and participant responses
        to usable dataframe types.

        Attributes
        ----------
        df_clean : pd.DataFrame
            Clean demographic info for study participants
        df_pilot : pd.DataFrame
            Clean demographic info for pilot participants

        """
        # Reorder columns, drop rows without last name response
        col_names = self._df_raw.columns.tolist()
        col_reorder = col_names[-1:] + col_names[-2:-1] + col_names[:-2]
        self._df_raw = self._df_raw[col_reorder]
        self._df_raw = self._df_raw[self._df_raw["lastname"].notna()]

        # Convert DOB response to datetime
        dob_list = self._df_raw["dob"].tolist()
        dob_clean = self._dob_convert(dob_list)
        self._df_raw["dob"] = dob_clean

        # Fix years of education
        yrs_edu = self._get_educ_years()
        self._df_raw["years_education"] = yrs_edu

        # Fix City, State of birth
        self._clean_city_state()

        # Clean middle name responses for NA and special characters
        df_raw = self._df_raw
        na_mask = (
            df_raw.middle_name.eq("na")
            | df_raw.middle_name.eq("NA")
            | df_raw.middle_name.eq("N/A")
            | df_raw.middle_name.eq(" ")
        )
        df_raw.loc[na_mask, ["middle_name"]] = np.nan

        special_list = [x for x in punctuation]
        sp_mask = (df_raw["middle_name"].str.len() == 1) & df_raw[
            "middle_name"
        ].astype("str").isin(special_list)
        df_raw.loc[sp_mask, ["middle_name"]] = np.nan

        # Separate pilot from study data
        pilot_list = [int(x[-1]) for x in self._pilot_list]
        idx_pilot = df_raw[df_raw["record_id"].isin(pilot_list)].index.tolist()
        idx_study = df_raw[
            ~df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]

    def _clean_consent(self):
        """Cleaning method for RedCap consent survey.

        Only return participants who signed the consent form.

        Attributes
        ----------
        df_clean : pd.DataFrame
            Clean consent info for study participants
        df_pilot : pd.DataFrame
            Clean consent info for pilot participants

        """
        # Drop rows without signature, account for multiple consent forms
        col_names = self._df_raw.columns.tolist()
        col_drop = (
            "signature_v2" if "signature_v2" in col_names else "signature"
        )
        df_raw = self._df_raw[self._df_raw[col_drop].notna()]

        # Separate pilot from study data
        pilot_list = [int(x[-1]) for x in self._pilot_list]
        idx_pilot = df_raw[df_raw["record_id"].isin(pilot_list)].index.tolist()
        idx_study = df_raw[
            ~df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]

    def _clean_guid(self):
        """Cleaning method for RedCap GUID survey.

        Only return participants who have an assigned GUID.

        Attributes
        ----------
        df_clean : pd.DataFrame
            Clean guid info for study participants
        df_pilot : pd.DataFrame
            Clean guid info for pilot participants

        """
        # Drop rows without guid
        df_raw = self._df_raw[self._df_raw["guid"].notna()]

        # Separate pilot from study data
        idx_pilot = df_raw[
            df_raw["study_id"].isin(self._pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self._pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]

    def _clean_bdi_day23(self):
        """Cleaning method for RedCap BDI surveys.

        Attributes
        ----------
        df_clean : pd.DataFrame
            Clean bdi info for study participants
        df_pilot : pd.DataFrame
            Clean bdi info for pilot participants

        """
        # Rename then remove unneeded columns, reorder
        drop_list = ["guid_timestamp", "redcap_survey_identifier"]
        df_raw = self._df_raw.drop(drop_list, axis=1)
        if "q_1_v2" in df_raw.columns.tolist():
            df_raw.columns = df_raw.columns.str.replace("_v2", "")
        df_raw.columns = df_raw.columns.str.replace("q_", "BDI_")

        col_names = df_raw.columns.tolist()
        col_reorder = col_names[:1] + col_names[-1:] + col_names[1:-1]
        df_raw = df_raw[col_reorder]

        # Remove rows without responses or study_id (from guid survey)
        df_raw = df_raw[df_raw["study_id"].notna()]
        df_raw = df_raw[df_raw["BDI_1"].notna()]

        # Enforce datetime format
        col_rename = (
            "bdi_visit_2_timestamp"
            if "bdi_visit_2_timestamp" in col_names
            else "bdi_visit_3_timestamp"
        )
        df_raw.rename(
            {col_rename: "datetime"},
            axis=1,
            inplace=True,
        )
        df_raw["datetime"] = pd.to_datetime(df_raw["datetime"])
        df_raw["datetime"] = df_raw["datetime"].dt.strftime("%Y-%m-%d")

        # Separate pilot from study data
        idx_pilot = df_raw[
            df_raw["study_id"].isin(self._pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self._pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]


class CleanQualtrics:
    """Clean Qualtrics surveys.

     Find downloaded original/raw Qualtrics survey responses, and
     convert values into usable dataframe tyeps and formats.

    Attributes
    ----------
    data_clean : dict
        Cleaned survey data of study participant responses
        {survey_name: pd.DataFrame}, or
        {visit: {survey_name: pd.DataFrame}}
    data_pilot : dict
        Cleaned survey data of pilot participant responses
        {survey_name: pd.DataFrame}, or
        {visit: {survey_name: pd.DataFrame}}

    Methods
    -------
    clean_surveys(survey_name)
        Load original survey data and coordinate cleaning methods.

    """

    def __init__(self, proj_dir):
        """Set helper attributes.

        Parameters
        ----------
        proj_dir : path
            Location of parent directory for project

        Attributes
        ----------
        _pilot_list : make_reports.report_helper.pilot_list
            Pilot participants
        _proj_dir : path
            Location of parent directory for project
        _qualtrics_dict : make_reports.report_helper.qualtrics_dict
            Mapping of survey name to directory organization
        _withdrew_list : make_reports.report_helper.withdrew_list
            Participant who have withdrawn from study

        """
        self._proj_dir = proj_dir
        self._qualtrics_dict = report_helper.qualtrics_dict()
        self._pilot_list = report_helper.pilot_list()
        self._withdrew_list = report_helper.withdrew_list()

    def clean_surveys(self, survey_name):
        """Split and clean original Qualtrics survey data.

        Find original omnibus session data and coordinate cleaning methods.

        Parameters
        ----------
        survey_name : str, make_reports.report_helper.qualtrics_dict key
            Name of Qualtrics survey session

        Attributes
        ----------
        _df_raw : pd.DataFrame
            Original qualtrics survey session data

        Raises
        ------
        FileNotFoundError
            When dataframe not encountered in data_raw

        """
        # Find data_raw path, visit_day2 has raw qualtrics for both
        # visit_day2 and visit_day3
        visit_raw = (
            "visit_day2"
            if survey_name != "EmoRep_Session_1"
            else self._qualtrics_dict[survey_name]
        )
        raw_file = os.path.join(
            self._proj_dir,
            "data_survey",
            visit_raw,
            "data_raw",
            f"{survey_name}_latest.csv",
        )
        if not os.path.exists(raw_file):
            raise FileNotFoundError(f"Failed to find {raw_file}.")
        self._df_raw = pd.read_csv(raw_file)

        # Get and run cleaning method
        print(f"Cleaning survey : {survey_name}")
        meth_switch = {
            "EmoRep_Session_1": "session_1",
            "Session 2 & 3 Survey": "session_23",
            "FINAL - EmoRep Stimulus Ratings "
            + "- fMRI Study": "post_scan_ratings",
        }
        clean_method = getattr(self, f"_clean_{meth_switch[survey_name]}")
        clean_method()

    def _clean_session_1(self):
        """Cleaning method for visit 1 surveys.

        Split session into individual surveys, convert participant
        responses to usable dataframe values and types.

        Attributes
        ----------
        data_clean : dict
            Cleaned survey data of study participant responses
            {survey_name: pd.DataFrame}
        data_pilot : dict
            Cleaned survey data of pilot participant responses
            {survey_name: pd.DataFrame}

        """
        # Identify surveys in visit1 session
        surveys_visit1 = [
            "ALS",
            "AIM",
            "ERQ",
            "PSWQ",
            "RRS",
            "STAI_Trait",
            "TAS",
        ]

        # Setup and identify column names
        subj_cols = ["study_id", "datetime"]
        self._df_raw.rename(
            {"RecipientLastName": subj_cols[0], "StartDate": subj_cols[1]},
            axis=1,
            inplace=True,
        )
        col_names = self._df_raw.columns

        # Subset session dataframe by survey
        data_clean = {}
        data_pilot = {}
        for sur_name in surveys_visit1:
            print(f"\tCleaning survey data : day1, {sur_name}")
            sur_cols = [x for x in col_names if sur_name in x]
            ext_cols = subj_cols + sur_cols
            df_sur = self._df_raw[ext_cols]

            # Clean subset dataframe
            df_sur = df_sur.dropna()
            df_sur = df_sur[df_sur[subj_cols[0]].str.contains("ER")]
            df_sur = df_sur.sort_values(by=[subj_cols[0]])

            # Enforce datetime format
            df_sur["datetime"] = pd.to_datetime(df_sur["datetime"])
            df_sur["datetime"] = df_sur["datetime"].dt.strftime("%Y-%m-%d")

            # Drop first duplicate and withdrawn participant responses
            df_sur = df_sur.drop_duplicates(subset="study_id", keep="last")
            df_sur = df_sur[
                ~df_sur.study_id.str.contains("|".join(self._withdrew_list))
            ]
            df_sur = df_sur.reset_index(drop=True)

            # Separate pilot from study data
            idx_pilot = df_sur[
                df_sur["study_id"].isin(self._pilot_list)
            ].index.tolist()
            idx_study = df_sur[
                ~df_sur["study_id"].isin(self._pilot_list)
            ].index.tolist()
            df_pilot = df_sur.loc[idx_pilot]
            df_clean = df_sur.loc[idx_study]

            # Update dictionaries
            data_pilot[sur_name] = df_pilot
            data_clean[sur_name] = df_clean
            del df_sur
            del df_pilot
            del df_clean

        self.data_clean = data_clean
        self.data_pilot = data_pilot

    def _clean_session_23(self):
        """Cleaning method for visit 2 & 3 surveys.

        Split session into individual surveys, convert participant
        responses to usable dataframe values and types. Keep visit
        day straight.

        Attributes
        ----------
        data_clean : dict
            Cleaned survey data of study participant responses
            {visit: {survey_name: pd.DataFrame}}
        data_pilot : dict
            Cleaned survey data of pilot participant responses
            {visit: {survey_name: pd.DataFrame}}

        """
        # Identify surveys in visit1 session and how session is coded
        surveys_visit23 = ["PANAS", "STAI_State"]
        day_dict = {"day2": "1", "day3": "2"}

        # Get dataframe and setup output column names
        subj_cols = ["study_id", "visit", "datetime"]
        self._df_raw.rename(
            {
                "SubID": subj_cols[0],
                "Session_Num": subj_cols[1],
                "StartDate": subj_cols[2],
            },
            axis=1,
            inplace=True,
        )
        col_names = self._df_raw.columns

        # Get relevant info from dataframe for each day
        data_clean = {}
        data_pilot = {}
        for day_str, day_code in day_dict.items():
            data_clean[f"visit_{day_str}"] = {}
            data_pilot[f"visit_{day_str}"] = {}
            for sur_key in surveys_visit23:
                print(f"\tCleaning survey data : {day_str}, {sur_key}")
                sur_cols = [x for x in col_names if sur_key in x]
                ext_cols = subj_cols + sur_cols
                df_sub = self._df_raw[ext_cols]

                # Organize rows and columns, get visit-specific responses
                df_sub = df_sub.dropna()
                df_sub = df_sub[df_sub[subj_cols[0]].str.contains("ER")]
                df_sub[subj_cols[1]] = np.where(
                    df_sub[subj_cols[1]] == day_code,
                    day_str,
                    df_sub[subj_cols[1]],
                )
                df_sub = df_sub[df_sub[subj_cols[1]].str.contains(day_str)]
                df_sub = df_sub.sort_values(by=[subj_cols[0]])

                # Enforce datetime format
                df_sub["datetime"] = pd.to_datetime(df_sub["datetime"])
                df_sub["datetime"] = df_sub["datetime"].dt.strftime("%Y-%m-%d")

                # Drop first duplicate and withdrawn participant responses
                df_sub = df_sub.drop_duplicates(subset="study_id", keep="last")
                df_sub = df_sub[
                    ~df_sub.study_id.str.contains(
                        "|".join(self._withdrew_list)
                    )
                ]
                df_sub = df_sub.reset_index(drop=True)

                # Separate pilot from study data
                idx_pilot = df_sub[
                    df_sub["study_id"].isin(self._pilot_list)
                ].index.tolist()
                idx_study = df_sub[
                    ~df_sub["study_id"].isin(self._pilot_list)
                ].index.tolist()
                df_pilot = df_sub.loc[idx_pilot]
                df_clean = df_sub.loc[idx_study]

                # Update dicts
                data_clean[f"visit_{day_str}"][sur_key] = df_clean
                data_pilot[f"visit_{day_str}"][sur_key] = df_pilot
                del df_sub
                del df_clean
                del df_pilot

        self.data_clean = data_clean
        self.data_pilot = data_pilot

    def _clean_post_scan_ratings(self):
        """Cleaning method for stimuli ratings.

        Mine "FINAL - EmoRep Stimulus Ratings - fMRI Study" and generate
        a long-formatted dataframe of participant responses. Stimuli order
        and name are stored in "txtFile" column values, and mappings of
        Qualtrics stimulus names with LabrLab's are found in
        make_reports.reference_files.EmoRep_PostScan_Task_<*>_2022.csv.
        Participants give valence and arousal ratings for each stimulus, and
        supply 1+ emotion endorsements.

        Attributes
        ----------
        data_clean : dict
            Cleaned survey data of study participant responses
            {visit: {survey_name: pd.DataFrame}}
        data_pilot : dict
            Cleaned survey data of pilot participant responses
            {visit: {survey_name: pd.DataFrame}}

        """
        print("\tCleaning survey data : post_scan_ratings")

        # Get scenario reference file for mapping prompt to stimulus
        with pkg_resources.open_text(
            reference_files, "EmoRep_PostScan_Task_ScenarioIDs_2022.csv"
        ) as rf:
            _df_keys_scenario = pd.read_csv(rf, index_col="Qualtrics_ID")
        _keys_scenarios = _df_keys_scenario.to_dict()["Stimulus_ID"]

        # Remove punctuation, setup reference dictionary:
        #   key = qualtrics id, value = stimulus id.
        keys_scenarios = {}
        for h_key, h_val in _keys_scenarios.items():
            new_key = h_key.translate(
                str.maketrans("", "", string.punctuation)
            )
            keys_scenarios[new_key] = h_val

        # Get video reference file, setup reference dict
        with pkg_resources.open_text(
            reference_files, "EmoRep_PostScan_Task_VideoIDs_2022.csv"
        ) as rf:
            _df_keys_video = pd.read_csv(rf, index_col="Qualtrics_ID")
        keys_videos = _df_keys_video.to_dict()["Stimulus_ID"]

        # Remove header rows, test IDs, txtFile NaNs, and restarted
        # sessions (Finished=False).
        df_clean = self._df_raw
        df_clean = df_clean.drop([0, 1])
        df_clean = df_clean[~df_clean["SubID"].str.contains("ER9")]
        df_clean = df_clean[df_clean["txtFile"].notna()]
        df_clean = df_clean[~df_clean["Finished"].str.contains("False")]
        df_clean = df_clean.reset_index(drop=True)

        # Remove some unneeded columns
        drop_list = [
            "Status",
            "IPAddress",
            "Recipient",
            "ExternalReference",
            "Location",
            "Click",
            "Categories",
            "Submit",
            "Duration",
            "EndDate",
            "ResponseID",
            "DistributionChannel",
            "StimulusFile_Size",
            "ScenInstruct",
        ]
        for drop_name in drop_list:
            drop_cols = [x for x in df_clean.columns if drop_name in x]
            df_clean = df_clean.drop(drop_cols, axis=1)

        # Extract subject, session, and stimulus type columns
        sub_list = df_clean["SubID"].tolist()
        sess_list = df_clean["SessionID"].tolist()

        # Unpack txtFile column to get list and order of stimuli that
        # were presented to participant.
        stim_all = df_clean["txtFile"].tolist()
        stim_all = [x.replace("<br>", " ") for x in stim_all]
        stim_unpack = [re.split(r"\t+", x.rstrip("\t")) for x in stim_all]

        # Setup output dataframe
        out_names = [
            "study_id",
            "datetime",
            "session",
            "type",
            "emotion",
            "stimulus",
            "prompt",
            "response",
        ]
        df_study = pd.DataFrame(columns=out_names)
        df_pilot = pd.DataFrame(columns=out_names)

        # Setup inner function for mining df_clean
        def _fill_psr(sub, sess, stim_list):
            """Find participant responses and fill dataframe."""
            print(f"\t\tGetting data for : {sub} {sess} ...")

            # Subset df_raw for subject, session data. Remove empty columns.
            df_sub = df_clean.loc[
                (df_clean["SubID"] == sub) & (df_clean["SessionID"] == sess)
            ]
            df_sub = df_sub.dropna(axis=1)

            # Identify session, datetime, and type values
            day = f"day{int(sess) + 1}"
            sur_date = df_sub.iloc[0]["RecordedDate"].split(" ")[0]
            stim_type = df_sub.iloc[0]["StimulusType"]

            # Get relevant reference dictionary for stimulus type, remove
            # punctuation from scenario prompts and deal with \u2019.
            if stim_type == "Scenarios":
                ref_dict = keys_scenarios
                stim_list = [
                    x.translate(
                        str.maketrans("", "", string.punctuation)
                    ).replace("can’t", "cant")
                    for x in stim_list
                ]
            else:
                ref_dict = keys_videos

            # Add a number lines to df_study for each stimulus
            nonlocal df_study
            for h_cnt, stim in enumerate(stim_list):

                # Determine stimulus number in qualtrics, extract
                # emotion and trial stimulus from reference dict.
                cnt = h_cnt + 1
                emotion, trial_stim = ref_dict[stim].split("_")

                # Add a line to df_study for each prompt type
                for prompt in ["Arousal", "Valence", "Endorsement"]:

                    # Get response value from proper column, manage multiple,
                    # single, or non responses.
                    df_resp = df_sub.loc[
                        :,
                        df_sub.columns.str.startswith(f"{cnt}_{prompt}"),
                    ]
                    if df_resp.empty:
                        resp = np.nan
                    elif df_resp.shape == (1, 1):
                        resp = df_resp.values[0][0]
                    else:
                        resp = ";".join(df_resp.values[0])

                    # Add stimulus prompt values to df_study
                    update_dict = {
                        "study_id": sub,
                        "datetime": sur_date,
                        "session": day,
                        "type": stim_type,
                        "emotion": emotion,
                        "stimulus": trial_stim,
                        "prompt": prompt,
                        "response": resp,
                    }
                    df_study = pd.concat(
                        [
                            df_study,
                            pd.DataFrame.from_records([update_dict]),
                        ]
                    )
            del df_sub

        # Update df_study with each participant's responses
        for sub, sess, stim_list in zip(sub_list, sess_list, stim_unpack):
            if sub in self._withdrew_list:
                continue
            _fill_psr(sub, sess, stim_list)
        df_study = df_study.sort_values(
            by=["study_id", "session", "emotion", "stimulus", "prompt"]
        )

        # Split dataframe by session
        sess_mask = df_study["session"] == "day2"
        df_study_day2 = df_study[sess_mask]
        df_study_day3 = df_study[~sess_mask]

        # Make ouput attributes, just use df_pilot as filler
        self.data_clean = {
            "visit_day2": {"post_scan_ratings": df_study_day2},
            "visit_day3": {"post_scan_ratings": df_study_day3},
        }
        self.data_pilot = {
            "visit_day2": {"post_scan_ratings": df_pilot},
            "visit_day3": {"post_scan_ratings": df_pilot},
        }


class CombineRestRatings:
    """Aggregate post resting ratings.

    Find and combine subject rest ratings, output by dcm_conversion.

    Attributes
    ----------
    df_sess : pd.DataFrame
        Aggregated rest ratings for session

    Methods
    -------
    get_rest_ratings(sess, rawdata_path)
        Aggregate session rest ratings

    """

    def __init__(self, proj_dir):
        """Setup helper attributes.

        Parameters
        ----------
        proj_dir : path
            Location of parent directory for project

        Attributes
        ----------
        _sess_valid : list
            Valid session days

        """
        self._sess_valid = ["day2", "day3"]

    def get_rest_ratings(self, sess, rawdata_path):
        """Find and aggregate participant rest ratings for session.

        Parameters
        ----------
        sess : str
            ["day2" | "day3"]
        rawdata_path : path
            Location of BIDS rawdata

        Attributes
        ----------
        df_sess : pd.DataFrame
            Aggregated rest ratings for session

        Raises
        ------
        ValueError
            Invalid sess argument supplied
        FileNotFoundError
            Participant rest-rating files are not found

        """
        # Check arg
        if sess not in self._sess_valid:
            raise ValueError(f"Improper session argument : sess={sess}")

        # Setup session dataframe
        col_names = [
            "study_id",
            "visit",
            "task",
            "datetime",
            "resp_type",
            "AMUSEMENT",
            "ANGER",
            "ANXIETY",
            "AWE",
            "CALMNESS",
            "CRAVING",
            "DISGUST",
            "EXCITEMENT",
            "FEAR",
            "HORROR",
            "JOY",
            "NEUTRAL",
            "ROMANCE",
            "SADNESS",
            "SURPRISE",
        ]
        df_sess = pd.DataFrame(columns=col_names)

        # Find all session files
        beh_search_path = f"{rawdata_path}/sub-*/ses-{sess}/beh"
        beh_list = sorted(glob.glob(f"{beh_search_path}/*rest-ratings*.tsv"))
        if not beh_list:
            raise FileNotFoundError(
                f"No rest-ratings files found in {beh_search_path}."
                + "\n\tTry running dcm_conversion."
            )

        # Add each participant's responses to df_sess
        for beh_path in beh_list:

            # Get session info
            beh_file = os.path.basename(beh_path)
            subj, sess, _, date_ext = beh_file.split("_")
            subj_str = subj.split("-")[1]
            date_str = date_ext.split(".")[0]

            # Determine sesion stimulus type by finding a BIDS
            # events file.
            search_path = os.path.join(rawdata_path, subj, sess, "func")
            try:
                task_path = glob.glob(f"{search_path}/*_run-02_events.tsv")[0]
                _, _, task, _, _ = os.path.basename(task_path).split("_")
            except (IndexError):
                print(
                    f"\n\t\tNo run-02 BIDS event file detected for {subj}, "
                    + f"{sess}. Continuing ..."
                )
                continue

            # Get data, organize for concat with df_sess
            df_beh = pd.read_csv(beh_path, sep="\t", index_col="prompt")
            df_beh_trans = df_beh.T
            df_beh_trans.reset_index(inplace=True)
            df_beh_trans = df_beh_trans.rename(columns={"index": "resp_type"})
            df_beh_trans["study_id"] = subj_str
            df_beh_trans["visit"] = sess.split("-")[-1]
            df_beh_trans["datetime"] = date_str
            df_beh_trans["task"] = task.split("-")[-1]

            # Add info to df_sess
            df_sess = pd.concat([df_sess, df_beh_trans], ignore_index=True)
            del df_beh, df_beh_trans

        self.df_sess = df_sess
