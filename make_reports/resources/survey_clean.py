"""Clean survey data from RedCap and Qualtrics.

CleanRedcap : organize, clean REDCap survey responses
CleanQualtrics : organize, clean Qualtrics survey responses
clean_rest_ratings : aggregate post-rest responses into dataframe

"""
import os
import string
import glob
import re
import pandas as pd
import numpy as np
from string import punctuation
from make_reports.resources import report_helper
import importlib.resources as pkg_resources
from make_reports import reference_files


class CleanRedcap:
    """Clean RedCap surveys.

    Clean REDCap surveys and reports. Participants who have
    withdrawn consent are included in the Consent, GUID, and
    demographic dataframes for NIH/Duke reporting purposes but are
    removed from the BDI dataframes.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project directory
    pilot_list : list
        Pilot participant IDs

    Attributes
    ----------
    df_study : pd.DataFrame
        Cleaned survey responses for study participants
    df_pilot : pd.DataFrame
        Cleaned survey responses for pilot participants

    Methods
    -------
    clean_bdi_day23()
        Generate clean attrs for day2 and day3 BDI data
    clean_consent()
        Generate clean attrs for consent data
    clean_demographics()
        Generate clean attrs for demographic data
    clean_guid()
        Generate clean attrs for GUID report

    """

    def __init__(self, proj_dir, pilot_list):
        """Initialize."""
        self._proj_dir = proj_dir
        self._pilot_list = pilot_list

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

        # Set switch for extra special cases, wrong DOB entered
        dob_switch = {
            "October 6 2000": "2000-10-06",
            "2023-01-05": "1998-07-30",
        }

        # Convert each dob free response or redcap datetime
        dob_clean = []
        for dob in dob_list:
            if dob in dob_switch:
                dob_clean.append(pd.to_datetime(dob_switch[dob]).date())
            elif "/" in dob or "-" in dob:
                dob_clean.append(
                    pd.to_datetime(dob, infer_datetime_format=True).date()
                )
            elif dob.isnumeric():
                dob_clean.append(_num_convert(dob))
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
        year_str_switch = {
            "1984": 20,
            "PhD degree (18 years of school in total)": 18,
            "Undergraduate Degree": 16,
        }

        # Get education level, and self-report of years educated
        edu_year = self._df_raw["years_education"].tolist()
        edu_level = self._df_raw["level_education"].tolist()
        record_id = self._df_raw["record_id"].tolist()

        # Convert education into years
        subj_educate = []
        for _year, _level, _id in zip(edu_year, edu_level, record_id):
            # Patch education level issues, deal with self-reports
            if _year in year_str_switch.keys():
                edu_value = year_str_switch[_year]
            else:
                try:
                    edu_value = int(_year)
                except ValueError:
                    edu_value = educate_switch[_level]

            # Adjust for self-report years vs education level
            if edu_value <= 12 and _level >= 3:
                edu_value = educate_switch[_level]

            subj_educate.append(edu_value)
        return subj_educate

    def _clean_city_state(self):
        """Fix city-of-birth free response.

        Account for formats:
            - San Jose
            - San Jose, California
            - San Jose CA

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

    def _clean_middle_name(self):
        """Clean middle name field.

        Accounts for:
            - Any single special char
            - Period after middle initial
            - space, na, NA, n/a, N/A

        """
        # Clean middle name responses for NA
        na_mask = (
            self._df_raw.middle_name.eq("na")
            | self._df_raw.middle_name.eq("NA")
            | self._df_raw.middle_name.eq("n/a")
            | self._df_raw.middle_name.eq("N/A")
            | self._df_raw.middle_name.eq(" ")
        )
        self._df_raw.loc[na_mask, ["middle_name"]] = np.nan

        # Clean middle name single special chars
        special_list = [x for x in punctuation]
        sp_mask = (self._df_raw["middle_name"].str.len() == 1) & self._df_raw[
            "middle_name"
        ].astype("str").isin(special_list)
        self._df_raw.loc[sp_mask, ["middle_name"]] = np.nan

        # Drop period after initials
        self._df_raw["middle_name"] = self._df_raw["middle_name"].str.replace(
            ".", "", regex=False
        )

    def _res_idx(self):
        """Reset df_[pilot|study] indices."""
        self.df_study.reset_index(drop=True, inplace=True)
        self.df_pilot.reset_index(drop=True, inplace=True)

    def clean_demographics(self, df_raw):
        """Cleaning method for RedCap demographics survey.

        Convert RedCap report values and participant responses
        to usable dataframe types.

        Parameters
        ----------
        df_raw : pd.DataFrame
            Original REDCap export of demographic survey

        Attributes
        ----------
        df_study : pd.DataFrame
            Clean demographic info for study participants
        df_pilot : pd.DataFrame
            Clean demographic info for pilot participants

        """
        self._df_raw = df_raw

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
        self._df_raw["years_education"] = self._get_educ_years()

        # Fix place of birth, middle name responses
        self._clean_city_state()
        self._clean_middle_name()

        # Drop participants
        self._df_raw = self._drop_subj(self._df_raw)

        # Separate pilot from study data
        pilot_list = [int(x[-1]) for x in self._pilot_list]
        idx_pilot = self._df_raw[
            self._df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        idx_study = self._df_raw[
            ~self._df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        self.df_pilot = self._df_raw.loc[idx_pilot]
        self.df_study = self._df_raw.loc[idx_study]
        self._res_idx()

    def clean_consent(self, df_raw):
        """Cleaning method for RedCap consent survey.

        Only return participants who signed the consent form.

        Parameters
        ----------
        df_raw : pd.DataFrame
            Original REDCap export of consent survey

        Attributes
        ----------
        df_study : pd.DataFrame
            Clean consent info for study participants
        df_pilot : pd.DataFrame
            Clean consent info for pilot participants

        """
        # Drop rows without signature, account for multiple consent forms
        col_names = df_raw.columns.tolist()
        col_drop = (
            "signature_v2" if "signature_v2" in col_names else "signature"
        )
        df_raw = df_raw[df_raw[col_drop].notna()]

        # Drop participants
        df_raw = self._drop_subj(df_raw)

        # Separate pilot from study data
        pilot_list = [int(x[-1]) for x in self._pilot_list]
        idx_pilot = df_raw[df_raw["record_id"].isin(pilot_list)].index.tolist()
        idx_study = df_raw[
            ~df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_study = df_raw.loc[idx_study]
        self._res_idx()

    def clean_guid(self, df_raw):
        """Cleaning method for RedCap GUID survey.

        Only return participants who have an assigned GUID.

        Parameters
        ----------
        df_raw : pd.DataFrame
            Original REDCap export of GUID survey

        Attributes
        ----------
        df_study : pd.DataFrame
            Clean guid info for study participants
        df_pilot : pd.DataFrame
            Clean guid info for pilot participants

        """
        # Drop rows without guid
        df_raw = df_raw[df_raw["guid"].notna()]

        # Drop participants
        df_raw = self._drop_subj(df_raw)

        # Separate pilot from study data
        idx_pilot = df_raw[
            df_raw["study_id"].isin(self._pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self._pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_study = df_raw.loc[idx_study]
        self._res_idx()

    def clean_bdi_day23(self, df_raw):
        """Cleaning method for RedCap BDI surveys.

        Parameters
        ----------
        df_raw : pd.DataFrame
            Original REDCap export of BDI survey

        Attributes
        ----------
        df_study : pd.DataFrame
            Clean bdi info for study participants
        df_pilot : pd.DataFrame
            Clean bdi info for pilot participants

        """
        # Rename then remove unneeded columns, reorder
        drop_list = ["guid_timestamp", "redcap_survey_identifier"]
        df_raw = df_raw.drop(drop_list, axis=1)
        if "q_1_v2" in df_raw.columns.tolist():
            df_raw.columns = df_raw.columns.str.replace("_v2", "")
        df_raw.columns = df_raw.columns.str.replace("q_", "BDI_")

        col_names = df_raw.columns.tolist()
        col_reorder = col_names[:1] + col_names[-1:] + col_names[1:-1]
        df_raw = df_raw[col_reorder]

        # Remove rows without responses or study_id (from guid survey)
        df_raw = df_raw[df_raw["study_id"].notna()]
        df_raw = df_raw[df_raw["BDI_1"].notna()]

        # Drop participants
        df_raw = self._drop_subj(df_raw)

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

        # Drop participants who have withdrawn consent
        part_comp = report_helper.CheckStatus()
        part_comp.status_change("withdrew")
        if part_comp.all:
            withdrew_list = [int(x[2:]) for x in part_comp.all.keys()]
            df_raw = df_raw[~df_raw["study_id"].isin(withdrew_list)]
            df_raw = df_raw.reset_index(drop=True)

        # Separate pilot from study data
        idx_pilot = df_raw[
            df_raw["study_id"].isin(self._pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self._pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_study = df_raw.loc[idx_study]
        self._res_idx()

    def _drop_subj(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop certain subjects from dataframe."""
        drop_list = [80]
        for subj in drop_list:
            df = report_helper.drop_participant(subj, df, "record_id")
        return df


class CleanQualtrics:
    """Clean Qualtrics surveys.

    Find downloaded original/raw Qualtrics survey responses, and
    convert values into usable dataframe tyeps and formats.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project directory
    pilot_list : list
        Pilot participant IDs
    withdrew_list : list
        Participants IDs who withdrew consent

    Attributes
    ----------
    data_study : dict
        Cleaned survey data of study participant responses
        {visit: {survey_name: pd.DataFrame}}
    data_pilot : dict
        Cleaned survey data of pilot participant responses
        {visit: {survey_name: pd.DataFrame}}

    Methods
    -------
    clean_session_1()
        Organize and clean session 1 surveys
    clean_session_23()
        Organize and clean surveys for sessions 2, 3
    clean_postscan_ratings()
        Organize and clean post-scan stimulus ratings

    """

    def __init__(self, proj_dir, pilot_list, withdrew_list):
        """Initialize."""
        self._proj_dir = proj_dir
        self._pilot_list = pilot_list
        self._withdrew_list = withdrew_list

    def clean_session_1(self, df_raw):
        """Cleaning method for visit 1 surveys.

        Split session into individual surveys, convert participant
        responses to usable dataframe values and types.

        Parameters
        ----------
        df_raw : pd.DataFrame
            Original Qualtrics session 1 export

        Attributes
        ----------
        data_study : dict
            Cleaned survey data of study participant responses
            {visit: {survey_name: pd.DataFrame}}
        data_pilot : dict
            Cleaned survey data of pilot participant responses
            {visit: {survey_name: pd.DataFrame}}

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
        df_raw.rename(
            {"RecipientLastName": subj_cols[0], "StartDate": subj_cols[1]},
            axis=1,
            inplace=True,
        )
        df_raw = report_helper.drop_participant("ER0080", df_raw, "study_id")
        col_names = df_raw.columns

        # Subset session dataframe by survey
        data_study = {}
        data_pilot = {}
        for sur_name in surveys_visit1:
            print(f"Cleaning survey data : day1, {sur_name}")
            sur_cols = [x for x in col_names if sur_name in x]
            ext_cols = subj_cols + sur_cols
            df_sur = df_raw[ext_cols]

            # Clean subset dataframe
            df_sur = df_sur.dropna()
            df_sur = df_sur[df_sur[subj_cols[0]].str.contains("ER")]
            df_sur = df_sur.sort_values(by=[subj_cols[0]])

            # Enforce datetime format
            df_sur["datetime"] = pd.to_datetime(df_sur["datetime"])
            df_sur["datetime"] = df_sur["datetime"].dt.strftime("%Y-%m-%d")

            # Drop first duplicate and withdrawn participant responses
            df_sur = df_sur.drop_duplicates(subset="study_id", keep="last")
            if self._withdrew_list:
                df_sur = df_sur[
                    ~df_sur.study_id.str.contains(
                        "|".join(self._withdrew_list)
                    )
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
            df_study = df_sur.loc[idx_study]

            # Update dictionaries
            data_pilot[sur_name] = df_pilot
            data_study[sur_name] = df_study
            del df_sur
            del df_pilot
            del df_study

        self.data_study = {"visit_day1": data_study}
        self.data_pilot = {"visit_day1": data_pilot}

    def clean_session_23(self, df_raw):
        """Cleaning method for visit 2 & 3 surveys.

        Split session into individual surveys, convert participant
        responses to usable dataframe values and types. Keep visit
        day straight.

        Parameters
        ----------
        df_raw : pd.DataFrame
            Original Qualtrics session 2 & 3 export

        Attributes
        ----------
        data_study : dict
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
        df_raw.rename(
            {
                "SubID": subj_cols[0],
                "Session_Num": subj_cols[1],
                "StartDate": subj_cols[2],
            },
            axis=1,
            inplace=True,
        )
        df_raw = report_helper.drop_participant("ER0080", df_raw, "study_id")
        col_names = df_raw.columns

        # Get relevant info from dataframe for each day
        data_study = {}
        data_pilot = {}
        for day_str, day_code in day_dict.items():
            data_study[f"visit_{day_str}"] = {}
            data_pilot[f"visit_{day_str}"] = {}
            for sur_key in surveys_visit23:
                print(f"Cleaning survey data : {day_str}, {sur_key}")
                sur_cols = [x for x in col_names if sur_key in x]
                ext_cols = subj_cols + sur_cols
                df_sub = df_raw[ext_cols]

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
                if self._withdrew_list:
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
                df_study = df_sub.loc[idx_study]

                # Update dicts
                data_study[f"visit_{day_str}"][sur_key] = df_study
                data_pilot[f"visit_{day_str}"][sur_key] = df_pilot
                del df_sub
                del df_study
                del df_pilot

        self.data_study = data_study
        self.data_pilot = data_pilot

    def clean_postscan_ratings(self, df_raw):
        """Cleaning method for stimuli ratings.

        Mine "FINAL - EmoRep Stimulus Ratings - fMRI Study" and generate
        a long-formatted dataframe of participant responses. Stimuli order
        and name are stored in "txtFile" column values, and mappings of
        Qualtrics stimulus names with LabrLab's are found in
        make_reports.reference_files.EmoRep_PostScan_Task_<*>_2022.csv.
        Participants give valence and arousal ratings for each stimulus, and
        supply 1+ emotion endorsements.

        Parameters
        ----------
        df_raw : pd.DataFrame
            Original Qualtrics postscan ratings export

        Attributes
        ----------
        data_study : dict
            Cleaned survey data of study participant responses
            {visit: {survey_name: pd.DataFrame}}
        data_pilot : dict
            Cleaned survey data of pilot participant responses
            {visit: {survey_name: pd.DataFrame}}

        """
        self._df_raw = df_raw
        print("Cleaning survey data : post_scan_ratings ...")

        # Remove header rows, test IDs, txtFile NaNs, and restarted
        # sessions (Finished=False).
        self._df_raw = self._df_raw.drop([0, 1])
        self._df_raw = self._df_raw[~self._df_raw["SubID"].str.contains("ER9")]
        self._df_raw = self._df_raw[self._df_raw["txtFile"].notna()]
        self._df_raw = self._df_raw[
            ~self._df_raw["Finished"].str.contains("False")
        ]
        self._df_raw = self._df_raw.reset_index(drop=True)

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
            drop_cols = [x for x in self._df_raw.columns if drop_name in x]
            self._df_raw = self._df_raw.drop(drop_cols, axis=1)

        # Extract subject, session, and stimulus type columns
        self._df_raw = report_helper.drop_participant(
            "ER0080", self._df_raw, "SubID"
        )
        sub_list = self._df_raw["SubID"].tolist()
        sess_list = self._df_raw["SessionID"].tolist()

        # Unpack txtFile column to get list and order of stimuli that
        # were presented to participant.
        stim_all = self._df_raw["txtFile"].tolist()
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
        self._df_study = pd.DataFrame(columns=out_names)
        df_pilot = pd.DataFrame(columns=out_names)

        # Update self._df_raw with each participant's responses
        self._stim_keys()
        for sub, sess, stim_list in zip(sub_list, sess_list, stim_unpack):
            if sub in self._withdrew_list:
                continue
            self._fill_resp(sub, sess, stim_list)
        self._df_study = self._df_study.sort_values(
            by=["study_id", "session", "emotion", "stimulus", "prompt"]
        )

        # Split dataframe by session
        sess_mask = self._df_study["session"] == "day2"
        self._df_study["type"] = self._df_study["type"].replace(
            "Videos", "Movies"
        )
        df_study_day2 = self._df_study[sess_mask]
        df_study_day3 = self._df_study[~sess_mask]

        # Make ouput attributes, just use df_pilot as filler
        self.data_study = {
            "visit_day2": {"post_scan_ratings": df_study_day2},
            "visit_day3": {"post_scan_ratings": df_study_day3},
        }
        self.data_pilot = {
            "visit_day2": {"post_scan_ratings": df_pilot},
            "visit_day3": {"post_scan_ratings": df_pilot},
        }

    def _stim_keys(self):
        """Set attrs _keys_[scenarios|videos]."""
        # Get scenario reference file for mapping prompt to stimulus
        with pkg_resources.open_text(
            reference_files, "EmoRep_PostScan_Task_ScenarioIDs_2022.csv"
        ) as rf:
            _df_keys_scenario = pd.read_csv(rf, index_col="Qualtrics_ID")
        _keys_scenarios = _df_keys_scenario.to_dict()["Stimulus_ID"]

        # Remove punctuation, setup reference dictionary:
        #   key = qualtrics id, value = stimulus id.
        self._keys_scenarios = {}
        for h_key, h_val in _keys_scenarios.items():
            new_key = h_key.translate(
                str.maketrans("", "", string.punctuation)
            )
            self._keys_scenarios[new_key] = h_val

        # Get video reference file, setup reference dict
        with pkg_resources.open_text(
            reference_files, "EmoRep_PostScan_Task_VideoIDs_2022.csv"
        ) as rf:
            _df_keys_video = pd.read_csv(rf, index_col="Qualtrics_ID")
        self._keys_videos = _df_keys_video.to_dict()["Stimulus_ID"]

    def _fill_resp(self, sub, sess, stim_list):
        """Find participant responses and fill dataframe."""
        # Subset df_raw for subject, session data. Remove empty columns.
        df_sub = self._df_raw.loc[
            (self._df_raw["SubID"] == sub)
            & (self._df_raw["SessionID"] == sess)
        ].copy(deep=True)
        df_sub = df_sub.dropna(axis=1)

        # Identify session, datetime, and type values
        day = f"day{int(sess) + 1}"
        sur_date = df_sub.iloc[0]["RecordedDate"].split(" ")[0]
        stim_type = df_sub.iloc[0]["StimulusType"]

        # Get relevant reference dictionary for stimulus type, remove
        # punctuation from scenario prompts and deal with \u2019.
        if stim_type == "Scenarios":
            ref_dict = self._keys_scenarios
            stim_list = [
                x.translate(str.maketrans("", "", string.punctuation)).replace(
                    "can’t", "cant"
                )
                for x in stim_list
            ]
        else:
            ref_dict = self._keys_videos

        # Add a number lines to df_study for each stimulus
        # nonlocal df_study
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
                ].copy(deep=True)
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
                self._df_study = pd.concat(
                    [
                        self._df_study,
                        pd.DataFrame.from_records([update_dict]),
                    ]
                )


def clean_rest_ratings(sess, rawdata_path):
    """Find and aggregate participant rest ratings for session.

    Parameters
    ----------
    sess : str
        ["day2" | "day3"]
    rawdata_path : path
        Location of BIDS rawdata

    Returns
    -------
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
    if sess not in ["day2", "day3"]:
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
        except IndexError:
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

    # Remove responses from withdrawn participants
    part_comp = report_helper.CheckStatus()
    part_comp.status_change("withdrew")
    if not part_comp.all:
        return df_sess
    withdrew_list = [x for x in part_comp.all.keys()]
    df_sess = df_sess[~df_sess.study_id.str.contains("|".join(withdrew_list))]
    df_sess = df_sess.reset_index(drop=True)
    return df_sess
