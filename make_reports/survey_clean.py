"""Clean survey data from RedCap and Qualtrics."""
# %%
import os
import pandas as pd
import numpy as np
from make_reports import report_helper


# %%
class CleanRedcap:
    """Title.

    Desc.

    Parameters
    ----------

    Attributes
    ----------
    df_clean
    df_pilot

    """

    def __init__(self, proj_dir):
        """Title.

        Desc.

        Parameters
        ----------
        proj_dir

        Attributes
        ----------
        proj_dir
        redcap_dict
        pilot_list

        """
        self.proj_dir = proj_dir
        self.redcap_dict = report_helper.redcap_dict()
        self.pilot_list = report_helper.pilot_list()

    def clean_surveys(self, survey_name):
        """Title.

        Desc.

        Parameters
        ----------

        Attributes
        ----------
        df_raw

        """
        raw_path = os.path.join(
            self.proj_dir,
            "data_survey",
            self.redcap_dict[survey_name],
            "data_raw",
        )
        self.df_raw = pd.read_csv(
            os.path.join(raw_path, f"df_{survey_name}_latest.csv")
        )

        # Get and run cleaning method
        print(f"Cleaning survey : {survey_name}")
        if survey_name == "bdi_day2" or survey_name == "bdi_day3":
            self._clean_bdi_day23()
        elif survey_name == "consent_new" or survey_name == "consent_orig":
            self._clean_consent()
        else:
            clean_method = getattr(self, f"_clean_{survey_name}")
            clean_method()

    def _dob_convert(self, dob_list):
        """Helper method for _clean_demographics.

        Convert list of date-of-birth responses to datetime objects.

        Parameters
        ----------
        dob_list : list
            Participant date-of-birth responses

        Returns
        -------
        list

        Raises
        ------
        ValueError
            When numeric dob response does not match expected formats
        TypeError
            When dob response does not match expected formats

        """

        def _num_convert(dob):
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

        Use info from years_education column of df_demo when they are numeric,
        otherwise use the educate_switch to convert from level of education
        to number of years.


            Number of years completed of education (int)

        """
        # Convert education level to years
        educate_switch = {2: 12, 3: 13, 4: 14, 5: 16, 6: 17, 7: 18, 8: 20}

        # Get education level, and self-report of years educated
        edu_year = self.df_raw["years_education"].tolist()
        edu_level = self.df_raw["level_education"].tolist()
        record_id = self.df_raw["record_id"].tolist()

        # Convert into years (deal with self-reports)
        subj_educate = []
        for h_year, h_level, h_id in zip(edu_year, edu_level, record_id):
            # print(h_year, h_level, h_id)
            # Patch for 1984 education issue
            if h_year == "1984":
                subj_educate.append(educate_switch[8])
            else:
                try:
                    subj_educate.append(int(h_year))
                except ValueError:
                    subj_educate.append(educate_switch[h_level])
        return subj_educate

    def _clean_demographics(self):
        """Title.

        Desc.

        """
        # Reorder columns, drop rows without last name response
        col_names = self.df_raw.columns.tolist()
        col_reorder = col_names[-1:] + col_names[-2:-1] + col_names[:-2]
        self.df_raw = self.df_raw[col_reorder]
        self.df_raw = self.df_raw[self.df_raw["lastname"].notna()]

        # Convert DOB response to datetime
        dob_list = self.df_raw["dob"].tolist()
        dob_clean = self._dob_convert(dob_list)
        self.df_raw["dob"] = dob_clean

        # Fix years of education
        yrs_edu = self._get_educ_years()
        self.df_raw["years_education"] = yrs_edu

        # Separate pilot from study data
        pilot_list = [int(x[-1]) for x in self.pilot_list]
        idx_pilot = self.df_raw[
            self.df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        idx_study = self.df_raw[
            ~self.df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        self.df_pilot = self.df_raw.loc[idx_pilot]
        self.df_clean = self.df_raw.loc[idx_study]

    def _clean_consent(self):
        """Title.

        Desc.

        """
        # Drop rows without signature
        col_names = self.df_raw.columns.tolist()
        col_drop = (
            "signature_v2" if "signature_v2" in col_names else "signature"
        )
        df_raw = self.df_raw[self.df_raw[col_drop].notna()]

        # Separate pilot from study data
        pilot_list = [int(x[-1]) for x in self.pilot_list]
        idx_pilot = df_raw[df_raw["record_id"].isin(pilot_list)].index.tolist()
        idx_study = df_raw[
            ~df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]

    def _clean_guid(self):
        """Title.

        Desc.

        """
        # Drop rows without guid
        df_raw = self.df_raw[self.df_raw["guid"].notna()]

        # Separate pilot from study data
        idx_pilot = df_raw[
            df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]

    def _clean_bdi_day23(self):
        """Title.

        Desc.

        Attributes
        ----------
        df_clean
        df_pilot

        """
        # Remove unneeded columns and reorder
        drop_list = ["guid_timestamp", "redcap_survey_identifier"]
        df_raw = self.df_raw.drop(drop_list, axis=1)
        col_names = df_raw.columns.tolist()
        col_reorder = (
            col_names[:1] + col_names[-1:] + col_names[-2:-1] + col_names[1:-2]
        )
        df_raw = df_raw[col_reorder]

        # Remove rows without responses or study_id (from guid survey)
        df_raw = df_raw[df_raw["study_id"].notna()]
        col_drop = "q_1_v2" if "q_1_v2" in col_names else "q_1"
        df_raw = df_raw[df_raw[col_drop].notna()]

        # Enforce datetime format
        col_rename = (
            "bdi_2_timestamp"
            if "bdi_2_timestamp" in col_names
            else "bdi_timestamp"
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
            df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]


# %%
class CleanQualtrics:
    """Title.

    Desc.

    Parameters
    ----------

    Attributes
    ----------
    df_clean
    df_pilot

    """

    def __init__(self, proj_dir):
        """Title.

        Desc.

        Parameters
        ----------
        proj_dir

        Attributes
        ----------
        proj_dir
        redcap_dict
        pilot_list

        """
        self.proj_dir = proj_dir
        self.qualtrics_dict = report_helper.qualtrics_dict()
        self.pilot_list = report_helper.pilot_list()
        self.withdrew_list = report_helper.withdrew_list()

    def clean_surveys(self, survey_name):
        """Title.

        Desc.

        Parameters
        ----------

        Attributes
        ----------
        df_raw

        """
        visit_name = self.qualtrics_dict[survey_name]
        visit_raw = (
            "visit_day2"
            if survey_name == "Session 2 & 3 Survey"
            else self.qualtrics_dict[survey_name]
        )
        raw_path = os.path.join(
            self.proj_dir,
            "data_survey",
            visit_raw,
            "data_raw",
        )
        self.df_raw = pd.read_csv(
            os.path.join(raw_path, f"{survey_name}_latest.csv")
        )

        # Get and run cleaning method
        print(f"Cleaning survey : {survey_name}")
        clean_method = getattr(self, f"_clean_{visit_name}")
        clean_method()

    def _clean_visit_day1(self):
        """Title.

        Desc.

        Attributes
        ----------
        data_clean : dict
        data_pilot : dict

        """
        surveys_visit1 = [
            "ALS",
            "AIM",
            "ERQ",
            "PSWQ",
            "RRS",
            "STAI",
            "TAS",
        ]

        # Setup and identify column names
        subj_cols = ["study_id", "datetime"]
        self.df_raw.rename(
            {"RecipientLastName": subj_cols[0], "StartDate": subj_cols[1]},
            axis=1,
            inplace=True,
        )
        col_names = self.df_raw.columns

        # Subset dataframe by survey key
        data_clean = {}
        data_pilot = {}
        for sur_name in surveys_visit1:
            print(f"\tCleaning survey data : day1, {sur_name}")
            sur_cols = [x for x in col_names if sur_name in x]
            ext_cols = subj_cols + sur_cols
            df_sur = self.df_raw[ext_cols]

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
                ~df_sur.study_id.str.contains("|".join(self.withdrew_list))
            ]
            df_sur = df_sur.reset_index(drop=True)

            # Separate pilot from study data
            idx_pilot = df_sur[
                df_sur["study_id"].isin(self.pilot_list)
            ].index.tolist()
            idx_study = df_sur[
                ~df_sur["study_id"].isin(self.pilot_list)
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

    def _clean_visit_day23(self):
        """Title.

        Desc.

        Attributes
        ----------
        data_clean
        data_pilot

        """
        surveys_visit23 = ["PANAS", "STAI_State"]

        # Identify session, how session is coded in raw data
        day_dict = {"day2": "1", "day3": "2"}
        # day_str = visit_name.split("_")[1]
        # if day_str not in day_dict.keys():
        #     raise ValueError(f"Inapproproiate visit_name : {visit_name}")
        # print(f"Cleaning raw survey data : {day_str}")
        # day_code = day_dict[day_str]

        # Get dataframe and setup output column names
        subj_cols = ["study_id", "visit", "datetime"]
        self.df_raw.rename(
            {
                "SubID": subj_cols[0],
                "Session_Num": subj_cols[1],
                "StartDate": subj_cols[2],
            },
            axis=1,
            inplace=True,
        )
        col_names = self.df_raw.columns

        # Get relevant info from dataframe
        data_clean = {}
        data_pilot = {}
        for day_str, day_code in day_dict.items():
            data_clean[f"visit_{day_str}"] = {}
            data_pilot[f"visit_{day_str}"] = {}
            for sur_key in surveys_visit23:
                print(f"\tCleaning survey data : {day_str}, {sur_key}")
                sur_cols = [x for x in col_names if sur_key in x]
                ext_cols = subj_cols + sur_cols
                df_sub = self.df_raw[ext_cols]

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
                    ~df_sub.study_id.str.contains("|".join(self.withdrew_list))
                ]
                df_sub = df_sub.reset_index(drop=True)

                # Separate pilot from study data
                idx_pilot = df_sub[
                    df_sub["study_id"].isin(self.pilot_list)
                ].index.tolist()
                idx_study = df_sub[
                    ~df_sub["study_id"].isin(self.pilot_list)
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

    def _clean_post_scan_ratings():
        """Title.

        Desc.

        Attributes
        ----------
        df_clean
        df_pilot

        """
        pass
