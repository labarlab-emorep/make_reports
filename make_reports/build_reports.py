"""Build requested reports.

Each class has the attribute df_report which contains
the class' output final report that complies to either
the NDAR data dictionary guidelines (Ndar* classes) or
the NIH/Duke guidelines (ManagerRegular).

Ndar* classes also have the attribute nda_label that can
be prepended to the dataframe, per NDARs double-header.

"""
# %%
import os
import pandas as pd
import numpy as np
from datetime import datetime
from make_reports import report_helper


# %%
class DemoAll:
    """Title

    Desc.

    Attributes
    ----------

    """

    def __init__(self, proj_dir):
        """Title

        Desc.

        Attributes
        ----------
        proj_dir

        """
        print(
            "\nBuilding final_demo from RedCap demographic,"
            + " guid, consent reports ..."
        )
        self.proj_dir = proj_dir

        # Read-in pilot, study data and combine dataframes
        self._read_data()

        # Run methods
        print("\tCompiling needed demographic info ...")
        self.make_complete()

    def _read_data(self):
        """Title

        Desc.

        Attributes
        ----------
        df_guid
        df_demo
        df_consent

        """
        # Set key, df mapping
        map_dict = {
            "cons_orig": "df_consent_orig.csv",
            "cons_new": "df_consent_new.csv",
            "demo": "df_demographics.csv",
            "guid": "df_guid.csv",
        }

        # Read in study reports
        redcap_clean = os.path.join(
            self.proj_dir, "data_survey", "redcap_demographics", "data_clean"
        )
        clean_dict = {}
        for h_key, h_df in map_dict.items():
            clean_dict[h_key] = pd.read_csv(os.path.join(redcap_clean, h_df))

        # Read in pilot reports
        redcap_pilot = os.path.join(
            self.proj_dir,
            "data_pilot/data_survey",
            "redcap_demographics",
            "data_clean",
        )
        pilot_dict = {}
        for h_key, h_df in map_dict.items():
            pilot_dict[h_key] = pd.read_csv(os.path.join(redcap_pilot, h_df))

        # Merge study, pilot dataframes
        df_cons_orig = pd.concat(
            [pilot_dict["cons_orig"], clean_dict["cons_orig"]],
            ignore_index=True,
        )
        df_cons_new = pd.concat(
            [pilot_dict["cons_new"], clean_dict["cons_new"]], ignore_index=True
        )
        df_demo = pd.concat(
            [pilot_dict["demo"], clean_dict["demo"]], ignore_index=True
        )
        df_guid = pd.concat(
            [pilot_dict["guid"], clean_dict["guid"]], ignore_index=True
        )
        del pilot_dict, clean_dict

        # Update consent_new column names from original and merge
        cols_new = df_cons_new.columns.tolist()
        cols_orig = df_cons_orig.columns.tolist()
        cols_replace = {}
        for h_new, h_orig in zip(cols_new, cols_orig):
            cols_replace[h_new] = h_orig
        df_cons_new = df_cons_new.rename(columns=cols_replace)
        df_consent = pd.concat([df_cons_orig, df_cons_new], ignore_index=True)
        df_consent = df_consent.drop_duplicates(subset=["record_id"])
        df_consent = df_consent.sort_values(by=["record_id"])
        del df_cons_new, df_cons_orig

        # Merge dataframes, use merge how=inner to keep only participants
        # who have data in all dataframes.
        df_merge = pd.merge(df_consent, df_guid, on="record_id")
        df_merge = pd.merge(df_merge, df_demo, on="record_id")
        self.df_merge = df_merge
        del df_guid, df_demo, df_consent, df_merge

    def _get_race(self):
        """Get participant race response.

        Account for single response, single response of
        multiple, multiple responses (which may not include
        the multiple option), and "other" responses.

        Attributes
        ----------


        list
            Participant responses to race question

        """
        # Get attribute for readibility, testing
        df_merge = self.df_merge

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
            (df_merge["race___1"] == 1),
            (df_merge["race___2"] == 1),
            (df_merge["race___3"] == 1),
            (df_merge["race___4"] == 1),
            (df_merge["race___5"] == 1),
            (df_merge["race___7"] == 1),
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
        df_merge["race_resp"] = np.select(get_race_resp, set_race_str)

        # Capture "Other" responses, stitch response together
        idx_other = df_merge.index[df_merge["race___8"] == 1].tolist()
        race_other = [
            f"Other - {x}"
            for x in df_merge.loc[idx_other, "race_other"].tolist()
        ]
        df_merge.loc[idx_other, "race_resp"] = race_other

        # Capture "More than one race" responses, write
        # to df_merge["race_more"].
        idx_more = df_merge.index[df_merge["race___6"] == 1].tolist()
        df_merge["race_more"] = np.nan
        df_merge.loc[idx_more, "race_more"] = "More"

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
        df_merge["race_sum"] = df_merge[col_list].sum(axis=1)
        idx_mult = df_merge.index[df_merge["race_sum"] > 1].tolist()
        df_merge.loc[idx_mult, "race_more"] = "More"

        # Update race_resp col with responses in df_merge["race_more"]
        idx_more = df_merge.index[df_merge["race_more"] == "More"].tolist()
        df_merge.loc[idx_more, "race_resp"] = "More than one race"
        race_resp = df_merge["race_resp"].tolist()
        self.race_resp = race_resp
        del df_merge

    def _get_ethnic_minority(self):
        """Determine if participant is considered a minority.

        Parameters
        ----------
        subj_race : list
            Participant race responses

        Attributes
        -------

        tuple
            [0] list of participants' ethnicity status
            [1] list of whether participants' are minority

        """
        # Get ethnicity
        h_ethnic = self.df_merge["ethnicity"].tolist()
        ethnic_switch = {
            1.0: "Hispanic or Latino",
            2.0: "Not Hispanic or Latino",
        }
        subj_ethnic = [ethnic_switch[x] for x in h_ethnic]

        # Determine if minority - not white or hispanic
        subj_minor = []
        for race, ethnic in zip(self.race_resp, subj_ethnic):
            if race != "White" and ethnic == "Not Hispanic or Latino":
                subj_minor.append("Minority")
            else:
                subj_minor.append("Not Minority")
        self.subj_ethnic = subj_ethnic
        self.subj_minor = subj_minor

    def make_complete(self):
        """Make a demographic dataframe with all needed information.

        Pull relevant data from consent, GUID, and demographic reports
        to compile data for all participants in RedCap who have consented.

        Attributes
        ----------
        final_demo : pd.DataFrame
            Complete report containing demographic info for NDA submission

        """
        # Capture attribute for easy testing
        df_merge = self.df_merge

        # Get GUID, study IDs
        subj_guid = df_merge["guid"].tolist()
        subj_study = df_merge["study_id"].tolist()

        # Get consent date
        df_merge["datetime"] = pd.to_datetime(df_merge["date"])
        df_merge["datetime"] = df_merge["datetime"].dt.date
        subj_consent_date = df_merge["datetime"].tolist()

        # Get age, sex
        subj_age = df_merge["age"].tolist()
        h_sex = df_merge["gender"].tolist()
        sex_switch = {1.0: "Male", 2.0: "Female", 3.0: "Neither"}
        subj_sex = [sex_switch[x] for x in h_sex]

        # Get DOB, age in months, education
        subj_dob = df_merge["dob"]
        subj_dob_dt = [
            datetime.strptime(x, "%Y-%m-%d").date() for x in subj_dob
        ]
        subj_age_mo = report_helper.calc_age_mo(subj_dob_dt, subj_consent_date)
        subj_educate = df_merge["years_education"]

        # Get race, ethnicity, minority status
        self._get_race()
        self._get_ethnic_minority()

        # Write dataframe
        out_dict = {
            "subjectkey": subj_guid,
            "src_subject_id": subj_study,
            "interview_date": subj_consent_date,
            "interview_age": subj_age_mo,
            "sex": subj_sex,
            "age": subj_age,
            "dob": subj_dob,
            "ethnicity": self.subj_ethnic,
            "race": self.race_resp,
            "is_minority": self.subj_minor,
            "years_education": subj_educate,
        }
        self.final_demo = pd.DataFrame(out_dict, columns=out_dict.keys())
        del df_merge

    def remove_withdrawn(self):
        """Title.

        Desc.

        """
        withdrew_list = report_helper.withdrew_list()
        self.final_demo = self.final_demo[
            ~self.final_demo.src_subject_id.str.contains(
                "|".join(withdrew_list)
            )
        ]
        self.final_demo = self.final_demo.reset_index(drop=True)


# %%
class ManagerRegular:
    """Make reports regularly submitted by lab manager.

    Query data from the appropriate period for the period, and
    construct a dataframe containing the required information
    for the report.

    Parameters
    ----------
    query_date : datetime
        Date for finding report range
    final_demo : pd.DataFrame
        Compiled demographic information, attribute of
        by general_info.MakeDemographic
    report : str
        Type of report e.g. nih4 or duke3

    Attributes
    ----------
    df_range : pd.DataFrame
        Data found within the range_start, range_end period
    df_report : pd.DataFrame
        Relevant info and format for requested report
    final_demo : pd.DataFrame
        Compiled demographic information, attribute of
        by gather_surveys.GetRedcapDemographic
    query_date : datetime
        Date for finding report range
    range_start : datetime
        Start of period for report
    range_end : datetime
        End of period for report
    report : str
        Type of report e.g. nih4 or duke3

    """

    def __init__(self, query_date, final_demo, report):
        """Make desired report.

        Parameters
        ----------
        query_date : datetime
            Date for finding report range
        final_demo : pd.DataFrame
            Compiled demographic information, attribute of
            by make_reports.general_info.MakeDemographic
        report : str
            Type of report e.g. nih4 or duke3

        Attributes
        ----------
        query_date : datetime
            Date for finding report range
        final_demo : pd.DataFrame
            Compiled demographic information, from
            general_info.MakeDemographic.final_demo
        report : str
            Type of report e.g. nih4 or duke3

        Raises
        ------
        ValueError
            If report is not found in valid_reports

        """
        print(f"Buiding manager report : {report} ...")
        self.query_date = query_date
        self.final_demo = final_demo

        # Trigger appropriate method
        valid_reports = ["nih12", "nih4", "duke3"]
        if report not in valid_reports:
            raise ValueError(f"Inappropriate report requested : {report}")
        report_method = getattr(self, f"make_{report}")
        report_method()

    def _find_start_end(self, range_list):
        """Find the period start and end date.

        Identfiy the start and end dates from range_list
        given the query date.

        Parameters
        ----------
        range_list : list
            Tuples of start (0) and end (1) dates

        Returns
        -------
        start_end : tuple
            [0] start date
            [1] end date

        Raises
        ------
        ValueError
            When a range cannot be found for query_date

        """
        # Search through start, end dates
        start_end = None
        for h_ranges in range_list:
            h_start = datetime.strptime(h_ranges[0], "%Y-%m-%d").date()
            h_end = datetime.strptime(h_ranges[1], "%Y-%m-%d").date()

            # Check if query_date is found in range, allow for
            # first/last days.
            if h_start <= self.query_date <= h_end:
                start_end = (h_start, h_end)
                break
        if not start_end:
            raise ValueError(f"Date range not found for {self.query_date}.")
        return start_end

    def _get_data_range(self, range_list, start_date=None):
        """Get data from date range.

        Make a dataframe of the data from final_demo that
        is found within the date range.

        Parameters
        ----------
        range_list : list
            Tuples of start (0) and end (1) dates
        start_date : datetime, optional
            Known start date

        Attributes
        ----------
        range_start : datetime
            Start of period for report
        range_end : datetime
            End of period for report
        df_range : pd.DataFrame
            Data found within the range_start, range_end period

        Raises
        ------
        ValueError
            df_range is empty, meaning no participants were consented
            within the period range

        """
        # Get date ranges, use known start date if supplied
        h_start, self.range_end = self._find_start_end(range_list)
        self.range_start = start_date if start_date else h_start

        # Mask the dataframe for the dates of interest
        range_bool = (
            self.final_demo["interview_date"] >= self.range_start
        ) & (self.final_demo["interview_date"] <= self.range_end)

        # Subset final_demo according to mask, check if data exist
        self.df_range = self.final_demo.loc[range_bool]
        if self.df_range.empty:
            raise ValueError(
                "No data collected for query range : "
                + f"{self.range_start} - {self.range_end}"
            )
        print(f"\tReport range : {self.range_start} - {self.range_end}")

    def make_nih4(self):
        """Create report submitted to NIH every 4 months.

        Count the total number of participants who identify
        as minority or Hispanic, and total number of participants,
        since the beginning of the experiment.

        Attributes
        ----------
        df_report : pd.DataFrame
            Relevant info, format for requested report

        """
        # Hardcode mturk values
        mturk_nums = {
            "minority": 122,
            "hispanic": 67,
            "total": 659,
        }

        # Set start, end dates for report periods
        nih_4mo_ranges = [
            ("2020-12-01", "2021-03-31"),
            ("2021-04-01", "2021-07-31"),
            ("2021-08-01", "2021-11-30"),
            ("2021-12-01", "2022-03-31"),
            ("2022-04-01", "2022-07-31"),
            ("2022-08-01", "2022-11-30"),
            ("2022-12-01", "2023-03-31"),
            ("2023-04-01", "2023-07-31"),
            ("2023-08-01", "2023-11-30"),
            ("2023-12-01", "2024-03-31"),
            ("2024-04-01", "2024-07-31"),
            ("2024-08-01", "2024-11-30"),
            ("2024-12-01", "2025-03-31"),
            ("2025-04-01", "2025-07-31"),
            ("2025-08-01", "2025-11-30"),
        ]

        # Set project start date (approximately)
        proj_start = datetime.strptime("2020-06-30", "%Y-%m-%d").date()

        # Find data within range
        self._get_data_range(
            nih_4mo_ranges,
            start_date=proj_start,
        )

        # Calculate number of minority, hispanic, and total recruited
        num_minority = len(
            self.df_range.index[self.df_range["is_minority"] == "Minority"]
        )
        num_hispanic = len(
            self.df_range.index[
                self.df_range["ethnicity"] == "Hispanic or Latino"
            ]
        )
        num_total = len(self.df_range.index)

        # Update calculations with mturk values
        num_minority += mturk_nums["minority"]
        num_hispanic += mturk_nums["hispanic"]
        num_total += mturk_nums["total"]

        # Make report
        report_dict = {
            "Category": ["Minority", "Hispanic", "Total"],
            "Values": [
                num_minority,
                num_hispanic,
                num_total,
            ],
        }
        self.df_report = pd.DataFrame(report_dict)

    def make_duke3(self):
        """Create report submitted to Duke every 3 months.

        Determine the number of participants that belong to
        gender * ethnicity * race group combinations which have
        been recruited in the current period.

        Attributes
        ----------
        df_report : pd.DataFrame
            Relevant info, format for requested report

        """
        # Set start, end dates for report periods
        duke_3mo_ranges = [
            ("2020-11-01", "2020-12-31"),
            ("2021-01-01", "2021-03-31"),
            ("2021-04-01", "2021-06-30"),
            ("2021-07-01", "2021-09-30"),
            ("2021-10-01", "2021-12-31"),
            ("2022-01-01", "2022-03-31"),
            ("2022-04-01", "2022-06-30"),
            ("2022-07-01", "2022-09-30"),
            ("2022-10-01", "2022-12-31"),
            ("2023-01-01", "2023-03-31"),
            ("2023-04-01", "2023-06-30"),
            ("2023-07-01", "2023-09-30"),
            ("2023-10-01", "2023-12-31"),
            ("2024-01-01", "2024-03-31"),
            ("2024-04-01", "2024-06-30"),
            ("2024-07-01", "2024-09-30"),
            ("2024-10-01", "2024-12-31"),
            ("2025-01-01", "2025-03-31"),
            ("2025-04-01", "2025-06-30"),
            ("2025-07-01", "2025-09-30"),
            ("2025-10-01", "2025-12-31"),
        ]

        # Find data within range
        self._get_data_range(duke_3mo_ranges)

        # Get gender, ethnicity, race responses
        df_hold = self.df_range[["src_subject_id", "sex", "ethnicity", "race"]]

        # Combine responses for easy tabulation, deal with pd warnings
        pd.options.mode.chained_assignment = None
        df_hold["comb"] = (
            df_hold["sex"] + "," + df_hold["ethnicity"] + "," + df_hold["race"]
        )
        pd.options.mode.chained_assignment = "warn"

        # Count number of unique groups, convert to dataframe
        self.df_report = (
            df_hold["comb"]
            .value_counts()
            .rename_axis("Groups")
            .reset_index(name="Counts")
        )

        # Reformat dataframe into desired format
        self.df_report = pd.concat(
            [
                self.df_report["Groups"].str.split(",", expand=True),
                self.df_report["Counts"],
            ],
            axis=1,
        )
        col_rename = {
            0: "Gender",
            1: "Ethnicity",
            2: "Race",
            "Counts": "Total",
        }
        self.df_report = self.df_report.rename(columns=col_rename)
        self.df_report = self.df_report.sort_values(
            by=["Gender", "Ethnicity", "Race"]
        )

    def make_nih12(self):
        """Create report submitted to NIH every 12 months.

        Pull participant-level information those recruited
        within the report period.

        Attributes
        ----------
        df_report : pd.DataFrame
            Relevant info, format for requested report

        """
        # Set start, end dates for report periods
        nih_annual_ranges = [
            ("2020-04-01", "2020-03-31"),
            ("2021-04-01", "2022-03-31"),
            ("2022-04-01", "2023-03-31"),
            ("2023-04-01", "2024-03-31"),
            ("2024-04-01", "2025-03-31"),
            ("2025-04-01", "2026-03-31"),
        ]

        # Get data from query range
        self._get_data_range(nih_annual_ranges)

        # Extract relevant columns for the report
        cols_desired = [
            "src_subject_id",
            "race",
            "ethnicity",
            "sex",
            "age",
        ]
        self.df_report = self.df_range[cols_desired]

        # Reformat dataframe into desired format
        col_names = {
            "src_subject_id": "Record_ID",
            "race": "Race",
            "ethnicity": "Ethnicity",
            "sex": "Gender",
            "age": "Age",
        }
        self.df_report = self.df_report.rename(columns=col_names)
        self.df_report["Age Units"] = "Years"


# %%
class NdarAffim01:
    """Make affim01 report for NDAR submission.

    Pull subject demographic info from gather_surveys.GetRedcapDemographic
    and survey data from gather_surveys.GetQualtricsSurveys.

    Attributes
    ----------
    df_aim : pd.DataFrame
        Cleaned AIM Qualtrics survey
    df_report : pd.DataFrame
        Report of AIM data that complies with NDAR data definitions
    final_demo : pd.DataFrame
        Compiled demographic information
    nda_label : list
        NDA report template column label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned AIM Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------


        Attributes
        ----------
        df_aim : pd.DataFrame
            Cleaned AIM Qualtrics survey
        final_demo : pd.DataFrame
            Compiled demographic information
        nda_label : list
            NDA report template column label

        """
        print("Buiding NDA report : affim01 ...")
        # Read in template
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "affim01_template.csv"
        )

        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_pilot/data_survey",
                "visit_day1/data_clean",
                "df_AIM.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_AIM.csv",
            )
        )
        df_aim = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, drop NaN rows
        df_aim = df_aim.rename(columns={"study_id": "src_subject_id"})
        df_aim.columns = df_aim.columns.str.lower()
        df_aim = df_aim.replace("NaN", np.nan)
        self.df_aim = df_aim[df_aim["aim_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        self.final_demo = final_demo.dropna(subset=["subjectkey"])
        self._make_aim()

    def _make_aim(self):
        """Combine dataframes to generate requested report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of AIM data that complies with NDAR data definitions

        """
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        # pd.options.mode.chained_assignment = None
        df_nda["sex"] = df_nda["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        df_aim_nda = pd.merge(self.df_aim, df_nda, on="src_subject_id")

        df_aim_nda = report_helper.get_survey_age(
            df_aim_nda, self.final_demo, "src_subject_id"
        )

        # Sum aim responses, toggle pandas warning mode
        aim_list = [x for x in df_aim_nda.columns if "aim" in x]
        df_aim_nda[aim_list] = df_aim_nda[aim_list].astype("Int64")
        df_aim_nda["aimtot"] = df_aim_nda[aim_list].sum(axis=1)
        # pd.options.mode.chained_assignment = "warn"

        # Merge final_demo with aim
        self.df_report = pd.DataFrame(
            columns=self.nda_cols, index=df_aim_nda.index
        )
        self.df_report.update(df_aim_nda)


# %%
class NdarAls01:
    """Make als01 report for NDAR submission.

    Pull subject demographic info from gather_surveys.GetRedcapDemographic
    and survey data from gather_surveys.GetQualtricsSurveys.

    Attributes
    ----------
    df_als : pd.DataFrame
        Cleaned ALS Qualtrics survey
    df_report : pd.DataFrame
        Report of ALS data that complies with NDAR data definitions
    final_demo : pd.DataFrame
        Compiled demographic information
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template column label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned ALS Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------
        qualtrics_data : make_reports.gather_surveys.GetQualtricsSurveys
        redcap_demo : make_reports.gather_surveys.GetRedcapDemographic

        Attributes
        ----------
        df_als : pd.DataFrame
            Cleaned ALS Qualtrics survey
        final_demo : pd.DataFrame
            Compiled demographic information
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template column label

        """
        print("Buiding NDA report : als01 ...")
        # Read in template
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "als01_template.csv"
        )

        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_pilot/data_survey",
                "visit_day1/data_clean",
                "df_ALS.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_ALS.csv",
            )
        )
        df_als = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, frop NaN rows
        df_als = df_als.rename(columns={"study_id": "src_subject_id"})
        df_als = df_als.replace("NaN", np.nan)
        self.df_als = df_als[df_als["ALS_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        self.final_demo = final_demo.dropna(subset=["subjectkey"])
        self._make_als()

    def _make_als(self):
        """Make als01 report.

        Remap column names and response values, add demographic info.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of ALS data that complies with NDAR data definitions

        """
        # Remap response values and column names
        resp_qual = ["1", "2", "3", "4"]
        resp_ndar = [3, 2, 1, 0]
        map_item = {
            "ALS_1": "als5",
            "ALS_2": "als8",
            "ALS_3": "als12",
            "ALS_4": "als14",
            "ALS_5": "als16",
            "ALS_6": "als17",
            "ALS_7": "als20",
            "ALS_8": "als21",
            "ALS_9": "als23",
            "ALS_10": "als25",
            "ALS_11": "als33",
            "ALS_12": "als34",
            "ALS_13": "als36",
            "ALS_14": "als41",
            "ALS_15": "als42",
            "ALS_16": "als43",
            "ALS_17": "als45",
            "ALS_18": "als46",
        }
        df_als_remap = self.df_als.rename(columns=map_item)
        als_cols = [x for x in df_als_remap.columns if "als" in x]
        df_als_remap[als_cols] = df_als_remap[als_cols].replace(
            resp_qual, resp_ndar
        )

        # Calculate totals
        df_als_remap[als_cols] = df_als_remap[als_cols].astype("Int64")
        df_als_remap["als_glob"] = df_als_remap[als_cols].sum(axis=1)
        df_als_remap["als_sf_total"] = df_als_remap[als_cols].sum(axis=1)

        # Add pilot notes for certain subjects
        pilot_list = report_helper.pilot_list()
        idx_pilot = df_als_remap[
            df_als_remap["src_subject_id"].isin(pilot_list)
        ].index.tolist()
        df_als_remap.loc[idx_pilot, "comments"] = "PILOT PARTICIPANT"

        # Combine demographic and als dataframes
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_nda["sex"] = df_nda["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        df_als_nda = pd.merge(df_als_remap, df_nda, on="src_subject_id")
        df_als_nda = report_helper.get_survey_age(
            df_als_nda, self.final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with demo and als data
        self.df_report = pd.DataFrame(
            columns=self.nda_cols, index=df_als_nda.index
        )
        self.df_report.update(df_als_nda)


# %%
class NdarBdi01:
    """Make bdi01 report for NDAR submission.

    Pull subject demographic info from gather_surveys.GetRedcapDemographic
    and data from gather_surveys.GetRedcapSurveys.

    Attributes
    ----------
    df_bdi_day2 : pd.DataFrame
        Cleaned visit_day2 BDI RedCap survey
    df_bdi_day3 : pd.DataFrame
        Cleaned visit_day3 BDI RedCap survey
    df_report : pd.DataFrame
        Report of BDI data that complies with NDAR data definitions
    final_demo : pd.DataFrame
        Compiled demographic information
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template column label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned BDI RedCap survey from visit_day2 and
        visit_day3, and finalized demographic information.

        Parameters
        ----------
        redcap_data : make_reports.gather_surveys.GetRedcapSurveys
        redcap_demo : make_reports.gather_surveys.GetRedcapDemographic

        Attributes
        ----------
        df_bdi_day2 : pd.DataFrame
            Cleaned visit_day2 BDI RedCap survey
        df_bdi_day3 : pd.DataFrame
            Cleaned visit_day3 BDI RedCap survey
        df_report : pd.DataFrame
            Report of BDI data that complies with NDAR data definitions
        final_demo : pd.DataFrame
            Compiled demographic information
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template column label

        """
        print("Buiding NDA report : bdi01 ...")
        self.proj_dir = proj_dir

        # Read in template
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "bdi01_template.csv"
        )

        # Get pilot, study data for both day2, day3
        self._get_clean()

        # Get final demographics
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self.final_demo = final_demo.dropna(subset=["subjectkey"])

        # Make nda reports for each session
        df_nda_day2 = self._make_bdi("day2")
        df_nda_day3 = self._make_bdi("day3")

        # Combine into final report
        df_report = pd.concat([df_nda_day2, df_nda_day3])
        df_report = df_report.sort_values(by=["src_subject_id"])
        self.df_report = df_report[df_report["interview_date"].notna()]

    def _get_clean(self):
        """Title.

        Desc.

        """
        # Get clean survey data
        df_pilot2 = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_pilot/data_survey",
                "visit_day2/data_clean",
                "df_bdi_day2.csv",
            )
        )
        df_study2 = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_survey",
                "visit_day2/data_clean",
                "df_bdi_day2.csv",
            )
        )
        df_bdi_day2 = pd.concat([df_pilot2, df_study2], ignore_index=True)
        df_bdi_day2 = df_bdi_day2.rename(
            columns={"study_id": "src_subject_id"}
        )

        df_pilot3 = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_pilot/data_survey",
                "visit_day3/data_clean",
                "df_bdi_day3.csv",
            )
        )
        df_study3 = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_survey",
                "visit_day3/data_clean",
                "df_bdi_day3.csv",
            )
        )
        df_bdi_day3 = pd.concat([df_pilot3, df_study3], ignore_index=True)
        df_bdi_day3 = df_bdi_day3.rename(
            columns={"study_id": "src_subject_id"}
        )
        df_bdi_day3.columns = df_bdi_day2.columns.values

        self.df_bdi_day2 = df_bdi_day2
        self.df_bdi_day3 = df_bdi_day3

    def _make_bdi(self, sess):
        """Make an NDAR compliant report for visit.

        Remap column names, add demographic info, get session
        age, and generate report.

        Parameters
        ----------
        sess : str
            [day2 | day3], visit/session name

        Returns
        -------
        pd.DataFrame

        """
        # Get session data
        df_bdi = getattr(self, f"df_bdi_{sess}")

        # Convert response values to int
        q_cols = [x for x in df_bdi.columns if "q_" in x]
        df_bdi[q_cols] = df_bdi[q_cols].astype("Int64")

        # Remap column names
        map_item = {
            "q_1": "bdi1",
            "q_2": "bdi2",
            "q_3": "bdi3",
            "q_4": "bdi4",
            "q_5": "bdi5",
            "q_6": "bdi6",
            "q_7": "beck07",
            "q_8": "beck08",
            "q_9": "bdi9",
            "q_10": "bdi10",
            "q_11": "bdi_irritated",
            "q_12": "bdi_lost",
            "q_13": "bdi_indecision",
            "q_14": "beck14",
            "q_15": "beck15",
            "q_16": "beck16",
            "q_17": "beck17",
            "q_18": "bd_017",
            "q_19": "beck19",
            "q_19b": "beck20",
            "q_20": "beck21",
            "q_21": "beck22",
        }
        df_bdi_remap = df_bdi.rename(columns=map_item)

        # Drop non-ndar columns
        df_bdi_remap = df_bdi_remap.drop(
            ["bdi_complete", "record_id"],
            axis=1,
        )

        # Sum BDI responses, exclude bdi_[irritated|lost|indecision],
        # convert type to int.
        bdi_cols = [
            x for x in df_bdi_remap.columns if "b" in x and len(x) <= 6
        ]
        df_bdi_remap["bdi_tot"] = df_bdi_remap[bdi_cols].sum(axis=1)
        df_bdi_remap["bdi_tot"] = df_bdi_remap["bdi_tot"].astype("Int64")

        # Combine demo and bdi dataframes
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_bdi_demo = pd.merge(df_bdi_remap, df_nda, on="src_subject_id")

        # Calculate age in months of visit
        df_bdi_demo = report_helper.get_survey_age(
            df_bdi_demo, self.final_demo, "src_subject_id"
        )
        df_bdi_demo["visit"] = sess

        # Build dataframe from nda columns, update with df_final_bdi data
        df_nda = pd.DataFrame(columns=self.nda_cols, index=df_bdi_demo.index)
        df_nda.update(df_bdi_demo)
        return df_nda


class NdarBrd01:
    pass


class NdarDemoInfo01:
    """Make demo_info01 report for NDAR submission.

    Use subject demographic info from gather_surveys.GetRedcapDemographic
    to build report.

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of demographic data that complies with NDAR data definitions
    final_demo : pd.DataFrame
        Compiled demographic information
    nda_label : list
        NDA report template column label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in demographic info and make report.

        Get demographic info from redcap_demo, and extract required values.

        Parameters
        ----------
        redcap_demo : make_reports.gather_surveys.GetRedcapDemographic

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of demographic data that complies with NDAR data definitions
        final_demo : pd.DataFrame
            Compiled demographic information
        nda_label : list
            NDA report template column label

        """
        print("Buiding NDA report : demo_info01 ...")
        self.final_demo = final_demo
        self.nda_label, nda_cols = report_helper.mine_template(
            "demo_info01_template.csv"
        )
        self.df_report = pd.DataFrame(columns=nda_cols)
        self._make_demo()

    def _make_demo(self):
        """Extract relevant values and make report.

        Generate demo_info01 report that will comply with NDAR data
        definition standards by mining data supplied in final_demo.

        """
        # Get subject key, src_id
        subj_key = self.final_demo["subjectkey"]
        subj_src_id = self.final_demo["src_subject_id"]

        # Get inverview age, date
        subj_inter_date = [
            x.strftime("%m/%d/%Y") for x in self.final_demo["interview_date"]
        ]
        subj_inter_age = self.final_demo["interview_age"]

        # Get subject sex
        subj_sex = [x[:1] for x in self.final_demo["sex"]]
        subj_sex = list(map(lambda x: x.replace("N", "O"), subj_sex))

        # Get subject race
        subj_race = self.final_demo["race"]
        subj_race = list(
            map(
                lambda x: x.replace("African-American", "African American"),
                subj_race,
            )
        )
        subj_race_other = []
        for idx, resp in enumerate(subj_race):
            if "Other" in resp:
                subj_race[idx] = "Other"
                subj_race_other.append(resp.split(" - ")[1])
            else:
                subj_race_other.append(np.nan)

        # Get education lavel
        subj_educat = self.final_demo["years_education"]

        # Make comments for pilot subjs
        pilot_list = report_helper.pilot_list()
        subj_comments_misc = []
        for subj in subj_src_id:
            if subj in pilot_list:
                subj_comments_misc.append("PILOT PARTICIPANT")
            else:
                subj_comments_misc.append(np.nan)

        # Organize values, add to report
        report_dict = {
            "subjectkey": subj_key,
            "src_subject_id": subj_src_id,
            "interview_date": subj_inter_date,
            "interview_age": subj_inter_age,
            "sex": subj_sex,
            "race": subj_race,
            "otherrace": subj_race_other,
            "educat": subj_educat,
            "comments_misc": subj_comments_misc,
        }
        for h_col, h_value in report_dict.items():
            self.df_report[h_col] = h_value


class NdarEmoEndo01:
    pass


class NdarEmrq01:
    pass


class NdarImage03:
    pass


class NdarInclExcl01:
    pass


class NdarSubject01:
    pass


class NdarPanas01:
    pass


class NdarPhysioRec01:
    pass


class NdarPswq01:
    pass


class NdarRrs01:
    pass


class NdarStai01:
    pass


class NdarTas01:
    pass
