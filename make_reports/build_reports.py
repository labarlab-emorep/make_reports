"""Generate requested reports and organize relevant data.

All Ndar* classes contain the following attributes (in addition to others):
    df_report : pd.DataFrame
        NDAR-compliant dataframe, without the NDA template label
    nda_label : list
        NDAR report template label, e.g. ["image", "03"]

"""
import os
import re
import glob
import json
import subprocess
import distutils.spawn
import pandas as pd
import numpy as np
from datetime import datetime
import pydicom
from make_reports import report_helper


class DemoAll:
    """Gather demographic information from RedCap surveys.

    Only includes participants who have signed the consent form,
    have an assigned GUID, and have completed the demographic
    survey.

    Parameters
    ----------
    proj_dir : path
        Location of parent directory for project

    Attributes
    ----------
    df_merge : pd.DataFrame
        Participant data from consent, demographic, and GUID surveys
    final_demo : pd.DataFrame
        Complete report containing demographic info for NDA submission,
        regular manager reports.
    proj_dir : path
        Location of parent directory for project
    race_resp : list
        Participant responses to race item
    subj_ethnic : list
        Participants' ethnicity status
    subj_minor : list
        Particiapnts' minority status

    Methods
    -------
    make_complete
        Used for regular manager reports and common NDAR fields
    remove_withdrawn
        Remove participants from final_demo that have withdrawn consent

    """

    def __init__(self, proj_dir):
        """Read-in data and generate final_demo.

        Attributes
        ----------
        proj_dir : path
            Location of parent directory for project

        """
        # Read-in pilot, study data and combine dataframes
        print(
            "\nBuilding final_demo from RedCap demographic,"
            + " guid, consent reports ..."
        )
        self.proj_dir = proj_dir
        self._read_data()

        # Generate final_demo
        print("\tCompiling needed demographic info ...")
        self.make_complete()

    def _read_data(self):
        """Get required pilot and study dataframes.

        Attributes
        ----------
        df_merge : pd.DataFrame
            Participant data from consent, demographic, and GUID surveys

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

        # Merge dataframes, use merge how=inner (default) to keep
        # only participants who have data in all dataframes.
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
        race_resp : list
            Participant responses to race item

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

        Attributes
        ----------
        subj_ethnic : list
            Participants' ethnicity status
        subj_minor : list
            Particiapnts' minority status

        """
        # Get ethnicity selection, convert to english
        h_ethnic = self.df_merge["ethnicity"].tolist()
        ethnic_switch = {
            1.0: "Hispanic or Latino",
            2.0: "Not Hispanic or Latino",
        }
        subj_ethnic = [ethnic_switch[x] for x in h_ethnic]

        # Determine if minority i.e. not white or hispanic
        subj_minor = []
        for race, ethnic in zip(self.race_resp, subj_ethnic):
            if race != "White" and ethnic == "Not Hispanic or Latino":
                subj_minor.append("Minority")
            else:
                subj_minor.append("Not Minority")
        self.subj_ethnic = subj_ethnic
        self.subj_minor = subj_minor

    def make_complete(self):
        """Make a demographic dataframe.

        Pull relevant data from consent, GUID, and demographic reports
        to compile data for all participants in RedCap.

        Attributes
        ----------
        final_demo : pd.DataFrame
            Complete report containing demographic info for NDA submission,
            regular manager reports.

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
        """Remove participants from final_demo who have withdrawn consent."""
        withdrew_list = report_helper.withdrew_list()
        self.final_demo = self.final_demo[
            ~self.final_demo.src_subject_id.str.contains(
                "|".join(withdrew_list)
            )
        ]
        self.final_demo = self.final_demo.reset_index(drop=True)


class ManagerRegular:
    """Make reports regularly submitted by lab manager.

    Query data from the appropriate period for the period, and
    construct a dataframe containing the required information
    for the report.

    Parameters
    ----------
    query_date : datetime
        Date for finding report range
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    report : str
        Type of report e.g. nih4 or duke3

    Attributes
    ----------
    df_range : pd.DataFrame
        Data found within the range_start, range_end period
    df_report : pd.DataFrame
        Relevant info and format for requested report
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    query_date : datetime
        Date for finding report range
    range_end : datetime
        End of period for report
    range_start : datetime
        Start of period for report
    report : str
        Type of report e.g. nih4 or duke3

    Methods
    -------
    make_duke3
        Generate report submitted to Duke every 3 months
    make_nih4
        Generate report submitted to NIH every 4 months
    make_nih12
        Generate report submitted to NIH every 12 months

    """

    def __init__(self, query_date, final_demo, report):
        """Generate requested report.

        Parameters
        ----------
        query_date : datetime
            Date for finding report range
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        report : str
            [nih4 | nih12 | duke3]

        Attributes
        ----------
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        query_date : datetime
            Date for finding report range

        Raises
        ------
        ValueError
            If improper report argument supplied

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
        """Find the report period start and end dates.

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
        df_range : pd.DataFrame
            Data found within the range_start, range_end period
        range_end : datetime
            End of period for report
        range_start : datetime
            Start of period for report

        Raises
        ------
        ValueError
            df_range is empty, meaning no participants were consented
            within the period range.

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

        # Combine responses for easy tabulation, silence pd warnings
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

        # Restructure dataframe into desired format
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


class GenerateGuids:
    """Generate GUIDs for EmoRep.

    Use existing RedCap demographic information to produce
    a batch of GUIDs via NDA's guid-tool.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    user_name : str
        NDA user name
    user_pass : str
        NDA user password

    Attributes
    ----------
    df_guid : pd.DataFrame
        Formatted for use with guid-tool
    df_guid_file : path
        Location of intermediate file for subprocess accessibility
    mismatch_list : list
        Record IDs of participants who differ between RedCap and
        generated GUIDs
    proj_dir : path
        Project's experiment directory
    user_name : str
        NDA user name
    user_pass : str
        NDA user password

    Methods
    -------
    check_guids
        Compare generate GUIDs to those in RedCap
    make_guids
        Use compiled demographic info from _get_demo to generate GUIDs

    """

    def __init__(self, proj_dir, user_pass, user_name):
        """Setup instance and compile demographic information.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        user_name : str
            NDA user name
        user_pass : str
            NDA user password

        Attributes
        ----------
        proj_dir : path
            Project's experiment directory
        user_name : str
            NDA user name
        user_pass : str
            NDA user password

        """
        self.proj_dir = proj_dir
        self.user_name = user_name
        self.user_pass = user_pass
        self._get_demo()

    def _get_demo(self):
        """Make a dataframe with fields required by guid-tool.

        Mine cleaned RedCap demographics survey and compile needed
        fields for the guid-tool.

        Attributes
        ----------
        df_guid : pd.DataFrame
            Formatted for use with guid-tool
        df_guid_file : path
            Location of intermediate file for subprocess accessibility

        Raises
        ------
        FileNotFoundError
            Cleaned RedCap demographic information not found

        Notes
        -----
        Writes df_guid to df_guid_file.

        """
        print("Compiling RedCap demographic info ...")

        # Check for, read-in demographic info
        chk_demo = os.path.join(
            self.proj_dir,
            "data_survey/redcap_demographics/data_clean",
            "df_demographics.csv",
        )
        if not os.path.exists(chk_demo):
            raise FileNotFoundError(
                f"Missing expected demographic info : {chk_demo}"
            )
        df_demo = pd.read_csv(chk_demo)

        # Remap, extract relevant columns
        demo_cols = [
            "record_id",
            "firstname",
            "middle_name",
            "lastname",
            "dob",
            "city",
            "gender",
        ]
        guid_cols = [
            "ID",
            "FIRSTNAME",
            "MIDDLENAME",
            "LASTNAME",
            "dob_datetime",
            "COB",
            "SEX",
        ]
        cols_remap = {}
        for demo, guid in zip(demo_cols, guid_cols):
            cols_remap[demo] = guid
        df_guid = df_demo[demo_cols]
        df_guid = df_guid.rename(columns=cols_remap)
        del df_demo

        # Split date-of-birth and make needed columns
        dt_list = df_guid["dob_datetime"].tolist()
        mob_list = []
        dob_list = []
        yob_list = []
        for h_dob in dt_list:
            yob, mob, dob = h_dob.split("-")
            yob_list.append(yob)
            mob_list.append(mob)
            dob_list.append(dob)
        df_guid["DOB"] = dob_list
        df_guid["MOB"] = mob_list
        df_guid["YOB"] = yob_list
        df_guid = df_guid.drop("dob_datetime", axis=1)

        # Make sex column compliant, provide whether participants
        # have a middle name.
        df_guid["SEX"] = df_guid["SEX"].map({1.0: "M", 2.0: "F"})
        df_guid["SUBJECTHASMIDDLENAME"] = "Yes"
        df_guid.loc[
            df_guid["MIDDLENAME"].isnull(), "SUBJECTHASMIDDLENAME"
        ] = "No"

        # Write out intermediate dataframe
        # TODO validate required fields
        self.df_guid_file = os.path.join(
            os.path.join(
                self.proj_dir,
                "data_survey/redcap_demographics/data_clean",
                "tmp_df_for_guid_tool.csv",
            )
        )
        df_guid.to_csv(self.df_guid_file, index=False, na_rep="")
        self.df_guid = df_guid

    def make_guids(self):
        """Generate GUIDs via guid-tool.

        Output of guid-tool is written to:
            <proj_dir>/data_survey/redcap_demographics/data_clean/output_guid_*.txt

        Attributes
        ----------
        guid_file : path
            Location, file out guid-tool output

        Raises
        ------
        FileNotFoundError
            Intermediate demographic info from _get_demo is not found
            A new guid_file is not detected in output directory
        BaseException
            guid-tool is not installed or available in sub-shell

        Notes
        -----
        Requires guid-tool to be installed in operating system and available
        in the sub-shell environment.

        """
        print("Generating GUIDs ...")

        # Check for guid file
        if not os.path.exists(self.df_guid_file):
            raise FileNotFoundError(
                f"Missing expected file : {self.df_guid_file}"
            )

        # Check for tool
        if not distutils.spawn.find_executable("guid-tool"):
            raise BaseException("Failed to find guid-tools in OS")

        # Existing guid list
        guid_path = os.path.dirname(self.df_guid_file)
        guid_old = sorted(glob.glob(f"{guid_path}/output_guid_*.txt"))

        # Run guid command
        bash_guid = f"""\
            guid-tool \
                -a get \
                -u {self.user_name} \
                -p {self.user_pass} \
                -b "{self.df_guid_file}"
        """
        h_sp = subprocess.Popen(bash_guid, shell=True, stdout=subprocess.PIPE)
        h_out, h_err = h_sp.communicate()
        h_sp.wait()

        # Check for file
        guid_output = sorted(glob.glob(f"{guid_path}/output_guid_*.txt"))
        if not len(guid_output) > len(guid_old) or not guid_output:
            raise FileNotFoundError("Failed to generate new GUID output.")
        print(f"\tWrote : {guid_output[-1]}")
        self.guid_file = guid_output[-1]

    def check_guids(self):
        """Check RedCap GUID database against newly generate GUIDs.

        Useful for identifying copy-paste error in manual GUID
        generation or entry.

        Attributes
        ----------
        mismatch_list : list
            Record IDs of participants who differ between RedCap and
            generated GUIDs

        Raises
        ------
        FileNotFoundError
            Cleaned version of RedCap GUID survey missing

        """
        print("Comparing RedCap to generated GUIDs ...")

        # Get cleaned RedCap GUID survey
        guid_redcap = os.path.join(
            self.proj_dir,
            "data_survey/redcap_demographics/data_clean",
            "df_guid.csv",
        )
        if not os.path.exists(guid_redcap):
            raise FileNotFoundError(
                f"Missing required GUID info : {guid_redcap}"
            )
        df_guid_rc = pd.read_csv(guid_redcap)

        # Get generated GUIDs
        try:
            df_guid_gen = pd.read_csv(
                self.guid_file,
                sep="-",
                names=["record_id", "guid_new", "notes"],
            )
        except AttributeError:
            print(
                "\tAttribute guid_file does not exist in instance, "
                + "attempting to generate now ..."
            )
            self.make_guids()
            df_guid_gen = pd.read_csv(
                self.guid_file,
                sep="-",
                names=["record_id", "guid_new", "notes"],
            )

        # Merge, keep only those existing in RedCap
        df_merge = pd.merge(df_guid_rc, df_guid_gen, on="record_id")

        # Identify mismatching GUIDs
        df_merge["mismatch"] = df_merge["guid"] == df_merge["guid_new"]
        self.mismatch_list = df_merge.loc[
            df_merge["mismatch"], "record_id"
        ].tolist()


class NdarAffim01:
    """Make affim01 report for NDAR submission.

    Parameters
    ----------
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    proj_dir : path
        Project's experiment directory

    Attributes
    ----------
    df_aim : pd.DataFrame
        Cleaned AIM Qualtrics survey
    df_report : pd.DataFrame
        Report of AIM data that complies with NDAR data definitions
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned AIM Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        df_aim : pd.DataFrame
            Cleaned AIM Qualtrics survey
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label

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
        # Get needed demographic info, combine with survey data
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_nda["sex"] = df_nda["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        df_aim_nda = pd.merge(self.df_aim, df_nda, on="src_subject_id")

        # Find survey age-in-months
        df_aim_nda = report_helper.get_survey_age(
            df_aim_nda, self.final_demo, "src_subject_id"
        )

        # Sum aim responses
        aim_list = [x for x in df_aim_nda.columns if "aim" in x]
        df_aim_nda[aim_list] = df_aim_nda[aim_list].astype("Int64")
        df_aim_nda["aimtot"] = df_aim_nda[aim_list].sum(axis=1)

        # Make an empty dataframe from the report column names, fill
        self.df_report = pd.DataFrame(
            columns=self.nda_cols, index=df_aim_nda.index
        )
        self.df_report.update(df_aim_nda)


class NdarAls01:
    """Make als01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_als : pd.DataFrame
        Cleaned ALS Qualtrics survey
    df_report : pd.DataFrame
        Report of ALS data that complies with NDAR data definitions
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned ALS Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        df_als : pd.DataFrame
            Cleaned ALS Qualtrics survey
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label

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

        # Rename columns, drop NaN rows
        df_als = df_als.rename(columns={"study_id": "src_subject_id"})
        df_als = df_als.replace("NaN", np.nan)
        self.df_als = df_als[df_als["ALS_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        self.final_demo = final_demo.dropna(subset=["subjectkey"])
        self._make_als()

    def _make_als(self):
        """Combine dataframes to generate requested report.

        Remap values and calculate totals.

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


class NdarBdi01:
    """Make bdi01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_bdi_day2 : pd.DataFrame
        Cleaned visit_day2 BDI RedCap survey
    df_bdi_day3 : pd.DataFrame
        Cleaned visit_day3 BDI RedCap survey
    df_report : pd.DataFrame
        Report of BDI data that complies with NDAR data definitions
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label
    proj_dir : path
        Project's experiment directory

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned BDI RedCap survey from visit_day2 and
        visit_day3, and finalized demographic information.
        Generate BDI report.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of BDI data that complies with NDAR data definitions
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label
        proj_dir : path
            Project's experiment directory

        """
        # Get needed column values from report template
        print("Buiding NDA report : bdi01 ...")
        self.proj_dir = proj_dir
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

        # Add pilot notes for certain subjects
        pilot_list = report_helper.pilot_list()
        idx_pilot = df_report[
            df_report["src_subject_id"].isin(pilot_list)
        ].index.tolist()
        df_report.loc[idx_pilot, "comments_misc"] = "PILOT PARTICIPANT"
        self.df_report = df_report[df_report["interview_date"].notna()]

    def _get_clean(self):
        """Find and combine cleaned BDI data.

        Get pilot, study data for day2, day3.

        Attributes
        ----------
        df_bdi_day2 : pd.DataFrame
            Cleaned visit_day2 BDI RedCap survey
        df_bdi_day3 : pd.DataFrame
            Cleaned visit_day3 BDI RedCap survey

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

        # Combine pilot and study data, updated subj id column, set attribute
        df_bdi_day2 = pd.concat([df_pilot2, df_study2], ignore_index=True)
        df_bdi_day2 = df_bdi_day2.rename(
            columns={"study_id": "src_subject_id"}
        )
        self.df_bdi_day2 = df_bdi_day2

        # Repeat above for day3
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
        self.df_bdi_day3 = df_bdi_day3

    def _make_bdi(self, sess):
        """Make an NDAR compliant report for visit.

        Remap column names, add demographic info, get session
        age, and generate report.

        Parameters
        ----------
        sess : str
            [day2 | day3]
            visit/session name

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        ValueError
            If sess is not day2 or day3

        """
        # Check sess value
        sess_list = ["day2", "day3"]
        if sess not in sess_list:
            raise ValueError(f"Incorrect visit day : {sess}")

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

        # Build dataframe from nda columns, update with df_bdi_demo data
        df_nda = pd.DataFrame(columns=self.nda_cols, index=df_bdi_demo.index)
        df_nda.update(df_bdi_demo)
        return df_nda


class NdarBrd01:
    pass


class NdarDemoInfo01:
    """Make demo_info01 report for NDAR submission.

    Parameters
    ----------
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of demographic data that complies with NDAR data definitions
    final_demo : pd.DataFrame
        Compiled demographic information
    nda_label : list
        NDA report template column label

    """

    def __init__(self, final_demo):
        """Read in demographic info and make report.

        Read-in data, setup empty df_report, and coordinate
        filling df_report.

        Parameters
        ----------
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of demographic data that complies with NDAR data definitions
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : demo_info01 ...")
        self.final_demo = final_demo
        self.nda_label, nda_cols = report_helper.mine_template(
            "demo_info01_template.csv"
        )
        self.df_report = pd.DataFrame(columns=nda_cols)
        self._make_demo()

    def _make_demo(self):
        """Update df_report with NDAR-required demographic information."""
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


class NdarEmrq01:
    """Make emrq01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of EMRQ data that complies with NDAR data definitions
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned ERQ Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : emrq01 ...")
        # Read in template
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "emrq01_template.csv"
        )

        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_pilot/data_survey",
                "visit_day1/data_clean",
                "df_ERQ.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_ERQ.csv",
            )
        )
        df_emrq = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, drop NaN rows
        df_emrq = df_emrq.rename(columns={"study_id": "src_subject_id"})
        df_emrq = df_emrq.replace("NaN", np.nan)
        self.df_emrq = df_emrq[df_emrq["ERQ_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        self.final_demo = final_demo.dropna(subset=["subjectkey"])
        self._make_emrq()

    def _make_emrq(self):
        """Generate ERMQ report for NDAR submission.

        Remap values and calculate totals.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of EMRQ data that complies with NDAR data definitions

        """
        # Update column names, make data integer
        df_emrq = self.df_emrq.rename(columns=str.lower)
        emrq_cols = [x for x in df_emrq.columns if "erq" in x]
        df_emrq[emrq_cols] = df_emrq[emrq_cols].astype("Int64")

        # Calculate reappraisal, suppression scores:
        #   Reappraisal = sum of items 1, 3, 5, 7, 8, 10
        #   Suppression = sum of items 2, 4, 6, 9
        cols_reap = ["erq_1", "erq_3", "erq_5", "erq_7", "erq_8", "erq_10"]
        cols_supp = ["erq_2", "erq_4", "erq_6", "erq_9"]
        df_emrq["erq_reappraisal"] = df_emrq[cols_reap].sum(axis=1)
        df_emrq["erq_suppression"] = df_emrq[cols_supp].sum(axis=1)
        df_emrq[["erq_reappraisal", "erq_suppression"]] = df_emrq[
            ["erq_reappraisal", "erq_suppression"]
        ].astype("Int64")

        # Combine demographic and erq dataframes
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_nda["sex"] = df_nda["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        df_emrq_nda = pd.merge(df_emrq, df_nda, on="src_subject_id")
        df_emrq_nda = report_helper.get_survey_age(
            df_emrq_nda, self.final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with df_final_emrq data
        df_report = pd.DataFrame(
            columns=self.nda_cols, index=df_emrq_nda.index
        )
        df_report.update(df_emrq_nda)

        # Add pilot comments
        pilot_list = report_helper.pilot_list()
        idx_pilot = df_report[
            df_report["src_subject_id"].isin(pilot_list)
        ].index.tolist()
        df_report.loc[idx_pilot, "comments_misc"] = "PILOT PARTICIPANT"
        self.df_report = df_report


class NdarImage03:
    """Make image03 report line-by-line.

    Identify all data in rawdata and add a line to image03 for each
    MRI file in rawdata. Utilize BIDS JSON sidecar and DICOM header
    information to identify required values.

    Make copies of study participants' NIfTI and events files in:
        <proj_dir>/ndar_report/data_mri

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    test_subj : str, optional
        BIDS subject identifier, for testing class

    Attributes
    ----------
    df_report_pilot : pd.DataFrame
        Image03 for pilot participants
    df_report_study : pd.DataFrame
        Image03 values for experiment/study participants
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    local_path : str
        Output location for NDA's report package builder
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label
    proj_dir : path
        Project's experiment directory
    sess : str
        BIDS session, iteratively set in _make_image03
    source_dir : path
        Location of project sourcedata
    subj : str
        BIDS subject, iteratively set in _make_image03
    subj_nda : str
        Participant ID, iteratively set in _make_image03
    subj_sess : path
        Location of subject's session data, iteratively set in _make_image03
    subj_sess_list : list
        Paths to all participant's sessions,
        e.g. ["/path/sub-12/ses-A", "/path/sub-12/ses-B"]

    Methods
    -------
    _make_image03
        Conducts all work by matching MRI type to _info_<mri-type> method
        _info_anat = Get anatomical information
        _info_fmap = Get field map information
        _info_func = Get functional information

    """

    def __init__(self, proj_dir, final_demo, test_subj=None):
        """Coordinate report generation for MRI data.

        Assumes BIDS organization of <proj_dir>/data_scanner_BIDS. Identify
        all subject sessions in rawdata, generate image03 report for all
        data types found within each session, integrate demographic
        information, and combine with previously-generated image03 info
        for the pilot participants.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        test_subj : str, optional
            BIDS subject identifier, for testing class

        Attributes
        ----------
        df_report_study : pd.DataFrame
            Image03 values for experiment/study participants
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        local_path : str
            Output location for NDA's report package builder
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label
        proj_dir : path
            Project's experiment directory
        source_dir : path
            Location of project sourcedata
        subj_sess_list : list
            Paths to all participant's sessions,
            e.g. ["/path/sub-12/ses-A", "/path/sub-12/ses-B"]

        Raises
        ------
        FileNotFoundError
            Missing pilot image03 csv
        ValueError
            Empty subj_sess_list

        """
        print("Buiding NDA report : image03 ...")

        # Read in template, start empty dataframe
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "image03_template.csv"
        )
        self.df_report_study = pd.DataFrame(columns=self.nda_cols)

        # Set reference and orienting attributes
        self.proj_dir = proj_dir
        self.local_path = (
            "/run/user/1001/gvfs/smb-share:server"
            + "=ccn-keoki.win.duke.edu,share=experiments2/EmoRep/"
            + "Exp2_Compute_Emotion/ndar_upload/data_mri"
        )
        self.source_dir = os.path.join(
            proj_dir, "data_scanner_BIDS/sourcedata"
        )

        # Identify all session in rawdata, check that data is found
        rawdata_dir = os.path.join(proj_dir, "data_scanner_BIDS/rawdata")
        if test_subj:
            self.subj_sess_list = sorted(
                glob.glob(f"{rawdata_dir}/{test_subj}/ses-day*")
            )
        else:
            self.subj_sess_list = sorted(
                glob.glob(f"{rawdata_dir}/sub-ER*/ses-day*")
            )
        if not self.subj_sess_list:
            raise ValueError(
                f"Subject, session paths not found in {rawdata_dir}"
            )

        # Get demographic info
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo = final_demo.dropna(subset=["subjectkey"])
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self.final_demo = final_demo

        # Get pilot df, make df_report for all study participants
        self._make_pilot()
        self._make_image03()

        # Combine df_report_study and df_report_pilot
        self.df_report = pd.concat(
            [self.df_report_pilot, self.df_report_study]
        )

    def _make_pilot(self):
        """Read previously-generated NDAR report for pilot participants.

        Attributes
        ----------
        df_report_pilot : pd.DataFrame
            Image03 for pilot participants

        Raises
        ------
        FileNotFoundError
            Expected pilot image dataset not found

        """
        # Read-in dataframe of pilot participants
        pilot_report = os.path.join(
            self.proj_dir,
            "data_pilot/ndar_resources",
            "image03_dataset.csv",
        )
        if not os.path.exists(pilot_report):
            raise FileNotFoundError(
                f"Expected to find pilot image03 at {pilot_report}"
            )
        df_report_pilot = pd.read_csv(pilot_report)
        df_report_pilot = df_report_pilot[1:]
        df_report_pilot.columns = self.nda_cols
        df_report_pilot["comments_misc"] = "PILOT PARTICIPANT"
        self.df_report_pilot = df_report_pilot

    def _make_image03(self):
        """Generate image03 report for study participants.

        Iterate through subj_sess_list, identify the data types of
        the session, and for each data type trigger appropriate method.
        Each iteration resets the attributes of sess, subj, subj_nda, and
        subj_sess so they are available for private methods.

        Attributes
        ----------
        sess : str
            BIDS session
        subj : str
            BIDS subject
        subj_nda : str
            Participant ID
        subj_sess : path
            Location of subject's session data

        Raises
        ------
        AttributeError
            An issue with the value of subj, subj_nda, or sess

        """
        # Specify MRI data types - these are used to match
        # and use private class methods to the data of the
        # participants session.
        type_list = ["anat", "func", "fmap"]
        cons_list = self.final_demo["src_subject_id"].tolist()

        # Set attributes for each subject's session
        for subj_sess in self.subj_sess_list:
            self.subj_sess = subj_sess
            self.subj = os.path.basename(os.path.dirname(subj_sess))
            self.subj_nda = self.subj.split("-")[1]
            self.sess = os.path.basename(subj_sess)

            # Check attributes
            chk_subj = True if len(self.subj) == 10 else False
            chk_subj_nda = True if len(self.subj_nda) == 6 else False
            chk_sess = True if len(self.sess) == 7 else False
            if not chk_subj and not chk_subj_nda and not chk_sess:
                raise AttributeError(
                    f"""\
                Unexpected value of one of the following:
                    self.subj : {self.subj}
                    self.subj_nda : {self.subj_nda}
                    self.sess : {self.sess}

                Possible cause is non-BIDS organization of rawdata,
                Check build_reports.NdarImage03.__init__ for rawdata_dir.
                """
                )

            # Only use participants found in final_demo, reflecting
            # current consent and available demo info.
            if self.subj_nda not in cons_list:
                print(
                    f"""
                    {self.subj_nda} not found in self.final_demo,
                        continuing ...
                    """
                )
                continue

            # Identify types of data in subject's session, use appropriate
            # method for data type.
            print(f"\tMining data for {self.subj}, {self.sess}")
            scan_type_list = [
                x for x in os.listdir(subj_sess) if x in type_list
            ]
            if not scan_type_list:
                print(f"No data types found at {subj_sess}\n\tContinuing ...")
                continue
            for scan_type in scan_type_list:
                info_meth = getattr(self, f"_info_{scan_type}")
                info_meth()

    def _get_subj_demo(self, scan_date):
        """Gather required participant demographic information.

        Find participant demographic info and calculate age-in-months
        at time of scan.

        Parameters
        ----------
        scan_date : datetime
            Date of scan

        Returns
        -------
        dict
            Keys = image03 column names
            Values = demographic info

        """
        # Identify participant date of birth, sex, and GUID
        # TODO deal with participants not found in final_demo (withdrawn)
        final_demo = self.final_demo
        idx_subj = final_demo.index[
            final_demo["src_subject_id"] == self.subj_nda
        ].tolist()[0]
        subj_guid = final_demo.iloc[idx_subj]["subjectkey"]
        subj_id = final_demo.iloc[idx_subj]["src_subject_id"]
        subj_sex = final_demo.iloc[idx_subj]["sex"]
        subj_dob = datetime.strptime(
            final_demo.iloc[idx_subj]["dob"], "%Y-%m-%d"
        )

        # Calculate age in months
        interview_age = report_helper.calc_age_mo([subj_dob], [scan_date])[0]
        interview_date = datetime.strftime(scan_date, "%m/%d/%Y")

        return {
            "subjectkey": subj_guid,
            "src_subject_id": subj_id,
            "interview_date": interview_date,
            "interview_age": interview_age,
            "gender": subj_sex,
        }

    def _get_std_info(self, nii_json, dicom_hdr):
        """Extract values reported for all scan types.

        Mine the DICOM header and BIDS JSON sidecar file for
        required scan information reported for every scan.

        Parameters
        ----------
        nii_json : dict
            Sidecar JSON information
        dicom_hdr : obj, pydicom.read_file()
            DICOM header information

        Returns
        -------
        dict
            Keys = image03 column names
            Values = Derived or hardcoded info about scan

        """
        # Identify scanner information
        scanner_manu = dicom_hdr[0x08, 0x70].value.split(" ")[0]
        scanner_type = dicom_hdr[0x08, 0x1090].value
        phot_int = dicom_hdr[0x08, 0x9205].value

        # Identify grid information
        num_frames = int(dicom_hdr[0x28, 0x08].value)
        num_rows = int(dicom_hdr[0x28, 0x10].value)
        num_cols = int(dicom_hdr[0x28, 0x11].value)
        h_mat = nii_json["AcquisitionMatrixPE"]
        acq_mat = f"{h_mat} {h_mat}"
        # h_fov = dicom_hdr[0x18, 0x9058].value
        acq_fov = "256 256"

        return {
            "scan_object": "Live",
            "image_file_format": "NIFTI",
            "image_modality": f"{nii_json['Modality']}I",
            "scanner_manufacturer_pd": scanner_manu,
            "scanner_type_pd": scanner_type,
            "scanner_software_versions_pd": str(nii_json["SoftwareVersions"]),
            "magnetic_field_strength": str(nii_json["MagneticFieldStrength"]),
            "mri_repetition_time_pd": nii_json["RepetitionTime"],
            "mri_echo_time_pd": str(nii_json["EchoTime"]),
            "flip_angle": str(nii_json["FlipAngle"]),
            "acquisition_matrix": acq_mat,
            "mri_field_of_view_pd": acq_fov,
            "patient_position": "supine",
            "photomet_interpret": phot_int,
            "receive_coil": nii_json["CoilString"],
            "transformation_performed": "No",
            "image_extent1": num_rows,
            "image_extent2": num_cols,
            "image_extent3": num_frames,
            "image_unit1": "Millimeters",
            "image_unit2": "Millimeters",
            "image_unit3": "Millimeters",
            "image_orientation": "Axial",
            "visnum": float(self.sess[-1]),
        }

    def _make_host(self, share_file, out_name):
        """Copy a file for hosting with NDA package builder.

        Data will be copied to <proj_dir>/ndar_upload/data_mri.

        Parameters
        ----------
        share_file : path
            Location of desired file to share
        out_name : str
            Output name of file

        Raises
        ------
        FileNotFoundError
            share_file does not exist
            output <host_file> does not exist

        """
        # Check for existing share_file
        if not os.path.exists(share_file):
            raise FileNotFoundError(f"Expected to find : {share_file}")

        # Setup output path
        host_file = os.path.join(
            self.proj_dir, "ndar_upload/data_mri", out_name
        )

        # Submit copy subprocess
        if not os.path.exists(host_file):
            print(f"\t\t\tMaking host file : {host_file}")
            bash_cmd = f"cp {share_file} {host_file}"
            h_sp = subprocess.Popen(
                bash_cmd, shell=True, stdout=subprocess.PIPE
            )
            h_out, h_err = h_sp.communicate()
            h_sp.wait()

        # Check for output
        if not os.path.exists(host_file):
            raise FileNotFoundError(
                f"""
                Copy failed, expected to find : {host_file}
                Check build_reports.NdarImage03._make_host().
                """
            )

    def _info_anat(self):
        """Write image03 line for anat data.

        Use _get_subj_demo and _get_std_info to find demographic and
        common field entries, and then determine values specific for
        anatomical scans. Update self.df_report_study with these data.
        Host a defaced anatomical image.

        Raises
        ------
        FileNotFoundError
            Missing JSON sidecar
            Missing DICOM file
            Missing defaced version in derivatives

        """
        print(f"\t\tWriting line for {self.subj} {self.sess} : anat ...")

        # Get JSON info
        json_file = sorted(glob.glob(f"{self.subj_sess}/anat/*.json"))[0]
        if not os.path.exists(json_file):
            raise FileNotFoundError(
                f"""
                Expected to find a JSON sidecar file at :
                    {self.subj_ses}/anat
                """
            )
        with open(json_file, "r") as jf:
            nii_json = json.load(jf)

        # Get DICOM info
        day = self.sess.split("-")[1]
        dicom_file = glob.glob(
            f"{self.source_dir}/{self.subj_nda}/{day}*/DICOM/EmoRep_anat/*.dcm"
        )[0]
        if not os.path.exists(dicom_file):
            raise FileNotFoundError(
                f"""
                Expected to find a DICOM file at :
                    {self.source_dir}/{self.subj_nda}/{day}*/DICOM/EmoRep_anat
                """
            )
        dicom_hdr = pydicom.read_file(dicom_file)

        # Get demographic info
        scan_date = datetime.strptime(dicom_hdr[0x08, 0x20].value, "%Y%m%d")
        demo_dict = self._get_subj_demo(scan_date)

        # Setup host file
        deface_file = os.path.join(
            self.proj_dir,
            "data_scanner_BIDS/derivatives/deface",
            self.subj,
            self.sess,
            f"{self.subj}_{self.sess}_T1w_defaced.nii.gz",
        )
        if not os.path.exists(deface_file):
            raise FileNotFoundError(
                f"Expected to find defaced file : {deface_file}"
            )
        host_name = f"{demo_dict['subjectkey']}_{day}_T1_anat.nii.gz"
        self._make_host(deface_file, host_name)

        # Get general, anat specific acquisition info
        std_dict = self._get_std_info(nii_json, dicom_hdr)
        anat_image03 = {
            "image_file": f"{self.local_path}/{host_name}",
            "image_description": "MPRAGE",
            "scan_type": "MR structural (T1)",
            "image_history": "Face removed",
            "image_num_dimensions": 3,
            "image_resolution1": 1.0,
            "image_resolution2": 1.0,
            "image_resolution3": float(nii_json["SliceThickness"]),
            "image_slice_thickness": float(nii_json["SliceThickness"]),
            "software_preproc": "pydeface version=2.0.2",
        }

        # Combine demographic, common MRI, and anat-specific dicts
        anat_image03.update(demo_dict)
        anat_image03.update(std_dict)

        # Add scan info to report
        new_row = pd.DataFrame(anat_image03, index=[0])
        self.df_report_study = pd.concat(
            [self.df_report_study.loc[:], new_row]
        ).reset_index(drop=True)
        del new_row

    def _info_fmap(self):
        """Write image03 line for fmap data.

        Use _get_subj_demo and _get_std_info to find demographic and
        common field entries, and then determine values specific for
        field map scans. Update self.df_report_study with these data.
        Host a field map file.

        Raises
        ------
        FileNotFoundError
            Missing JSON sidecar
            Missing DICOM file
            Missing NIfTI file

        """
        print(f"\t\tWriting line for {self.subj} {self.sess} : fmap ...")

        # Set paths for nii/json files, get JSON info
        nii_file = sorted(glob.glob(f"{self.subj_sess}/fmap/*.nii.gz"))[0]
        json_file = sorted(glob.glob(f"{self.subj_sess}/fmap/*.json"))[0]
        if not nii_file:
            raise FileNotFoundError(
                f"Expected to find : {self.subj_sess}/fmap/*.nii.gz"
            )
        if not json_file:
            raise FileNotFoundError(
                f"Expected to find : {self.subj_sess}/fmap/*.json"
            )
        with open(json_file, "r") as jf:
            nii_json = json.load(jf)

        # Get DICOM header info
        day = self.sess.split("-")[1]
        dicom_dir = os.path.join(
            self.source_dir, self.subj_nda, f"{day}*", "DICOM", "Field_Map_PA"
        )
        dicom_list = sorted(glob.glob(f"{dicom_dir}/*.dcm"))
        if not dicom_list:
            raise FileNotFoundError(
                f"Expected to find DICOMs in : {dicom_dir}"
            )
        dicom_file = dicom_list[0]
        dicom_hdr = pydicom.read_file(dicom_file)

        # Get demographic info
        scan_date = datetime.strptime(dicom_hdr[0x08, 0x20].value, "%Y%m%d")
        demo_dict = self._get_subj_demo(scan_date)

        # Make a host file
        h_guid = demo_dict["subjectkey"]
        host_nii = f"{h_guid}_{day}_fmap1_revpol.nii.gz"
        self._make_host(nii_file, host_nii)

        # Get general, anat specific acquisition info
        std_dict = self._get_std_info(nii_json, dicom_hdr)

        # Setup fmap-specific values
        fmap_image03 = {
            "image_file": f"{self.local_path}/{host_nii}",
            "image_description": "fmap (reverse phase polarity)",
            "scan_type": "Field Map",
            "image_history": "No modifications",
            "image_num_dimensions": 4,
            "image_extent4": len(dicom_list),
            "extent4_type": "time",
            "image_unit4": "number of Volumes (across time)",
            "image_resolution1": 2.0,
            "image_resolution2": 2.0,
            "image_resolution3": float(nii_json["SliceThickness"]),
            "image_resolution4": float(nii_json["RepetitionTime"]),
            "image_slice_thickness": float(nii_json["SliceThickness"]),
            "slice_timing": f"{nii_json['SliceTiming']}",
        }

        # Combine all dicts
        fmap_image03.update(demo_dict)
        fmap_image03.update(std_dict)

        # Add scan info to report
        new_row = pd.DataFrame(fmap_image03, index=[0])
        self.df_report_study = pd.concat(
            [self.df_report_study.loc[:], new_row]
        ).reset_index(drop=True)

    def _info_func(self):
        """Write image03 line for func data.

        Identify all func files and iterate through. Use _get_subj_demo
        and _get_std_info to find demographic and common field entries,
        and then determine values specific for func scans. Update
        self.df_report_study with these data.
        Host the func and events files.

        Raises
        ------
        FileNotFoundError
            Missing JSON sidecars
            Missing DICOM files
            Missing NIfTI files
            Missing events files

        """
        # Determine if participant is pilot, set experiment ID
        pilot_list = report_helper.pilot_list()
        exp_dict = {"old": 1683, "new": 2113}
        exp_id = (
            exp_dict["old"] if self.subj_nda in pilot_list else exp_dict["new"]
        )

        # Find all func niftis
        nii_list = sorted(glob.glob(f"{self.subj_sess}/func/*.nii.gz"))
        if not nii_list:
            raise FileNotFoundError(
                f"Expected NIfTIs at : {self.subj_sess}/func/"
            )

        # Write line for each func nifti
        for nii_path in nii_list:

            # Get JSON for func run
            json_path = re.sub(".nii.gz$", ".json", nii_path)
            if not os.path.exists(json_path):
                raise FileNotFoundError(f"Expected to find : {json_path}")
            with open(json_path, "r") as jf:
                nii_json = json.load(jf)

            # Identify appropriate DICOM, load header
            _, _, task, run, _ = os.path.basename(nii_path).split("_")
            print(
                f"\t\tWriting line for {self.subj} {self.sess} : "
                + f"func {task} {run} ..."
            )
            day = self.sess.split("-")[1]
            task_dir = (
                "Rest_run01"
                if task == "task-rest"
                else f"EmoRep_run{run.split('-')[1]}"
            )
            task_source = os.path.join(
                self.source_dir, self.subj_nda, f"{day}*/DICOM", task_dir
            )
            dicom_list = glob.glob(f"{task_source}/*.dcm")
            if not dicom_list:
                raise FileNotFoundError(
                    f"Expected to find DICOMs at : {task_source}"
                )
            dicom_file = dicom_list[0]
            dicom_hdr = pydicom.read_file(dicom_file)

            # Get demographic info
            scan_date = datetime.strptime(
                dicom_hdr[0x08, 0x20].value, "%Y%m%d"
            )
            demo_dict = self._get_subj_demo(scan_date)

            # Setup host nii
            h_guid = demo_dict["subjectkey"]
            h_task = task.split("-")[1]
            h_run = run[-1]
            # host_nii = f"{h_guid}_{day}_func_{h_task}_run{h_run}.nii.gz"
            host_nii = f"{h_guid}_{day}_func_emostim_run{h_run}.nii.gz"
            self._make_host(nii_path, host_nii)

            # Setup host events, account for no rest
            # events and missing task files.
            events_exists = False
            if not h_task == "rest":
                # host_events = (
                #     f"{h_guid}_{day}_func_{h_task}_run{h_run}_events.tsv"
                # )
                host_events = (
                    f"{h_guid}_{day}_func_emostim_run{h_run}_events.tsv"
                )
                events_path = re.sub("_bold.nii.gz$", "_events.tsv", nii_path)
                events_exists = True if os.path.exists(events_path) else False
                if events_exists:
                    self._make_host(events_path, host_events)

            # Get general, anat specific acquisition info
            std_dict = self._get_std_info(nii_json, dicom_hdr)

            # Setup func-specific values
            func_image03 = {
                "image_file": f"{self.local_path}/{host_nii}",
                "image_description": "EPI fMRI",
                "experiment_id": exp_id,
                "scan_type": "fMRI",
                "image_history": "No modifications",
                "image_num_dimensions": 4,
                "image_extent4": len(dicom_list),
                "extent4_type": "time",
                "image_unit4": "number of Volumes (across time)",
                "image_resolution1": 2.0,
                "image_resolution2": 2.0,
                "image_resolution3": float(nii_json["SliceThickness"]),
                "image_resolution4": float(nii_json["RepetitionTime"]),
                "image_slice_thickness": float(nii_json["SliceThickness"]),
                "slice_timing": f"{nii_json['SliceTiming']}",
            }
            if not h_task == "rest" and events_exists:
                func_image03["data_file2"] = f"{self.local_path}/{host_events}"
                func_image03["data_file2_type"] = "task event information"

            # Combine all dicts
            func_image03.update(demo_dict)
            func_image03.update(std_dict)

            # Add scan info to report
            new_row = pd.DataFrame(func_image03, index=[0])
            self.df_report_study = pd.concat(
                [self.df_report_study.loc[:], new_row]
            ).reset_index(drop=True)


class NdarPanas01:
    """Make panas01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_panas_day2 : pd.DataFrame
        Cleaned visit_day2 PANAS Qualtrics survey
    df_panas_day3 : pd.DataFrame
        Cleaned visit_day3 PANAS Qualtrics survey
    df_report : pd.DataFrame
        Report of PANAS data that complies with NDAR data definitions
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label
    proj_dir : path
        Project's experiment directory

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned PANAS Qualtrics survey from visit_day2 and
        visit_day3, and finalized demographic information.
        Generate PANAS report.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of PANAS data that complies with NDAR data definitions
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label
        proj_dir : path
            Project's experiment directory

        """
        # Get needed column values from report template
        print("Buiding NDA report : panas01 ...")
        self.proj_dir = proj_dir
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "panas01_template.csv"
        )

        # Get pilot, study data for both day2, day3
        df_pilot = self._get_pilot()
        self._get_clean()

        # Get final demographics
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self.final_demo = final_demo

        # Make reports for each visit
        df_nda_day2 = self._make_panas("day2")
        df_nda_day3 = self._make_panas("day3")

        # Combine into final report
        df_report = pd.concat([df_pilot, df_nda_day2, df_nda_day3])
        df_report = df_report.sort_values(by=["src_subject_id"])
        self.df_report = df_report[df_report["interview_date"].notna()]

    def _calc_metrics(self, df):
        """Calculate positive and negative metrics.

        Parameters
        ----------
        df : pd.DataFrame
            Uses NDAR column names

        Returns
        -------
        pd.DataFrame

        """
        cols_pos = [
            "interested_q1",
            "excited_q3",
            "strong_q5",
            "enthusiastic_q9",
            "proud_q10",
            "alert_q12",
            "determined_q16",
            "attentive_q17",
            "active_q19",
        ]
        cols_neg = [
            "distressed_q2",
            "upset1_q4",
            "guilty_q6",
            "scared_q7",
            "hostile_q8",
            "irritable_q11",
            "ashamed_q13",
            "nervous_q15",
            "jittery_q18",
            "afraid_q20",
        ]

        # Calculate positive metrics
        df["sum_pos"] = df[cols_pos].sum(axis=1)
        df["sum_pos"] = df["sum_pos"].astype("Int64")
        df["mean_pos_moment"] = df[cols_pos].mean(axis=1, skipna=True).round(3)
        df["mean_pos_moment_sd"] = (
            df[cols_pos].std(axis=1, skipna=True).round(3)
        )

        # Calculate negative metrics
        df["sum_neg"] = df[cols_neg].sum(axis=1)
        df["sum_neg"] = df["sum_neg"].astype("Int64")
        df["mean_neg_moment"] = df[cols_neg].mean(axis=1, skipna=True).round(3)
        df["mean_neg_moment_sd"] = (
            df[cols_neg].std(axis=1, skipna=True).round(3)
        )
        return df

    def _get_pilot(self):
        """Get PANAS data of pilot participants.

        Import data from previous NDAR submission.

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        FileNotFoundError
            Missing dataframe of pilot data

        """
        # Read-in dataframe of pilot participants
        pilot_report = os.path.join(
            self.proj_dir,
            "data_pilot/ndar_resources",
            "panas01_dataset.csv",
        )
        if not os.path.exists(pilot_report):
            raise FileNotFoundError(
                f"Expected to find pilot panas01 at {pilot_report}"
            )
        df_pilot = pd.read_csv(pilot_report)
        df_pilot = df_pilot[1:]
        df_pilot.columns = self.nda_cols

        # Add visit, get metrics
        df_pilot["visit"] = "day1"
        p_cols = [x for x in df_pilot.columns if "_q" in x]
        df_pilot[p_cols] = df_pilot[p_cols].astype("Int64")
        df_pilot = self._calc_metrics(df_pilot)
        df_pilot["comments_misc"] = "PILOT PARTICIPANT"
        return df_pilot

    def _get_clean(self):
        """Find and combine cleaned PANAS data.

        Get pilot, study data for day2, day3.

        Attributes
        ----------
        df_panas_day2 : pd.DataFrame
            Cleaned visit_day2 PANAS Qualtrics survey
        df_panas_day3 : pd.DataFrame
            Cleaned visit_day3 PANAS Qualtrics survey

        """
        # Get visit_day2 data
        df_panas_day2 = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_survey",
                "visit_day2/data_clean",
                "df_PANAS.csv",
            )
        )
        df_panas_day2 = df_panas_day2.rename(
            columns={"study_id": "src_subject_id"}
        )
        self.df_panas_day2 = df_panas_day2

        # Get visit_day3 data
        df_panas_day3 = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_survey",
                "visit_day3/data_clean",
                "df_PANAS.csv",
            )
        )
        df_panas_day3 = df_panas_day3.rename(
            columns={"study_id": "src_subject_id"}
        )
        df_panas_day3.columns = df_panas_day2.columns.values
        self.df_panas_day3 = df_panas_day3

    def _make_panas(self, sess):
        """Make an NDAR compliant report for visit.

        Remap column names, add demographic info, get session
        age, and generate report.

        Parameters
        ----------
        sess : str
            [day2 | day3]
            visit/session name

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        ValueError
            If sess is not day2 or day3

        """
        # Check sess value
        sess_list = ["day2", "day3"]
        if sess not in sess_list:
            raise ValueError(f"Incorrect visit day : {sess}")

        # Get session data
        df_panas = getattr(self, f"df_panas_{sess}")

        # Convert response values to int, set answer_type
        p_cols = [x for x in df_panas.columns if "PANAS_" in x]
        df_panas[p_cols] = df_panas[p_cols].astype("Int64")
        df_panas["answer_type"] = 1

        # Remap column names, get metrics
        map_item = {
            "PANAS_1": "interested_q1",
            "PANAS_2": "distressed_q2",
            "PANAS_3": "excited_q3",
            "PANAS_4": "strong_q5",
            "PANAS_5": "scared_q7",
            "PANAS_6": "enthusiastic_q9",
            "PANAS_7": "ashamed_q13",
            "PANAS_8": "nervous_q15",
            "PANAS_9": "attentive_q17",
            "PANAS_10": "active_q19",
            "PANAS_11": "irritable_q11",
            "PANAS_12": "alert_q12",
            "PANAS_13": "upset1_q4",
            "PANAS_14": "guilty_q6",
            "PANAS_15": "hostile_q8",
            "PANAS_16": "proud_q10",
            "PANAS_17": "inspired_q14",
            "PANAS_18": "determined_q16",
            "PANAS_19": "jittery_q18",
            "PANAS_20": "afraid_q20",
        }
        df_panas_remap = df_panas.rename(columns=map_item)
        df_panas_remap = self._calc_metrics(df_panas_remap)

        # Add visit
        df_panas_remap["visit"] = sess

        # Combine demo and panas dataframes, get survey age
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_panas_demo = pd.merge(df_panas_remap, df_nda, on="src_subject_id")
        df_panas_demo = report_helper.get_survey_age(
            df_panas_demo, self.final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with df_panas_demo data
        df_nda = pd.DataFrame(columns=self.nda_cols, index=df_panas_demo.index)
        df_nda.update(df_panas_demo)
        return df_nda


class NdarPhysio01:
    pass


class NdarPswq01:
    """Make pswq01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of PSWQ data that complies with NDAR data definitions
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned PSWQ Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : pswq01 ...")
        # Read in template
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "pswq01_template.csv"
        )

        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_pilot/data_survey",
                "visit_day1/data_clean",
                "df_PSWQ.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_PSWQ.csv",
            )
        )
        df_pswq = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, drop NaN rows
        df_pswq = df_pswq.replace("NaN", np.nan)
        self.df_pswq = df_pswq[df_pswq["PSWQ_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self.final_demo = final_demo.dropna(subset=["subjectkey"])
        self._make_pswq()

    def _make_pswq(self):
        """Generate PSWQ report for NDAR submission.

        Remap values and calculate totals.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of PSWQ data that complies with NDAR data definitions

        """
        # Update column names, make data integer
        df_pswq = self.df_pswq.rename(columns=str.lower)
        df_pswq.columns = df_pswq.columns.str.replace("_", "")
        pswq_cols = [x for x in df_pswq.columns if "pswq" in x]
        df_pswq[pswq_cols] = df_pswq[pswq_cols].astype("Int64")
        df_pswq = df_pswq.rename(columns={"studyid": "src_subject_id"})

        # Calculate sum
        df_pswq["pswq_total"] = df_pswq[pswq_cols].sum(axis=1)
        df_pswq["pswq_total"] = df_pswq["pswq_total"].astype("Int64")

        # Combine demographic and erq dataframes
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_pswq_nda = pd.merge(df_pswq, df_nda, on="src_subject_id")
        df_pswq_nda = report_helper.get_survey_age(
            df_pswq_nda, self.final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with df_final_emrq data
        df_report = pd.DataFrame(
            columns=self.nda_cols, index=df_pswq_nda.index
        )
        df_report.update(df_pswq_nda)
        pilot_list = report_helper.pilot_list()
        idx_pilot = df_report[
            df_report["src_subject_id"].isin(pilot_list)
        ].index.tolist()
        df_report.loc[idx_pilot, "comments_misc"] = "PILOT PARTICIPANT"
        self.df_report = df_report


class NdarRest01:
    pass


class NdarRrs01:
    """Make rrs01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of RRS data that complies with NDAR data definitions
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned RRS Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of RRS data that complies with NDAR data definitions
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label
        proj_dir : path
            Project's experiment directory

        """
        print("Buiding NDA report : rrs01 ...")
        # Read in template
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "rrs01_template.csv"
        )
        self.proj_dir = proj_dir

        # Get final demographics
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self.final_demo = final_demo.dropna(subset=["subjectkey"])

        # Make pilot, study dataframes
        df_pilot = self._get_pilot()
        df_study = self._make_rrs()

        # Combine into final report
        df_report = pd.concat([df_pilot, df_study], ignore_index=True)
        self.df_report = df_report[df_report["interview_date"].notna()]

    def _get_pilot(self):
        """Get RRS data of pilot participants.

        Import data from previous NDAR submission.

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        FileNotFoundError
            Missing dataframe of pilot data

        """
        # Read in pilot report
        pilot_report = os.path.join(
            self.proj_dir,
            "data_pilot/ndar_resources",
            "rrs01_dataset.csv",
        )
        if not os.path.exists(pilot_report):
            raise FileNotFoundError(
                f"Expected to find pilot rrs01 at {pilot_report}"
            )
        df_pilot = pd.read_csv(pilot_report)
        df_pilot = df_pilot[1:]
        df_pilot.columns = self.nda_cols
        df_pilot["comments_misc"] = "PILOT PARTICIPANT"

        # Calculate sum
        p_cols = [x for x in df_pilot.columns if "rrs" in x]
        df_pilot[p_cols] = df_pilot[p_cols].astype("Int64")
        df_pilot["rrs_total"] = df_pilot[p_cols].sum(axis=1)
        df_pilot["rrs_total"] = df_pilot["rrs_total"].astype("Int64")
        return df_pilot

    def _make_rrs(self):
        """Combine dataframes to generate requested report.

        Calculate totals and determine survey age.

        Returns
        -------
        pd.DataFrame
            Report of study RRS data that complies with NDAR data definitions

        """
        # Get clean survey data
        df_rrs = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_RRS.csv",
            )
        )

        # Rename columns, drop NaN rows
        df_rrs = df_rrs.rename(columns={"study_id": "src_subject_id"})
        df_rrs = df_rrs.replace("NaN", np.nan)
        df_rrs = df_rrs[df_rrs["RRS_1"].notna()]

        # Update column names, make data integer
        df_rrs = df_rrs.rename(columns=str.lower)
        rrs_cols = [x for x in df_rrs.columns if "rrs" in x]
        df_rrs[rrs_cols] = df_rrs[rrs_cols].astype("Int64")

        # Calculate sum
        df_rrs["rrs_total"] = df_rrs[rrs_cols].sum(axis=1)
        df_rrs["rrs_total"] = df_rrs["rrs_total"].astype("Int64")

        # Combine demographic and rrs dataframes
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_rrs_nda = pd.merge(df_rrs, df_nda, on="src_subject_id")
        df_rrs_nda = report_helper.get_survey_age(
            df_rrs_nda, self.final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with demo and rrs data
        df_study_report = pd.DataFrame(
            columns=self.nda_cols, index=df_rrs_nda.index
        )
        df_study_report.update(df_rrs_nda)
        return df_study_report


class NdarStai01:
    """Make stai01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_stai : pd.DataFrame
        Cleaned STAI Qualtrics survey
    df_report : pd.DataFrame
        Report of STAI data that complies with NDAR data definitions
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned STAI Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        df_stai : pd.DataFrame
            Cleaned STAI Qualtrics survey
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : stai01 ...")
        # Read in template
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "stai01_template.csv"
        )

        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_pilot/data_survey",
                "visit_day1/data_clean",
                "df_STAI.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_STAI.csv",
            )
        )
        df_stai = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, drop NaN rows
        df_stai = df_stai.rename(columns={"study_id": "src_subject_id"})
        df_stai = df_stai.replace("NaN", np.nan)
        self.df_stai = df_stai[df_stai["STAI_Trait_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self.final_demo = final_demo.dropna(subset=["subjectkey"])
        self._make_stai()

    def _make_stai(self):
        """Combine dataframes to generate requested report.

        Remap columns, calculate totals, and determine survey age.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of STAI data that complies with NDAR data definitions

        """
        # Make data integer
        df_stai = self.df_stai
        stai_cols = [x for x in df_stai.columns if "STAI" in x]
        df_stai[stai_cols] = df_stai[stai_cols].astype("Int64")

        # Remap column names
        map_item = {
            "STAI_Trait_1": "stai21",
            "STAI_Trait_2": "stai22",
            "STAI_Trait_3": "stai23",
            "STAI_Trait_4": "stai24",
            "STAI_Trait_5": "stai25",
            "STAI_Trait_6": "stai26",
            "STAI_Trait_7": "stai27",
            "STAI_Trait_8": "stai28",
            "STAI_Trait_9": "stai29",
            "STAI_Trait_10": "stai30",
            "STAI_Trait_11": "stai31",
            "STAI_Trait_12": "stai32",
            "STAI_Trait_13": "stai33",
            "STAI_Trait_14": "stai34",
            "STAI_Trait_15": "stai35",
            "STAI_Trait_16": "stai36",
            "STAI_Trait_17": "stai37",
            "STAI_Trait_18": "stai38",
            "STAI_Trait_19": "stai39",
            "STAI_Trait_20": "stai40",
        }
        df_stai_remap = df_stai.rename(columns=map_item)

        # Get trait sum
        trait_cols = [x for x in df_stai_remap.columns if "stai" in x]
        df_stai_remap["staiy_trait"] = df_stai_remap[trait_cols].sum(axis=1)
        df_stai_remap["staiy_trait"] = df_stai_remap["staiy_trait"].astype(
            "Int64"
        )

        # Combine demographic and stai dataframes
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_stai_nda = pd.merge(df_stai_remap, df_nda, on="src_subject_id")
        df_stai_nda = report_helper.get_survey_age(
            df_stai_nda, self.final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with demo and stai data
        self.df_report = pd.DataFrame(
            columns=self.nda_cols, index=df_stai_nda.index
        )
        self.df_report.update(df_stai_nda)


class NdarTas01:
    """Make tas01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_tas : pd.DataFrame
        Cleaned TAS Qualtrics survey
    df_report : pd.DataFrame
        Report of TAS data that complies with NDAR data definitions
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    nda_cols : list
        NDA report template column names
    nda_label : list
        NDA report template label

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned TAS Qualtrics survey from visit_day1, and
        finalized demographic information.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        df_stai : pd.DataFrame
            Cleaned TAS Qualtrics survey
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        nda_cols : list
            NDA report template column names
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : tas01 ...")
        # Read in template
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "tas01_template.csv"
        )

        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_pilot/data_survey",
                "visit_day1/data_clean",
                "df_TAS.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_TAS.csv",
            )
        )
        df_tas = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, drop NaN rows
        df_tas = df_tas.rename(columns={"study_id": "src_subject_id"})
        df_tas = df_tas.replace("NaN", np.nan)
        self.df_tas = df_tas[df_tas["TAS_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self.final_demo = final_demo.dropna(subset=["subjectkey"])
        self._make_tas()

    def _make_tas(self):
        """Combine dataframes to generate requested report.

        Rename columns, calculate totals, and determine survey age.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of TAS data that complies with NDAR data definitions

        """
        # Make data integer, calculate sum, and rename columns
        df_tas = self.df_tas
        tas_cols = [x for x in df_tas.columns if "TAS" in x]
        df_tas[tas_cols] = df_tas[tas_cols].astype("Int64")
        df_tas["tas_totalscore"] = df_tas[tas_cols].sum(axis=1)
        df_tas["tas_totalscore"] = df_tas["tas_totalscore"].astype("Int64")
        df_tas.columns = df_tas.columns.str.replace("TAS", "tas20")

        # Combine demographic and stai dataframes
        df_nda = self.final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_tas_nda = pd.merge(df_tas, df_nda, on="src_subject_id")
        df_tas_nda = report_helper.get_survey_age(
            df_tas_nda, self.final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with demo and stai data
        self.df_report = pd.DataFrame(
            columns=self.nda_cols, index=df_tas_nda.index
        )
        self.df_report.update(df_tas_nda)
