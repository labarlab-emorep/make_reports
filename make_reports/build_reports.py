"""Generate requested reports and organize relevant data."""
import os
import glob
import subprocess
import distutils.spawn
import pandas as pd
import numpy as np
from datetime import datetime
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
    """Title.

    Desc.

    Parameters
    ----------

    Attributes
    ----------
    df_guid_file
    df_guid
    proj_dir
    user_name
    user_pass

    Methods
    -------
    make_guids

    """

    def __init__(self, proj_dir, user_pass, user_name):
        """Title.

        Desc.

        Attributes
        ----------
        proj_dir
        user_name
        user_pass

        """
        self.proj_dir = proj_dir
        self.user_name = user_name
        self.user_pass = user_pass
        self._get_demo()

    def _get_demo(self):
        """Title.

        Desc.

        Attributes
        ----------
        df_guid_file
        df_guid

        Notes
        -----
        Writes ...

        """
        df_demo = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_survey/redcap_demographics/data_clean",
                "df_demographics.csv",
            )
        )
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

        df_guid["SEX"] = df_guid["SEX"].map({1.0: "M", 2.0: "F"})
        df_guid["SUBJECTHASMIDDLENAME"] = "Yes"
        df_guid.loc[
            df_guid["MIDDLENAME"].isnull(), "SUBJECTHASMIDDLENAME"
        ] = "No"

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
        """Title.

        Desc.

        Attributes
        ----------
        guid_file

        Raises
        ------

        Notes
        -----
        Requires
        Writes

        """
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
        self.guid_file = guid_output[-1]


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

        # Rename columns, frop NaN rows
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

        # Build dataframe from nda columns, update with df_final_bdi data
        df_nda = pd.DataFrame(columns=self.nda_cols, index=df_bdi_demo.index)
        df_nda.update(df_bdi_demo)
        return df_nda


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


class NdarEmoEndo01:
    pass


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

        # Rename columns, frop NaN rows
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
        self.df_report = pd.DataFrame(
            columns=self.nda_cols, index=df_emrq_nda.index
        )
        self.df_report.update(df_emrq_nda)


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
