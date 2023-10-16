"""Build reports and dataframes needed by lab manager.

DemoAll : aggregate demgraphic info into single dataframe
ManagerRegular : generate reports regularly submitted
GenerateGuids : generate, check GUIDs

"""
import os
import glob
import subprocess
import distutils.spawn
import pandas as pd
import numpy as np
from datetime import datetime
from make_reports.resources import manage_data
from make_reports.resources import report_helper


class DemoAll(manage_data.GetRedcap):
    """Gather demographic information from RedCap surveys.

    Inherits manage_data.GetRedcap.

    Only includes participants who have signed the consent form,
    have an assigned GUID, and have completed the demographic
    survey.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Project's parent directory
    redcap_token : str
        Personal access token for RedCap

    Attributes
    ----------
    final_demo : pd.DataFrame
        Complete report containing demographic info for NDA submission,
        regular manager reports.

    Methods
    -------
    make_complete()
        Used for regular manager reports and common NDAR fields
    remove_withdrawn()
        Remove participants from final_demo that have withdrawn consent
    submission_cycle(datetime)
        Remove participants from final_demo after a certain date

    Example
    -------
    get_demo = DemoAll("/path/to/proj", "token")
    df_demo_all = get_demo.final_demo

    get_demo.remove_withdrawn()
    get_demo.submission_cycle(2023-06-31)
    df_nda = get_demo.final_demo

    """

    def __init__(self, proj_dir, redcap_token):
        """Initialize, build final_demo."""
        print("Initializing DemoAll")
        super().__init__(proj_dir, redcap_token)
        self._get_demo()

        # Generate final_demo
        print("\tCompiling needed demographic info ...")
        self.make_complete()

    def _get_demo(self):
        """Get required pilot and study dataframes."""
        # Download and clean relevant RedCap surveys
        self.get_redcap(
            survey_list=[
                "guid",
                "demographics",
                "consent_pilot",
                "consent_v1.22",
            ]
        )

        # Extract and merge relevant pilot/study dfs
        df_cons_orig = self._get_pilot_study("visit_day1", "consent_pilot")
        df_cons_new = self._get_pilot_study("visit_day1", "consent_v1.22")
        df_demo = self._get_pilot_study("visit_day1", "demographics")
        df_guid = self._get_pilot_study("visit_day0", "guid")

        # Update consent_v1.22 column names from pilot and merge
        cols_new = df_cons_new.columns.tolist()
        cols_orig = df_cons_orig.columns.tolist()
        cols_replace = {}
        for h_new, h_orig in zip(cols_new, cols_orig):
            cols_replace[h_new] = h_orig
        df_cons_new = df_cons_new.rename(columns=cols_replace)
        df_consent = pd.concat([df_cons_orig, df_cons_new], ignore_index=True)
        df_consent = df_consent.drop_duplicates(subset=["record_id"])
        df_consent = df_consent.sort_values(by=["record_id"])
        df_consent = df_consent.drop(["date_of_birth"], axis=1)
        del df_cons_new, df_cons_orig

        # Merge dataframes, use merge how=inner (default) to keep
        # only participants who have data in all dataframes.
        self._df_merge = pd.merge(df_consent, df_guid, on="record_id")
        self._df_merge = pd.merge(self._df_merge, df_demo, on="record_id")

    def _get_pilot_study(self, visit: str, sur_name: str) -> pd.DataFrame:
        """Merge study, pilot dataframes."""
        return pd.concat(
            [
                self.clean_redcap["pilot"][visit][sur_name],
                self.clean_redcap["study"][visit][sur_name],
            ],
            ignore_index=True,
        )

    def _get_race(self):
        """Get participant race response.

        Account for single response, single response of
        multiple, multiple responses (which may not include
        the multiple option), and "other" responses.

        """
        # Get attribute for readibility, testing
        df_merge = self._df_merge

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
        self._race_resp = race_resp
        del df_merge

    def _get_ethnic_minority(self):
        """Determine if participant is considered a minority."""
        # Get ethnicity selection, convert to english
        h_ethnic = self._df_merge["ethnicity"].tolist()
        ethnic_switch = {
            1.0: "Hispanic or Latino",
            2.0: "Not Hispanic or Latino",
        }
        subj_ethnic = [ethnic_switch[x] for x in h_ethnic]

        # Determine if minority i.e. not white or hispanic
        subj_minor = []
        for race, ethnic in zip(self._race_resp, subj_ethnic):
            if race != "White" and ethnic == "Not Hispanic or Latino":
                subj_minor.append("Minority")
            else:
                subj_minor.append("Not Minority")
        self._subj_ethnic = subj_ethnic
        self._subj_minor = subj_minor

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
        # Get GUID, study IDs
        subj_guid = self._df_merge["guid"].tolist()
        subj_study = self._df_merge["study_id"].tolist()

        # Get consent date
        self._df_merge["datetime"] = pd.to_datetime(self._df_merge["date"])
        self._df_merge["datetime"] = self._df_merge["datetime"].dt.date
        subj_consent_date = self._df_merge["datetime"].tolist()

        # Get age, sex
        subj_age = self._df_merge["age"].tolist()
        h_sex = self._df_merge["gender"].tolist()
        sex_switch = {1.0: "Male", 2.0: "Female", 3.0: "Neither"}
        subj_sex = [sex_switch[x] for x in h_sex]

        # Get DOB, age in months, education
        subj_dob = self._df_merge["dob"]
        subj_age_mo = report_helper.calc_age_mo(subj_dob, subj_consent_date)
        subj_educate = self._df_merge["years_education"]

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
            "ethnicity": self._subj_ethnic,
            "race": self._race_resp,
            "is_minority": self._subj_minor,
            "years_education": subj_educate,
        }
        self.final_demo = pd.DataFrame(out_dict, columns=out_dict.keys())

    def remove_withdrawn(self):
        """Remove participants from final_demo who have withdrawn consent."""
        part_comp = report_helper.CheckStatus()
        part_comp.status_change("withdrew")
        if not part_comp.all:
            return
        withdrew_list = [x for x in part_comp.all.keys()]
        self.final_demo = self.final_demo[
            ~self.final_demo.src_subject_id.str.contains(
                "|".join(withdrew_list)
            )
        ]
        self.final_demo = self.final_demo.reset_index(drop=True)

    def submission_cycle(self, close_date):
        """Determine participants for NDAR submission cycle.

        Remove participants consented on or after close_date.

        Parameters
        ----------
        close_date : datetime
            Submission cycle close date

        """
        include_mask = self.final_demo["interview_date"] < close_date
        self.final_demo = self.final_demo.loc[include_mask]
        self.final_demo = self.final_demo.reset_index(drop=True)


class ManagerRegular(DemoAll):
    """Make reports regularly submitted by lab manager.

    Inherits DemoAll.

    Query data from the appropriate period for the period, and
    construct a dataframe containing the required information
    for the report.

    Parameters
    ----------
    query_date : datetime
        Date for finding report range
    proj_dir : str, os.PathLike
        Project's experiment directory
    redcap_token : str
        Personal access token for RedCap

    Attributes
    ----------
    df_report : pd.DataFrame, None
        Relevant info and format for requested report
    range_end : datetime
        End of period for report
    range_start : datetime
        Start of period for report

    Methods
    -------
    make_report(report)
        Entrypoint, trigger appropriate report method
    make_duke3()
        Generate report submitted to Duke every 3 months
    make_nih4()
        Generate report submitted to NIH every 4 months
    make_nih12()
        Generate report submitted to NIH every 12 months

    Examples
    --------
    make_rep = ManagerRegular(*args)
    make_rep.make_report("nih12")
    df_report = make_rep.df_report

    """

    def __init__(self, query_date, proj_dir, redcap_token):
        """Generate requested report."""
        super().__init__(proj_dir, redcap_token)
        self._query_date = query_date

    def make_report(self, report):
        """Generate requested report.

        Parameters
        ----------
        report : str
            [nih4 | nih12 | duke3]
            Select desired report

        """
        # Trigger appropriate method
        if report not in ["nih12", "nih4", "duke3"]:
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
            if h_start <= self._query_date <= h_end:
                start_end = (h_start, h_end)
                break
        if not start_end:
            raise ValueError(f"Date range not found for {self._query_date}.")
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
        range_end : datetime
            End of period for report
        range_start : datetime
            Start of period for report
        _df_range : pd.DataFrame
            Data found within the range_start, range_end period

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

        # Subset final_demo according to mask
        self._df_range = self.final_demo.loc[range_bool].copy(deep=True)
        print(f"\tReport range : {self.range_start} - {self.range_end}")

    def make_nih4(self):
        """Create report submitted to NIH every 4 months.

        Count the total number of participants who identify
        as minority or Hispanic, and total number of participants,
        since the beginning of the experiment.

        Attributes
        ----------
        df_report : pd.DataFrame, None
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

        # Find data within range, check for data
        self._get_data_range(
            nih_4mo_ranges,
            start_date=proj_start,
        )
        if self._df_range.empty:
            print(
                "\t\tNo data collected for query range : "
                + f"{self.range_start} - {self.range_end}, skipping ..."
            )
            self.df_report = None
            return

        # Calculate number of minority, hispanic, and total recruited
        num_minority = len(
            self._df_range.index[self._df_range["is_minority"] == "Minority"]
        )
        num_hispanic = len(
            self._df_range.index[
                self._df_range["ethnicity"] == "Hispanic or Latino"
            ]
        )
        num_total = len(self._df_range.index)

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
        df_report : pd.DataFrame, None
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

        # Find data within range, check for data
        self._get_data_range(duke_3mo_ranges)
        if self._df_range.empty:
            print(
                "\t\tNo data collected for query range : "
                + f"{self.range_start} - {self.range_end}, skipping ..."
            )
            self.df_report = None
            return

        # Get gender, ethnicity, race responses
        df_hold = self._df_range[
            ["src_subject_id", "sex", "ethnicity", "race"]
        ]

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
        df_report : pd.DataFrame, None
            Relevant info, format for requested report

        """
        # Set start, end dates for report periods.
        # Update - now include all since the beginning of the project.
        nih_annual_ranges = [
            ("2020-04-01", "2020-03-31"),
            ("2020-04-01", "2022-03-31"),
            ("2020-04-01", "2023-03-31"),
            ("2020-04-01", "2024-03-31"),
            ("2020-04-01", "2025-03-31"),
            ("2020-04-01", "2026-03-31"),
        ]

        # Get data from query range, check for data
        self._get_data_range(nih_annual_ranges)
        if self._df_range.empty:
            print(
                "\t\tNo data collected for query range : "
                + f"{self.range_start} - {self.range_end}, skipping ..."
            )
            self.df_report = None
            return

        # Extract relevant columns for the report
        cols_desired = [
            "src_subject_id",
            "race",
            "ethnicity",
            "sex",
            "age",
        ]
        self.df_report = self._df_range[cols_desired]

        # Reformat dataframe into desired format
        col_names = {
            "src_subject_id": "Record_ID",
            "race": "Race",
            "ethnicity": "Ethnicity",
            "sex": "Gender",
            "age": "Age",
        }
        self.df_report = self.df_report.rename(columns=col_names)
        self.df_report["Age Unit"] = "Years"

        # Fine-tune formatting per NIH reqs
        self.df_report = self.df_report.drop("Record_ID", axis=1)
        self.df_report["Age"] = self.df_report["Age"].astype(int)
        self.df_report["Race"] = self.df_report["Race"].str.replace("-", " ")
        idx_other = self.df_report.index[
            self.df_report["Race"].str.contains("Other")
        ]
        self.df_report.loc[idx_other, "Race"] = "Unknown"
        idx_unkn = self.df_report.index[
            self.df_report["Race"].str.contains("Unknown")
        ]
        self.df_report.loc[idx_unkn, "Race"] = "Unknown"


class GenerateGuids(manage_data.GetRedcap):
    """Generate GUIDs for EmoRep.

    Inherits manage_data.GetRedcap.

    Use existing RedCap demographic information to produce
    a batch of GUIDs via NDA's guid-tool.

    Writes GUIDs as txt file to:
        <proj_dir>/data_survey/redcap_demographics/data_clean

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    user_name : str
        NDA user name
    user_pass : str
        NDA user password
    redcap_token : str
        Personal access token for RedCap

    Attributes
    ----------
    df_guid_file : path
        Location of intermediate file for subprocess accessibility
    mismatch_list : list
        Record IDs of participants who differ between RedCap and
        generated GUIDs

    Methods
    -------
    check_guids()
        Compare generate GUIDs to those in RedCap
    make_guids()
        Use compiled demographic info from _get_demo to generate GUIDs

    """

    def __init__(self, proj_dir, user_pass, user_name, redcap_token):
        """Setup instance and compile demographic information."""
        super().__init__(proj_dir, redcap_token)
        self._user_name = user_name
        self._user_pass = user_pass
        self._get_demo()

    def _get_demo(self):
        """Make a dataframe with fields required by guid-tool."""
        print("Compiling RedCap demographic info ...")

        # Get demographic info
        self.get_redcap(survey_list=["demographics"])
        df_demo = self.clean_redcap["study"]["visit_day1"][
            "demographics"
        ].copy(deep=True)

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
        df_guid["dob_datetime"] = df_guid["dob_datetime"].astype(str)
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
        self.df_guid_file = os.path.join(
            os.path.join(
                self._proj_dir,
                "data_survey/redcap",
                "tmp_df_for_guid_tool.csv",
            )
        )
        df_guid.to_csv(self.df_guid_file, index=False, na_rep="")

    def make_guids(self):
        """Generate GUIDs via guid-tool.

        Output of guid-tool is written to:
            <proj_dir>/data_survey/redcap_demographics/data_clean/output_guid_*.txt

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
                -u {self._user_name} \
                -p {self._user_pass} \
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
        self._guid_file = guid_output[-1]

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
        self.get_redcap(survey_list=["guid"])
        df_guid_rc = self.clean_redcap["study"]["visit_day0"]["guid"]

        # Get generated GUIDs
        try:
            df_guid_gen = pd.read_csv(
                self._guid_file,
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
