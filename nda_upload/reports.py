"""Make reports of various types."""
import pandas as pd
import numpy as np
from datetime import datetime
from nda_upload import report_helper


class RegularReports:
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
    query_date : datetime
        Date for finding report range
    final_demo : pd.DataFrame
        Compiled demographic information, attribute of
        by general_info.MakeDemographic
    report : str
        Type of report e.g. nih4 or duke3
    range_start : datetime
        Start of period for report
    range_end : datetime
        End of period for report
    df_range : pd.DataFrame
        Data found within the range_start, range_end period
    df_report : pd.DataFrame
        Relevant info, format for requested report

    """

    def __init__(self, query_date, final_demo, report):
        """Make desired report.

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
        query_date : datetime
            Date for finding report range
        final_demo : pd.DataFrame
            Compiled demographic information, attribute of
            by general_info.MakeDemographic
        report : str
            Type of report e.g. nih4 or duke3

        """
        print(f"Buiding manager report : {report} ...")
        self.query_date = query_date
        self.final_demo = final_demo

        # Trigger appropriate method
        if report == "nih4":
            self.nih_4mo()
        elif report == "nih12":
            self.nih_12mo()
        elif report == "duke3":
            self.duke_3mo()
        else:
            raise ValueError("Incorrect report arguments specified.")

    def _find_start_end(self, range_list):
        """Find the period start and end date.

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

    def nih_4mo(self):
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

    def duke_3mo(self):
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

    def nih_12mo(self):
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


class DemoInfo:
    """Title.

    Desc.

    Attributes
    ----------
    final_demo : pd.DataFrame
        Compiled demographic information, attribute of
        by general_info.MakeDemographic

    """

    def __init__(self, final_demo):
        """Title.

        Desc.

        Parameters
        ----------
        final_demo : pd.DataFrame
            Compiled demographic information, attribute of
            by general_info.MakeDemographic

        Attributes
        ----------
        final_demo : pd.DataFrame
            Compiled demographic information, attribute of
            by general_info.MakeDemographic

        """
        print("Buiding NDA report : demo_info01 ...")
        self.final_demo = final_demo
        self.nda_label, self.nda_cols = report_helper.mine_template(
            "demo_info01_template.csv"
        )
        self.df_report = pd.DataFrame(columns=self.nda_cols)
        self.make_demo()

    def make_demo(self):
        """Title.

        Desc.
        """
        # Get subject key, src_id
        subj_key = self.final_demo["subjectkey"]
        subj_src_id = self.final_demo["src_subject_id"]

        # Get inverview age, date
        subj_inter_date = [
            x.strftime("%m/%d/%Y") for x in self.final_demo["interview_date"]
        ]
        subj_inter_age = self.final_demo["interview_age"]

        # Get subject sex, race
        # TODO deal with other race responses
        subj_sex = [x[:1] for x in self.final_demo["sex"]]
        subj_sex = list(map(lambda x: x.replace("N", "O"), subj_sex))
        subj_race = self.final_demo["race"]
        subj_race = list(
            map(
                lambda x: x.replace("African-American", "African American"),
                subj_race,
            )
        )

        # Get education lavel
        subj_educat = self.final_demo["years_education"]

        # Make comments for pilot subjs
        pilot_list = ["ER0001", "ER0002", "ER0003", "ER0004", "ER0005"]
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
            "educat": subj_educat,
            "comments_misc": subj_comments_misc,
        }
        for h_col, h_value in report_dict.items():
            self.df_report[h_col] = h_value
