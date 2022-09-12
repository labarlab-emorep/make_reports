"""Title.

Desc.
"""
# %%
import pandas as pd
from datetime import datetime, date
from nda_upload import reference_files


class MakeRegularReports:
    """Title.

    Desc.

    Attributes
    ----------

    """

    def __init__(self, query_date, final_demo, report):
        """Title.

        Desc.

        Attributes
        ----------

        """
        self.query_date = query_date
        self.final_demo = final_demo

        if report == "nih4":
            self.nih_4mo()
        elif report == "nih12":
            self.nih_12mo()
        elif report == "duke3":
            self.duke_3mo()
        else:
            raise ValueError("Incorrect report arguments specified.")

    def _find_start_end(self, range_list):
        """Title.

        Desc.
        """
        start_end = None
        for h_ranges in range_list:
            h_start = datetime.strptime(h_ranges[0], "%Y-%m-%d").date()
            h_end = datetime.strptime(h_ranges[1], "%Y-%m-%d").date()
            if h_start <= self.query_date <= h_end:
                start_end = (h_start, h_end)
                break
        if not start_end:
            raise ValueError(f"Date range not found for {self.query_date}.")
        return start_end

    def _get_data_range(self, range_list, start_date=None):
        """Title.

        Desc.
        """
        # find data within range
        h_start, self.range_end = self._find_start_end(range_list)
        self.range_start = start_date if start_date else h_start
        range_bool = (
            self.final_demo["interview_date"] >= self.range_start
        ) & (self.final_demo["interview_date"] <= self.range_end)
        self.df_range = self.final_demo.loc[range_bool]
        if self.df_range.empty:
            raise ValueError(
                f"No data collected for query date {self.query_date}"
            )

    def nih_4mo(self):
        """Title.

        Desc.
        """

        #
        mturk_nums = {
            "minority": 122,
            "hispanic": 67,
            "total": 659,
        }

        #
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

        proj_start = datetime.strptime("2020-06-30", "%Y-%m-%d").date()

        # find data within range
        self._get_data_range(
            nih_4mo_ranges,
            start_date=proj_start,
        )

        # num minority, num hispanic, num total
        num_minority = len(
            self.df_range.index[self.df_range["is_minority"] == "Minority"]
        )
        num_hispanic = len(
            self.df_range.index[
                self.df_range["ethnicity"] == "Hispanic or Latino"
            ]
        )
        num_total = len(self.df_range.index)

        # update with mturk
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

    # %%
    def duke_3mo(self):
        """Title.

        Desc.
        """

        # Setup date ranges for report
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

        # find data within range
        self._get_data_range(duke_3mo_ranges)

        # Get gender/ethnicity/race values
        self.df_report = self.df_range[
            ["src_subject_id", "race", "ethnicity", "sex", "age"]
        ]
        col_names = {
            "src_subject_id": "Record_ID",
            "race": "Race",
            "ethnicity": "Ethnicity",
            "sex": "Gender",
            "age": "Age",
        }
        self.df_report = self.df_report.rename(columns=col_names)
        self.df_report["Age Unit"] = "Years"

    def nih_12mo(self):
        """Title.

        Desc.
        """

        nih_annual_ranges = [
            ("2020-04-01", "2020-03-31"),
            ("2021-04-01", "2022-03-31"),
            ("2022-04-01", "2023-03-31"),
            ("2023-04-01", "2024-03-31"),
            ("2024-04-01", "2025-03-31"),
            ("2025-04-01", "2026-03-31"),
        ]
        self._get_data_range(nih_annual_ranges)

        cols_desired = [
            "src_subject_id",
            "ethnicity",
            "race",
            "sex",
            "years_education",
            "age",
        ]
        self.df_report = self.df_range[cols_desired]
        self.df_report["age_unit"] = "Years"
