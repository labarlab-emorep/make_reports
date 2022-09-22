"""Pull Qualtrics reports and make dataframes."""
# %%
import os
import json
from datetime import date
import importlib.resources as pkg_resources
from nda_upload import report_helper
from nda_upload import reference_files


# %%
class MakeQualtrics:
    """Title.

    Desc.
    """

    def __init__(self, qualtrics_token, survey_par):
        """Title.

        Desc.
        """
        self.qualtrics_token = qualtrics_token
        self.survey_par = survey_par

        # Load report keys
        with pkg_resources.open_text(
            reference_files, "report_keys_qualtrics.json"
        ) as jf:
            self.report_keys_qualtrics = json.load(jf)

        # Specify survey names
        self.name_visit1 = "EmoRep_Session_1"
        self.name_visit23 = "Session 2 & 3 Survey"
        self.name_post = "FINAL - EmoRep Stimulus Ratings - fMRI Study"

        # Get visit dataframes
        self.df_raw_visit1 = self._pull_df(self.name_visit1)
        self.df_raw_visit23 = self._pull_df(self.name_visit23)
        self.df_raw_post = self._pull_df(self.name_post)

    def _pull_df(self, survey_name):
        """Title

        Desc.
        """
        report_id = self.report_keys_qualtrics[survey_name]
        df = report_helper.pull_qualtrics_data_new(
            self.qualtrics_token,
            report_id,
            self.report_keys_qualtrics["organization_ID"],
            self.report_keys_qualtrics["datacenter_ID"],
            survey_name,
        )
        return df

    def write_raw_reports(self, visit_name):
        """Title

        Desc.
        """
        today_date = date.today().strftime("%Y-%m-%d")
        visit_dict = {
            "visit_day1": ["df_raw_visit1", "name_visit1"],
            "visit_day2": ["df_raw_visit23", "name_visit23"],
            "visit_day3": ["df_raw_visit23", "name_visit23"],
            "post_scan_ratings": ["df_raw_post", "name_post"],
        }
        report_name = getattr(self, visit_dict[visit_name][1])
        out_file = os.path.join(
            self.survey_par,
            visit_name,
            "data_raw",
            f"{report_name}_{today_date}.csv",
        )
        print(f"Writing raw survey data : \n\t{out_file}")
        df_out = getattr(self, visit_dict[visit_name][0])
        df_out.to_csv(out_file, index=False, na_rep="")

    def _clean_visit_day1(self, visit_name):
        """Title

        Desc.
        """
        # Setup and identify column names
        print(f"Cleaning raw survey data : {self.name_visit1}")
        subj_col = ["SubID"]
        self.df_raw_visit1.rename(
            {"RecipientLastName": subj_col[0]}, axis=1, inplace=True
        )
        col_names = self.df_raw_visit1.columns

        # Subset dataframe by survey key
        visit1_surveys = ["ALS", "AIM", "ERQ", "PSWQ", "RRS", "STAI", "TAS"]
        for sur_key in visit1_surveys:
            out_file = os.path.join(
                self.survey_par, visit_name, "data_clean", f"df_{sur_key}.csv"
            )
            print(f"\tWriting clean survey data : {out_file}")
            sur_cols = [x for x in col_names if sur_key in x]
            ext_cols = subj_col + sur_cols
            df_sub = self.df_raw_visit1[ext_cols]
            df_sub = df_sub.fillna("NaN")

            # Clean subset dataframe, writeout
            df_clean = df_sub[df_sub[subj_col[0]].str.contains("ER")]
            df_clean = df_clean.sort_values(by=[subj_col[0]])
            df_clean.to_csv(out_file, index=False, na_rep="")
            del df_sub, df_clean

    def _clean_visit_day23(self, visit_name):
        """Title

        Desc.
        """
        day = f"day{visit_name[-1]}"

        # Setup and identify column names
        print(f"Cleaning raw survey data : {self.name_visit23}, {day}")
        subj_col = ["SubID"]
        self.df_raw_visit23.rename(
            {"RecipientLastName": subj_col[0]}, axis=1, inplace=True
        )
        col_names = self.df_raw_visit23.columns

        # Subset dataframe by survey key
        visit23_surveys = ["PANAS", "STAI_State"]

    def write_clean_reports(self, visit_name):
        """Title

        Desc.
        """
        if visit_name == "visit_day2" or visit_name == "visit_day3":
            self._clean_visit_day23(visit_name)
        else:
            clean_method = getattr(self, f"_clean_{visit_name}")
            clean_method(visit_name)


# %%
