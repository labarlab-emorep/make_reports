"""Clean survey data from RedCap and Qualtrics."""
# %%
import os
import re
import pandas as pd
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

        # Get clean method
        if survey_name == "bdi_day2" or survey_name == "bdi_day3":
            self._clean_bdi_day23()
        else:
            clean_method = getattr(self, f"_clean_{survey_name}")
            clean_method()

    def _clean_demographics():
        """Title.

        Desc.

        """
        pass

    def _clean_consent_orig():
        """Title.

        Desc.

        """
        pass

    def _clean_consent_new():
        """Title.

        Desc.

        """
        pass

    def _clean_guid():
        """Title.

        Desc.

        """
        pass

    def _clean_bdi_day23(self):
        """Title.

        Desc.

        """
        drop_list = ["record_id", "guid_timestamp", "redcap_survey_identifier"]
        df_raw = self.df_raw.drop(drop_list, axis=1)
        col_names = df_raw.columns.tolist()
        col_reorder = col_names[-1:] + col_names[-2:-1] + col_names[:-2]
        df_raw = df_raw[col_reorder]
        df_raw = df_raw[df_raw["study_id"].notna()]
        df_raw = df_raw[df_raw["q_1"].notna()]

        idx_pilot = df_raw[
            df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]
