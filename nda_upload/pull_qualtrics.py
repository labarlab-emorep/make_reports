"""Pull Qualtrics reports and make dataframes."""
# %%
import os
import json
import importlib.resources as pkg_resources
from nda_upload import report_helper
from nda_upload import reference_files


# %%
class MakeQualtrics:
    """Title.

    Desc.
    """

    def __init__(self, qualtrics_token):
        """Title.

        Desc.
        """
        self.qualtrics_token = qualtrics_token

        # Load report keys
        with pkg_resources.open_text(
            reference_files, "report_keys_qualtrics.json"
        ) as jf:
            self.report_keys_qualtrics = json.load(jf)

        # Get visit dataframes
        self.df_raw_visit1 = self._pull_df("EmoRep_Session_1")
        self.df_raw_visit23 = self._pull_df("Session 2 & 3 Survey")
        self.df_raw_post = self._pull_df(
            "FINAL - EmoRep Stimulus Ratings - fMRI Study"
        )
        # survey_name = "EmoRep_Session_1"
        # report_id = report_keys_qualtrics[survey_name]
        # df_visit1_raw = report_helper.pull_qualtrics_data(
        #     qualtrics_token,
        #     report_id,
        #     organization_id,
        #     datacenter_id,
        #     survey_name,
        # )

    def _pull_df(self, survey_name):
        """Title

        Desc.
        """
        report_id = self.report_keys_qualtrics[survey_name]
        df = report_helper.pull_qualtrics_data(
            self.qualtrics_token,
            report_id,
            self.report_keys_qualtrics["organization_ID"],
            self.report_keys_qualtrics["datacenter_ID"],
            survey_name,
        )
        return df

    def write_raw_reports(self):
        pass

    def write_clean_reports(self):
        pass


# %%
