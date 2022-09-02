"""Title.

Desc.
"""
import pandas as pd
from nda_upload import request_redcap


class DetermineSubjs:
    """Title.

    Desc.
    """

    def __init__(self, report_keys, api_token):
        """Title.

        Desc.
        """
        self.api_token = api_token
        self.report_keys = report_keys

    def _get_basic(self):
        """Title.

        Desc.
        """
        self.df_consent = request_redcap.pull_data(
            self.api_token, self.report_keys["consent_new"]
        )
        self.df_guid = request_redcap.pull_data(
            self.api_token, self.report_keys["guid"]
        )
        self.df_demo = request_redcap.pull_data(
            self.api_token, self.report_keys["demographics"]
        )

    def make_complete(self):
        """Title.

        Desc.
        """
        self._get_basic()
        idx_consent = self.df_consent.index[
            self.df_consent["consent_v2"] == 1.0
        ].tolist()
        subjs_consented = self.df_consent.loc[
            idx_consent, "record_id"
        ].tolist()
        return subjs_consented
