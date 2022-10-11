"""Clean survey data from RedCap and Qualtrics."""
# %%
import os
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

        # Get and run cleaning method
        print(f"Cleaning survey : {survey_name}")
        if survey_name == "bdi_day2" or survey_name == "bdi_day3":
            self._clean_bdi_day23()
        elif survey_name == "consent_new" or survey_name == "consent_orig":
            self._clean_consent()
        else:
            clean_method = getattr(self, f"_clean_{survey_name}")
            clean_method()

    def _dob_convert(self, dob_list):
        """Helper method for _clean_demographics.

        Convert list of date-of-birth responses to datetime objects.

        Parameters
        ----------
        dob_list : list
            Participant date-of-birth responses

        Returns
        -------
        list

        Raises
        ------
        ValueError
            When numeric dob response does not match expected formats
        TypeError
            When dob response does not match expected formats

        """

        def _num_convert(dob):
            # Attempt to parse date string, account for formats
            # 20000606, 06062000, and 6062000.
            if dob[:2] == "19" or dob[:2] == "20":
                date_c, date_b, date_a = dob[:4], dob[4:6], dob[6:]
            elif len(dob) == 8:
                date_a, date_b, date_c = dob[:2], dob[2:4], dob[4:]
            elif len(dob) == 7:
                date_a, date_b, date_c = dob[:1], dob[1:3], dob[3:]
            else:
                raise ValueError(f"Unrecognized format date response: {dob}.")

            # Convert parsed dates, account for formats
            # 06152000 and 15062000.
            if int(date_a) < 13:
                return pd.to_datetime(f"{date_a}-{date_b}-{date_c}").date()
            else:
                return pd.to_datetime(f"{date_b}-{date_a}-{date_c}").date()

        # Set switch for extra special cases
        dob_switch = {"October 6 2000": "2000-10-06"}

        # Convert each dob free response or redcap datetime
        dob_clean = []
        for dob in dob_list:
            if "/" in dob or "-" in dob:
                dob_clean.append(
                    pd.to_datetime(dob, infer_datetime_format=True).date()
                )
            elif dob.isnumeric():
                dob_clean.append(_num_convert(dob))
            elif dob in dob_switch:
                dob_clean.append(pd.to_datetime(dob_switch[dob]).date())
            else:
                raise TypeError(f"Unrecognized datetime str: {dob}.")
        return dob_clean

    def _clean_demographics(self):
        """Title.

        Desc.

        """
        # Reorder columns, drop rows without last name response
        col_names = self.df_raw.columns.tolist()
        col_reorder = col_names[-1:] + col_names[-2:-1] + col_names[:-2]
        df_raw = self.df_raw[col_reorder]
        df_raw = df_raw[df_raw["lastname"].notna()]

        # Convert DOB response to datetime
        dob_list = df_raw["dob"].tolist()
        dob_clean = self._dob_convert(dob_list)
        df_raw["dob"] = dob_clean

        # Separate pilot from study data
        pilot_list = [int(x[-1]) for x in self.pilot_list]
        idx_pilot = df_raw[df_raw["record_id"].isin(pilot_list)].index.tolist()
        idx_study = df_raw[
            ~df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]

    def _clean_consent(self):
        """Title.

        Desc.

        """
        # Drop rows without signature
        col_names = self.df_raw.columns.tolist()
        col_drop = (
            "signature_v2" if "signature_v2" in col_names else "signature"
        )
        df_raw = self.df_raw[self.df_raw[col_drop].notna()]

        # Separate pilot from study data
        pilot_list = [int(x[-1]) for x in self.pilot_list]
        idx_pilot = df_raw[df_raw["record_id"].isin(pilot_list)].index.tolist()
        idx_study = df_raw[
            ~df_raw["record_id"].isin(pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]

    def _clean_guid(self):
        """Title.

        Desc.

        """
        # Drop rows without guid
        df_raw = self.df_raw[self.df_raw["guid"].notna()]

        # Separate pilot from study data
        idx_pilot = df_raw[
            df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]

    def _clean_bdi_day23(self):
        """Title.

        Desc.

        Attributes
        ----------
        df_clean
        df_pilot

        """
        # Remove unneeded columns and reorder
        drop_list = ["guid_timestamp", "redcap_survey_identifier"]
        df_raw = self.df_raw.drop(drop_list, axis=1)
        col_names = df_raw.columns.tolist()
        col_reorder = (
            col_names[:1] + col_names[-1:] + col_names[-2:-1] + col_names[1:-2]
        )
        df_raw = df_raw[col_reorder]

        # Remove rows without responses or study_id (from guid survey)
        df_raw = df_raw[df_raw["study_id"].notna()]
        col_drop = "q_1_v2" if "q_1_v2" in col_names else "q_1"
        df_raw = df_raw[df_raw[col_drop].notna()]

        # Separate pilot from study data
        idx_pilot = df_raw[
            df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        idx_study = df_raw[
            ~df_raw["study_id"].isin(self.pilot_list)
        ].index.tolist()
        self.df_pilot = df_raw.loc[idx_pilot]
        self.df_clean = df_raw.loc[idx_study]
