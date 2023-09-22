"""Build datsets and data hosts for NDAR uploads.

Each class is self-contained, requiring only demographic info input. Many
classes are highly similar and a more generic class structure could have
done the necessary work for several NDAR reports, but here each report
is kept separate for ease of fulfilling idiosyncratic NDAR requirements.

Each class contains two attributes used for writing the NDAR dataset reports:
    -   df_report : pd.DataFrame, a NDAR-compliant report for the class'
            respective data
    -   nda_label : list, the file identifier that should be prepended
            to df_report (e.g. image,03)

Additionally, each class accepts as input two parameters:
    -   proj_dir : path, project's parent directory
    -   final_demo : make_reports.build_reports.DemoAll.final_demo, a
            pd.DataFrame containing needed demographic and identification
            information.

Finally, instantiating the class triggers the construction of df_report and
nda_label, as well as setting up any data for hosting. This consistent
structure allows for the dynamic instantiation and use of the classes.

"""
import os
import re
import glob
import json
import subprocess
import pandas as pd
import numpy as np
from datetime import datetime
import pydicom
from make_reports.resources import report_helper


def _drop_subjectkey_nan(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN str and drop empty subjectkey rows."""
    df = df.replace("NaN", np.nan)
    return df.dropna(subset=["subjectkey"])


class NdarAffim01:
    """Make affim01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    df_pilot : pd.DataFrame
        Pilot AIM data
    df_study : pd.DataFrame
        Study AIM data

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of AIM data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_aim()
        Generate affim01 dataset, builds df_report

    """

    def __init__(self, proj_dir, final_demo, df_pilot, df_study):
        """Read in survey data and make report.

        Get cleaned AIM Qualtrics survey from visit_day1, and
        finalized demographic information.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : affim01 ...")
        # Read in template, concat dfs
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "affim01_template.csv"
        )
        df_aim = pd.concat([df_pilot, df_study], ignore_index=True)

        # Rename columns, drop NaN rows
        df_aim = df_aim.rename(columns={"study_id": "src_subject_id"})
        df_aim.columns = df_aim.columns.str.lower()
        df_aim = df_aim.replace("NaN", np.nan)
        self._df_aim = df_aim[df_aim["aim_1"].notna()]

        # Get final demographics, make report
        self._final_demo = _drop_subjectkey_nan(final_demo)
        self.make_aim()

    def make_aim(self):
        """Combine dataframes to generate requested report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of AIM data that complies with NDAR data definitions

        """
        # Get needed demographic info, combine with survey data
        df_nda = self._final_demo[
            ["subjectkey", "src_subject_id", "sex"]
        ].copy()
        df_nda["sex"] = df_nda["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        df_aim_nda = pd.merge(self._df_aim, df_nda, on="src_subject_id")

        # Find survey age-in-months
        df_aim_nda = report_helper.get_survey_age(
            df_aim_nda, self._final_demo, "src_subject_id"
        )

        # Sum aim responses
        aim_list = [x for x in df_aim_nda.columns if "aim" in x]
        df_aim_nda[aim_list] = df_aim_nda[aim_list].astype("Int64")
        df_aim_nda["aimtot"] = df_aim_nda[aim_list].sum(axis=1)

        # Make an empty dataframe from the report column names, fill
        self.df_report = pd.DataFrame(
            columns=self._nda_cols, index=df_aim_nda.index
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
    df_report : pd.DataFrame
        Report of ALS data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_als()
        Generate als01 dataset, builds df_report

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned ALS Qualtrics survey from visit_day1, and
        finalized demographic information.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : als01 ...")
        # Read in template
        self.nda_label, self._nda_cols = report_helper.mine_template(
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
        self._df_als = df_als[df_als["ALS_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        self._final_demo = final_demo.dropna(subset=["subjectkey"])
        self.make_als()

    def make_als(self):
        """Combine dataframes to generate requested report.

        Remap values and calculate totals.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of ALS data that complies with NDAR data definitions

        """
        # Remap response values and column names
        resp_qual = [1, 2, 3, 4]
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
        df_als_remap = self._df_als.rename(columns=map_item)
        als_cols = [x for x in df_als_remap.columns if "als" in x]
        df_als_remap[als_cols] = df_als_remap[als_cols].replace(
            resp_qual, resp_ndar
        )

        # Calculate totals
        df_als_remap[als_cols] = df_als_remap[als_cols].astype("Int64")
        df_als_remap["als_glob"] = df_als_remap[als_cols].sum(axis=1)
        df_als_remap["als_sf_total"] = df_als_remap[als_cols].sum(axis=1)
        df_als_remap["als_sf_total"] = df_als_remap["als_sf_total"].astype(
            "Int64"
        )

        # Add pilot notes for certain subjects
        pilot_list = report_helper.pilot_list()
        idx_pilot = df_als_remap[
            df_als_remap["src_subject_id"].isin(pilot_list)
        ].index.tolist()
        df_als_remap.loc[idx_pilot, "comments"] = "PILOT PARTICIPANT"

        # Combine demographic and als dataframes
        df_nda = self._final_demo[
            ["subjectkey", "src_subject_id", "sex"]
        ].copy()
        df_nda["sex"] = df_nda["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        df_als_nda = pd.merge(df_als_remap, df_nda, on="src_subject_id")
        df_als_nda = report_helper.get_survey_age(
            df_als_nda, self._final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with demo and als data
        self.df_report = pd.DataFrame(
            columns=self._nda_cols, index=df_als_nda.index
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
    df_report : pd.DataFrame
        Report of BDI data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_bdi(sess: str)
        Generate bdi01 dataset, builds df_report for one session

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned BDI RedCap survey from visit_day2 and
        visit_day3, and finalized demographic information.
        Generate BDI report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of BDI data that complies with NDAR data definitions
        nda_label : list
            NDA report template label

        """
        # Get needed column values from report template
        print("Buiding NDA report : bdi01 ...")
        self._proj_dir = proj_dir
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "bdi01_template.csv"
        )

        # Get pilot, study data for both day2, day3
        self._get_clean()

        # Get final demographics
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo.dropna(subset=["subjectkey"])

        # Make nda reports for each session
        df_nda_day2 = self.make_bdi("day2")
        df_nda_day3 = self.make_bdi("day3")

        # Combine into final report
        df_report = pd.concat([df_nda_day2, df_nda_day3], ignore_index=True)
        df_report = df_report.sort_values(by=["src_subject_id", "visit"])

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

        """
        # Get clean survey data
        df_pilot2 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                "visit_day2/data_clean",
                "df_BDI.csv",
            )
        )
        df_study2 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day2/data_clean",
                "df_BDI.csv",
            )
        )

        # Combine pilot and study data, updated subj id column, set attribute
        df_bdi_day2 = pd.concat([df_pilot2, df_study2], ignore_index=True)
        df_bdi_day2 = df_bdi_day2.rename(
            columns={"study_id": "src_subject_id"}
        )
        self._df_bdi_day2 = df_bdi_day2

        # Repeat above for day3
        df_pilot3 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                "visit_day3/data_clean",
                "df_BDI.csv",
            )
        )
        df_study3 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day3/data_clean",
                "df_BDI.csv",
            )
        )
        df_bdi_day3 = pd.concat([df_pilot3, df_study3], ignore_index=True)
        df_bdi_day3 = df_bdi_day3.rename(
            columns={"study_id": "src_subject_id"}
        )
        df_bdi_day3.columns = df_bdi_day2.columns.values
        self._df_bdi_day3 = df_bdi_day3

    def make_bdi(self, sess):
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
        df_bdi = getattr(self, f"_df_bdi_{sess}")

        # Convert response values to int
        q_cols = [x for x in df_bdi.columns if "BDI_" in x]
        df_bdi[q_cols] = df_bdi[q_cols].astype("Int64")

        # Remap column names
        map_item = {
            "BDI_1": "bdi1",
            "BDI_2": "bdi2",
            "BDI_3": "bdi3",
            "BDI_4": "bdi4",
            "BDI_5": "bdi5",
            "BDI_6": "bdi6",
            "BDI_7": "beck07",
            "BDI_8": "beck08",
            "BDI_9": "bdi9",
            "BDI_10": "bdi10",
            "BDI_11": "bdi_irritated",
            "BDI_12": "bdi_loss",
            "BDI_13": "bdi_indecision",
            "BDI_14": "beck14",
            "BDI_15": "beck15",
            "BDI_16": "beck16",
            "BDI_17": "beck17",
            "BDI_18": "bd_017",
            "BDI_19": "beck19",
            "BDI_19b": "beck20",
            "BDI_20": "beck21",
            "BDI_21": "beck22",
        }
        df_bdi_remap = df_bdi.rename(columns=map_item)

        # Drop non-ndar columns
        df_bdi_remap = df_bdi_remap.drop(
            ["record_id"],
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
        df_nda = self._final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_bdi_demo = pd.merge(df_bdi_remap, df_nda, on="src_subject_id")

        # Calculate age in months of visit
        df_bdi_demo = report_helper.get_survey_age(
            df_bdi_demo, self._final_demo, "src_subject_id"
        )
        df_bdi_demo["visit"] = sess

        # Build dataframe from nda columns, update with df_bdi_demo data
        df_nda = pd.DataFrame(columns=self._nda_cols, index=df_bdi_demo.index)
        df_nda.update(df_bdi_demo)
        return df_nda


class NdarBrd01:
    """Make brd01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of rest rating data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_brd(sess: str)
        Generate brd01 dataset, builds df_report for one session

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get finalized demographic information, set orienting
        attributes, and trigger report generation method for
        each session.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of rest rating data that complies with NDAR data definitions
        nda_label : list
            NDA report template label

        """
        # Get needed column values from report template
        print("Buiding NDA report : brd01 ...")
        self.nda_label, nda_cols = report_helper.mine_template(
            "brd01_template.csv"
        )

        # Start empty report for filling
        self.df_report = pd.DataFrame(columns=nda_cols)

        # Set helper paths
        self._proj_dir = proj_dir
        self._local_path = (
            "/run/user/1001/gvfs/smb-share:server"
            + "=ccn-keoki.win.duke.edu,share=experiments2/EmoRep/"
            + "Exp2_Compute_Emotion/ndar_upload/data_beh"
        )

        # Get final demographics
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo.dropna(subset=["subjectkey"])

        # Fill df_report for each session
        self.make_brd("day2")
        self.make_brd("day3")
        self.df_report = self.df_report.sort_values(
            by=["src_subject_id", "visit"]
        )

    def _get_subj_demo(self, survey_date, sub):
        """Gather required participant demographic information.

        Find participant demographic info and calculate age-in-months
        at time of scan.

        Parameters
        ----------
        survey_date : datetime
            Time survey took place
        sub : str
            Subject identifier

        Returns
        -------
        dict
            Keys = brd01 column names
            Values = demographic info

        """
        # Identify participant date of birth, sex, and GUID
        final_demo = self._final_demo
        idx_subj = final_demo.index[
            final_demo["src_subject_id"] == sub
        ].tolist()[0]
        subj_guid = final_demo.iloc[idx_subj]["subjectkey"]
        subj_id = final_demo.iloc[idx_subj]["src_subject_id"]
        subj_sex = final_demo.iloc[idx_subj]["sex"]
        subj_dob = datetime.strptime(
            final_demo.iloc[idx_subj]["dob"], "%Y-%m-%d"
        )

        # Calculate age in months
        interview_age = report_helper.calc_age_mo([subj_dob], [survey_date])[0]
        interview_date = datetime.strftime(survey_date, "%m/%d/%Y")

        return {
            "subjectkey": subj_guid,
            "src_subject_id": subj_id,
            "interview_date": interview_date,
            "interview_age": interview_age,
            "sex": subj_sex,
        }

    def make_brd(self, sess):
        """Make brd01 report for session.

        Get Qualtrics survey data for post scan ratings,
        make a host file, and generate NDAR dataframe.

        Parameters
        ----------
        sess : str
            [day2 | day3]
            Session identifier

        Raises
        ------
        ValueError
            Inappropriate sess input

        """
        # Check sess value
        sess_list = ["day2", "day3"]
        if sess not in sess_list:
            raise ValueError(f"Incorrect visit day : {sess}")

        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                f"visit_{sess}/data_clean",
                "df_post_scan_ratings.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                f"visit_{sess}/data_clean",
                "df_post_scan_ratings.csv",
            )
        )

        # Combine pilot and study data, updated subj id column, set attribute
        df_brd = pd.concat([df_pilot, df_study], ignore_index=True)
        df_brd = df_brd.rename(columns={"study_id": "src_subject_id"})

        # Setup hosting directory
        host_dir = os.path.join(self._proj_dir, "ndar_upload/data_beh")
        if not os.path.exists(host_dir):
            os.makedirs(host_dir)

        # Mine each participant's data
        sub_list = df_brd["src_subject_id"].unique().tolist()
        sub_demo = self._final_demo["src_subject_id"].tolist()
        for sub in sub_list:

            # Skip participants not in final_demo (cycle date or withdrawn)
            if sub not in sub_demo:
                continue

            # Extract participant info
            df_sub = df_brd[df_brd["src_subject_id"] == sub]
            df_sub = df_sub.reset_index(drop=True)

            # Make host file
            task = df_sub.loc[0, "type"].lower()
            out_file = f"sub-{sub}_ses-{sess}_task-{task}_ratings.csv"
            out_path = os.path.join(host_dir, out_file)
            if not os.path.exists(out_path):
                print(f"\tMaking host file : {out_path}")
                df_sub.to_csv(out_path, index=False, na_rep="")

            # Determine date survey was taken, get demographic info
            h_date = df_sub.loc[0, "datetime"]
            survey_date = datetime.strptime(h_date, "%Y-%m-%d")
            demo_info = self._get_subj_demo(survey_date, sub)

            # Set values required by brd01
            brd01_info = {
                "visit": sess[-1],
                "data_file1": os.path.join(self._local_path, out_file),
            }
            brd01_info.update(demo_info)

            # Add brd info to report
            new_row = pd.DataFrame(brd01_info, index=[0])
            self.df_report = pd.concat(
                [self.df_report.loc[:], new_row]
            ).reset_index(drop=True)
            del new_row


class NdarDemoInfo01:
    """Make demo_info01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory, unused
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of demographic data that complies with NDAR data definitions
    nda_label : list
        NDA report template column label

    Methods
    -------
    make_demo()
        Generate demo_info01 dataset, builds df_report

    """

    def __init__(self, proj_dir, final_demo):
        """Read in demographic info and make report.

        Read-in data, setup empty df_report, and coordinate
        filling df_report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of demographic data that complies with NDAR data definitions
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : demo_info01 ...")
        self._final_demo = final_demo
        self.nda_label, nda_cols = report_helper.mine_template(
            "demo_info01_template.csv"
        )
        self.df_report = pd.DataFrame(columns=nda_cols)
        self.make_demo()

    def make_demo(self):
        """Update df_report with NDAR-required demographic information."""
        # Get subject key, src_id
        subj_key = self._final_demo["subjectkey"]
        subj_src_id = self._final_demo["src_subject_id"]

        # Get inverview age, date
        subj_inter_date = [
            x.strftime("%m/%d/%Y") for x in self._final_demo["interview_date"]
        ]
        subj_inter_age = self._final_demo["interview_age"]

        # Get subject sex
        subj_sex = [x[:1] for x in self._final_demo["sex"]]
        subj_sex = list(map(lambda x: x.replace("N", "O"), subj_sex))

        # Get subject race
        subj_race = self._final_demo["race"]
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
        subj_educat = self._final_demo["years_education"]

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
    nda_label : list
        NDA report template label

    Methods
    -------
    make_emrq()
        Generate emrq01 dataset, builds df_report

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned ERQ Qualtrics survey from visit_day1, and
        finalized demographic information.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : emrq01 ...")
        # Read in template
        self.nda_label, self._nda_cols = report_helper.mine_template(
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
        self._df_emrq = df_emrq[df_emrq["ERQ_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        self._final_demo = final_demo.dropna(subset=["subjectkey"])
        self.make_emrq()

    def make_emrq(self):
        """Generate ERMQ report for NDAR submission.

        Remap values and calculate totals.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of EMRQ data that complies with NDAR data definitions

        """
        # Update column names, make data integer
        df_emrq = self._df_emrq.rename(columns=str.lower)
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
        df_nda = self._final_demo[
            ["subjectkey", "src_subject_id", "sex"]
        ].copy()
        df_nda["sex"] = df_nda["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        df_emrq_nda = pd.merge(df_emrq, df_nda, on="src_subject_id")
        df_emrq_nda = report_helper.get_survey_age(
            df_emrq_nda, self._final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with df_final_emrq data
        df_report = pd.DataFrame(
            columns=self._nda_cols, index=df_emrq_nda.index
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
        <proj_dir>/ndar_upload/data_mri

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
    df_report : pd.DataFrame
        Image03 values for experiment/study participants
    nda_label : list
        NDA report template label

    Methods
    -------
    make_pilot()
        Pull dataframe for pilot data
    make_image03()
        Make dataframe of experiment data, coordinates work by matching
        MRI type to info_<mri-type> method.
    info_anat()
        Write image03 line for anatomical information
    info_fmap()
        Write image03 line for field map information
    info_func()
        Write image03 line for functional information

    """

    def __init__(self, proj_dir, final_demo, test_subj=None):
        """Coordinate report generation for MRI data.

        Assumes BIDS organization of <proj_dir>/data_scanner_BIDS. Identify
        all subject sessions in rawdata, generate image03 report for all
        data types found within each session, integrate demographic
        information, and combine with previously-generated image03 info
        for the pilot participants.

        Attributes
        ----------
        df_report : pd.DataFrame
            Image03 values for experiment/study participants
        nda_label : list
            NDA report template label

        Raises
        ------
        FileNotFoundError
            Missing pilot image03 csv
        ValueError
            Empty subj_sess_list

        """
        print("Buiding NDA report : image03 ...")

        # Read in template, start empty dataframe
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "image03_template.csv"
        )
        self._df_report_study = pd.DataFrame(columns=self._nda_cols)

        # Set reference and orienting attributes
        self._proj_dir = proj_dir
        self._source_dir = os.path.join(
            proj_dir, "data_scanner_BIDS/sourcedata"
        )

        # Identify all session in rawdata, check that data is found
        rawdata_dir = os.path.join(proj_dir, "data_scanner_BIDS/rawdata")
        if test_subj:
            self._subj_sess_list = sorted(
                glob.glob(f"{rawdata_dir}/{test_subj}/ses-day*")
            )
        else:
            self._subj_sess_list = sorted(
                glob.glob(f"{rawdata_dir}/sub-ER*/ses-day*")
            )
        if not self._subj_sess_list:
            raise ValueError(
                f"Subject, session paths not found in {rawdata_dir}"
            )

        # Get demographic info
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo = final_demo.dropna(subset=["subjectkey"])
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo

        # Get pilot df, make df_report for all study participants
        self.make_pilot()
        self.make_image03()

        # Combine df_report_study and df_report_pilot
        self.df_report = pd.concat(
            [self._df_report_pilot, self._df_report_study], ignore_index=True
        )

    def make_pilot(self):
        """Read previously-generated NDAR report for pilot participants.

        Raises
        ------
        FileNotFoundError
            Expected pilot image dataset not found

        """
        # Read-in dataframe of pilot participants
        pilot_report = os.path.join(
            self._proj_dir,
            "data_pilot/ndar_resources",
            "image03_dataset.csv",
        )
        if not os.path.exists(pilot_report):
            raise FileNotFoundError(
                f"Expected to find pilot image03 at {pilot_report}"
            )
        df_report_pilot = pd.read_csv(pilot_report)
        df_report_pilot = df_report_pilot[1:]
        df_report_pilot.columns = self._nda_cols
        df_report_pilot["comments_misc"] = "PILOT PARTICIPANT"
        self._df_report_pilot = df_report_pilot

    def make_image03(self):
        """Generate image03 report for study participants.

        Iterate through subj_sess_list, identify the data types of
        the session, and for each data type trigger appropriate method.
        Each iteration resets the attributes of sess, subj, subj_nda, and
        subj_sess so they are available for private methods.

        Raises
        ------
        AttributeError
            An issue with the value of subj, subj_nda, or sess

        """
        # Specify MRI data types - these are used to match
        # and use private class methods to the data of the
        # participant's session.
        type_list = ["anat", "func", "fmap"]
        cons_list = self._final_demo["src_subject_id"].tolist()

        # Set attributes for each subject's session
        for subj_sess in self._subj_sess_list:
            self._subj_sess = subj_sess
            self._subj = os.path.basename(os.path.dirname(subj_sess))
            self._subj_nda = self._subj.split("-")[1]
            self._sess = os.path.basename(subj_sess)

            # Check attributes
            chk_subj = True if len(self._subj) == 10 else False
            chk_subj_nda = True if len(self._subj_nda) == 6 else False
            chk_sess = True if len(self._sess) == 7 else False
            if not chk_subj and not chk_subj_nda and not chk_sess:
                raise AttributeError(
                    f"""\
                Unexpected value of one of the following:
                    self._subj : {self._subj}
                    self._subj_nda : {self._subj_nda}
                    self._sess : {self._sess}

                Possible cause is non-BIDS organization of rawdata,
                Check build_reports.NdarImage03.__init__ for rawdata_dir.
                """
                )

            # Only use participants found in final_demo, reflecting
            # current consent and available demo info.
            if self._subj_nda not in cons_list:
                print(
                    f"""
                    {self._subj_nda} not found in self._final_demo,
                        continuing ...
                    """
                )
                continue

            # Identify types of data in subject's session, use appropriate
            # method for data type.
            print(f"\tMining data for {self._subj}, {self._sess}")
            scan_type_list = [
                x for x in os.listdir(subj_sess) if x in type_list
            ]
            if not scan_type_list:
                print(f"No data types found at {subj_sess}\n\tContinuing ...")
                continue
            for scan_type in scan_type_list:
                info_meth = getattr(self, f"info_{scan_type}")
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
        final_demo = self._final_demo
        idx_subj = final_demo.index[
            final_demo["src_subject_id"] == self._subj_nda
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
            "sex": subj_sex,
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
            "visnum": float(self._sess[-1]),
        }

    def _make_host(self, share_file, out_name, out_dir="data_mri"):
        """Copy a file for hosting with NDA package builder.

        Data will be copied to <proj_dir>/ndar_upload/<out_dir>.

        Parameters
        ----------
        share_file : path
            Location of desired file to share
        out_name : str
            Output name of file
        out_dir : str, optional
            Sub-directory destination of <proj_dir>/ndar_upload,

        Raises
        ------
        FileNotFoundError
            share_file does not exist
            output <host_path> does not exist

        """
        # Check for existing share_file
        if not os.path.exists(share_file):
            raise FileNotFoundError(f"Expected to find : {share_file}")

        # Setup output path
        host_dir = os.path.join(self._proj_dir, "ndar_upload", out_dir)
        if not os.path.exists(host_dir):
            os.makedirs(host_dir)
        host_path = os.path.join(host_dir, out_name)

        # Submit copy subprocess
        if not os.path.exists(host_path):
            print(f"\t\t\tMaking host file : {host_path}")
            bash_cmd = f"cp {share_file} {host_path}"
            h_sp = subprocess.Popen(
                bash_cmd, shell=True, stdout=subprocess.PIPE
            )
            h_out, h_err = h_sp.communicate()
            h_sp.wait()

        # Check for output
        if not os.path.exists(host_path):
            raise FileNotFoundError(
                f"""
                Copy failed, expected to find : {host_path}
                Check build_reports.NdarImage03._make_host().
                """
            )

    def info_anat(self):
        """Write image03 line for anat data.

        Use _get_subj_demo and _get_std_info to find demographic and
        common field entries, and then determine values specific for
        anatomical scans. Update self._df_report_study with these data.
        Host a defaced anatomical image.

        Raises
        ------
        FileNotFoundError
            Missing JSON sidecar
            Missing DICOM file
            Missing defaced version in derivatives

        """
        print(f"\t\tWriting line for {self._subj} {self._sess} : anat ...")

        # Get JSON info
        json_file = sorted(glob.glob(f"{self._subj_sess}/anat/*.json"))[0]
        if not os.path.exists(json_file):
            raise FileNotFoundError(
                f"""
                Expected to find a JSON sidecar file at :
                    {self._subj_ses}/anat
                """
            )
        with open(json_file, "r") as jf:
            nii_json = json.load(jf)

        # Get DICOM info
        day = self._sess.split("-")[1]
        dicom_file = glob.glob(
            f"{self._source_dir}/{self._subj_nda}/{day}*/"
            + "DICOM/EmoRep_anat/*.dcm"
        )[0]
        if not os.path.exists(dicom_file):
            raise FileNotFoundError(
                f"""
                Expected to find a DICOM file at :
                    {self._source_dir}/{self._subj_nda}/{day}*/DICOM/EmoRep_anat
                """
            )
        dicom_hdr = pydicom.read_file(dicom_file)

        # Get demographic info
        scan_date = datetime.strptime(dicom_hdr[0x08, 0x20].value, "%Y%m%d")
        demo_dict = self._get_subj_demo(scan_date)

        # Setup host file
        deface_file = os.path.join(
            self._proj_dir,
            "data_scanner_BIDS/derivatives/deface",
            self._subj,
            self._sess,
            f"{self._subj}_{self._sess}_T1w_defaced.nii.gz",
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
            "image_file": f"data_mri/{host_name}",
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
        self._df_report_study = pd.concat(
            [self._df_report_study.loc[:], new_row]
        ).reset_index(drop=True)
        del new_row

    def info_fmap(self):
        """Write image03 line for fmap data.

        Use _get_subj_demo and _get_std_info to find demographic and
        common field entries, and then determine values specific for
        field map scans. Update self._df_report_study with these data.
        Host a field map file.

        Raises
        ------
        FileNotFoundError
            Missing JSON sidecar
            Missing DICOM file
            Missing NIfTI file

        """
        print(f"\t\tWriting line for {self._subj} {self._sess} : fmap ...")

        # Find nii, json files
        nii_list = sorted(glob.glob(f"{self._subj_sess}/fmap/*.nii.gz"))
        json_list = sorted(glob.glob(f"{self._subj_sess}/fmap/*.json"))
        if not nii_list:
            raise FileNotFoundError(
                f"Expected to find : {self._subj_sess}/fmap/*.nii.gz"
            )
        if not json_list:
            raise FileNotFoundError(
                f"Expected to find : {self._subj_sess}/fmap/*.json"
            )
        if len(nii_list) != len(json_list):
            raise ValueError(
                "Detected uneven number of NIfTI and JSON files "
                + f"in {self._subj_sess}/fmap"
            )

        # Add row for each fmap
        for nii_path, json_path in zip(nii_list, json_list):
            with open(json_path, "r") as jf:
                nii_json = json.load(jf)

            # Setup dcm path so glob finds the appropriate Field_Map_PA
            # or Field_Map_PA_run[1|2] (new fmap protocol).
            day = self._sess.split("-")[1]
            h_path = os.path.join(
                self._source_dir,
                self._subj_nda,
                f"{day}*",
                "DICOM",
            )
            nii_file = os.path.basename(nii_path)
            if "run" in nii_file:
                fmap_num = nii_file.split("run-")[1].split("_")[0][1]
                dicom_dir = os.path.join(h_path, f"Field_Map_PA_run{fmap_num}")
            else:
                dicom_dir = os.path.join(h_path, "Field_Map_PA")

            # Find dcms, get header
            dicom_list = sorted(glob.glob(f"{dicom_dir}/*.dcm"))
            if not dicom_list:
                raise FileNotFoundError(
                    f"Expected to find DICOMs in : {dicom_dir}"
                )
            dicom_hdr = pydicom.read_file(dicom_list[0])

            # Get demographic info
            scan_date = datetime.strptime(
                dicom_hdr[0x08, 0x20].value, "%Y%m%d"
            )
            demo_dict = self._get_subj_demo(scan_date)

            # Make a host file
            h_guid = demo_dict["subjectkey"]
            host_nii = (
                f"{h_guid}_{day}_fmap{fmap_num}_revpol.nii.gz"
                if "fmap_num" in locals()
                else f"{h_guid}_{day}_fmap1_revpol.nii.gz"
            )
            self._make_host(nii_path, host_nii)

            # Get general, anat specific acquisition info
            std_dict = self._get_std_info(nii_json, dicom_hdr)

            # Setup fmap-specific values
            fmap_image03 = {
                "image_file": f"data_mri/{host_nii}",
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
            self._df_report_study = pd.concat(
                [self._df_report_study.loc[:], new_row]
            ).reset_index(drop=True)

    def info_func(self):
        """Write image03 line for func data.

        Identify all func files and iterate through. Use _get_subj_demo
        and _get_std_info to find demographic and common field entries,
        and then determine values specific for func scans. Update
        self._df_report_study with these data.
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
            exp_dict["old"]
            if self._subj_nda in pilot_list
            else exp_dict["new"]
        )

        # Find all func niftis
        nii_list = sorted(glob.glob(f"{self._subj_sess}/func/*.nii.gz"))
        if not nii_list:
            raise FileNotFoundError(
                f"Expected NIfTIs at : {self._subj_sess}/func/"
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
                f"\t\tWriting line for {self._subj} {self._sess} : "
                + f"func {task} {run} ..."
            )
            day = self._sess.split("-")[1]
            task_dir = (
                "Rest_run01"
                if task == "task-rest"
                else f"EmoRep_run{run.split('-')[1]}"
            )
            task_source = os.path.join(
                self._source_dir, self._subj_nda, f"{day}*/DICOM", task_dir
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
            subj_guid = demo_dict["subjectkey"]
            host_nii = f"{subj_guid}_{day}_func_emostim_run{run[-1]}.nii.gz"
            self._make_host(nii_path, host_nii)

            # Setup host events, account for no rest
            # events and missing task files.
            events_exists = False
            if not task == "task-rest":
                host_events = (
                    f"{subj_guid}_{day}_func_emostim_run{run[-1]}_events.tsv"
                )
                events_path = re.sub("_bold.nii.gz$", "_events.tsv", nii_path)
                events_exists = os.path.exists(events_path)
                if events_exists:
                    self._make_host(events_path, host_events)

            # Get general, anat specific acquisition info
            std_dict = self._get_std_info(nii_json, dicom_hdr)

            # Setup func-specific values
            func_image03 = {
                "image_file": f"data_mri/{host_nii}",
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

            # Combine all dicts, write row
            func_image03.update(demo_dict)
            func_image03.update(std_dict)

            # Add task events files in data_file2 fields, accounting for
            # missing events files.
            if events_exists:
                func_image03["data_file2"] = f"data_mri/{host_events}"
                func_image03["data_file2_type"] = "task event information"

            # Write one row for task-resting with physio in data_file2 fields,
            # and two rows for task-movies|scenarios where the first row
            # has the events and the seond the physio in data_file2_fields.
            # Account for missing physio data.
            if task == "task-rest":
                new_row, _ = self._info_phys(
                    task, run, day, subj_guid, func_image03
                )
                self._df_report_study = pd.concat(
                    [self._df_report_study.loc[:], new_row]
                ).reset_index(drop=True)
            else:
                new_row = pd.DataFrame(func_image03, index=[0])
                self._df_report_study = pd.concat(
                    [self._df_report_study.loc[:], new_row]
                ).reset_index(drop=True)

                phys_row, phys_exists = self._info_phys(
                    task, run, day, subj_guid, func_image03
                )
                if phys_exists:
                    self._df_report_study = pd.concat(
                        [self._df_report_study.loc[:], phys_row]
                    ).reset_index(drop=True)

    def _info_phys(self, task, run, day, subj_guid, func_image03):
        """Helper function of info_func.

        Find the physio file corresponding to a functional run,
        host it and then update the data_file2 fields in func_image03.

        Parameters
        ----------
        task : str
            BIDS task identifier
        run : str
            BIDS run idenfitier
        day : str
            [day2|day3]
            For keeping host data file name consistent
        subj_guid : str
            Participant GUID
        func_image03 : dict
            Info needed for writing an image03 line for functional data

        Returns
        -------
        tuple
            [0] = pd.DataFrame
                func_image03 as a dataframe, contains data_file2 fields with
                physio info if physio files exist
            [1] = bool
                Whether a physio file was detected

        """
        # Identify corresponding physio file
        phys_file = (
            f"{self._subj}_{self._sess}_{task}_{run}_"
            + "recording-biopack_physio.acq"
        )
        phys_path = os.path.join(self._subj_sess, "phys", phys_file)
        phys_exists = os.path.exists(phys_path)
        if phys_exists:

            # Host physio file
            task_name = "rest" if task == "task-rest" else "emostim"
            host_phys = (
                f"{subj_guid}_{day}_func_{task_name}_"
                + f"run{run[-1]}_physio.acq"
            )
            self._make_host(phys_path, host_phys, "data_phys")

            # Update func_image03 with physio info
            func_image03["data_file2"] = f"data_phys/{host_phys}"
            func_image03["data_file2_type"] = "psychophysiological recordings"

        new_row = pd.DataFrame(func_image03, index=[0])
        return (new_row, phys_exists)


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
    df_report : pd.DataFrame
        Report of PANAS data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_panas(sess: str)
        Generate panas01 dataset, builds df_report for one session

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned PANAS Qualtrics survey from visit_day2 and
        visit_day3, and finalized demographic information.
        Generate PANAS report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of PANAS data that complies with NDAR data definitions
        nda_label : list
            NDA report template label

        """
        # Get needed column values from report template
        print("Buiding NDA report : panas01 ...")
        self._proj_dir = proj_dir
        self.nda_label, self._nda_cols = report_helper.mine_template(
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
        self._final_demo = final_demo

        # Make reports for each visit
        df_nda_day2 = self.make_panas("day2")
        df_nda_day3 = self.make_panas("day3")

        # Combine into final report
        df_report = pd.concat(
            [df_pilot, df_nda_day2, df_nda_day3], ignore_index=True
        )
        df_report = df_report.sort_values(by=["src_subject_id", "visit"])
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
            self._proj_dir,
            "data_pilot/ndar_resources",
            "panas01_dataset.csv",
        )
        if not os.path.exists(pilot_report):
            raise FileNotFoundError(
                f"Expected to find pilot panas01 at {pilot_report}"
            )
        df_pilot = pd.read_csv(pilot_report)
        df_pilot = df_pilot[1:]
        df_pilot.columns = self._nda_cols

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
        _df_panas_day2 : pd.DataFrame
            Cleaned visit_day2 PANAS Qualtrics survey
        _df_panas_day3 : pd.DataFrame
            Cleaned visit_day3 PANAS Qualtrics survey

        """
        # Get visit_day2 data
        df_panas_day2 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day2/data_clean",
                "df_PANAS.csv",
            )
        )
        df_panas_day2 = df_panas_day2.rename(
            columns={"study_id": "src_subject_id"}
        )
        self._df_panas_day2 = df_panas_day2

        # Get visit_day3 data
        df_panas_day3 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day3/data_clean",
                "df_PANAS.csv",
            )
        )
        df_panas_day3 = df_panas_day3.rename(
            columns={"study_id": "src_subject_id"}
        )
        df_panas_day3.columns = df_panas_day2.columns.values
        self._df_panas_day3 = df_panas_day3

    def make_panas(self, sess):
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
        df_panas = getattr(self, f"_df_panas_{sess}")

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
        df_nda = self._final_demo[
            ["subjectkey", "src_subject_id", "sex"]
        ].copy()
        df_panas_demo = pd.merge(df_panas_remap, df_nda, on="src_subject_id")
        df_panas_demo = report_helper.get_survey_age(
            df_panas_demo, self._final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with df_panas_demo data
        df_nda = pd.DataFrame(
            columns=self._nda_cols, index=df_panas_demo.index
        )
        df_nda.update(df_panas_demo)
        return df_nda


class NdarPhysio:
    """Make psychophys_subj_exp01 report line-by-line.

    DEPRECATED
        NDAR is now recommending we submit our physio data as a second
        line for each EPI run in image03.

    Identify all physio data in rawdata and add a line the NDAR report
    for each file.

    Make copies of physio files in:
        <proj_dir>/ndar_upload/data_phys

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of physio data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    """

    def __init__(self, proj_dir, final_demo):
        """Coordinate report generation for physio data.

        Assumes physio data exists within a BIDS-organized "phys"
        directory, within each participants' session.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        nda_label : list
            NDA report template label
        _final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info
        _nda_cols : list
            NDA report template column names
        _physio_all : list
            Locations of all physio files
        _proj_dir : path
            Project's experiment directory

        """
        print("Buiding NDA report : psychophys_subj_exp01 ...")
        self._proj_dir = proj_dir

        # Read in template
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "psychophys_subj_exp01_template.csv"
        )

        # Identify all physio files
        rawdata_pilot = os.path.join(
            proj_dir, "data_pilot/data_scanner_BIDS/rawdata"
        )
        rawdata_study = os.path.join(proj_dir, "data_scanner_BIDS/rawdata")
        physio_pilot = sorted(
            glob.glob(f"{rawdata_pilot}/sub-ER*/ses-day*/phys/*acq")
        )
        physio_study = sorted(
            glob.glob(f"{rawdata_study}/sub-ER*/ses-day*/phys/*acq")
        )
        self._physio_all = physio_pilot + physio_study

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo.dropna(subset=["subjectkey"])
        self._make_physio()

    def _get_subj_demo(self, subj_nda, acq_date):
        """Gather required participant demographic information.

        Find participant demographic info and calculate age-in-months
        at time of scan.

        Parameters
        ----------
        subj_nda : str
            Participant identifier
        acq_date : datetime
            Date of physio data acquisition

        Returns
        -------
        dict
            Keys = nda column names
            Values = demographic info

        """
        # Identify participant date of birth, sex, and GUID
        final_demo = self._final_demo
        idx_subj = final_demo.index[
            final_demo["src_subject_id"] == subj_nda
        ].tolist()[0]
        subj_guid = final_demo.iloc[idx_subj]["subjectkey"]
        subj_id = final_demo.iloc[idx_subj]["src_subject_id"]
        subj_sex = final_demo.iloc[idx_subj]["sex"]
        subj_dob = datetime.strptime(
            final_demo.iloc[idx_subj]["dob"], "%Y-%m-%d"
        )

        # Calculate age in months
        interview_age = report_helper.calc_age_mo([subj_dob], [acq_date])[0]
        interview_date = datetime.strftime(acq_date, "%m/%d/%Y")

        # Set babysex to M/F for requirements
        bsex = 1 if subj_sex == "M" else 2

        return {
            "subjectkey": subj_guid,
            "src_subject_id": subj_id,
            "interview_date": interview_date,
            "interview_age": interview_age,
            "sex": subj_sex,
            "child_subjectkey": subj_guid,
            "ch_src_id": subj_id,
            "bio_childage_1": interview_age,
            "babysex": bsex,
            "family_id": subj_id,
        }

    def _get_std_info(self):
        """Return dict of common dataset values."""
        # Split long strings
        return {
            "data_file1_type": "physiological recordings",
            "experiment_description": "emotion induction (see design file in "
            + "linked experiment)",
            "hardware_manufacturer": "BIOPAC",
            "hardware_model": "MP160",
            "sampling_rate": 2000,
            "collection_settings": "Resp: high-pass filter=0.05Hz, Gain=200;"
            + " Pulse Ox.: high-pass filter=DC, DC=10Hz, Gain=100;"
            + " GSR: low-pass filter=10Hz, Gain=10",
            "connections_used": "pneumatic respiration belt, finger pulse"
            + " oximeter, electrodes on left hand for GSR",
            "imaging_present": 1,
            "image_modality": "MRI",
        }

    def _make_physio(self):
        """Generate NDAR report for physio data.

        Determine acquisition datetime, copy files for hosting,
        and find all NDAR-required values.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of physio data that complies with NDAR data definitions

        """
        # Setup for determining experiment id
        exp_dict = {"old": 1683, "new": 2113}
        pilot_list = report_helper.pilot_list()

        # Set local path for upload building
        local_path = (
            "/run/user/1001/gvfs/smb-share:server"
            + "=ccn-keoki.win.duke.edu,share=experiments2/EmoRep/"
            + "Exp2_Compute_Emotion/ndar_upload/data_phys"
        )

        # Start empty dataframe, fill with physio data
        df_report = pd.DataFrame(columns=self._nda_cols)
        for phys_path in self._physio_all:

            # Determine session info
            phys_file = os.path.basename(phys_path)
            print(f"\tMining data for {phys_file}")
            subj, sess, task, run, _, _ = phys_file.split("_")
            subj_nda = subj.split("-")[1]

            # # Extract datetime from acq file - I could not suppress
            # # stdout print by air.run() for the life of me.
            # air = acq_info.AcqInfoRunner(phys_path)
            # air.run()
            # acq_date_str = (
            #     air.reader.datafile.earliest_marker_created_at.isoformat()
            # ).split("T")[0]

            bash_cmd = f"""
                line=$(acq_info {phys_path} | grep "Earliest")
                IFS=":" read -ra dt_arr <<< $line
                echo ${{dt_arr[1]%T*}}
            """
            h_sp = subprocess.Popen(
                bash_cmd,
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
            )
            h_out = h_sp.stdout.read()
            h_acq_date_str = h_out.decode("utf-8").strip()
            acq_date_str = h_acq_date_str.split("T")[0]
            acq_date = datetime.strptime(acq_date_str, "%Y-%m-%d")

            # Copy file for hosting
            host_path = os.path.join(
                self._proj_dir, "ndar_upload/data_phys", phys_file
            )
            if not os.path.exists(host_path):
                bash_cmd = f"cp {phys_path} {host_path}"
                h_sp = subprocess.Popen(
                    bash_cmd, shell=True, stdout=subprocess.PIPE
                )
                h_out, h_err = h_sp.communicate()
                h_sp.wait()

            # Identify participant-specific info
            local_file = os.path.join(local_path, phys_file)
            exp_id = (
                exp_dict["old"] if subj_nda in pilot_list else exp_dict["new"]
            )
            # exp_id = 3639
            stim_pres = 0 if task == "task-rest" else 1
            visit = sess[-1]
            phys_dict = {
                "experiment_id": exp_id,
                "data_file1": local_file,
                "stimulus_present": stim_pres,
                "visit": visit,
            }

            # Update with standard, demographic info
            phys_dict.update(self._get_std_info())
            phys_dict.update(self._get_subj_demo(subj_nda, acq_date))

            # Write new row with determined info
            new_row = pd.DataFrame(phys_dict, index=[0])
            df_report = pd.concat([df_report.loc[:], new_row]).reset_index(
                drop=True
            )

        # Set attribute
        self.df_report = df_report


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
    nda_label : list
        NDA report template label

    Methods
    -------
    make_pswq()
        Generate pswq01 dataset, builds df_report

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned PSWQ Qualtrics survey from visit_day1, and
        finalized demographic information.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : pswq01 ...")
        # Read in template
        self.nda_label, self._nda_cols = report_helper.mine_template(
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
        self._df_pswq = df_pswq[df_pswq["PSWQ_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo.dropna(subset=["subjectkey"])
        self.make_pswq()

    def make_pswq(self):
        """Generate PSWQ report for NDAR submission.

        Remap values and calculate totals.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of PSWQ data that complies with NDAR data definitions

        """
        # Update column names, make data integer
        df_pswq = self._df_pswq.rename(columns=str.lower)
        df_pswq.columns = df_pswq.columns.str.replace("_", "")
        pswq_cols = [x for x in df_pswq.columns if "pswq" in x]
        df_pswq[pswq_cols] = df_pswq[pswq_cols].astype("Int64")
        df_pswq = df_pswq.rename(columns={"studyid": "src_subject_id"})

        # Calculate sum
        df_pswq["pswq_total"] = df_pswq[pswq_cols].sum(axis=1)
        df_pswq["pswq_total"] = df_pswq["pswq_total"].astype("Int64")

        # Combine demographic and erq dataframes
        df_nda = self._final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_pswq_nda = pd.merge(df_pswq, df_nda, on="src_subject_id")
        df_pswq_nda = report_helper.get_survey_age(
            df_pswq_nda, self._final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with df_final_emrq data
        df_report = pd.DataFrame(
            columns=self._nda_cols, index=df_pswq_nda.index
        )
        df_report.update(df_pswq_nda)
        pilot_list = report_helper.pilot_list()
        idx_pilot = df_report[
            df_report["src_subject_id"].isin(pilot_list)
        ].index.tolist()
        df_report.loc[idx_pilot, "comments_misc"] = "PILOT PARTICIPANT"
        self.df_report = df_report


class NdarRest01:
    """Make restsurv01 report for NDAR submission.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of rest data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_rest(sess: str)
        Generate restsurv01 dataset, builds df_report for one session

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned rest rating surveys from visit_day2 and
        visit_day3, and finalized demographic information.
        Generate NDAR report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of rest data that complies with NDAR data definitions
        nda_label : list
            NDA report template label

        """
        # Get needed column values from report template
        print("Buiding NDA report : restsurv01 ...")
        self._proj_dir = proj_dir
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "restsurv01_template.csv"
        )

        # Get pilot, study data for both day2, day3
        self._get_clean()

        # Get final demographics
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo.dropna(subset=["subjectkey"])

        # Make nda reports for each session
        df_nda_day2 = self.make_rest("day2")
        df_nda_day3 = self.make_rest("day3")

        # Combine into final report
        df_report = pd.concat([df_nda_day2, df_nda_day3], ignore_index=True)
        df_report = df_report.sort_values(by=["src_subject_id", "visit"])

        # Add pilot notes for certain subjects
        pilot_list = report_helper.pilot_list()
        idx_pilot = df_report[
            df_report["src_subject_id"].isin(pilot_list)
        ].index.tolist()
        df_report.loc[idx_pilot, "comments_misc"] = "PILOT PARTICIPANT"
        self.df_report = df_report[df_report["interview_date"].notna()]

    def _get_clean(self):
        """Find and combine cleaned rest rating data.

        Get pilot, study data for day2, day3.

        """
        # Get clean survey data
        df_pilot2 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                "visit_day2/data_clean",
                "df_rest-ratings.csv",
            )
        )
        df_study2 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day2/data_clean",
                "df_rest-ratings.csv",
            )
        )

        # Combine pilot and study data, drop resp_alpha rows
        df_rest_day2 = pd.concat([df_pilot2, df_study2], ignore_index=True)
        idx_alpha = df_rest_day2.index[
            df_rest_day2["resp_type"] == "resp_alpha"
        ].tolist()
        df_rest_day2 = df_rest_day2.drop(
            df_rest_day2.index[idx_alpha]
        ).reset_index(drop=True)

        # Update subj id column, set attribute
        df_rest_day2 = df_rest_day2.rename(
            columns={"study_id": "src_subject_id"}
        )
        self._df_rest_day2 = df_rest_day2

        # Repeat above for day3
        df_pilot3 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                "visit_day3/data_clean",
                "df_rest-ratings.csv",
            )
        )
        df_study3 = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day3/data_clean",
                "df_rest-ratings.csv",
            )
        )
        df_rest_day3 = pd.concat([df_pilot3, df_study3], ignore_index=True)
        idx_alpha = df_rest_day3.index[
            df_rest_day3["resp_type"] == "resp_alpha"
        ].tolist()
        df_rest_day3 = df_rest_day3.drop(
            df_rest_day3.index[idx_alpha]
        ).reset_index(drop=True)
        df_rest_day3 = df_rest_day3.rename(
            columns={"study_id": "src_subject_id"}
        )
        df_rest_day3.columns = df_rest_day2.columns.values
        self._df_rest_day3 = df_rest_day3

    def make_rest(self, sess):
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
        df_rest = getattr(self, f"_df_rest_{sess}")

        # Convert response values to int and remap column names
        map_item = {
            "AMUSEMENT": "amusement_01",
            "ANGER": "anger_02",
            "ANXIETY": "anxiety_03",
            "AWE": "awe_04",
            "CALMNESS": "calmness_05",
            "CRAVING": "craving_06",
            "DISGUST": "disgust_07",
            "EXCITEMENT": "excitement_08",
            "FEAR": "fear_09",
            "HORROR": "horror_10",
            "JOY": "joy_12",
            "NEUTRAL": "neutral_13",
            "ROMANCE": "romantic_love_14",
            "SADNESS": "sadness_15",
            "SURPRISE": "surprise_16",
            "INTEREST": "interest_11",
        }
        h_cols = [x for x in df_rest.columns if x in map_item.keys()]
        df_rest[h_cols] = df_rest[h_cols].astype("Int64")
        df_rest_remap = df_rest.rename(columns=map_item)

        # Drop non-ndar columns
        df_rest_remap = df_rest_remap.drop(
            ["resp_type"],
            axis=1,
        )

        # Combine demo and bdi dataframes
        df_nda = self._final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_rest_demo = pd.merge(df_rest_remap, df_nda, on="src_subject_id")

        # Calculate age in months of visit, update visit
        df_rest_demo = report_helper.get_survey_age(
            df_rest_demo, self._final_demo, "src_subject_id"
        )
        df_rest_demo["visit"] = sess

        # Build dataframe from nda columns, update with df_bdi_demo data
        df_nda = pd.DataFrame(columns=self._nda_cols, index=df_rest_demo.index)
        df_nda.update(df_rest_demo)
        return df_nda


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
    nda_label : list
        NDA report template label

    Methods
    -------
    make_rrs()
        Generate rrs01 dataset, builds df_report

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned RRS Qualtrics survey from visit_day1, and
        finalized demographic information.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of RRS data that complies with NDAR data definitions
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : rrs01 ...")
        # Read in template
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "rrs01_template.csv"
        )
        self._proj_dir = proj_dir

        # Get final demographics
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo.dropna(subset=["subjectkey"])

        # Make pilot, study dataframes
        df_pilot = self._get_pilot()
        df_study = self.make_rrs()

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
            self._proj_dir,
            "data_pilot/ndar_resources",
            "rrs01_dataset.csv",
        )
        if not os.path.exists(pilot_report):
            raise FileNotFoundError(
                f"Expected to find pilot rrs01 at {pilot_report}"
            )
        df_pilot = pd.read_csv(pilot_report)
        df_pilot = df_pilot[1:]
        df_pilot.columns = self._nda_cols
        df_pilot["comments_misc"] = "PILOT PARTICIPANT"

        # Calculate sum
        p_cols = [x for x in df_pilot.columns if "rrs" in x]
        df_pilot[p_cols] = df_pilot[p_cols].astype("Int64")
        df_pilot["rrs_total"] = df_pilot[p_cols].sum(axis=1)
        df_pilot["rrs_total"] = df_pilot["rrs_total"].astype("Int64")
        return df_pilot

    def make_rrs(self):
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
                self._proj_dir,
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
        df_nda = self._final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_rrs_nda = pd.merge(df_rrs, df_nda, on="src_subject_id")
        df_rrs_nda = report_helper.get_survey_age(
            df_rrs_nda, self._final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with demo and rrs data
        df_study_report = pd.DataFrame(
            columns=self._nda_cols, index=df_rrs_nda.index
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
    df_report : pd.DataFrame
        Report of STAI data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    mask_stai_state()
        Generate stai01 dataset, builds df_report of state survey data
    make_stai_trait(sess: str)
        Generate stai01 dataset, builds df_report for one session
        of trait survey data

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned STAI Qualtrics survey from visits, and
        finalized demographic information. Coordinate creation
        of state, trait reports. Generate df_report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of STAI data that complies with NDAR data definitions
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : stai01 ...")
        # Read in template
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "stai01_template.csv"
        )
        self._proj_dir = proj_dir

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo.dropna(subset=["subjectkey"])

        # Generate trait report
        df_trait = self.make_stai_trait()

        # Generate state reports for day2, day3
        self._get_state()
        df_nda_day2 = self.make_stai_state("day2")
        df_nda_day3 = self.make_stai_state("day3")

        # Combine into final report
        df_report = pd.concat(
            [df_trait, df_nda_day2, df_nda_day3], ignore_index=True
        )
        df_report = df_report.sort_values(by=["src_subject_id", "visit"])
        self.df_report = df_report[df_report["interview_date"].notna()]

    def _get_trait(self):
        """Compile trait (visit1) responses.

        Returns
        -------
        pd.DataFrame
            Pilot and study stai state responses

        """
        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                "visit_day1/data_clean",
                "df_STAI_Trait.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_STAI_Trait.csv",
            )
        )
        df_stai_trait = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, drop NaN rows
        df_stai_trait = df_stai_trait.rename(
            columns={"study_id": "src_subject_id"}
        )
        df_stai_trait = df_stai_trait.replace("NaN", np.nan)
        df_stai_trait = df_stai_trait[df_stai_trait["STAI_Trait_1"].notna()]
        return df_stai_trait

    def _get_state(self):
        """Compile state (visit2, visit3) responses.

        Attributes
        ----------
        _df_stai_state_day2 : pd.DataFrame
            Pilot and study stai state responses for day2
        _df_stai_state_day3 : pd.DataFrame
            Pilot and study stai state responses for day3

        """
        # Get clean survey data
        df_pilot = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                "visit_day2/data_clean",
                "df_STAI_State.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day2/data_clean",
                "df_STAI_State.csv",
            )
        )
        df_stai_state2 = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, drop NaN rows
        df_stai_state2 = df_stai_state2.rename(
            columns={"study_id": "src_subject_id"}
        )
        df_stai_state2 = df_stai_state2.replace("NaN", np.nan)
        df_stai_state2 = df_stai_state2[df_stai_state2["STAI_State_1"].notna()]
        self._df_stai_state_day2 = df_stai_state2

        # Repeat for visit 3
        df_pilot = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                "visit_day3/data_clean",
                "df_STAI_State.csv",
            )
        )
        df_study = pd.read_csv(
            os.path.join(
                self._proj_dir,
                "data_survey",
                "visit_day3/data_clean",
                "df_STAI_State.csv",
            )
        )
        df_stai_state3 = pd.concat([df_pilot, df_study], ignore_index=True)
        del df_pilot, df_study

        # Rename columns, drop NaN rows
        df_stai_state3 = df_stai_state3.rename(
            columns={"study_id": "src_subject_id"}
        )
        df_stai_state3 = df_stai_state3.replace("NaN", np.nan)
        df_stai_state3 = df_stai_state3[df_stai_state3["STAI_State_1"].notna()]
        self._df_stai_state_day3 = df_stai_state3

    def make_stai_trait(self):
        """Combine dataframes to generate trait report.

        Remap columns, calculate totals, and determine survey age.

        Returns
        -------
        pd.DataFrame
            Report of STAI trait data that complies with
            NDAR data definitions

        """
        # Make data integer
        df_stai_trait = self._get_trait()
        stai_cols = [x for x in df_stai_trait.columns if "STAI" in x]
        df_stai_trait[stai_cols] = df_stai_trait[stai_cols].astype("Int64")

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
        df_stai_trait_remap = df_stai_trait.rename(columns=map_item)

        # Get trait sum
        trait_cols = [x for x in df_stai_trait_remap.columns if "stai" in x]
        df_stai_trait_remap["staiy_trait"] = df_stai_trait_remap[
            trait_cols
        ].sum(axis=1)
        df_stai_trait_remap["staiy_trait"] = df_stai_trait_remap[
            "staiy_trait"
        ].astype("Int64")

        # Combine demographic and stai dataframes
        df_nda = self._final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_stai_trait_nda = pd.merge(
            df_stai_trait_remap, df_nda, on="src_subject_id"
        )
        df_stai_trait_nda = report_helper.get_survey_age(
            df_stai_trait_nda, self._final_demo, "src_subject_id"
        )

        # Add visit info
        df_stai_trait_nda["visit"] = "day1"

        # Build dataframe from nda columns, update with demo and stai data
        df_nda = pd.DataFrame(
            columns=self._nda_cols, index=df_stai_trait_nda.index
        )
        df_nda.update(df_stai_trait_nda)
        return df_nda

    def make_stai_state(self, sess):
        """Combine dataframes to generate state report.

        Remap columns, calculate totals, and determine survey age.

        Parameters
        ----------
        sess : str
            [day2 | day3]
            visit/session name

        Returns
        -------
        pd.DataFrame
            Report of STAI state data that complies with
            NDAR data definitions

        """
        # Check sess value
        sess_list = ["day2", "day3"]
        if sess not in sess_list:
            raise ValueError(f"Incorrect visit day : {sess}")

        # Get session data
        df_stai_state = getattr(self, f"_df_stai_state_{sess}")

        # Make data integer
        stai_cols = [x for x in df_stai_state.columns if "STAI" in x]
        df_stai_state[stai_cols] = df_stai_state[stai_cols].astype("Int64")

        # Remap column names
        map_item = {
            "STAI_State_1": "stai1",
            "STAI_State_2": "stai2",
            "STAI_State_3": "stai3",
            "STAI_State_4": "stai_state4_i",
            "STAI_State_5": "stai5",
            "STAI_State_6": "stai6",
            "STAI_State_7": "stai7",
            "STAI_State_8": "stai_state8_i",
            "STAI_State_9": "stai_state9_i",
            "STAI_State_10": "stai10",
            "STAI_State_11": "stai11",
            "STAI_State_12": "stai12",
            "STAI_State_13": "stai13",
            "STAI_State_14": "stai_state14_i",
            "STAI_State_15": "stai15",
            "STAI_State_16": "stai16",
            "STAI_State_17": "stai17",
            "STAI_State_18": "stai_state18_i",
            "STAI_State_19": "stai_state19_i",
            "STAI_State_20": "stai20",
        }
        df_stai_state_remap = df_stai_state.rename(columns=map_item)

        # Get state sum
        state_cols = [x for x in df_stai_state_remap.columns if "stai" in x]
        df_stai_state_remap["staiy_state"] = df_stai_state_remap[
            state_cols
        ].sum(axis=1)
        df_stai_state_remap["staiy_state"] = df_stai_state_remap[
            "staiy_state"
        ].astype("Int64")

        # Combine demographic and stai dataframes
        df_nda = self._final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_stai_state_nda = pd.merge(
            df_stai_state_remap, df_nda, on="src_subject_id"
        )
        df_stai_state_nda = report_helper.get_survey_age(
            df_stai_state_nda, self._final_demo, "src_subject_id"
        )

        # Add visit info
        df_stai_state_nda["visit"] = sess

        # Build dataframe from nda columns, update with demo and stai data
        df_nda = pd.DataFrame(
            columns=self._nda_cols, index=df_stai_state_nda.index
        )
        df_nda.update(df_stai_state_nda)
        return df_nda


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
    df_report : pd.DataFrame
        Report of TAS data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_tas
        Generate tas01 dataset, builds df_report

    """

    def __init__(self, proj_dir, final_demo):
        """Read in survey data and make report.

        Get cleaned TAS Qualtrics survey from visit_day1, and
        finalized demographic information.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : tas01 ...")
        # Read in template
        self.nda_label, self._nda_cols = report_helper.mine_template(
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
        self._df_tas = df_tas[df_tas["TAS_1"].notna()]

        # Get final demographics, make report
        final_demo = final_demo.replace("NaN", np.nan)
        final_demo["sex"] = final_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._final_demo = final_demo.dropna(subset=["subjectkey"])
        self.make_tas()

    def make_tas(self):
        """Combine dataframes to generate requested report.

        Rename columns, calculate totals, and determine survey age.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of TAS data that complies with NDAR data definitions

        """
        # Make data integer, calculate sum, and rename columns
        df_tas = self._df_tas
        tas_cols = [x for x in df_tas.columns if "TAS" in x]
        df_tas[tas_cols] = df_tas[tas_cols].astype("Int64")
        df_tas["tas_totalscore"] = df_tas[tas_cols].sum(axis=1)
        df_tas["tas_totalscore"] = df_tas["tas_totalscore"].astype("Int64")
        df_tas.columns = df_tas.columns.str.replace("TAS", "tas20")

        # Combine demographic and stai dataframes
        df_nda = self._final_demo[["subjectkey", "src_subject_id", "sex"]]
        df_tas_nda = pd.merge(df_tas, df_nda, on="src_subject_id")
        df_tas_nda = report_helper.get_survey_age(
            df_tas_nda, self._final_demo, "src_subject_id"
        )

        # Build dataframe from nda columns, update with demo and stai data
        self.df_report = pd.DataFrame(
            columns=self._nda_cols, index=df_tas_nda.index
        )
        self.df_report.update(df_tas_nda)
