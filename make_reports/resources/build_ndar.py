"""Build datsets and data hosts for NDAR uploads.

NdarAffim01 : build affim01 report
NdarAls01 : build als01 report
NdarBdi01 : build bdi01 report
NdarBrd01 : build brd01 report
NdarDemoInfo01 : build demo_info01 report
NdarEmrq01 : build emrq01 report
NdarImage03 : build image03 report
NdarIec01 : build iec01 report
NdarPanas01 : build panas01 report
NdarPhysio : Deprecated
NdarPswq01 : build pswq01 report
NdarRest01 : build restsurv01 report
NdarRrs01 : build rrs01 report
NdarStai01 : build stai01 report
NdarSubject01 : build ndar_subject01 report
NdarTas01 : build tas01 report

Notes
-----
Classes are highly similar and a more generic class structure could have
done the necessary work for several NDAR reports, but here each report
is kept separate for ease of fulfilling idiosyncratic NDAR requirements.

Each class contains two attributes used for writing the NDAR dataset reports:
    -   df_report : pd.DataFrame, a NDAR-compliant report for the class'
            respective data
    -   nda_label : list, the file identifier that should be prepended
            to df_report (e.g. image,03)

Instantiating the class triggers the construction of df_report and
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
from dateutil.relativedelta import relativedelta
import pydicom
from make_reports.resources import report_helper


class _CleanDemo:
    """Clean demographic dataframe.

    Clean make_reports.build_reports.DemoAll.final_demo for NDAR report
    generation and compliance.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    drop_subjectkey : bool, optional
        Drop rows that have NaN in subjectkey col
    fix_nan : bool, optional
        Replace "NaN" str with np.nan
    remap_sex : bool, optional
        Replace Male, Female, Neither in sex col with M, F, O
    remap_race : bool, optional
        Replace NDA incompliant race responses

    """

    def __init__(
        self,
        df_demo,
        drop_subjectkey=True,
        fix_nan=True,
        remap_sex=True,
        remap_race=False,
    ):
        """Set _df_demo attr, trigger cleaning methods."""
        self._df_demo = df_demo
        if drop_subjectkey:
            self._drop_subjectkey()
        if fix_nan:
            self._fix_nan()
        if remap_sex:
            self._remap_sex()
        if remap_race:
            self._remap_race()

    def _drop_subjectkey(self):
        """Drop rows that have NaN in subjectkey col."""
        self._df_demo.dropna(subset=["subjectkey"])

    def _fix_nan(self):
        """Replace "NaN" str with np.nan."""
        self._df_demo = self._df_demo.replace("NaN", np.nan)

    def _remap_sex(self):
        """Replace Male, Female, Neither in sex col with M, F, O."""
        self._df_demo["sex"] = self._df_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )

    def _remap_race(self):
        """Replace NDA incompatible race labels."""
        self._df_demo["race"] = self._df_demo["race"].replace(
            ["Black or African-American", "American Indian or Alaska Native"],
            ["Black or African American", "American Indian/Alaska Native"],
        )


def _local_path() -> str:
    """Return path to local files for ndar upload."""
    return (
        "/run/user/1001/gvfs/smb-share:server"
        + "=ccn-keoki.win.duke.edu,share=experiments2/EmoRep/"
        + "Exp2_Compute_Emotion/ndar_upload"
    )


def _task_id() -> dict:
    """Return old, new EmoRep Task IDs."""
    return {"old": 1683, "new": 2113}


class NdarAffim01(_CleanDemo):
    """Make affim01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
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

    def __init__(self, df_demo, df_pilot, df_study):
        """Read in survey data and make report.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : affim01 ...")
        # Read in template, concat dfs
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "affim01_template.csv"
        )
        df_aim = pd.concat([df_pilot, df_study], ignore_index=True)

        # Rename columns, drop NaN rows
        df_aim = df_aim.rename(columns={"study_id": "src_subject_id"})
        df_aim.columns = df_aim.columns.str.lower()
        df_aim = df_aim.replace("NaN", np.nan)
        self._df_aim = df_aim[df_aim["aim_1"].notna()]

        # Make report
        self.make_aim()

    def make_aim(self):
        """Combine dataframes to generate requested report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of AIM data that complies with NDAR data definitions

        """
        # Get needed demographic info, combine with survey data
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]].copy()
        df_aim_nda = pd.merge(self._df_aim, df_nda, on="src_subject_id")

        # Find survey age-in-months
        df_aim_nda = report_helper.get_survey_age(df_aim_nda, self._df_demo)

        # Sum aim responses
        aim_list = [x for x in df_aim_nda.columns if "aim" in x]
        df_aim_nda[aim_list] = df_aim_nda[aim_list].astype("Int64")
        df_aim_nda["aimtot"] = df_aim_nda[aim_list].sum(axis=1)

        # Make an empty dataframe from the report column names, fill
        self.df_report = pd.DataFrame(
            columns=self._nda_cols, index=df_aim_nda.index
        )
        self.df_report.update(df_aim_nda)


class NdarAls01(_CleanDemo):
    """Make als01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    df_pilot : pd.DataFrame
        Pilot ALS data
    df_study : pd.DataFrame
        Study ALS data

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

    def __init__(self, df_demo, df_pilot, df_study):
        """Read in survey data and make report.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : als01 ...")
        # Read in template, concat dfs
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "als01_template.csv"
        )
        df_als = pd.concat([df_pilot, df_study], ignore_index=True)

        # Rename columns, drop NaN rows
        df_als = df_als.rename(columns={"study_id": "src_subject_id"})
        df_als = df_als.replace("NaN", np.nan)
        self._df_als = df_als[df_als["ALS_1"].notna()]

        # Make report
        self.make_als()

    def make_als(self):
        """Combine dataframes to generate requested report.

        Remap values and calculate totals.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of ALS data that complies with NDAR data definitions

        """
        # Remap response values (reverse code) and column names
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
        self._df_als = self._df_als.rename(columns=map_item)
        als_cols = [x for x in self._df_als.columns if "als" in x]
        self._df_als[als_cols] = self._df_als[als_cols].astype("Int64")
        self._df_als[als_cols] = self._df_als[als_cols].replace(
            resp_qual, resp_ndar
        )

        # Calculate totals
        self._df_als["als_glob"] = self._df_als[als_cols].sum(axis=1)
        self._df_als["als_sf_total"] = self._df_als[als_cols].sum(axis=1)
        self._df_als["als_sf_total"] = self._df_als["als_sf_total"].astype(
            "Int64"
        )

        # Add pilot notes for certain subjects
        pilot_list = report_helper.pilot_list()
        idx_pilot = self._df_als[
            self._df_als["src_subject_id"].isin(pilot_list)
        ].index.tolist()
        self._df_als.loc[idx_pilot, "comments"] = "PILOT PARTICIPANT"

        # Combine demographic and als dataframes
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]].copy()
        df_als_nda = pd.merge(self._df_als, df_nda, on="src_subject_id")
        df_als_nda = report_helper.get_survey_age(df_als_nda, self._df_demo)

        # Build dataframe from nda columns, update with demo and als data
        self.df_report = pd.DataFrame(
            columns=self._nda_cols, index=df_als_nda.index
        )
        self.df_report.update(df_als_nda)


class NdarBdi01(_CleanDemo):
    """Make bdi01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    df_pilot_day2 : pd.DataFrame
        Pilot BDI data from ses-day2
    df_study_day2 : pd.DataFrame
        Study BDI data from ses-day2
    df_pilot_day3 : pd.DataFrame
        Pilot BDI data from ses-day3
    df_study_day3 : pd.DataFrame
        Study BDI data from ses-day3

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of BDI data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_bdi()
        Generate bdi01 dataset, builds df_report for one session

    """

    def __init__(
        self,
        df_demo,
        df_pilot_day2,
        df_study_day2,
        df_pilot_day3,
        df_study_day3,
    ):
        """Read in survey data and make report.

        Attributes
        ----------
        df_report : pd.DataFrame
            Report of BDI data that complies with NDAR data definitions
        nda_label : list
            NDA report template label

        """
        # Get needed column values from report template
        print("Buiding NDA report : bdi01 ...")
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "bdi01_template.csv"
        )

        # Combine pilot and study data, updated subj id column, set attribute
        self._df_bdi_day2 = self._make_df(df_pilot_day2, df_study_day2)
        self._df_bdi_day3 = self._make_df(df_pilot_day3, df_study_day3)

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

    def _make_df(
        self, df_pilot: pd.DataFrame, df_study: pd.DataFrame
    ) -> pd.DataFrame:
        """Return df of concatenated pilot, study data."""
        # Combine pilot and study data, updated subj id column, set attribute
        df = pd.concat([df_pilot, df_study], ignore_index=True)
        df = df.rename(columns={"study_id": "src_subject_id"})
        return df

    def make_bdi(self, sess):
        """Make an NDAR compliant report for visit.

        Remap column names, add demographic info, get session
        age, and generate report.

        Parameters
        ----------
        sess : str
            {"day2", "day3"}
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
        if sess not in ["day2", "day3"]:
            raise ValueError(f"Incorrect visit day : {sess}")

        # Get session data, convert response values to int
        df_bdi = getattr(self, f"_df_bdi_{sess}")
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
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]]
        df_bdi_demo = pd.merge(df_bdi_remap, df_nda, on="src_subject_id")

        # Calculate age in months of visit
        df_bdi_demo = report_helper.get_survey_age(df_bdi_demo, self._df_demo)
        df_bdi_demo["visit"] = sess

        # Build dataframe from nda columns, update with df_bdi_demo data
        df_nda = pd.DataFrame(columns=self._nda_cols, index=df_bdi_demo.index)
        df_nda.update(df_bdi_demo)
        return df_nda


class NdarBrd01(_CleanDemo):
    """Make brd01 report for NDAR submission.

    Inherits _CleanDemo.

    Receives clean study data and finds pilot data in
    proj_dir/data_pilot/ndar_resources.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    proj_dir : str, os.PathLike
        Project's experiment directory
    df_study_day2 : pd.DataFrame
        Study post-scan ratings data from ses-day2
    df_study_day3 : pd.DataFrame
        Study post-scan ratings data from ses-day3

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of rest rating data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_brd()
        Generate brd01 dataset, builds df_report for one session

    """

    def __init__(
        self,
        df_demo,
        proj_dir,
        df_study_day2,
        df_study_day3,
    ):
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
        # Get needed column values from report template, start output df
        print("Buiding NDA report : brd01 ...")
        super().__init__(df_demo)
        self._proj_dir = proj_dir
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "brd01_template.csv"
        )
        self.df_report = pd.DataFrame(columns=self._nda_cols)

        # Fill df_report for each session
        self._get_pilot()
        self.make_brd("day2", df_study_day2)
        self.make_brd("day3", df_study_day3)
        self.df_report = self.df_report.sort_values(
            by=["src_subject_id", "visit"]
        )

    def make_brd(self, sess, df_study):
        """Make brd01 report for session.

        Get Qualtrics survey data for post scan ratings,
        make a host file, and generate NDAR dataframe.

        Parameters
        ----------
        sess : str
            {"day2", "day3"}
            Session identifier
        df_study : pd.DataFrame
            Study post-scan ratings data

        Raises
        ------
        ValueError
            Inappropriate sess input

        """
        # Check sess value
        if sess not in ["day2", "day3"]:
            raise ValueError(f"Incorrect visit day : {sess}")

        # Combine pilot and study data, updated subj id column
        df_brd = df_study.copy()
        df_brd = df_brd.rename(columns={"study_id": "src_subject_id"})
        df_brd["datetime"] = pd.to_datetime(df_brd["datetime"])

        # Setup hosting directory
        host_dir = os.path.join(self._proj_dir, "ndar_upload/data_beh")
        if not os.path.exists(host_dir):
            os.makedirs(host_dir)

        # Get task ids
        id_dict = _task_id()

        # Mine each participant's data
        sub_list = df_brd["src_subject_id"].unique().tolist()
        sub_demo = self._df_demo["src_subject_id"].tolist()
        for sub in sub_list:
            # Skip participants not in df_demo (cycle date or withdrawn)
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

            # Set values required by brd01
            brd01_info = {
                "visit": sess[-1],
                "experiment_description": "(see design details of "
                + f"experiment_ID {id_dict['new']})",
                "stimuli_detail": "post-scan stimulus rating",
                "data_file1": os.path.join(
                    _local_path(), "data_beh", out_file
                ),
                # "experiment_id": id_dict["new"],
            }
            survey_date = df_sub.loc[0, "datetime"]
            brd01_info.update(self._get_subj_demo(survey_date, sub))

            # Add brd info to report
            new_row = pd.DataFrame(brd01_info, index=[0])
            self.df_report = pd.concat(
                [self.df_report.loc[:], new_row]
            ).reset_index(drop=True)
            del new_row

    def _get_pilot(self):
        """Get pilot data from previous NDAR submission."""
        # Read-in dataframe of pilot participants
        pilot_report = os.path.join(
            self._proj_dir,
            "data_pilot/ndar_resources",
            "brd01_dataset.csv",
        )
        if not os.path.exists(pilot_report):
            raise FileNotFoundError(
                f"Expected to find pilot brd at {pilot_report}"
            )
        df_pilot = pd.read_csv(pilot_report)

        # Clean up df
        df_pilot = df_pilot[1:]
        df_pilot.columns = self._nda_cols
        # df_pilot["experiment_id"] = df_pilot["experiment_id"].astype("Int64")
        df_pilot["experiment_id"] = df_pilot["experiment_id"].replace(
            "1683", np.NaN
        )
        df_pilot["comments_misc"] = "PILOT PARTICIPANT"

        # Add to self.df_report
        self.df_report = pd.concat([self.df_report, df_pilot]).reset_index(
            drop=True
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
        idx_subj = self._df_demo.index[
            self._df_demo["src_subject_id"] == sub
        ].tolist()[0]
        subj_guid = self._df_demo.iloc[idx_subj]["subjectkey"]
        subj_id = self._df_demo.iloc[idx_subj]["src_subject_id"]
        subj_sex = self._df_demo.iloc[idx_subj]["sex"]
        subj_dob = self._df_demo.iloc[idx_subj]["dob"]

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


class NdarDemoInfo01(_CleanDemo):
    """Make demo_info01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
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

    def __init__(self, df_demo):
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
        super().__init__(df_demo, remap_race=True)
        self.nda_label, nda_cols = report_helper.mine_template(
            "demo_info01_template.csv"
        )
        self.df_report = pd.DataFrame(columns=nda_cols)
        self.make_demo()

    def make_demo(self):
        """Update df_report with NDAR-required demographic information."""
        # Manage race response 'other'
        subj_race = list(self._df_demo["race"])
        subj_race_other = []
        for idx, resp in enumerate(subj_race):
            if "Other" in resp:
                subj_race[idx] = "Other"
                subj_race_other.append(resp.split(" - ")[1])
            else:
                subj_race_other.append(np.nan)

        # Make comments for pilot subjs
        pilot_list = report_helper.pilot_list()
        subj_comments_misc = []
        for subj in self._df_demo["src_subject_id"]:
            if subj in pilot_list:
                subj_comments_misc.append("PILOT PARTICIPANT")
            else:
                subj_comments_misc.append(np.nan)

        # Organize values, add to report
        report_dict = {
            "subjectkey": self._df_demo["subjectkey"],
            "src_subject_id": self._df_demo["src_subject_id"],
            "interview_date": [
                x.strftime("%m/%d/%Y") for x in self._df_demo["interview_date"]
            ],
            "interview_age": self._df_demo["interview_age"],
            "sex": self._df_demo["sex"],
            "race": subj_race,
            "otherrace": subj_race_other,
            "educat": self._df_demo["years_education"],
            "comments_misc": subj_comments_misc,
        }
        for h_col, h_value in report_dict.items():
            self.df_report[h_col] = h_value


class NdarEmrq01(_CleanDemo):
    """Make emrq01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    df_pilot : pd.DataFrame
        Pilot ALS data
    df_study : pd.DataFrame
        Study ALS data

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

    def __init__(self, df_demo, df_pilot, df_study):
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
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "emrq01_template.csv"
        )
        df_emrq = pd.concat([df_pilot, df_study], ignore_index=True)

        # Rename columns, drop NaN rows
        df_emrq = df_emrq.rename(columns={"study_id": "src_subject_id"})
        df_emrq = df_emrq.replace("NaN", np.nan)
        self._df_emrq = df_emrq[df_emrq["ERQ_1"].notna()]

        # Make report
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
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]].copy()
        df_emrq_nda = pd.merge(df_emrq, df_nda, on="src_subject_id")
        df_emrq_nda = report_helper.get_survey_age(df_emrq_nda, self._df_demo)

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


class NdarImage03(_CleanDemo):
    """Make image03 report line-by-line.

    Inherits _CleanDemo.

    Identify all data in rawdata and add a line to image03 for each
    MRI file in rawdata. Utilize BIDS JSON sidecar and DICOM header
    information to identify required values.

    Make copies of study participants' MRI/events and physio files in:
        <proj_dir>/ndar_upload/data_[mri|phys]

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    proj_dir : str, os.PathLike
        Project's experiment directory
    close_date : datetime.date
        Submission cycle close date
    all_data : bool, optional
        Host and make image03 from all data rather than
        submission cycle.
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

    def __init__(
        self, df_demo, proj_dir, close_date, all_data=False, test_subj=None
    ):
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

        """
        print("Buiding NDA report : image03 ...")

        # Read in template, start empty dataframe
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "image03_template.csv"
        )
        self._df_report_study = pd.DataFrame(columns=self._nda_cols)

        # Set reference and orienting attributes
        self._proj_dir = proj_dir
        self._close_date = datetime.combine(close_date, datetime.min.time())
        self._all_data = all_data
        self._source_dir = os.path.join(
            proj_dir, "data_scanner_BIDS/sourcedata"
        )

        # Calc start date
        if not all_data:
            self._start_date = self._close_date + relativedelta(months=-6)

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

        # Get pilot df, make df_report for all study participants
        if self._all_data:
            self.make_pilot()
        self.make_image03()

        # Combine df_report_study and df_report_pilot
        df_list = (
            [self._df_report_pilot, self._df_report_study]
            if self._all_data
            else [self._df_report_study]
        )
        self.df_report = pd.concat(df_list, ignore_index=True)

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
        self._df_report_pilot = pd.read_csv(pilot_report)
        self._df_report_pilot = self._df_report_pilot[1:]
        self._df_report_pilot.columns = self._nda_cols
        self._df_report_pilot["comments_misc"] = "PILOT PARTICIPANT"

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
        cons_list = self._df_demo["src_subject_id"].tolist()

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

            # Only use participants found in df_demo, reflecting
            # current consent and available demo info.
            if self._subj_nda not in cons_list:
                print(
                    f"""
                    {self._subj_nda} not found in self._df_demo,
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
        idx_subj = self._df_demo.index[
            self._df_demo["src_subject_id"] == self._subj_nda
        ].tolist()[0]
        subj_guid = self._df_demo.iloc[idx_subj]["subjectkey"]
        subj_id = self._df_demo.iloc[idx_subj]["src_subject_id"]
        subj_sex = self._df_demo.iloc[idx_subj]["sex"]
        subj_dob = self._df_demo.iloc[idx_subj]["dob"]

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
        print(f"\t\tWorking on {self._subj} {self._sess} : anat ...")

        # Get JSON info
        json_list = sorted(glob.glob(f"{self._subj_sess}/anat/*.json"))
        if not json_list:
            print(f"No files found at {self._subj_sess}/anat, continuing ...")
            return
        json_file = json_list[0]
        with open(json_file, "r") as jf:
            nii_json = json.load(jf)

        # Get DICOM info
        day = self._sess.split("-")[1]
        dicom_dir = (
            f"{self._source_dir}/{self._subj_nda}/{day}*/"
            + "DICOM/EmoRep_anat"
        )
        dicom_list = glob.glob(f"{dicom_dir}/*.dcm")
        if not dicom_list:
            # Account for reconstruction issues showing in dir names
            dicom_dir = dicom_dir + "*"
            dicom_list = glob.glob(f"{dicom_dir}/*.dcm")
            if not dicom_list:
                raise FileNotFoundError(
                    f"""
                    Expected to find a DICOM file at : {dicom_dir}
                    """
                )
        dicom_hdr = pydicom.read_file(dicom_list[0])

        # Get demographic info
        scan_date = datetime.strptime(dicom_hdr[0x08, 0x20].value, "%Y%m%d")
        demo_dict = self._get_subj_demo(scan_date)

        # Account for submission window
        if not self._include_scan(scan_date):
            return

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
            "software_preproc": "@afni_refacer_run -mode_deface "
            + "Version AFNI_23.0.03",
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

    def _include_scan(self, scan_date: datetime) -> bool:
        """Check if scan date is in submission window."""
        if not self._all_data:
            if scan_date < self._start_date or scan_date > self._close_date:
                p_scan = scan_date.strftime("%Y-%m-%d")
                p_start = self._start_date.strftime("%Y-%m-%d")
                p_end = self._close_date.strftime("%Y-%m-%d")
                print(
                    f"\t\t\tScan date {p_scan} not in range : "
                    + f"{p_start} - {p_end}, skipping"
                )
                return False
        return True

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
        print(f"\t\tWorking on {self._subj} {self._sess} : fmap ...")

        # Find nii, json files
        nii_list = sorted(glob.glob(f"{self._subj_sess}/fmap/*.nii.gz"))
        json_list = sorted(glob.glob(f"{self._subj_sess}/fmap/*.json"))
        if not nii_list or not json_list:
            print(f"No files found at {self._subj_sess}/fmap, continuing ...")
            return

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
                # Account for reconstruction issues showing in dir names
                dicom_dir = dicom_dir + "*"
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

            # Account for submission window
            if not self._include_scan(scan_date):
                continue

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
        exp_dict = _task_id()
        exp_id = (
            exp_dict["old"]
            if self._subj_nda in pilot_list
            else exp_dict["new"]
        )

        # Find all func niftis
        nii_list = sorted(glob.glob(f"{self._subj_sess}/func/*.nii.gz"))
        if not nii_list:
            print(
                f"No NIfTIs found for {self._subj_sess}/func, continuing ..."
            )
            return

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
                f"\t\tWorking on {self._subj} {self._sess} : "
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
                # Account for reconstruction issues showing in dir names
                task_source = task_source + "*"
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

            # Account for submission window
            if not self._include_scan(scan_date):
                continue

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


class NdarIec01(_CleanDemo):
    """Make demo_info01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo, pd.DataFrame
        Compiled demographic info
    df_prescreen : pd.DataFrame
        REDCap prescreener report
    df_bdi_pilot_day2 : pd.DataFrame
        Pilot BDI data from ses-day2
    df_bdi_study_day2 : pd.DataFrame
        Study BDI data from ses-day2
    df_als_pilot : pd.DataFrame
        Pilot ALS survey responses
    df_als_study : pd.DataFrame
        Study ALS survey responses

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of exclusion data that complies with NDAR data definitions
    nda_label : list
        NDA report template label

    Methods
    -------
    make_iec()
        Generate iec01 dataset, builds df_report

    """

    def __init__(
        self,
        df_demo,
        df_prescreen,
        df_bdi_day2_pilot,
        df_bdi_day2_study,
        df_als_pilot,
        df_als_study,
    ):
        """Trigger report generation."""
        # Get needed column values from report template
        print("Buiding NDA report : iec01 ...")
        super().__init__(df_demo)
        self.nda_label, nda_cols = report_helper.mine_template(
            "iec01_template.csv"
        )

        # Get, organize input dfs
        self._df_pre = df_prescreen
        self._df_bdi = pd.concat(
            [df_bdi_day2_pilot, df_bdi_day2_study], ignore_index=True
        )
        self._df_als = pd.concat(
            [df_als_pilot, df_als_study], ignore_index=True
        )

        # Build report
        self.df_report = pd.DataFrame(columns=nda_cols)
        self.make_iec()

    @property
    def _bdi_change_date(self) -> datetime.date:
        """Return date of BDI exclusion criterion change."""
        return datetime.strptime("2022-07-18", "%Y-%m-%d").date()

    def _proc_als(self):
        """Process df_als for required input."""
        # Prep df for merge
        self._df_als["als_datetime"] = pd.to_datetime(self._df_als["datetime"])
        self._df_als["als_datetime"] = self._df_als["als_datetime"].dt.date
        self._df_als["src_subject_id"] = self._df_als["study_id"]
        self._df_als.drop("datetime", axis=1, inplace=True)

    def _proc_bdi(self):
        """Process df_bdi_day2 for required input."""
        # Prep df for merge
        self._df_bdi["bdi_datetime"] = pd.to_datetime(self._df_bdi["datetime"])
        self._df_bdi["bdi_datetime"] = self._df_bdi["bdi_datetime"].dt.date
        self._df_bdi["src_subject_id"] = self._df_bdi["study_id"]
        self._df_bdi.drop("datetime", axis=1, inplace=True)

        # Sum values
        val_cols = [x for x in self._df_bdi.columns if "BDI" in x]
        self._df_bdi["bdi_sum"] = self._df_bdi[val_cols].sum(axis=1)

        # Determine excl4 criterion
        self._df_bdi["excl_crit4"] = 0
        self._df_bdi.loc[self._df_bdi.BDI_9 > 2, "excl_crit4"] = 1

        # Determine excl3 criterion
        self._df_bdi["excl_crit3"] = np.nan
        self._df_bdi.loc[
            self._df_bdi.bdi_datetime >= self._bdi_change_date, "excl_crit3"
        ] = 0
        self._df_bdi.loc[
            (self._df_bdi["bdi_sum"] > 20)
            & (self._df_bdi["bdi_datetime"] >= self._bdi_change_date),
            "excl_crit3",
        ] = 1

        # Determine excl3_alt criterion
        self._df_bdi["excl_crit3_alt"] = np.nan
        self._df_bdi.loc[
            self._df_bdi.bdi_datetime < self._bdi_change_date, "excl_crit3_alt"
        ] = 0
        self._df_bdi.loc[
            (self._df_bdi["bdi_sum"] > 14)
            & (self._df_bdi["bdi_datetime"] < self._bdi_change_date),
            "excl_crit3_alt",
        ] = 1

        # Account for missing BDI datetimes, add value to excl3
        self._df_bdi.loc[
            (self._df_bdi["bdi_datetime"].isna())
            & (self._df_bdi["excl_crit3"].isna()),
            "excl_crit3",
        ] = 0
        self._df_bdi.loc[
            (self._df_bdi["bdi_datetime"].isna())
            & (self._df_bdi["bdi_sum"] > 20),
            "excl_crit3",
        ] = 1

    def _proc_pre(self):
        """Process df_pre for required input."""
        # Prep df for merge
        self._df_pre["src_subject_id"] = [
            f"ER{x:04}" for x in self._df_pre.record_id
        ]

        # Map new col to existing for inc/exclusion criteria
        col_map = {
            "excl_crit5": "head_injury",
            "excl_crit6": "psychiatric_illness",
            "excl_crit7": "neurological_illness",
            "excl_crit8": "substance_abuse",
            "excl_crit9": "headaches",
            "excl_crit12": "pregnancy",
            "incl_crit4": "english",
        }
        for new, orig in col_map.items():
            self._df_pre[new] = self._df_pre[orig]
            self._df_pre[new] = self._df_pre[new].astype("Int64")

    def _proc_demo(self):
        """Process df_demo for required input."""
        # Determine inclusion criteria
        self._df_demo["incl_crit2"] = 0
        self._df_demo.loc[
            (68 >= self._df_demo.age) & (self._df_demo.age >= 18), "incl_crit2"
        ] = 1
        self._df_demo["incl_crit3"] = 0
        self._df_demo.loc[
            self._df_demo.years_education >= 12, "incl_crit3"
        ] = 1

    def _make_all(self):
        """Combine dataframes into attr df_all."""
        # Process individual dataframes
        self._proc_als()
        self._proc_bdi()
        self._proc_pre()
        self._proc_demo()

        # Left merge to only host those with GUIDs and
        # account for missing data.
        self._df_all = self._df_demo.merge(
            self._df_bdi, how="left", on="src_subject_id"
        )
        self._df_all = self._df_all.merge(
            self._df_pre, how="left", on="src_subject_id"
        )
        self._df_all = self._df_all.merge(
            self._df_als, how="left", on="src_subject_id"
        )

        # Determine consent precede participation, manage NaNs.
        self._df_all["incl_crit1"] = 888
        self._df_all.loc[
            self._df_all.interview_date <= self._df_all.als_datetime,
            "incl_crit1",
        ] = 1
        self._df_all.loc[
            self._df_all.interview_date > self._df_all.als_datetime,
            "incl_crit1",
        ] = 0

        # Manage NaNs, account for meaningful NANs in excl_crit3
        # and excl_crit3_alt.
        not_excl3 = [x for x in self._df_all.columns if "excl_crit3" not in x]
        self._df_all[not_excl3] = self._df_all[not_excl3].fillna(888)

        # Manage remaining int types (not found in proc_pre)
        # for persnickety validation.
        for col_name in [
            "excl_crit3",
            "excl_crit4",
            "incl_crit2",
            "incl_crit3",
            "incl_crit1",
        ]:
            self._df_all[col_name] = self._df_all[col_name].astype("Int64")

    def make_iec(self):
        """Update df_report with NDAR-required in/exclusion information."""
        # Assemble required data, get NDA sex values
        self._make_all()

        # Setup dataframe, update df_report
        report_dict = {
            "subjectkey": self._df_all["subjectkey"],
            "src_subject_id": self._df_all["src_subject_id"],
            "interview_date": [
                x.strftime("%m/%d/%Y") for x in self._df_all["interview_date"]
            ],
            "interview_age": self._df_all["interview_age"],
            "sex": self._df_all["sex"],
            "exclusion_crit3": self._df_all["excl_crit3"],
            "exclusion_crit3_alt": self._df_all["excl_crit3_alt"],
            "exclusion_crit4": self._df_all["excl_crit4"],
            "exclusion_crit5": self._df_all["excl_crit5"],
            "exclusion_crit6": self._df_all["excl_crit6"],
            "exclusion_crit7": self._df_all["excl_crit7"],
            "exclusion_crit8": self._df_all["excl_crit8"],
            "exclusion_crit9": self._df_all["excl_crit9"],
            "exclusion_crit12": self._df_all["excl_crit12"],
            "inclusion_crit1": self._df_all["incl_crit1"],
            "inclusion_crit2": self._df_all["incl_crit2"],
            "inclusion_crit3": self._df_all["incl_crit3"],
            "inclusion_crit4": self._df_all["incl_crit4"],
        }
        for h_col, h_value in report_dict.items():
            self.df_report[h_col] = h_value


class NdarPanas01(_CleanDemo):
    """Make panas01 report for NDAR submission.

    Inherits _CleanDemo.

    Receives clean study data and finds pilot data in
    proj_dir/data_pilot/ndar_resources.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    proj_dir : str, os.PathLike
        Project's experiment directory
    df_study_day2 : pd.DataFrame
        Study PANAS data from ses-day2
    df_study_day3 : pd.DataFrame
        Study PANAS data from ses-day3

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

    def __init__(
        self,
        df_demo,
        proj_dir,
        df_study_day2,
        df_study_day3,
    ):
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
        super().__init__(df_demo)
        self._proj_dir = proj_dir
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "panas01_template.csv"
        )

        # Get pilot, organize study data for day2, day3
        df_pilot = self._get_pilot()
        self._df_study_day2 = df_study_day2.rename(
            columns={"study_id": "src_subject_id"}
        )
        self._df_study_day3 = df_study_day3.rename(
            columns={"study_id": "src_subject_id"}
        )
        self._df_study_day3.columns = self._df_study_day2.columns.values

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
        # Check sess value, get data
        if sess not in ["day2", "day3"]:
            raise ValueError(f"Incorrect visit day : {sess}")
        df_panas = getattr(self, f"_df_study_{sess}")

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
        df_panas = df_panas.rename(columns=map_item)
        df_panas = self._calc_metrics(df_panas)

        # Add visit
        df_panas["visit"] = sess

        # Combine demo and panas dataframes, get survey age
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]].copy()
        df_panas_demo = pd.merge(df_panas, df_nda, on="src_subject_id")
        df_panas_demo = report_helper.get_survey_age(
            df_panas_demo, self._df_demo
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

    def __init__(self, proj_dir, df_demo):
        """Coordinate report generation for physio data.

        Assumes physio data exists within a BIDS-organized "phys"
        directory, within each participants' session.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory
        df_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        Attributes
        ----------
        nda_label : list
            NDA report template label
        _df_demo : make_reports.build_reports.DemoAll.final_demo
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
        df_demo = df_demo.replace("NaN", np.nan)
        df_demo["sex"] = df_demo["sex"].replace(
            ["Male", "Female", "Neither"], ["M", "F", "O"]
        )
        self._df_demo = df_demo.dropna(subset=["subjectkey"])
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
        df_demo = self._df_demo
        idx_subj = df_demo.index[
            df_demo["src_subject_id"] == subj_nda
        ].tolist()[0]
        subj_guid = df_demo.iloc[idx_subj]["subjectkey"]
        subj_id = df_demo.iloc[idx_subj]["src_subject_id"]
        subj_sex = df_demo.iloc[idx_subj]["sex"]
        subj_dob = datetime.strptime(df_demo.iloc[idx_subj]["dob"], "%Y-%m-%d")

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
        exp_dict = _task_id()
        pilot_list = report_helper.pilot_list()

        # Set local path for upload building
        local_path = os.path.join(_local_path(), "data_phys")

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


class NdarPswq01(_CleanDemo):
    """Make pswq01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    df_pilot : pd.DataFrame
        Pilot PSWQ data
    df_study : pd.DataFrame
        Study PSWQ data

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

    def __init__(self, df_demo, df_pilot, df_study):
        """Read in survey data and make report.

        Get cleaned PSWQ Qualtrics survey from visit_day1, and
        finalized demographic information.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : pswq01 ...")
        # Read in template, concatenate survey data
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "pswq01_template.csv"
        )
        df_pswq = pd.concat([df_pilot, df_study], ignore_index=True)
        df_pswq = df_pswq.replace("NaN", np.nan)
        self._df_pswq = df_pswq[df_pswq["PSWQ_1"].notna()]

        # Make report
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
        self._df_pswq = self._df_pswq.rename(columns=str.lower)
        self._df_pswq.columns = self._df_pswq.columns.str.replace("_", "")
        pswq_cols = [x for x in self._df_pswq.columns if "pswq" in x]
        self._df_pswq[pswq_cols] = self._df_pswq[pswq_cols].astype("Int64")
        self._df_pswq = self._df_pswq.rename(
            columns={"studyid": "src_subject_id"}
        )

        # Calculate sum
        self._df_pswq["pswq_total"] = self._df_pswq[pswq_cols].sum(axis=1)
        self._df_pswq["pswq_total"] = self._df_pswq["pswq_total"].astype(
            "Int64"
        )

        # Combine demographic and erq dataframes
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]]
        df_pswq_nda = pd.merge(self._df_pswq, df_nda, on="src_subject_id")
        df_pswq_nda = report_helper.get_survey_age(df_pswq_nda, self._df_demo)

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


class NdarRest01(_CleanDemo):
    """Make restsurv01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of rest data that complies with NDAR data definitions
    nda_label : list
        NDA report template label
    df_pilot_day2 : pd.DataFrame
        Pilot rest ratings data from ses-day2
    df_study_day2 : pd.DataFrame
        Study rest ratings data from ses-day2
    df_pilot_day3 : pd.DataFrame
        Pilot rest ratings data from ses-day3
    df_study_day3 : pd.DataFrame
        Study rest ratings data from ses-day3

    Methods
    -------
    make_rest(sess: str)
        Generate restsurv01 dataset, builds df_report for one session

    """

    def __init__(
        self,
        df_demo,
        df_pilot_day2,
        df_study_day2,
        df_pilot_day3,
        df_study_day3,
    ):
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
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "restsurv01_template.csv"
        )

        # Get pilot, study data for both day2, day3
        self._get_clean(
            df_pilot_day2, df_study_day2, df_pilot_day3, df_study_day3
        )

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

    def _get_clean(
        self, df_pilot_day2, df_study_day2, df_pilot_day3, df_study_day3
    ):
        """Find and combine cleaned rest rating data.

        Get pilot, study data for day2, day3.

        """
        # Combine pilot and study data, drop resp_alpha rows
        df_rest_day2 = pd.concat(
            [df_pilot_day2, df_study_day2], ignore_index=True
        )
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

        # Repeat for day3
        df_rest_day3 = pd.concat(
            [df_pilot_day3, df_study_day3], ignore_index=True
        )
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
        if sess not in ["day2", "day3"]:
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
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]]
        df_rest_demo = pd.merge(df_rest_remap, df_nda, on="src_subject_id")

        # Calculate age in months of visit, update visit
        df_rest_demo = report_helper.get_survey_age(
            df_rest_demo, self._df_demo
        )
        df_rest_demo["visit"] = sess

        # Build dataframe from nda columns, update with df_bdi_demo data
        df_nda = pd.DataFrame(columns=self._nda_cols, index=df_rest_demo.index)
        df_nda.update(df_rest_demo)
        return df_nda


class NdarRrs01(_CleanDemo):
    """Make rrs01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    proj_dir : str, os.PathLike
        Project's experiment directory
    df_study : pd.DataFrame
        Study RRS data

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

    def __init__(self, df_demo, proj_dir, df_study):
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
        super().__init__(df_demo)
        self._proj_dir = proj_dir
        self._df_study = df_study
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "rrs01_template.csv"
        )

        # Make pilot, study dataframes
        df_pilot = self._get_pilot()
        df_rrs = self.make_rrs()

        # Combine into final report
        df_report = pd.concat([df_pilot, df_rrs], ignore_index=True)
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
        # Rename columns, drop NaN rows
        self._df_study = self._df_study.rename(
            columns={"study_id": "src_subject_id"}
        )
        self._df_study = self._df_study.replace("NaN", np.nan)
        self._df_study = self._df_study[self._df_study["RRS_1"].notna()]

        # Update column names, make data integer
        self._df_study = self._df_study.rename(columns=str.lower)
        rrs_cols = [x for x in self._df_study.columns if "rrs" in x]
        self._df_study[rrs_cols] = self._df_study[rrs_cols].astype("Int64")

        # Calculate sum
        self._df_study["rrs_total"] = self._df_study[rrs_cols].sum(axis=1)
        self._df_study["rrs_total"] = self._df_study["rrs_total"].astype(
            "Int64"
        )

        # Combine demographic and rrs dataframes
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]]
        df_rrs_nda = pd.merge(self._df_study, df_nda, on="src_subject_id")
        df_rrs_nda = report_helper.get_survey_age(df_rrs_nda, self._df_demo)

        # Build dataframe from nda columns, update with demo and rrs data
        df_study_report = pd.DataFrame(
            columns=self._nda_cols, index=df_rrs_nda.index
        )
        df_study_report.update(df_rrs_nda)
        return df_study_report


class NdarStai01(_CleanDemo):
    """Make stai01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    df_pilot_day1 : pd.DataFrame
        Pilot STAI Trait data from visit_day1
    df_study_day1 : pd.DataFrame
        Study STAI Trait data from visit_day1
    df_pilot_day2 : pd.DataFrame
        Pilot STAI State data from ses-day2
    df_study_day2 : pd.DataFrame
        Study STAI State data from ses-day2
    df_pilot_day3 : pd.DataFrame
        Pilot STAI State data from ses-day3
    df_study_day3 : pd.DataFrame
        Study STAI State data from ses-day3

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

    def __init__(
        self,
        df_demo,
        df_pilot_day1,
        df_study_day1,
        df_pilot_day2,
        df_study_day2,
        df_pilot_day3,
        df_study_day3,
    ):
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
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "stai01_template.csv"
        )

        # Generate trait report
        self._df_pilot_day1 = df_pilot_day1
        self._df_study_day1 = df_study_day1
        df_trait = self.make_stai_trait()

        # Generate state reports for day2, day3
        self._df_pilot_day2 = df_pilot_day2
        self._df_study_day2 = df_study_day2
        self._df_pilot_day3 = df_pilot_day3
        self._df_study_day3 = df_study_day3
        df_nda_day2 = self.make_stai_state("day2")
        df_nda_day3 = self.make_stai_state("day3")

        # Combine into final report
        df_report = pd.concat(
            [df_trait, df_nda_day2, df_nda_day3], ignore_index=True
        )
        df_report = df_report.sort_values(by=["src_subject_id", "visit"])
        self.df_report = df_report[df_report["interview_date"].notna()]

    def _clean_df(self, df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        """Clean col names, NaNs, empty col_name rows, and col types."""
        df = df.rename(columns={"study_id": "src_subject_id"})
        df = df.replace("NaN", np.nan)
        df = df[df[col_name].notna()]
        stai_cols = [x for x in df.columns if "STAI" in x]
        df[stai_cols] = df[stai_cols].astype("Int64")
        return df

    def make_stai_trait(self):
        """Combine dataframes to generate trait report.

        Remap columns, calculate totals, and determine survey age.

        Returns
        -------
        pd.DataFrame
            Report of STAI trait data that complies with
            NDAR data definitions

        """
        # Concat dfs
        df_stai_trait = pd.concat(
            [self._df_pilot_day1, self._df_study_day1], ignore_index=True
        )
        df_stai_trait = self._clean_df(df_stai_trait, "STAI_Trait_1")

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
        df_stai_trait = df_stai_trait.rename(columns=map_item)

        # Get trait sum
        trait_cols = [x for x in df_stai_trait.columns if "stai" in x]
        df_stai_trait["staiy_trait"] = df_stai_trait[trait_cols].sum(axis=1)
        df_stai_trait["staiy_trait"] = df_stai_trait["staiy_trait"].astype(
            "Int64"
        )

        # Combine demographic and stai dataframes
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]]
        df_stai_trait_nda = pd.merge(
            df_stai_trait, df_nda, on="src_subject_id"
        )
        df_stai_trait_nda = report_helper.get_survey_age(
            df_stai_trait_nda, self._df_demo
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
        if sess not in ["day2", "day3"]:
            raise ValueError(f"Incorrect visit day : {sess}")

        # Get session data
        df_pilot = getattr(self, f"_df_pilot_{sess}")
        df_study = getattr(self, f"_df_study_{sess}")
        df_stai_state = pd.concat([df_pilot, df_study], ignore_index=True)
        df_stai_state = self._clean_df(df_stai_state, "STAI_State_1")

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
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]]
        df_stai_state_nda = pd.merge(
            df_stai_state_remap, df_nda, on="src_subject_id"
        )
        df_stai_state_nda = report_helper.get_survey_age(
            df_stai_state_nda, self._df_demo
        )

        # Add visit info
        df_stai_state_nda["visit"] = sess

        # Build dataframe from nda columns, update with demo and stai data
        df_nda = pd.DataFrame(
            columns=self._nda_cols, index=df_stai_state_nda.index
        )
        df_nda.update(df_stai_state_nda)
        return df_nda


class NdarSubject01(_CleanDemo):
    """Make ndar_subject01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Attributes
    ----------
    df_report : pd.DataFrame
        Report of demographic data that complies with NDAR data definitions
    nda_label : list
        NDA report template column label

    Methods
    -------
    make_subject()
        Generate demo_info01 dataset, builds df_report

    """

    def __init__(self, df_demo):
        """Trigger report generation."""
        print("Buiding NDA report : ndar_subject01 ...")
        super().__init__(df_demo)
        self.nda_label, nda_cols = report_helper.mine_template(
            "ndar_subject01_template.csv"
        )
        self.df_report = pd.DataFrame(columns=nda_cols)
        self.make_subject()

    def make_subject(self):
        """Update df_report with NDAR-required demographic information."""
        # Manage race response 'other'
        subj_race = list(self._df_demo["race"])
        ethnic_group = []
        for idx, resp in enumerate(subj_race):
            if "Other" in resp:
                subj_race[idx] = "Other"
                ethnic_group.append(resp.split(" - ")[1])
            else:
                ethnic_group.append(np.nan)

        # Make phenotype responses
        self._df_demo["phenotype"] = "Healthy Control"
        self._df_demo["phenotype_description"] = "Healthy Control"

        # Make study type, sample responses
        for col_name in [
            "twins_study",
            "sibling_study",
            "family_study",
            "sample_taken",
        ]:
            self._df_demo[col_name] = "No"

        # Organize values, add to report
        report_dict = {
            "subjectkey": self._df_demo["subjectkey"],
            "src_subject_id": self._df_demo["src_subject_id"],
            "interview_date": [
                x.strftime("%m/%d/%Y") for x in self._df_demo["interview_date"]
            ],
            "interview_age": self._df_demo["interview_age"],
            "sex": self._df_demo["sex"],
            "race": subj_race,
            "ethnic_group": ethnic_group,
            "phenotype": self._df_demo["phenotype"],
            "phenotype_description": self._df_demo["phenotype_description"],
            "twins_study": self._df_demo["twins_study"],
            "sibling_study": self._df_demo["sibling_study"],
            "family_study": self._df_demo["family_study"],
            "sample_taken": self._df_demo["sample_taken"],
        }
        for h_col, h_value in report_dict.items():
            self.df_report[h_col] = h_value


class NdarTas01(_CleanDemo):
    """Make tas01 report for NDAR submission.

    Inherits _CleanDemo.

    Parameters
    ----------
    df_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info
    df_pilot : pd.DataFrame
        Pilot TAS data
    df_study : pd.DataFrame
        Study TAS data

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

    def __init__(self, df_demo, df_pilot, df_study):
        """Read in survey data and make report.

        Get cleaned TAS Qualtrics survey from visit_day1, and
        finalized demographic information.

        Attributes
        ----------
        nda_label : list
            NDA report template label

        """
        print("Buiding NDA report : tas01 ...")
        # Read in template, concat dfs
        super().__init__(df_demo)
        self.nda_label, self._nda_cols = report_helper.mine_template(
            "tas01_template.csv"
        )
        df_tas = pd.concat([df_pilot, df_study], ignore_index=True)

        # Rename columns, drop NaN rows
        df_tas = df_tas.rename(columns={"study_id": "src_subject_id"})
        df_tas = df_tas.replace("NaN", np.nan)
        self._df_tas = df_tas[df_tas["TAS_1"].notna()]

        # Make report
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
        self._df_tas = self._df_tas
        tas_cols = [x for x in self._df_tas.columns if "TAS" in x]
        self._df_tas[tas_cols] = self._df_tas[tas_cols].astype("Int64")
        self._df_tas["tas_totalscore"] = self._df_tas[tas_cols].sum(axis=1)
        self._df_tas["tas_totalscore"] = self._df_tas["tas_totalscore"].astype(
            "Int64"
        )
        self._df_tas.columns = self._df_tas.columns.str.replace("TAS", "tas20")

        # Combine demographic and stai dataframes
        df_nda = self._df_demo[["subjectkey", "src_subject_id", "sex"]]
        df_tas_nda = pd.merge(self._df_tas, df_nda, on="src_subject_id")
        df_tas_nda = report_helper.get_survey_age(df_tas_nda, self._df_demo)

        # Build dataframe from nda columns, update with demo and stai data
        self.df_report = pd.DataFrame(
            columns=self._nda_cols, index=df_tas_nda.index
        )
        self.df_report.update(df_tas_nda)
