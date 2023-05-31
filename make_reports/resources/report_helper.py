"""Supporting functions for making reports.

pull_redcap_data : download survey data from REDCAP
pull_qualtrics_data : download survey data from Qualtrics
mine_template : extract values from NDA templates
calc_age_mo : calculate age-in-months
get_survey_age : add survey age to dataframe
pilot_list : pilot participants
redcap_dict : REDCAP survey mappings
qualtrics_dict : Qualtrics survey mappings
ParticipantComplete : track participant, data completion status

"""
import sys
import io
import requests
import json
import csv
import zipfile
import pandas as pd
import numpy as np
import importlib.resources as pkg_resources
from make_reports import reference_files


def pull_redcap_data(
    redcap_token, report_id, content="report", return_format="csv"
):
    """Pull a RedCap report and make a pandas dataframe.

    Parameters
    ----------
    redcap_token : str
        RedCap API token
    report_id : str, int
        RedCap Report ID
    content : string, optional
        Data export type
    return_format : str, optional
        Data export format, e.g. csv

    Returns
    -------
    pandas.DataFrame

    """
    data = {
        "token": redcap_token,
        "content": content,
        "format": return_format,
        "report_id": report_id,
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "returnFormat": return_format,
    }
    r = requests.post("https://redcap.duke.edu/redcap/api/", data)
    df = pd.read_csv(io.StringIO(r.text), low_memory=False, na_values=None)
    return df


def pull_qualtrics_data(
    survey_name, survey_id, datacenter_id, qualtrics_token, post_labels=False
):
    """Pull a Qualtrics report and make a pandas dataframe.

    References guide at
        https://api.qualtrics.com/ZG9jOjg3NzY3Nw-new-survey-response-export-guide

    Parameters
    ----------
    survey_name : str
        Qualtrics survey name
    survey_id : str
        Qualtrics survey_ID
    data_center_id : str
        Qualtrics datacenter_ID
    qualtrics_token : str
        API token for Qualtrics
    post_labels : bool
        Whether to pull labeled [True] or numeric [False] reports

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    TimeoutError
        If response export progress takes too long

    """
    print(f"Downloading {survey_name} ...")

    # Setting static parameters
    request_check_progress = 0.0
    progress_status = "inProgress"
    url = (
        f"https://{datacenter_id}.qualtrics.com/API/v3/surveys/{survey_id}"
        + "/export-responses/"
    )
    headers = {
        "content-type": "application/json",
        "x-api-token": qualtrics_token,
    }

    # Create data export, submit download request
    data = {"format": "csv"}
    if post_labels:
        data["useLabels"] = True
    download_request_response = requests.request(
        "POST", url, json=data, headers=headers
    )
    try:
        progressId = download_request_response.json()["result"]["progressId"]
    except KeyError:
        print(download_request_response.json())
        sys.exit(2)

    # Check on data export progress, wait until export is ready
    is_file = None
    request_check_url = url + progressId
    while (
        progress_status != "complete"
        and progress_status != "failed"
        and is_file is None
    ):
        # Query status
        request_check_response = requests.request(
            "GET", request_check_url, headers=headers
        )

        # Update is_file when data export is ready
        try:
            is_file = request_check_response.json()["result"]["fileId"]
        except KeyError:
            pass

        # Write data export progress for user, update progress_status
        request_check_progress = request_check_response.json()["result"][
            "percentComplete"
        ]
        print(f"\tDownload is {request_check_progress} complete")
        progress_status = request_check_response.json()["result"]["status"]

    # Check for export error
    if progress_status == "failed":
        raise Exception(
            f"Export of {survey_name} failed, check "
            + "gather_surveys.GetQualtricsSurveys._pull_qualtrics_data"
        )
    file_id = request_check_response.json()["result"]["fileId"]

    # Download requested survey file
    request_download_url = url + file_id + "/file"
    request_download = requests.request(
        "GET", request_download_url, headers=headers, stream=True
    )

    # Extract compressed file
    req_file_zipped = io.BytesIO(request_download.content)
    with zipfile.ZipFile(req_file_zipped) as req_file:
        with req_file.open(f"{survey_name}.csv") as f:
            df = pd.read_csv(f)
    print(f"\n\tSuccessfully downloaded : {survey_name}.csv")
    return df


def mine_template(template_file):
    """Extract column values from NDA template.

    Parameters
    ----------
    template_file : str
        Name of nda template existing in make_reports.reference_files

    Returns
    -------
    tuple
        [0] = list of nda template label e.g. [image, 03]
        [1] = list of nda column names

    """
    with pkg_resources.open_text(reference_files, template_file) as tf:
        reader = csv.reader(tf)
        row_info = [row for idx, row in enumerate(reader)]
    return (row_info[0], row_info[1])


def calc_age_mo(subj_dob, subj_dos):
    """Calculate age in months.

    Convert each participant's age at consent into
    age in months. Account for partial years and months.

    Parameters
    ----------
    subj_dob : list
        Subjects' date-of-birth datetimes
    subj_dos : list
        Subjects' date-of-survey datetimes

    Returns
    -------
    list
        Participant ages in months (int)

    """
    subj_age_mo = []
    for dob, dos in zip(subj_dob, subj_dos):

        # Calculate years, months, and days
        num_years = dos.year - dob.year
        num_months = dos.month - dob.month
        num_days = dos.day - dob.day

        # Adjust for day-month wrap around
        if num_days < 0:
            num_days += 30

        # Avoid including current partial month
        if dos.day < dob.day:
            num_months -= 1

        # Adjust including current partial year
        while num_months < 0:
            num_months += 12
            num_years -= 1

        # Add month if participant is older than num_months
        # plus 15 days.
        if num_days >= 15:
            num_months += 1

        # Convert all to months, add to list
        total_months = (12 * num_years) + num_months
        subj_age_mo.append(total_months)
    return subj_age_mo


def get_survey_age(df_survey, df_demo, subj_col):
    """Add interview_age, interview_date to df_survey.

    interview_age addition will be age-in-months, and
    interview_date will be in format Month/Day/Year.

    Parameters
    ----------
    df_survey : pd.DataFrame
        Contains columns "src_subject_id", "datetime"
    df_demo : pd.DataFrame
        Contains columns "src_subject_id", "dob",
        e.g. make_reports.build_reports.DemoAll.final_demo

    Returns
    -------
    pd.DataFrame

    """
    # Extract survey datetime info
    df_survey["datetime"] = pd.to_datetime(df_survey["datetime"])
    subj_survey = df_survey[subj_col].tolist()
    subj_dos = df_survey["datetime"].tolist()

    # Extract date-of-birth info for participants in survey
    df_demo["dob"] = pd.to_datetime(df_demo["dob"])
    idx_demo = df_demo[
        df_demo["src_subject_id"].isin(subj_survey)
    ].index.tolist()
    subj_dob = df_demo.loc[idx_demo, "dob"].tolist()

    # Check that lists match
    if len(subj_dob) != len(subj_dos):
        raise IndexError("Length of subj DOB does not match subj DOS.")

    # Calculate age-in-months, update dataframe
    subj_age_mo = calc_age_mo(subj_dob, subj_dos)
    df_survey["interview_age"] = subj_age_mo
    df_survey["interview_age"] = df_survey["interview_age"].astype("Int64")
    df_survey["interview_date"] = df_survey["datetime"].dt.strftime("%m/%d/%Y")
    return df_survey


def pilot_list() -> list:
    """Return a list of pilot participants."""
    return ["ER0001", "ER0002", "ER0003", "ER0004", "ER0005"]


def redcap_dict() -> dict:
    """Return a dict of RedCap surveys."""
    # Key : RedCap dataframe, matches reference_files.report_keys_redcap.json
    # Value : output parent directory name
    return {
        "demographics": "redcap_demographics",
        "consent_pilot": "redcap_demographics",
        "consent_v1.22": "redcap_demographics",
        "guid": "redcap_demographics",
        "bdi_day2": "visit_day2",
        "bdi_day3": "visit_day3",
    }


def qualtrics_dict() -> dict:
    """Return a dict of Qualtrics surveys."""
    # Key : Qualtrics dataframe, matches
    #       reference_files.report_keys_qualtrics.json
    # Value : output parent directory identifier
    return {
        "EmoRep_Session_1": "visit_day1",
        "FINAL - EmoRep Stimulus Ratings - fMRI Study": [
            "visit_day2",
            "visit_day3",
        ],
        "Session 2 & 3 Survey": ["visit_day2", "visit_day3"],
    }


class ParticipantComplete:
    """Track participants who change status or have incomplete data.

    Determine visit-specific participants who have been lost-to-follow up
    excluded, or have withdrawn consent. Lost participants yields
    lists of participants, withdrawn or excluded yields dictionaries
    organized by visit and reason.

    Attributes
    ----------
    all : list
        Participants with status change at any point or any incomplete data
    visit1 : list, dict
        Participants with status change after visit 1 or missing visit 1 data
    visit2 : list, dict
        Participants with status change after visit 2 or missing visit 2 data
    visit3 : list, dict
        Participants with status change after visit 3 or missing visit 3 data

    Methods
    -------
    status_change(title)
        [lost | withdrew | excluded]
        Set all, visit attributes for lost, withdrawn, or excluded participants
    incomplete()
        Under development
        Set all, visit attributes for missing data

    Example
    -------
    pc = ParticipantComplete()
    pc.status_change("lost)
    v1_list = pc.visit1
    pc.status_change("excluded")
    v1_dict = pc.visit1
    pc.status_change("withdrew")
    all_list = pc.all

    """

    def status_change(self, title):
        """Participants whose status changed after enrollment.

        Set list (lost) or dict (excluded, withdrew) attributes
        holding which participants did not complete the study
        and the reason why.

        Parameters
        ----------
        title : str
            [lost | excluded | withdrew]
            Type of status change

        Attributes
        ----------
        all : list
            Participants with status change at any point
        visit1 : list, dict
            Participants with status change after visit 1
        visit2 : list, dict
            Participants with status change after visit 2
        visit3 : list, dict
            Participants with status change after visit 3

        """
        if title not in ["lost", "excluded", "withdrew"]:
            raise ValueError(f"Unexpected title value : {title}")

        self._title = title
        self._visit_dicts()
        if title == "lost":
            self.all = self.visit1 + self.visit2 + self.visit3
            self.all.sort()
        else:
            self._build_all()

    def incomplete(self):
        """Participants with incomplete data.

        Attributes
        ----------
        all : list
            Participants missing any data
        visit1 : dict
            Reasons and lists of participants missing visit 1 data
        visit2 : dict
            Reasons and lists of participants missing visit 2 data
        visit3 : dict
            Reasons and lists of participants missing visit 3 data

        """
        self._title = "incomplete"
        self._visit_dicts()
        self._build_all()

    def _build_all(self):
        """Unpack nested dicts to build a list of participants."""
        _all = []
        for visit in [self.visit1, self.visit2, self.visit3]:
            if len(visit) == 0:
                continue
            for reason in visit:
                _all.append(visit[reason])
        self.all = [y for x in _all for y in x]
        self.all.sort()

    def _visit_dicts(self):
        """Set visit1-3 attributes from reference json."""
        _dict = self._load_json()
        self.visit1 = _dict["visit1"]
        self.visit2 = _dict["visit2"]
        self.visit3 = _dict["visit3"]

    def _load_json(self) -> dict:
        """Return dict of participant status change or incomplete."""
        with pkg_resources.open_text(
            reference_files, f"participant_{self._title}.json"
        ) as jf:
            out_dict = json.load(jf)
        return out_dict


class AddStatus(ParticipantComplete):
    """Title.

    Inherits ParticipantComplete

    """

    def enroll_status(self, df, subj_col):
        """Title.

        Parameters
        ----------
        df : pd.DataFrame
            Contains column subj_col with participant IDs
        subj_col : str
            Column name containing subject ID

        Returns
        -------
        pd.DataFrame

        """
        if subj_col not in df.columns:
            raise KeyError(f"Dataframe missing column : {subj_col}")
        self._df = df
        self._subj_col = subj_col
        self._v_list = [1, 2, 3]
        for v_num in self._v_list:
            self._df[f"visit{v_num}_status"] = "enrolled"
            self._df[f"visit{v_num}_reason"] = np.NaN

        # Incomplete before excluded/withdrawn to account for subsequent
        # status changes.
        self._add_lost()
        self._add_ewi("incomplete")
        self._add_ewi("excluded")
        self._add_ewi("withdrew")
        return self._df

    def _add_lost(self):
        """Add lost status to dataframe."""
        self.status_change("lost")
        for v_num in self._v_list:
            subj_list = getattr(self, f"visit{v_num}")
            if not subj_list:
                continue
            idx_subj = self._subj_idx(subj_list)
            self._df.loc[idx_subj, f"visit{v_num}_status"] = "lost"
            self._clear_status(v_num, idx_subj)

    def _subj_idx(self, subj_list) -> list:
        """Return indices of subejcts."""
        return self._df.index[
            self._df[self._subj_col].isin(subj_list)
        ].to_list()

    def _clear_status(self, v_num: int, idx_subj: list):
        """Title."""
        while v_num < 3:
            v_num += 1
            self._df.loc[idx_subj, f"visit{v_num}_status"] = np.NaN

    def _add_ewi(self, stat: str):
        """Add excluded, withdrawn, incomplete statuses to dataframe."""
        if stat == "incomplete":
            self.incomplete()
        else:
            self.status_change(stat)

        for v_num in self._v_list:
            subj_dict = getattr(self, f"visit{v_num}")
            if not subj_dict:
                continue
            for reas, subj_list in subj_dict.items():
                idx_subj = self._subj_idx(subj_list)
                self._df.loc[idx_subj, f"visit{v_num}_status"] = stat
                self._df.loc[idx_subj, f"visit{v_num}_reason"] = reas
                if stat == "incomplete":
                    continue
                self._clear_status(v_num, idx_subj)
