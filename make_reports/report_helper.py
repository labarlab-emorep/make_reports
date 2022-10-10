"""Report-agnostic supporting methods."""
import sys
import io
import requests
import csv
import zipfile
import pandas as pd
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
    content : string
        Data export type
    return_format : str
        Data export format

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
    survey_name, survey_id, datacenter_id, qualtrics_token, post_labels
):
    """Pull a Qualtrics report and make a pandas dataframe.

    References guide at
        https://api.qualtrics.com/ZG9jOjg3NzY3Nw-new-survey-response-export-guide

    Parameters
    ----------
    survey_name : str
        Qualtrics survey name




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
    """Extract row values from NDA template.

    Parameters
    ----------
    template_file : str
        Name of nda template

    Returns
    -------
    tuple
        [0] = list of nda template label e.g. [image, 03]
        [1] = list of nda column names
    """
    with pkg_resources.open_text(reference_files, template_file) as tf:
        reader = csv.reader(tf)
        row_info = [row for idx, row in enumerate(reader)]
        # label_nda = [row for idx, row in enumerate(reader) if idx == 0][0]
        # cols_nda = [row for idx, row in enumerate(reader) if idx == 1][0]
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


def pilot_list():
    """Title.

    Desc.

    """
    return ["ER0001", "ER0002", "ER0003", "ER0004", "ER0005"]


def redcap_dict():
    """Title.

    Desc.

    """
    return {
        "demographics": "redcap_demographics",
        "consent_orig": "redcap_demographics",
        "consent_new": "redcap_demographics",
        "guid": "redcap_demographics",
        "bdi_day2": "visit_day2",
        "bdi_day3": "visit_day3",
    }


def qualtrics_dict():
    """Title.

    Desc.

    """
    return {
        "EmoRep_Session_1": "visit_day1",
        "FINAL - EmoRep Stimulus Ratings - fMRI Study": "post_scan_ratings",
        "Session 2 & 3 Survey": "visit_day23",
    }
