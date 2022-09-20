"""Report-agnostic methods."""
# %%
import io
import requests
import csv
import zipfile
import pandas as pd
import importlib.resources as pkg_resources
from nda_upload import reference_files


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


# %%
def pull_qualtrics_data(
    qualtrics_token,
    report_id,
    organization_id,
    datacenter_id,
    survey_name,
):
    """Pull a Qualtrics report and make a pandas dataframe.

    Parameters
    ----------
    qualtrics_token : str
        Qualtrics API token
    report_id : str
        Qualtrics survey ID
    organization_id : str
        Qualtrics organization ID
    datacenter_id : str
        Qualtrics datacenter ID
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
    # Create response export
    print(f"Pulling Qualtrics report : '{survey_name}.csv'")
    base_url = (
        f"https://{organization_id}.{datacenter_id}.qualtrics.com"
        + "/API/v3/responseexports/"
    )
    headers = {
        "content-type": "application/json",
        "x-api-token": qualtrics_token,
    }
    req_payload = f"""{{"format": "csv", "surveyId": "{report_id}"}}"""
    req_response = requests.request(
        "POST",
        base_url,
        data=req_payload,
        headers=headers,
    )
    progress_id = req_response.json()["result"]["id"]
    # print(req_response.text)

    # Get response export progress
    req_progress = 0
    stat_progress = None
    while req_progress < 100 and stat_progress != "complete":
        req_check_url = base_url + progress_id
        req_check_resp = requests.request(
            "GET", req_check_url, headers=headers
        )
        req_progress = req_check_resp.json()["result"]["percentComplete"]
        stat_progress = req_check_resp.json()["result"]["status"]

    if stat_progress != "complete":
        raise TimeoutError(
            f"Pulling Qualtrics report failed for '{survey_name}.csv'."
        )

    # Get response export file and unzip
    req_download_url = f"{base_url}{progress_id}/file"
    req_download = requests.request(
        "GET", req_download_url, headers=headers, stream=True
    )
    req_file_zipped = io.BytesIO(req_download.content)
    with zipfile.ZipFile(req_file_zipped) as req_file:
        with req_file.open(f"{survey_name}.csv") as f:
            df = pd.read_csv(f)

    return df


# %%
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
    # # For testing
    # template_file = "demo_info01_template.csv"
    with pkg_resources.open_text(reference_files, template_file) as tf:
        reader = csv.reader(tf)
        row_info = [row for idx, row in enumerate(reader)]
        # label_nda = [row for idx, row in enumerate(reader) if idx == 0][0]
        # cols_nda = [row for idx, row in enumerate(reader) if idx == 1][0]
    return (row_info[0], row_info[1])
