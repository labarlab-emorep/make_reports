"""Report-agnostic supporting methods."""
import io
import requests
import csv
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
