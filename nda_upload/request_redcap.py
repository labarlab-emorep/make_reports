"""Title.

Desc.
"""
import io
import requests
import pandas as pd


def pull_data(api_token, report_id, content="report", return_format="csv"):
    """Title.

    Desc.
    """
    data = {
        "token": api_token,
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
