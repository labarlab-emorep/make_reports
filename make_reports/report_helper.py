"""Report-agnostic supporting methods."""
import io
import requests
import csv
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


def give_ndar_demo(final_demo):
    """Title.

    Desc.

    Parameters
    ----------
    make_reports.gather_surveys.GetRedcapDemographic.final_demo

    Returns
    -------
    pd.DataFrame

    """
    final_demo = final_demo.replace("NaN", np.nan)
    final_demo["sex"] = final_demo["sex"].replace(
        ["Male", "Female", "Neither"], ["M", "F", "O"]
    )
    final_demo = final_demo.dropna(subset=["subjectkey"])
    final_demo["interview_date"] = pd.to_datetime(final_demo["interview_date"])
    final_demo["interview_date"] = final_demo["interview_date"].dt.strftime(
        "%m/%d/%Y"
    )
    return final_demo.iloc[:, 0:5]
