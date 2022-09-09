"""Title.

Desc.
"""
# %%
from datetime import datetime, date
from nda_upload import reference_files


def _find_start_end(range_list, query_date):
    """Title.

    Desc.
    """
    start_end = None
    for h_ranges in range_list:
        h_start = datetime.strptime(h_ranges[0], "%Y-%m-%d").date()
        h_end = datetime.strptime(h_ranges[1], "%Y-%m-%d").date()
        if h_start <= query_date <= h_end:
            start_end = (h_start, h_end)
            break
    if not start_end:
        raise ValueError(f"Date range not found for {query_date}.")
    return start_end


def _get_data_range(range_list, query_date, final_demo, start_date=None):
    """Title.

    Desc.
    """
    # find data within range
    h_start, range_end = _find_start_end(range_list, query_date)
    range_start = start_date if start_date else h_start
    range_msg = (
        f"{range_start.strftime('%Y-%m-%d')} through "
        + f"{range_end.strftime('%Y-%m-%d')}"
    )
    print(f"Finding data for range {range_msg}")
    range_bool = (final_demo["interview_date"] >= range_start) & (
        final_demo["interview_date"] <= range_end
    )
    df_range = final_demo.loc[range_bool]
    if df_range.empty:
        print(f"No data collected for specified range of {range_msg}")
    return (df_range, range_start, range_end)


# %%
def nih_4mo(final_demo, query_date):
    """Title.

    Desc.
    """
    # For testing
    query_date = datetime.strptime("2022-07-31", "%Y-%m-%d").date()

    #
    mturk_nums = {
        "minority": 122,
        "hispanic": 67,
        "total": 659,
    }

    #
    nih_4mo_ranges = [
        ("2020-12-01", "2021-03-31"),
        ("2021-04-01", "2021-07-31"),
        ("2021-08-01", "2021-11-30"),
        ("2021-12-01", "2022-03-31"),
        ("2022-04-01", "2022-07-31"),
        ("2022-08-01", "2022-11-30"),
        ("2022-12-01", "2023-03-31"),
        ("2023-04-01", "2023-07-31"),
        ("2023-08-01", "2023-11-30"),
        ("2023-12-01", "2024-03-31"),
        ("2024-04-01", "2024-07-31"),
        ("2024-08-01", "2024-11-30"),
        ("2024-12-01", "2025-03-31"),
        ("2025-04-01", "2025-07-31"),
        ("2025-08-01", "2025-11-30"),
    ]

    # find data within range
    proj_start = datetime.strptime("2020-06-30", "%Y-%m-%d").date()
    df_range, range_start, range_end = _get_data_range(
        nih_4mo_ranges, query_date, final_demo, start_date=proj_start
    )

    # num minority, num hispanic, num total
    num_minority = len(df_range.index[df_range["is_minority"] == "Minority"])
    num_hispanic = len(
        df_range.index[df_range["ethnicity"] == "Hispanic or Latino"]
    )
    num_total = len(df_range.index)

    # update with mturk
    num_minority += mturk_nums["minority"]
    num_hispanic += mturk_nums["hispanic"]
    num_total += mturk_nums["total"]
    return (num_minority, num_hispanic, num_total, range_start, range_end)


def duke_3mo(final_demo, query_date):
    """Title.

    Desc.
    """
    # For testing
    # query_date = date.today()
    # query_date = datetime.strptime("2022-06-06", "%Y-%m-%d").date()

    # Setup date ranges for report
    duke_3mo_ranges = [
        ("2020-11-01", "2020-12-31"),
        ("2021-01-01", "2021-03-31"),
        ("2021-04-01", "2021-06-30"),
        ("2021-07-01", "2021-09-30"),
        ("2021-10-01", "2021-12-31"),
        ("2022-01-01", "2022-03-31"),
        ("2022-04-01", "2022-06-30"),
        ("2022-07-01", "2022-09-30"),
        ("2022-10-01", "2022-12-31"),
        ("2023-01-01", "2023-03-31"),
        ("2023-04-01", "2023-06-30"),
        ("2023-07-01", "2023-09-30"),
        ("2023-10-01", "2023-12-31"),
        ("2024-01-01", "2024-03-31"),
        ("2024-04-01", "2024-06-30"),
        ("2024-07-01", "2024-09-30"),
        ("2024-10-01", "2024-12-31"),
        ("2025-01-01", "2025-03-31"),
        ("2025-04-01", "2025-06-30"),
        ("2025-07-01", "2025-09-30"),
        ("2025-10-01", "2025-12-31"),
    ]

    # find data within range
    df_range, range_start, range_end = _get_data_range(
        duke_3mo_ranges, query_date, final_demo
    )

    # TODO Calculate sex * ethnic * race numbers


# %%
def nih_annual(final_demo, query_date):
    """Title.

    Desc.
    """
    # For testing
    # query_date = datetime.strptime("2022-06-06", "%Y-%m-%d").date()

    nih_annual_ranges = [
        ("2020-04-01", "2020-03-31"),
        ("2021-04-01", "2022-03-31"),
        ("2022-04-01", "2023-03-31"),
        ("2023-04-01", "2024-03-31"),
        ("2024-04-01", "2025-03-31"),
        ("2025-04-01", "2026-03-31"),
    ]
    df_range, range_start, range_end = _get_data_range(
        nih_annual_ranges, query_date, final_demo
    )

    cols_desired = [
        "src_subject_id",
        "ethnicity",
        "race",
        "sex",
        "years_education",
        "age",
    ]
    df_report = df_range[cols_desired]
    df_report["age_unit"] = "Years"
    return (df_report, range_start, range_end)
