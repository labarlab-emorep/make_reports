"""Methods for generating reports required by the NIH or Duke.

make_regular_reports : Generate reports submited to NIH or Duke
make_ndar_reports : Generate reports, data submitted to NIH Data
                        Archive (NDAR)
gen_guids : generate or check GUIDs

"""
# %%
import os
import glob
from datetime import datetime
import pandas as pd
from make_reports.resources import build_reports
from make_reports.resources import manage_data


# %%
def make_regular_reports(regular_reports, query_date, proj_dir):
    """Make reports for the lab manager.

    Coordinate the use of build_reports.ManagerRegular to generate
    desired nih12, nih4, or duke3 report.

    Reports are written to:
        <proj_dir>/documents/regular_reports

    Parameters
    ----------
    regular_reports : list
        Desired report names e.g. ["nih4", "nih12"]
    query_date : str, datetime
        Date for finding report range
    proj_dir : path
        Project's experiment directory

    Raises
    ------
    ValueError
        redcap api token not supplied
        report requested is not found in valid_mr_args
        query_date occures before 2022-03-31

    """
    # Check for clean RedCap data, generate if needed
    redcap_clean = glob.glob(
        f"{proj_dir}/data_survey/redcap_demographics/data_clean/*.csv"
    )
    if len(redcap_clean) != 4:
        print("No clean data found in RedCap, cleaning ...")
        cl_data = manage_data.CleanSurveys(proj_dir)
        cl_data.clean_redcap()
        print("\tDone.")

    # Validate regular_reports arguments
    valid_mr_args = ["nih12", "nih4", "duke3"]
    for report in regular_reports:
        if report not in valid_mr_args:
            raise ValueError(
                "--manager-reports contained inappropriate "
                + f"argument : {report}"
            )

    # Validate query date
    if isinstance(query_date, str):
        query_date = datetime.strptime(query_date, "%Y-%m-%d").date()
    if query_date < datetime.strptime("2022-03-31", "%Y-%m-%d").date():
        raise ValueError(f"Query date {query_date} precedes 2022-03-31.")

    # Setup output location
    manager_dir = os.path.join(proj_dir, "documents/regular_reports")
    if not os.path.exists(manager_dir):
        os.makedirs(manager_dir)

    # Query RedCap demographic info
    redcap_demo = build_reports.DemoAll(proj_dir)

    # Generate reports
    for report in regular_reports:
        mr = build_reports.ManagerRegular(
            query_date, redcap_demo.final_demo, report
        )

        # Setup file name, write csv
        start_date = mr.range_start.strftime("%Y-%m-%d")
        end_date = mr.range_end.strftime("%Y-%m-%d")
        out_file = os.path.join(
            manager_dir, f"report_{report}_{start_date}_{end_date}.csv"
        )
        if isinstance(mr.df_report, pd.DataFrame):
            print(f"\tWriting : {out_file}")
            mr.df_report.to_csv(out_file, index=False, na_rep="")
        del mr


def make_ndar_reports(ndar_reports, proj_dir, close_date):
    """Make reports and organize data for NDAR upload.

    Generate requested NDAR reports and organize data (if required) for the
    biannual upload.

    Reports are written to:
        <proj_dir>/ndar_upload/cycle_<close_date>

    Parameters
    ----------
    ndar_reports : list
        Names of desired NDA reports e.g. ["demo_info01", "affim01"]
    proj_dir : path
        Project's experiment directory
    close_date : datetime
        Submission cycle close date

    Raises
    ------
    ValueError
        If both redcap and qualtrics api tokens are not provided
        If report name requested is not found in nda_switch

    """
    # Check for clean RedCap/visit data, generate if needed
    redcap_clean = glob.glob(
        f"{proj_dir}/data_survey/redcap_demographics/data_clean/*.csv"
    )
    visit_clean = glob.glob(f"{proj_dir}/data_survey/visit*/data_clean/*.csv")
    if len(redcap_clean) != 4 or len(visit_clean) != 17:
        print("Missing RedCap, Qualtrics clean data. Cleaning ...")
        cl_data = manage_data.CleanSurveys(proj_dir)
        cl_data.clean_redcap()
        cl_data.clean_qualtrics()
        print("\tDone.")

    # Set switch to find appropriate class in
    # make_reports.resources.build_ndar:
    #   key = user-specified report name
    #   value = relevant class
    nda_switch = {
        "demo_info01": "NdarDemoInfo01",
        "affim01": "NdarAffim01",
        "als01": "NdarAls01",
        "bdi01": "NdarBdi01",
        "brd01": "NdarBrd01",
        "emrq01": "NdarEmrq01",
        "image03": "NdarImage03",
        "panas01": "NdarPanas01",
        "pswq01": "NdarPswq01",
        "restsurv01": "NdarRest01",
        "rrs01": "NdarRrs01",
        "stai01": "NdarStai01",
        "tas01": "NdarTas01",
    }

    # Validate ndar_reports arguments
    for report in ndar_reports:
        if report not in nda_switch.keys():
            raise ValueError(
                f"Inappropriate --ndar-reports argument : {report}"
            )

    # Setup output directories
    report_dir = os.path.join(
        proj_dir,
        "ndar_upload",
        f"cycle_{close_date.strftime('%Y-%m-%d')}",
    )
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    # Get redcap demo info, use only consented data in submission cycle
    redcap_demo = build_reports.DemoAll(proj_dir)
    redcap_demo.remove_withdrawn()
    redcap_demo.submission_cycle(close_date)

    # Make requested reports
    for report in ndar_reports:

        # Get appropriate class for report
        mod = __import__(
            "make_reports.resources.build_ndar", fromlist=[nda_switch[report]]
        )
        rep_class = getattr(mod, nda_switch[report])

        # Generate, write report
        rep_obj = rep_class(proj_dir, redcap_demo.final_demo)
        out_file = os.path.join(report_dir, f"{report}_dataset.csv")
        print(f"\tWriting : {out_file}")
        rep_obj.df_report.to_csv(out_file, index=False, na_rep="")

        # Prepend header
        dummy_file = f"{out_file}.bak"
        with open(out_file, "r") as read_obj, open(
            dummy_file, "w"
        ) as write_obj:
            write_obj.write(f"{','.join(rep_obj.nda_label)}\n")
            for line in read_obj:
                write_obj.write(line)
        os.remove(out_file)
        os.rename(dummy_file, out_file)
        del rep_obj


def generate_guids(
    proj_dir, user_name, user_pass, find_mismatch, redcap_token
):
    """Compile needed demographic info and make GUIDs.

    Also supports checking newly generated GUIDs against those entered
    into RedCap to help detect clerical errors.

    Generated GUIDs are written to:
        <proj_dir>/data_survey/redcap_demographics/data_clean/output_guid_*.txt

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    user_name : str
        NDA user name
    user_pass : str
        NDA user password
    find_mismatch : bool
        Whether to check for mismatches between REDCap
        and generated GUIDs
    redcap_token : str
        Personal access token for RedCap

    """
    # Trigger build reports class and method, clean intermediate
    guid_obj = build_reports.GenerateGuids(
        proj_dir, user_pass, user_name, redcap_token
    )
    guid_obj.make_guids()
    os.remove(guid_obj.df_guid_file)

    if find_mismatch:
        guid_obj.check_guids()
        if guid_obj.mismatch_list:
            print(f"Mismatching GUIDs :\n\t{guid_obj.mismatch_list}")
        else:
            print("No mismatches found!")
