"""Setup workflows for specific types of reports."""
import os
import glob
from datetime import datetime
import pandas as pd
from make_reports import survey_download, survey_clean
from make_reports import build_reports, report_helper


def download_surveys(
    proj_dir,
    redcap_token=None,
    qualtrics_token=None,
    get_redcap=False,
    get_qualtrics=False,
):
    """Coordinate survey download resources.

    Parameters
    ----------
    proj_dir : path
        Location of parent directory for project
    redcap_token : str
        API token for RedCap
    qualtrics_token : str
        API token for Qualtrics
    get_redcap : bool
        Whether to download RedCap surveys
    get_qualtrics : bool
        Whether to download Qualtrics surveys

    Returns
    -------
    None

    """
    print("\nStarting survey download ...")
    if get_redcap:
        _ = survey_download.download_redcap(proj_dir, redcap_token)
    if get_qualtrics:
        _ = survey_download.download_qualtrics(proj_dir, qualtrics_token)
    print("\nDone with survey download!")


class CleanSurveys:
    """Coordinate cleaning of survey data.

    Methods
    -------
    clean_redcap
        Clean all RedCap surveys
    clean_rest
        Clean ratings of resting state experiences
    clean_qualtrics
        Clean all Qualtrics surveys

    """

    def __init__(self, proj_dir):
        """Set helping attributes.

        Parameters
        ----------
        proj_dir : path
            Project's experiment directory

        Attributes
        ----------
        proj_dir : path
            Project's experiment directory

        """
        self.proj_dir = proj_dir

    def clean_redcap(self):
        """Coordinate cleaning of RedCap surveys.

        Clean each survey specified in report_helper.redcap_dict and
        write out the cleaned dataframe.

        Raises
        ------
        FileNotFoundError
            Unexpected number of files in:
                <proj_dir>/data_survey/redcap_demographics/data_raw

        """
        # Check for data
        redcap_raw = glob.glob(
            f"{self.proj_dir}/data_survey/redcap*/data_raw/*latest.csv"
        )
        if len(redcap_raw) != 4:
            raise FileNotFoundError(
                "Missing raw survey data in redcap directories,"
                + " please download raw data via rep_dl."
            )

        # Get cleaning obj
        redcap_dict = report_helper.redcap_dict()
        clean_redcap = survey_clean.CleanRedcap(self.proj_dir)

        # Clean each planned survey, write out
        for sur_name, dir_name in redcap_dict.items():
            clean_redcap.clean_surveys(sur_name)

            # Write study data
            clean_file = os.path.join(
                self.proj_dir,
                "data_survey",
                dir_name,
                "data_clean",
                f"df_{sur_name}.csv",
            )
            out_dir = os.path.dirname(clean_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            clean_redcap.df_clean.to_csv(clean_file, index=False, na_rep="")

            # Write pilot data
            pilot_file = os.path.join(
                self.proj_dir,
                "data_pilot/data_survey",
                dir_name,
                "data_clean",
                f"df_{sur_name}.csv",
            )
            out_dir = os.path.dirname(pilot_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            clean_redcap.df_pilot.to_csv(pilot_file, index=False, na_rep="")

    def clean_qualtrics(self):
        """Coordinate cleaning of Qualtrics surveys.

        Clean each survey specified in report_helper.qualtrics_dict and
        write out the cleaned dataframe.

        Raises
        ------
        FileNotFoundError
            Unexpected number of files in:
                <proj_dir>/data_survey/visit_day*/data_raw

        """

        def _write_clean_qualtrics(clean_dict, pilot_dict, dir_name):
            """Write cleaned dataframes for RedCap surveys."""
            # Unpack study clean data
            for h_name, h_df in clean_dict.items():
                out_file = os.path.join(
                    self.proj_dir,
                    "data_survey",
                    dir_name,
                    "data_clean",
                    f"df_{h_name}.csv",
                )
                out_dir = os.path.dirname(out_file)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                print(f"\tWriting : {out_file}")
                h_df.to_csv(out_file, index=False, na_rep="")

            # Unpack pilot clean data
            for h_name, h_df in pilot_dict.items():
                out_file = os.path.join(
                    self.proj_dir,
                    "data_pilot/data_survey",
                    dir_name,
                    "data_clean",
                    f"df_{h_name}.csv",
                )
                out_dir = os.path.dirname(out_file)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                print(f"\tWriting : {out_file}")
                h_df.to_csv(out_file, index=False, na_rep="")

        # Check for data
        visit_raw = glob.glob(
            f"{self.proj_dir}/data_survey/visit*/data_raw/*latest.csv"
        )
        if len(visit_raw) != 7:
            raise FileNotFoundError(
                "Missing raw survey data in visit directories,"
                + " please download raw data via rep_dl."
            )

        # Get cleaning obj
        qualtrics_dict = report_helper.qualtrics_dict()
        clean_qualtrics = survey_clean.CleanQualtrics(self.proj_dir)

        # Clean each planned survey and write out
        for sur_name, dir_name in qualtrics_dict.items():
            clean_qualtrics.clean_surveys(sur_name)

            # Account for visit type, survey name/report organization
            if type(dir_name) == list:
                for vis_name in dir_name:
                    _write_clean_qualtrics(
                        clean_qualtrics.data_clean[vis_name],
                        clean_qualtrics.data_pilot[vis_name],
                        vis_name,
                    )
            elif dir_name == "visit_day1":
                _write_clean_qualtrics(
                    clean_qualtrics.data_clean,
                    clean_qualtrics.data_pilot,
                    dir_name,
                )

    def clean_rest(self):
        """Coordinate cleaning of rest ratings survey.

        Raises
        ------
        FileNotFoundError
            *rest-ratings*.tsv are not found in BIDS location

        """
        print("Cleaning survey : rest ratings")

        # Check for data
        raw_path = os.path.join(
            self.proj_dir,
            "data_scanner_BIDS",
            "rawdata",
        )
        rest_list = glob.glob(f"{raw_path}/sub-*/ses-*/beh/*rest-ratings*.tsv")
        if not rest_list:
            raise FileNotFoundError(
                "Missing rest ratings, try running dcm_conversion."
            )

        # Aggregate rest ratings, for each session day
        agg_rest = survey_clean.CombineRestRatings(self.proj_dir)
        for day in ["day2", "day3"]:

            # Get, write out study data
            agg_rest.get_rest_ratings(day, raw_path)
            out_file = os.path.join(
                self.proj_dir,
                "data_survey",
                f"visit_{day}",
                "data_clean",
                "df_rest-ratings.csv",
            )
            out_dir = os.path.dirname(out_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            print(f"\tWriting : {out_file}")
            agg_rest.df_sess.to_csv(out_file, index=False, na_rep="")

            # Get, write out pilot data
            rawdata_pilot = os.path.join(
                self.proj_dir, "data_pilot/data_scanner_BIDS", "rawdata"
            )
            agg_rest.get_rest_ratings(day, rawdata_pilot)
            out_file = os.path.join(
                self.proj_dir,
                "data_pilot/data_survey",
                f"visit_{day}",
                "data_clean",
                "df_rest-ratings.csv",
            )
            out_dir = os.path.dirname(out_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            print(f"\tWriting : {out_file}")
            agg_rest.df_sess.to_csv(out_file, index=False, na_rep="")


def make_manager_reports(manager_reports, query_date, proj_dir):
    """Make reports for the lab manager.

    Coordinate the use of build_reports.ManagerRegular to generate
    desired nih12, nih4, or duke3 report.

    Parameters
    ----------
    manager_reports : list
        Desired report names e.g. ["nih4", "nih12"]
    query_date : str, datetime
        Date for finding report range
    proj_dir : path
        Project's experiment directory

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If redcap api token not supplied
        If report requested is not found in valid_mr_args
        If query_date occures before 2022-03-31

    Notes
    -----
    Reports are written to:
        <proj_dir>/documents/manager_reports

    """
    # Check for clean RedCap data, generate if needed
    redcap_clean = glob.glob(
        f"{proj_dir}/data_survey/redcap_demographics/data_clean/*.csv"
    )
    if len(redcap_clean) != 4:
        print("No clean data found in RedCap, cleaning ...")
        cl_data = CleanSurveys(proj_dir)
        cl_data.clean_redcap()
        print("\tDone.")

    # Validate manager_reports arguments
    valid_mr_args = ["nih12", "nih4", "duke3"]
    for report in manager_reports:
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
    manager_dir = os.path.join(proj_dir, "documents/manager_reports")
    if not os.path.exists(manager_dir):
        os.makedirs(manager_dir)

    # Query RedCap demographic info
    redcap_demo = build_reports.DemoAll(proj_dir)

    # Generate reports
    for report in manager_reports:
        mr = build_reports.ManagerRegular(
            query_date, redcap_demo.final_demo, report
        )

        # Setup file name, write csv
        start_date = mr.range_start.strftime("%Y-%m-%d")
        end_date = mr.range_end.strftime("%Y-%m-%d")
        out_file = os.path.join(
            manager_dir, f"report_{report}_{start_date}_{end_date}.csv"
        )
        print(f"\tWriting : {out_file}")
        mr.df_report.to_csv(out_file, index=False, na_rep="")
        del mr


def make_nda_reports(nda_reports, proj_dir):
    """Make reports and organize data for NDAR upload.

    Generate requested NDAR reports and organize data (if required) for the
    biannual upload.

    Parameters
    ----------
    nda_reports : list
        Names of desired NDA reports e.g. ["demo_info01", "affim01"]
    proj_dir : path
        Project's experiment directory
    post_labels : bool
        Whether to use labels in post_scan_ratings pull
    qualtrics_token : str
        Qualtrics API token
    redcap_token : str
        RedCap API token

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If both redcap and qualtrics api tokens are not provided
        If report name requested is not found in nda_switch

    Notes
    -----
    Reports are written to:
        <proj_dir>/ndar_upload/reports

    """
    # Check for clean RedCap/visit data, generate if needed
    redcap_clean = glob.glob(
        f"{proj_dir}/data_survey/redcap_demographics/data_clean/*.csv"
    )
    visit_clean = glob.glob(f"{proj_dir}/data_survey/visit*/data_clean/*.csv")
    if len(redcap_clean) != 4 or len(visit_clean) != 17:
        print("Missing RedCap, Qualtrics clean data. Cleaning ...")
        cl_data = CleanSurveys(proj_dir)
        cl_data.clean_redcap()
        cl_data.clean_qualtrics()
        print("\tDone.")

    # Set switch to find appropriate class: key = user-specified
    # report name, value = relevant class.
    mod_build = "make_reports.build_reports"
    nda_switch = {
        "demo_info01": f"{mod_build}.NdarDemoInfo01",
        "affim01": f"{mod_build}.NdarAffim01",
        "als01": f"{mod_build}.NdarAls01",
        "bdi01": f"{mod_build}.NdarBdi01",
        "brd01": f"{mod_build}.NdarBrd01",
        "emrq01": f"{mod_build}.NdarEmrq01",
        "image03": f"{mod_build}.NdarImage03",
        "panas01": f"{mod_build}.NdarPanas01",
        "psychophys_subj_exp01": f"{mod_build}.NdarPhysio",
        "pswq01": f"{mod_build}.NdarPswq01",
        "restsurv01": f"{mod_build}.NdarRest01",
        "rrs01": f"{mod_build}.NdarRrs01",
        "stai01": f"{mod_build}.NdarStai01",
        "tas01": f"{mod_build}.NdarTas01",
    }

    # Validate nda_reports arguments
    for report in nda_reports:
        if report not in nda_switch.keys():
            raise ValueError(
                f"Inappropriate --nda-reports argument : {report}"
            )

    # Setup output directories
    report_dir = os.path.join(proj_dir, "ndar_upload/reports")
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    # Get redcap demo info, use only consented data
    redcap_demo = build_reports.DemoAll(proj_dir)
    redcap_demo.remove_withdrawn()

    # Ignore loc warning
    pd.options.mode.chained_assignment = None

    # Make requested reports
    for report in nda_reports:

        # Get appropriate class
        h_pkg, h_mod, h_class = nda_switch[report].split(".")
        mod = __import__(f"{h_pkg}.{h_mod}", fromlist=[h_class])
        rep_class = getattr(mod, h_class)

        # Generate report
        rep_obj = rep_class(proj_dir, redcap_demo.final_demo)

        # Write out report
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

    pd.options.mode.chained_assignment = "warn"


def generate_guids(proj_dir, user_name, user_pass, find_mismatch):
    """Compile needed demographic info and make GUIDs.

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

    """
    # Check for clean RedCap data, generate if needed
    chk_demo = os.path.join(
        proj_dir,
        "data_survey/redcap_demographics/data_clean",
        "df_demographics.csv",
    )
    if not os.path.exists(chk_demo):
        print("Missing clean RedCap demographics, cleaning ...")
        cl_data = CleanSurveys(proj_dir)
        cl_data.clean_redcap()
        print("\tDone.")

    # Trigger build reports class and method, clean intermediate
    guid_obj = build_reports.GenerateGuids(proj_dir, user_pass, user_name)
    guid_obj.make_guids()
    os.remove(guid_obj.df_guid_file)

    if find_mismatch:
        guid_obj.check_guids()
        if guid_obj.mismatch_list:
            print(f"Mismatching GUIDs :\n\t{guid_obj.mismatch_list}")
        else:
            print("No mismatches found!")


def calc_metrics(proj_dir):
    """Title.

    Desc.

    """
    # Check for clean RedCap/visit data, generate if needed
    redcap_clean = glob.glob(
        f"{proj_dir}/data_survey/redcap_demographics/data_clean/*.csv"
    )
    visit_clean = glob.glob(f"{proj_dir}/data_survey/visit*/data_clean/*.csv")
    if len(redcap_clean) != 4 or len(visit_clean) != 17:
        print("Missing RedCap, Qualtrics clean data. Cleaning ...")
        cl_data = CleanSurveys(proj_dir)
        cl_data.clean_redcap()
        cl_data.clean_qualtrics()
        print("\tDone.")

    # Get redcap demo info, use only consented data
    redcap_demo = build_reports.DemoAll(proj_dir)
    redcap_demo.remove_withdrawn()
