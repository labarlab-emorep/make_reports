"""Methods for generating reports required by the NIH or Duke.

make_regular_reports : Generate reports submited to NIH or Duke
MakeNdarReports : Generate reports, data submitted to NIH Data
                    Archive (NDAR)
gen_guids : generate or check GUIDs

"""

# %%
import os
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
    proj_dir : str, os.PathLike
        Project's experiment directory

    Raises
    ------
    ValueError
        - invalid requested report name
        - query_date occures before 2022-03-31

    """
    # Validate regular_reports arguments
    valid_mr_args = ["nih12", "nih4", "duke3", "duke12"]
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

    # Generate reports
    make_rep = build_reports.ManagerRegular(query_date, proj_dir)
    for report in regular_reports:
        make_rep.make_report(report)

        # Setup file name, write csv
        start_date = make_rep.range_start.strftime("%Y-%m-%d")
        end_date = make_rep.range_end.strftime("%Y-%m-%d")
        out_file = os.path.join(
            manager_dir, f"report_{report}_{start_date}_{end_date}.csv"
        )
        print(f"\tWriting : {out_file}")
        make_rep.df_report.to_csv(out_file, index=False, na_rep="")


# %%
class _GetData:
    """Get REDcap, Qualtrics, and rest-ratings data.

    Download, clean, and return necessary data for
    generating NDAR reports.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project directory

    Attributes
    ----------
    df_demo : pd.DataFrame, make_reports.build_reports.DemoAll.final_demo
        Compiled demographic information
    data_dict : dict
        RedCap, Qualtrics data organized in format:
        {pilot|study: {visit: {survey_name: pd.DataFrame}}}

    Methods
    -------
    get_data()
        Get required demographic data, download/clean
        survey data for values in self._report_names.

    """

    def __init__(self, proj_dir):
        """Initialize."""
        self._proj_dir = proj_dir

    def get_data(self, report_names: list, close_date: datetime.date):
        """Build df_demo and data_dict attrs."""
        self._report_names = report_names

        # Get redcap demographic data, use only consented data in
        # submission cycle.
        redcap_demo = build_reports.DemoAll(self._proj_dir)
        redcap_demo.remove_withdrawn()
        redcap_demo.submission_cycle(close_date)
        self.df_demo = redcap_demo.final_demo

        # Build data_dict with redcap, qualtrics, and rest data
        self.data_dict = {"study": {}, "pilot": {}}
        if "bdi01" in self._report_names:
            self._get_red()
        self._get_qual()
        if "restsurv01" in self._report_names:
            self._get_rest()

    def _get_red(self):
        """Add RedCap BDI to data_dict."""
        redcap_data = manage_data.GetRedcap(self._proj_dir)
        redcap_data.get_redcap(survey_list=["bdi_day2", "bdi_day3"])
        self._merge_dict(redcap_data.clean_redcap)

    def _get_qual(self):
        """Add Qualtrics surveys to data_dict."""
        # Align ndar reports to qualtrics surveys
        qual_s1 = [
            "affim01",
            "als01",
            "emrq01",
            "pswq01",
            "rrs01",
            "tas01",
        ]
        qual_s23 = ["panas01"]
        qual_s123 = ["stai01"]
        qual_sf = ["brd01"]

        # Determine which surveys are needed
        get_qs1 = [x for x in self._report_names if x in qual_s1]
        get_qs23 = [x for x in self._report_names if x in qual_s23]
        get_qs123 = [x for x in self._report_names if x in qual_s123]
        get_qsf = [x for x in self._report_names if x in qual_sf]

        # Initialize getting qualtrics
        if get_qs1 or get_qs23 or get_qs123 or get_qsf:
            qc_data = manage_data.GetQualtrics(self._proj_dir)

        # Get appropriate data
        if get_qsf:
            qc_data.get_qualtrics(
                survey_list=["FINAL - EmoRep Stimulus Ratings - fMRI Study"]
            )
            self._merge_dict(qc_data.clean_qualtrics)
        if get_qs123:
            qc_data.get_qualtrics(
                survey_list=["EmoRep_Session_1", "Session 2 & 3 Survey"]
            )
            self._merge_dict(qc_data.clean_qualtrics)
            return
        if get_qs1:
            qc_data.get_qualtrics(survey_list=["EmoRep_Session_1"])
            self._merge_dict(qc_data.clean_qualtrics)
        if get_qs23:
            qc_data.get_qualtrics(survey_list=["Session 2 & 3 Survey"])
            self._merge_dict(qc_data.clean_qualtrics)

    def _get_rest(self):
        """Add rest ratings to data_dict."""
        rest_data = manage_data.GetRest(self._proj_dir)
        rest_data.get_rest()
        self._merge_dict(rest_data.clean_rest)

    def _merge_dict(self, new_data: dict):
        """Update data_dict attr with new_data."""
        # Build omni dict of dataframes in format:
        #   {study|pilot: {visit: {survey_name: df}}}
        for status in self.data_dict.keys():
            for visit, sur_dict in new_data[status].items():
                for sur_name, sur_df in sur_dict.items():
                    if visit not in self.data_dict[status].keys():
                        self.data_dict[status][visit] = {sur_name: sur_df}
                    else:
                        self.data_dict[status][visit][sur_name] = sur_df


class _BuildArgs:
    """Build arguments for build_ndar.Ndar* classes.

    Unpack data_dict attr, return list of dataframes for
    df_name. Used to supply each build_ndar.Ndar*
    class the appropriate number of dataframes.

    Methods
    -------
    build_args()
        Return list of dataframes for df_name

    """

    def build_args(self, data_dict: dict, df_name: str) -> list:
        """Return list of pd.DataFrame for df_name."""
        self._data_dict = data_dict
        self._df_name = df_name

        # Align report name to unpacking method
        v1_list = ["AIM", "ALS", "ERQ", "PSWQ", "RRS", "TAS"]
        v23_list = ["BDI", "PANAS", "rest_ratings", "post_scan_ratings"]
        v123_list = ["STAI"]
        if self._df_name not in v1_list + v23_list + v123_list:
            raise ValueError(f"Unexpected df name : {self._df_name}")

        # Get, return list of dataframes
        if self._df_name in v1_list:
            arg_list = self._v1_pilot_study()
        elif self._df_name in v23_list:
            arg_list = self._v23_pilot_study()
        elif self._df_name in v123_list:
            arg_list = self._v123_pilot_study()
        return arg_list

    def _v1_pilot_study(self) -> list:
        """Return visit_day1 dataframes."""
        df_pilot = self._data_dict["pilot"]["visit_day1"][self._df_name]
        df_study = self._data_dict["study"]["visit_day1"][self._df_name]

        # RRS does not have pilot data in qualtrics
        return [df_pilot, df_study] if self._df_name != "RRS" else [df_study]

    def _v23_pilot_study(self) -> list:
        """Return visit_day2 and visit_day3 dataframes."""
        df_pilot_2 = self._data_dict["pilot"]["visit_day2"][self._df_name]
        df_study_2 = self._data_dict["study"]["visit_day2"][self._df_name]
        df_pilot_3 = self._data_dict["pilot"]["visit_day3"][self._df_name]
        df_study_3 = self._data_dict["study"]["visit_day3"][self._df_name]

        # PANAS and post_scan_ratings do not have pilot data in qualtrics
        if self._df_name in ["PANAS", "post_scan_ratings"]:
            return [df_study_2, df_study_3]
        else:
            return [df_pilot_2, df_study_2, df_pilot_3, df_study_3]

    def _v123_pilot_study(self) -> list:
        """Return STAI dataframes."""
        df_pilot_1 = self._data_dict["pilot"]["visit_day1"]["STAI_Trait"]
        df_study_1 = self._data_dict["study"]["visit_day1"]["STAI_Trait"]
        df_pilot_2 = self._data_dict["pilot"]["visit_day2"]["STAI_State"]
        df_study_2 = self._data_dict["study"]["visit_day2"]["STAI_State"]
        df_pilot_3 = self._data_dict["pilot"]["visit_day3"]["STAI_State"]
        df_study_3 = self._data_dict["study"]["visit_day3"]["STAI_State"]
        return [
            df_pilot_1,
            df_study_1,
            df_pilot_2,
            df_study_2,
            df_pilot_3,
            df_study_3,
        ]


class MakeNdarReports(_BuildArgs):
    """Make reports and organize data for NDAR upload.

    Inherits _BuildArgs.

    Generate requested NDAR reports and organize data (if required) for the
    biannual upload.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    close_date : datetime.date
        Submission cycle close date

    Methods
    -------
    make_report(report_list: list)
        Generate bi-annual NDAR reports.

    Example
    -------
    make_ndar = required_reports.MakeNdarReports(*args)
    make_ndar.make_report(["demo_info01", "affim01"])

    Notes
    -----
    Reports written to <proj_dir>/ndar_upload/cycle_<close_date>
    Data are hosted at <proj_dir>/ndar_upload/data_[mri|phys|beh]

    """

    def __init__(self, proj_dir, close_date):
        """Initialize."""
        self._proj_dir = proj_dir
        self._close_date = close_date
        super().__init__()

    @property
    def _nda_switch(self):
        """Map requested report to build_ndar class and dataset name."""
        return {
            "affim01": ["NdarAffim01", "AIM"],
            "als01": ["NdarAls01", "ALS"],
            "bdi01": ["NdarBdi01", "BDI"],
            "brd01": ["NdarBrd01", "post_scan_ratings"],
            "demo_info01": ["NdarDemoInfo01", None],
            "emrq01": ["NdarEmrq01", "ERQ"],
            "image03": ["NdarImage03", None],
            "panas01": ["NdarPanas01", "PANAS"],
            "pswq01": ["NdarPswq01", "PSWQ"],
            "restsurv01": ["NdarRest01", "rest_ratings"],
            "rrs01": ["NdarRrs01", "RRS"],
            "stai01": ["NdarStai01", "STAI"],
            "tas01": ["NdarTas01", "TAS"],
        }

    def make_report(self, report_names):
        """Generate requested NDAR report(s).

        Parameters
        ----------
        report_names : list
            Names of desired NDA reports e.g. ["demo_info01", "affim01"]

        """
        # Validate ndar_reports arguments, download and clean
        # relevant data for requested report.
        for report in report_names:
            if report not in self._nda_switch.keys():
                raise ValueError(f"Unexpected ndar_report value : {report}")

        # Download and clean data for requested reports
        gd = _GetData(self._proj_dir)
        gd.get_data(report_names, self._close_date)
        self.df_demo = gd.df_demo
        self.data_dict = gd.data_dict

        # Build each requested report
        for self._report in report_names:
            self._build_report()

    def _build_report(self):
        """Build requested report."""
        # Build args. All classes take df_demo as arg 1. Supply project_dir
        # to certain classes, give image03 close_date.
        args = [self.df_demo]
        if self._report in ["brd01", "image03", "panas01", "rrs01"]:
            args = args + [self._proj_dir]
        if self._report in ["image03"]:
            args = args + [self._close_date]

        # Identify class name and get data if needed
        class_name, df_name = self._nda_switch[self._report]
        if df_name:
            args = args + self.build_args(self.data_dict, df_name)

        # Get appropriate class for report, generate report.
        mod = __import__(
            "make_reports.resources.build_ndar",
            fromlist=[class_name],
        )
        rep_class = getattr(mod, class_name)
        rep_obj = rep_class(*args)
        self._write_report(rep_obj.df_report, rep_obj.nda_label)

    def _write_report(self, df: pd.DataFrame, nda_label: list):
        """Write ndar report to disk."""
        # Setup output directories
        report_dir = os.path.join(
            self._proj_dir,
            "ndar_upload",
            f"cycle_{self._close_date.strftime('%Y-%m-%d')}",
        )
        if not os.path.exists(report_dir):
            os.makedirs(report_dir)
        out_file = os.path.join(report_dir, f"{self._report}_dataset.csv")
        print(f"\tWriting : {out_file}")
        df.to_csv(out_file, index=False, na_rep="")

        # Prepend header
        dummy_file = f"{out_file}.bak"
        with open(out_file, "r") as read_obj, open(
            dummy_file, "w"
        ) as write_obj:
            write_obj.write(f"{','.join(nda_label)}\n")
            for line in read_obj:
                write_obj.write(line)
        os.remove(out_file)
        os.rename(dummy_file, out_file)


# %%
def generate_guids(proj_dir, user_name, user_pass, find_mismatch):
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

    """
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
