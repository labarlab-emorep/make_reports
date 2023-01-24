"""Sub-package workflows.

Each workflow function or class will coordinate the methods
and data needed.

"""
# %%
import os
import glob
from datetime import datetime
from make_reports import survey_download, survey_clean
from make_reports import build_reports, report_helper
from make_reports import calc_metrics, calc_surveys


# %%
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

    Each class method will read in raw data from their respective
    sources and then corrdinate cleaning methods from survey_clean.

    Cleaned dataframes are written to:
        <proj_dir>/data_survey/<visit>/data_clean

    Methods
    -------
    clean_redcap()
        Clean all RedCap surveys
    clean_rest()
        Clean ratings of resting state experiences
    clean_qualtrics()
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
        _proj_dir : path
            Project's experiment directory

        """
        print("Initializing CleanSurveys")
        self._proj_dir = proj_dir

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
            f"{self._proj_dir}/data_survey/redcap*/data_raw/*latest.csv"
        )
        if len(redcap_raw) != 4:
            raise FileNotFoundError(
                "Missing raw survey data in redcap directories,"
                + " please download raw data via rep_dl."
            )

        # Get cleaning obj
        print("\tRunning CleanSurveys.clean_redcap")
        redcap_dict = report_helper.redcap_dict()
        clean_redcap = survey_clean.CleanRedcap(self._proj_dir)

        # Clean each planned survey, write out
        for sur_name, dir_name in redcap_dict.items():
            clean_redcap.clean_surveys(sur_name)

            # Write study data
            out_name = "BDI" if "bdi" in sur_name else sur_name
            clean_file = os.path.join(
                self._proj_dir,
                "data_survey",
                dir_name,
                "data_clean",
                f"df_{out_name}.csv",
            )
            out_dir = os.path.dirname(clean_file)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            clean_redcap.df_clean.to_csv(clean_file, index=False, na_rep="")

            # Write pilot data
            pilot_file = os.path.join(
                self._proj_dir,
                "data_pilot/data_survey",
                dir_name,
                "data_clean",
                f"df_{out_name}.csv",
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

        def _write_clean_qualtrics(
            clean_dict: dict, pilot_dict: dict, dir_name: str
        ) -> None:
            """Write cleaned dataframes for RedCap surveys."""
            # Unpack study clean data
            for h_name, h_df in clean_dict.items():
                out_file = os.path.join(
                    self._proj_dir,
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
                    self._proj_dir,
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
            f"{self._proj_dir}/data_survey/visit*/data_raw/*latest.csv"
        )
        if len(visit_raw) != 7:
            raise FileNotFoundError(
                "Missing raw survey data in visit directories,"
                + " please download raw data via rep_dl."
            )

        # Get cleaning obj
        print("\tRunning CleanSurveys.clean_qualtrics")
        qualtrics_dict = report_helper.qualtrics_dict()
        clean_qualtrics = survey_clean.CleanQualtrics(self._proj_dir)

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
            self._proj_dir,
            "data_scanner_BIDS",
            "rawdata",
        )
        rest_list = glob.glob(f"{raw_path}/sub-*/ses-*/beh/*rest-ratings*.tsv")
        if not rest_list:
            raise FileNotFoundError(
                "Missing rest ratings, try running dcm_conversion."
            )

        # Aggregate rest ratings, for each session day
        print("\tRunning CleanSurveys.clean_rest")
        agg_rest = survey_clean.CombineRestRatings(self._proj_dir)
        for day in ["day2", "day3"]:

            # Get, write out study data
            agg_rest.get_rest_ratings(day, raw_path)
            out_file = os.path.join(
                self._proj_dir,
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
                self._proj_dir, "data_pilot/data_scanner_BIDS", "rawdata"
            )
            agg_rest.get_rest_ratings(day, rawdata_pilot)
            out_file = os.path.join(
                self._proj_dir,
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


def make_ndar_reports(ndar_reports, proj_dir, close_date):
    """Make reports and organize data for NDAR upload.

    Generate requested NDAR reports and organize data (if required) for the
    biannual upload.

    Parameters
    ----------
    ndar_reports : list
        Names of desired NDA reports e.g. ["demo_info01", "affim01"]
    proj_dir : path
        Project's experiment directory
    close_date : datetime
        Submission cycle close date

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
        <proj_dir>/ndar_upload/cycle_<close_date>

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

    # Set switch to find appropriate class in make_reports.build_ndar:
    #   key = user-specified report name
    #   value = relevant class
    mod_build = "make_reports.build_ndar"
    nda_switch = {
        "demo_info01": f"{mod_build}.NdarDemoInfo01",
        "affim01": f"{mod_build}.NdarAffim01",
        "als01": f"{mod_build}.NdarAls01",
        "bdi01": f"{mod_build}.NdarBdi01",
        "brd01": f"{mod_build}.NdarBrd01",
        "emrq01": f"{mod_build}.NdarEmrq01",
        "image03": f"{mod_build}.NdarImage03",
        "panas01": f"{mod_build}.NdarPanas01",
        "pswq01": f"{mod_build}.NdarPswq01",
        "restsurv01": f"{mod_build}.NdarRest01",
        "rrs01": f"{mod_build}.NdarRrs01",
        "stai01": f"{mod_build}.NdarStai01",
        "tas01": f"{mod_build}.NdarTas01",
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

        # Get appropriate class from make_reports.build_ndar
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

    Notes
    -----
    Attempts to trigger CleanSurveys.clean_redcap if a cleaned dataframe for
    demographic info is not detected.

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


def get_metrics(proj_dir, recruit_demo, pending_scans, redcap_token):
    """Title.

    Desc.

    Parameters
    ----------
    proj_dir : path
    recruit_demo : bool
    pending_scans : bool

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

    #
    if recruit_demo:
        # Get redcap demo info, use only consented data
        redcap_demo = build_reports.DemoAll(proj_dir)
        redcap_demo.remove_withdrawn()

        print("\nComparing planned vs. actual recruitment demographics ...")
        _ = calc_metrics.demographics(proj_dir, redcap_demo.final_demo)

    #
    if pending_scans:
        print("\nFinding participants missing day3 scan ...\n")
        pend_dict = calc_metrics.calc_pending(redcap_token)
        print("\tSubj \t Time since day2 scan")
        for subid, days in pend_dict.items():
            print(f"\t{subid} \t {days}")
        print("")


# %%
class CalcRedcapQualtricsStats:
    """Generate descriptive stats and plots for REDCap, Qualtrics surveys.

    Matched items in input survey list to respective visits, then generate
    descriptive stats and plots for each visit. Also generates stats/plots
    for survey subscales, when relevant.

    Attributes
    ----------
    has_subscales : list
        Surveys containing subscales
    out_dir : path
        Output destination for generated files
    visit1_list : list
        Visit 1 survey names
    visit23_list : list
        Visit 2, 3 survey names

    Methods
    -------
    wrap_visits()
        Align requested surveys with visits, submit desc_plots
    desc_plots()
        Generate descriptive stats and plots for survey

    """

    def __init__(self, proj_dir, sur_list):
        """Initialize.

        Parameters
        ----------
        proj_dir : path
            Location of project's experiment directory
        sur_list : list
            REDCap or Qualtrics survey names

        Attributes
        ----------
        has_subscales : list
            Surveys containing subscales
        out_dir : path
            Output destination for generated files
        visit1_list : list
            Visit 1 survey names
        visit23_list : list
            Visit 2, 3 survey names

        Raises
        ------
        ValueError
            Unexpected survey name

        """
        print("\nInitializing CalcRedcapQualtricsStats")
        self._proj_dir = proj_dir
        self._sur_list = sur_list
        self.out_dir = os.path.join(
            proj_dir, "analyses/surveys_stats_descriptive"
        )
        self.has_subscales = ["ALS", "ERQ", "RRS"]
        self.visit1_list = [
            "AIM",
            "ALS",
            "ERQ",
            "PSWQ",
            "RRS",
            "STAI_Trait",
            "TAS",
        ]
        self.visit23_list = ["STAI_State", "PANAS", "BDI"]
        for sur in sur_list:
            if sur not in self.visit1_list and sur not in self.visit23_list:
                raise ValueError(f"Unexpected survey requested : {sur}")

    def wrap_visits(self):
        """Wrap desc_plots for each visit's surveys.

        Identify visit(s) of survey and then execute desc_plots
        for each visit.

        """
        for self._sur_name in self._sur_list:
            if self._sur_name in self.visit1_list:

                # Use visit1 data
                print(f"\tGetting stats for {self._sur_name}")
                csv_path = os.path.join(
                    self._proj_dir,
                    "data_survey/visit_day1/data_clean",
                    f"df_{self._sur_name}.csv",
                )

                # Get info for plotting, generate files
                self._plot_dict = self._plot_switch()
                self.desc_plots(csv_path, self._sur_name)

            elif self._sur_name in self.visit23_list:

                # Use visit2, 3 data
                for day in ["day2", "day3"]:
                    print(f"\tGetting stats for {self._sur_name}, {day}")
                    csv_path = os.path.join(
                        self._proj_dir,
                        f"data_survey/visit_{day}/data_clean",
                        f"df_{self._sur_name}.csv",
                    )
                    out_name = f"{self._sur_name}_{day}"

                    # Get info for plotting, generate files
                    self._plot_dict = self._plot_switch(day=day)
                    self.desc_plots(csv_path, out_name)

    def desc_plots(self, csv_path, out_name):
        """Generate descriptive stats and plots.

        Stats files are written to:
            <out_dir>/stats_<out_name>*.json

        Plots are written to:
            <out_dir>/plot_violin_<out_name>*.png

        Parameters
        ----------
        csv_path : path
            Location of cleaned survey CSV
        out_name : str
            Survey identfier, substring of output filename

        Raises
        ------
        FileNotFoundError
            File missing at csv_path

        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Missing file : {csv_path}")

        # Generate stats and plots
        sur_stat = calc_surveys.DescriptRedcapQualtrics(
            self._proj_dir, csv_path, self._sur_name
        )
        _ = sur_stat.write_mean_std(
            os.path.join(self.out_dir, f"stats_{out_name}.json"),
            self._plot_dict["title"],
        )
        _ = sur_stat.violin_plot(
            out_path=os.path.join(self.out_dir, f"plot_violin_{out_name}.png"),
            title=self._plot_dict["title"],
        )
        if self._sur_name not in self.has_subscales:
            return

        # Generate stats and plots for subscales, capitalize on
        # mutability of sur_stat.df, sur_stat.df_col.
        df_work = sur_stat.df.copy()
        subscale_dict = self._subscale_switch()
        for sub_name, sub_cols in subscale_dict.items():
            sur_stat.df = df_work[sub_cols].copy()
            sur_stat.df_col = sub_cols

            # Setup file names, make files
            sub_title = self._plot_dict["title"] + f", {sub_name}"
            sub_out = os.path.join(
                self.out_dir, f"plot_violin_{out_name}_{sub_name}.png"
            )
            _ = sur_stat.write_mean_std(
                os.path.join(
                    self.out_dir, f"stats_{out_name}_{sub_name}.json"
                ),
                sub_title,
            )
            _ = sur_stat.violin_plot(
                out_path=sub_out,
                title=sub_title,
            )

    def _plot_switch(self, day: str = "day2") -> dict:
        """Supply plot variables for survey."""
        # Get visit name
        visit_switch = {
            "day2": "Visit 2",
            "day3": "Visit 3",
        }
        if day not in visit_switch:
            raise KeyError(f"Unexpected key : {day}")
        vis = visit_switch[day]

        # Set plotting values for surveys. Each list item in
        # _values should have a corresponding key in _keys.
        _values = {
            "AIM": ["Affective Intensity Measure"],
            "ALS": ["Affective Lability Scale"],
            "ERQ": ["Emotion Regulation Questionnaire"],
            "PSWQ": ["Penn State Worry Questionnaire"],
            "RRS": ["Ruminative Response Scale"],
            "STAI_Trait": ["Spielberg Trait Anxiety Inventory"],
            "TAS": ["Toronto Alexithymia Scale"],
            "STAI_State": [f"{vis} Spielberg State Anxiety Inventory"],
            "PANAS": [f"{vis} Positive and Negative Affect Schedule"],
            "BDI": [f"{vis} Beck Depression Inventory"],
        }
        _keys = ["title"]

        # Validate survey name
        if self._sur_name not in _values.keys():
            raise AttributeError(f"Unexpected survey name : {self._sur_name}")

        # Build dictionary from _keys and _values
        param_dict = {}
        for sur_name, sur_list in _values.items():
            param_dict[sur_name] = {}
            for c, val in enumerate(_keys):
                param_dict[sur_name][val] = sur_list[c]
        return param_dict[self._sur_name]

    def _subscale_switch(self) -> dict:
        """Align subscale items and names."""
        # Build specific dicts for matching subscale names to
        # survey items for each survey.
        _als_sub = {
            "Anx-Dep": [f"ALS_{x}" for x in [1, 3, 5, 6, 7]],
            "Dep-Ela": [f"ALS_{x}" for x in [2, 10, 12, 13, 15, 16, 17, 18]],
            "Anger": [f"ALS_{x}" for x in [4, 8, 9, 11, 14]],
        }
        _erq_sub = {
            "Reappraisal": [f"ERQ_{x}" for x in [1, 3, 5, 7, 8, 10]],
            "Suppression": [f"ERQ_{x}" for x in [2, 4, 6, 9]],
        }
        _rrs_sub = {
            "Depression": [
                f"RRS_{x}" for x in [1, 2, 3, 4, 6, 8, 9, 14, 17, 18, 19, 22]
            ],
            "Brooding": [f"RRS_{x}" for x in [5, 10, 13, 15, 16]],
            "Reflection": [f"RRS_{x}" for x in [7, 11, 12, 20, 21]],
        }

        # Align subscale dicts and survey names. _sub_names needs to
        # match values in has_subscales.
        _sub_names = ["ALS", "ERQ", "RRS"]
        _sub_dicts = [_als_sub, _erq_sub, _rrs_sub]

        # Construct ombnibus dict, return specific survey subdict
        all_dict = {}
        for sub_name, sub_dict in zip(_sub_names, _sub_dicts):
            all_dict[sub_name] = sub_dict
        return all_dict[self._sur_name]


# %%
def survey_scan(proj_dir, survey_list):
    """Calculate descriptives for survey responses.

    Calculate descriptive statistics and generate plots for
    rest-rating and stimulus-rating surveys.

    Parameters
    ----------
    proj_dir : path
        Location of project's experiment directory
    survey_list : list
        [rest | stim]
        Survey names, for triggering different workflows

    Raises
    ------
    ValueError
        Unexpected item in survey_list

    """
    for sur in survey_list:
        if sur not in ["rest", "stim"]:
            raise ValueError(f"Unexpected survey name : {sur}")

    if "rest" in survey_list:
        print("\nWorking on rest-ratings data")
        _ = calc_surveys.descript_rest_ratings(proj_dir)

    if "stim" in survey_list:
        stim_stats = calc_surveys.DescriptStimRatings(proj_dir)
        for stim_type in ["Videos", "Scenarios"]:
            _ = stim_stats.endorsement(stim_type)
            for prompt_name in ["Arousal", "Valence"]:
                _ = stim_stats.arousal_valence(stim_type, prompt_name)


# %%
