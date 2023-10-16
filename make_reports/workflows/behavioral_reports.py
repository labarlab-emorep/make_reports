"""Methods for generating descriptive stats on participant responses.

CalcRedcapQualtricsStats : generate descriptive stats from REDCap,
                            Qualtrics surveys
calc_task_stats : generate descriptive stats for EmoRep task
make_survey_table : organize multiple descriptive reports into tables

"""
# %%
import os
import json
import pandas as pd
from make_reports.resources import calc_surveys
from make_reports.resources import manage_data


# %%
class _Visit1:
    """Get visit1 data and trigger stats.

    Intended to be inherited by CalcRedcapQualtricsStats, references
    attrs set by child.

    """

    def _visit1_data(self):
        """Make clean_visit1 attr {survey_name: pd.DataFrame}."""
        get_qual = manage_data.GetQualtrics(
            self._proj_dir, self._qualtrics_token
        )
        get_qual.get_qualtrics(["EmoRep_Session_1"])
        self._clean_visit1 = get_qual.clean_qualtrics["study"]["visit_day1"]

    def _visit1_stats_plots(self):
        """Generate descriptive stats and figures for visit 1 surveys."""
        # Setup output dict for survey
        self.survey_descriptives[self._sur_name] = {}

        # Generate stats, update output dict
        sur_stat = calc_surveys.Visit1Stats(
            self._clean_visit1[self._sur_name], self._sur_name
        )
        report_dict = {"Title": self._get_title()}
        report_dict.update(sur_stat.calc_row_stats())
        self.survey_descriptives[self._sur_name]["full"] = report_dict

        # Write JSON and figures
        if self._write_json:
            stat_out = os.path.join(
                self.out_dir, f"stats_{self._sur_name}.json"
            )
            with open(stat_out, "w") as jf:
                json.dump(report_dict, jf)
                print(f"\tSaved descriptive stats : {stat_out}")
        if self._draw_plot:
            plot_out = os.path.join(
                self.out_dir, f"plot_{self._sur_name}_boxplot-single.png"
            )
            sur_stat.draw_single_boxplot(self._get_title(), plot_out)

        # Trigger subscale stats, plots
        if self._sur_name in self._has_subscales:
            self._visit1_subscale(sur_stat)

    def _visit1_subscale(self, sur_stat: calc_surveys.Visit1Stats):
        """Generate stats, plots for visit 1 survey subscales."""

        # Setup subscale names and column names for each survey
        subscale_dict = {
            "ALS": {
                "Anx-Dep": [f"ALS_{x}" for x in [1, 3, 5, 6, 7]],
                "Dep-Ela": [
                    f"ALS_{x}" for x in [2, 10, 12, 13, 15, 16, 17, 18]
                ],
                "Anger": [f"ALS_{x}" for x in [4, 8, 9, 11, 14]],
            },
            "ERQ": {
                "Reappraisal": [f"ERQ_{x}" for x in [1, 3, 5, 7, 8, 10]],
                "Suppression": [f"ERQ_{x}" for x in [2, 4, 6, 9]],
            },
            "RRS": {
                "Depression": [
                    f"RRS_{x}"
                    for x in [1, 2, 3, 4, 6, 8, 9, 14, 17, 18, 19, 22]
                ],
                "Brooding": [f"RRS_{x}" for x in [5, 10, 13, 15, 16]],
                "Reflection": [f"RRS_{x}" for x in [7, 11, 12, 20, 21]],
            },
        }

        # Generate stats, plots for each subscale
        df_work = sur_stat.df.copy()
        for sub_name, sub_cols in subscale_dict[self._sur_name].items():
            sur_stat.df = df_work[sub_cols].copy()
            sur_stat.col_data = sub_cols

            # Calculate subscale stats
            sub_title = self._get_title() + f", {sub_name}"
            _stat_dict = sur_stat.calc_row_stats()
            report_dict = {"Title": sub_title}
            report_dict.update(_stat_dict)
            self.survey_descriptives[self._sur_name][sub_name] = report_dict

            # Write stat JSON and boxplot
            if self._write_json:
                sub_stat_out = os.path.join(
                    self.out_dir, f"stats_{self._sur_name}_{sub_name}.json"
                )
                with open(sub_stat_out, "w") as jf:
                    json.dump(report_dict, jf)
                    print(f"\tSaved descriptive stats : {sub_stat_out}")
            if self._draw_plot:
                sub_plot_out = os.path.join(
                    self.out_dir,
                    f"plot_{self._sur_name}_{sub_name}_boxplot-single.png",
                )
                sur_stat.draw_single_boxplot(sub_title, sub_plot_out)


class _Visit23:
    """Get visit2 and 3 data and trigger stats.

    Intended to be inherited by CalcRedcapQualtricsStats, references
    attrs set by child.

    """

    def _visit23_redcap_data(self):
        """Make clean_visit23_rc attr {visit: {survey_name: pd.DataFrame}}."""
        get_red = manage_data.GetRedcap(self._proj_dir, self._redcap_token)
        get_red.get_redcap(survey_list=["bdi_day2", "bdi_day3"])
        self._clean_visit23_rc = get_red.clean_redcap["study"]

    def _visit23_qualtrics_data(self):
        """Make clean_visit23_qual attr in format
        {visit: {survey_name: pd.DataFrame}}.
        """
        get_qual = manage_data.GetQualtrics(
            self._proj_dir, self._qualtrics_token
        )
        get_qual.get_qualtrics(["Session 2 & 3 Survey"])
        self._clean_visit23_qual = get_qual.clean_qualtrics["study"]

    def _visit23_stats_plots(
        self, df_day2: pd.DataFrame, df_day3: pd.DataFrame
    ):
        """Generate descriptive stats and figures for visit 2, 3 surveys."""
        # Set factor column, values
        fac_col = "visit"
        fac_a = "Visit 2"
        fac_b = "Visit 3"

        def _flatten(in_dict: dict):
            """Reorganize Visit23Stats.calc_factor_stats output."""
            _title = in_dict["Title"]
            for visit in [fac_a, fac_b]:
                _dict = in_dict[visit]
                _dict["Title"] = _title + ", " + visit
                self.survey_descriptives[self._sur_name][visit] = _dict

        # Generate stats, update output dict
        self.survey_descriptives[self._sur_name] = {}
        sur_stat = calc_surveys.Visit23Stats(
            df_day2, df_day3, self._sur_name, fac_col, fac_a, fac_b
        )
        report_dict = {"Title": self._get_title()}
        report_dict.update(sur_stat.calc_factor_stats(fac_col, fac_a, fac_b))
        _flatten(report_dict)

        # Write stats to JSON, draw boxplot
        if self._write_json:
            stat_out = os.path.join(
                self.out_dir, f"stats_{self._sur_name}.json"
            )
            with open(stat_out, "w") as jf:
                json.dump(report_dict, jf)
                print(f"\tSaved descriptive stats : {stat_out}")
        if self._draw_plot:
            plot_out = os.path.join(
                self.out_dir, f"plot_{self._sur_name}_boxplot-double.png"
            )
            sur_stat.draw_double_boxplot(
                fac_col,
                fac_a,
                fac_b,
                self._get_title(),
                plot_out,
            )


# %%
class CalcRedcapQualtricsStats(_Visit1, _Visit23):
    """Generate descriptive stats and plots for REDCap, Qualtrics surveys.

    Match items in input survey list to respective visits, then generate
    descriptive stats and plots for each visit. Also generates stats/plots
    for survey subscales, when relevant.

    Written descriptive stats are titled <out_dir>/stats_<survey_name>.json,
    and plots are titled:
        <out_dir>/plot_<survey_name>_boxplot-<single|double>.png

    Inherits _Visit1, _Visit23.

    Parameters
    ----------
    proj_dir : path
        Location of project's experiment directory

    Attributes
    ----------
    survey_descriptives : dict
        Generated descriptive stats for specified surveys
    out_dir : path
        Output destination for generated files

    Methods
    -------
    gen_stats_plots(*args)
        Match survey in survey list to appropriate private method
        and then trigger method.

    Example
    -------
    survey_stats = CalcRedcapQualtricsStats("/path/to/project/dir")
    survey_stats.gen_stats_plots(
        ["AIM", "ALS"], True, True, qualtrics_token="foo",
    )
    all_stats = survey_stats.survey_descriptives

    """

    def __init__(
        self,
        proj_dir,
    ):
        """Initialize."""
        print("\nInitializing CalcRedcapQualtricsStats")
        self._proj_dir = proj_dir
        self.out_dir = os.path.join(proj_dir, "analyses/metrics_surveys")
        self.survey_descriptives = {}

    def gen_stats_plots(
        self,
        sur_list,
        draw_plot,
        write_json,
        qualtrics_token=None,
        redcap_token=None,
    ):
        """Coordinate plot and statistic generation.

        Entrypoint. Identify the appropriate method for each
        requested survey name and then trigger the method.

        Parameters
        ----------
        sur_list : list
            REDCap or Qualtrics survey abbreviations
        draw_plot : bool
            Whether to generate, write figure
        write_json : bool
            Whether to save generated descriptive
            statistics to JSON
        qualtrics_token : str, optional
            API token for Qualtrics. Required when sur_list contains
            qualtrics survey names.
        redcap_token : str, optional
            API token for RedCap. Required when sur_list contains
            redcap survey names.

        """
        # Capture params, setup
        self._draw_plot = draw_plot
        self._write_json = write_json
        self._sur_list = sur_list
        self._qualtrics_token = qualtrics_token
        self._redcap_token = redcap_token
        self._setup()

        # Unpack list of surveys
        get_visit1 = [x for x in self._sur_list if x in self._visit1_list]
        get_visit23_rc = [x for x in self._sur_list if x in self._visit23_rc]
        get_visit23_qual = [
            x for x in self._sur_list if x in self._visit23_qual
        ]

        # Get visit1 (qualtrics) data and generate stats/plots
        if get_visit1:
            self._visit1_data()
            for self._sur_name in get_visit1:
                self._visit1_stats_plots()

        # Get visit2/3 redcap data, generate stats/plots
        if get_visit23_rc:
            self._visit23_redcap_data()
            for self._sur_name in get_visit23_rc:
                self._visit23_stats_plots(
                    self._clean_visit23_rc["visit_day2"][self._sur_name],
                    self._clean_visit23_rc["visit_day3"][self._sur_name],
                )

        # Get visit2/3 qualtrics data, generate stats/plots
        if get_visit23_qual:
            self._visit23_qualtrics_data()
            for self._sur_name in get_visit23_qual:
                self._visit23_stats_plots(
                    self._clean_visit23_qual["visit_day2"][self._sur_name],
                    self._clean_visit23_qual["visit_day3"][self._sur_name],
                )

    def _setup(self):
        """Validate and set orienting attrs."""
        self._has_subscales = ["ALS", "ERQ", "RRS"]
        self._visit1_list = [
            "AIM",
            "ALS",
            "ERQ",
            "PSWQ",
            "RRS",
            "STAI_Trait",
            "TAS",
        ]
        self._visit23_qual = ["STAI_State", "PANAS"]
        self._qualtrics_list = self._visit1_list + self._visit23_qual
        self._visit23_rc = ["BDI"]
        self._visit23_list = self._visit23_qual + self._visit23_rc
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        # Validate survey list
        for sur in self._sur_list:
            if sur not in self._visit1_list + self._visit23_list:
                raise ValueError(f"Unexpected survey requested : {sur}")
            if not self._redcap_token and sur in self._visit23_rc:
                raise ValueError(f"Survey {sur} requires RedCap API token")
            if not self._qualtrics_token and sur in self._qualtrics_list:
                raise ValueError(f"Survey {sur} requires Qualtrics API token")

        # Validate types
        if not isinstance(self._draw_plot, bool):
            raise TypeError("Expected draw_plot type bool")
        if not isinstance(self._write_json, bool):
            raise TypeError("Expected type bool for write_json")

    def _get_title(self) -> str:
        """Switch survey abbreviation for name."""
        plot_titles = {
            "AIM": "Affective Intensity Measure",
            "ALS": "Affective Lability Scale",
            "ERQ": "Emotion Regulation Questionnaire",
            "PSWQ": "Penn State Worry Questionnaire",
            "RRS": "Ruminative Response Scale",
            "STAI_Trait": "Spielberg Trait Anxiety Inventory",
            "TAS": "Toronto Alexithymia Scale",
            "STAI_State": "Spielberg State Anxiety Inventory",
            "PANAS": "Positive and Negative Affect Schedule",
            "BDI": "Beck Depression Inventory",
        }
        return plot_titles[self._sur_name]


# %%
def calc_task_stats(proj_dir, survey_list, draw_plot, qualtrics_token=None):
    """Calculate stats for in- and post-scanner surveys.

    Generate dataframes for the requested surveys, calculcate
    descriptive stats, and draw figures. Output files are
    written to:
        <proj_dir>/analyses/survey_stats_descriptive

    Parameters
    ----------
    proj_dir : path
        Location of project's experiment directory
    survey_list : list
        [rest | stim | task]
        Survey names, for triggering different workflows
    draw_plot : bool
        Whether to draw plots
    qualtrics_token : str, optional
        API token for Qualtrics. Required when sur_list contains
        qualtrics survey names.

    Returns
    -------
    dict
        Generated descriptive stats for specified surveys

    Raises
    ------
    FileNotFoundError
        Project directory does not exist
    TypeError
        Unexpected type of parameter
    ValueError
        Unexpected item in survey_list

    """
    # Validate arguments
    if not isinstance(draw_plot, bool):
        raise TypeError("Expected draw_plot type bool")
    for sur in survey_list:
        if sur not in ["rest", "stim", "task"]:
            raise ValueError(f"Unexpected survey name : {sur}")
    if not os.path.exists(proj_dir):
        raise FileNotFoundError(f"Expected to find directory : {proj_dir}")

    out_dir = os.path.join(proj_dir, "analyses/metrics_surveys")
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Process in-scan post resting-state emotion frequency responses
    survey_descriptives = {}
    if "rest" in survey_list:
        print("\nWorking on rest-ratings data")
        rest_stats = calc_surveys.RestRatings(proj_dir)
        out_path = os.path.join(out_dir, "table_rest-ratings.csv")
        survey_descriptives["rest"] = rest_stats.write_stats(out_path)
        if draw_plot:
            out_plot = os.path.join(
                out_dir, "plot_rest-ratings_boxplot-long.png"
            )
            rest_stats.draw_long_boxplot(
                x_col="emotion",
                x_lab="Emotion Category",
                y_col="rating",
                y_lab="Frequency",
                hue_order=["Scenarios", "Videos"],
                hue_col="task",
                main_title="In-Scan Resting Emotion Frequency",
                out_path=out_plot,
            )

    # Process post-scan stimulus response task
    if "stim" in survey_list:
        gq = manage_data.GetQualtrics(proj_dir, qualtrics_token)
        gq.get_qualtrics(
            survey_list=["FINAL - EmoRep Stimulus Ratings - fMRI Study"]
        )
        stim_stats = calc_surveys.StimRatings(
            proj_dir,
            draw_plot,
            gq.clean_qualtrics["study"]["visit_day2"]["post_scan_ratings"],
            gq.clean_qualtrics["study"]["visit_day3"]["post_scan_ratings"],
        )
        for stim_type in ["Videos", "Scenarios"]:
            _ = stim_stats.endorsement(stim_type)
            survey_descriptives["stim"] = stim_stats.arousal_valence(stim_type)

    # Process in-scan emorep task responses
    if "task" in survey_list:
        task_stats = calc_surveys.EmorepTask(proj_dir, draw_plot)
        survey_descriptives["task"] = task_stats.select_intensity()
        for task in ["Videos", "Scenarios"]:
            _ = task_stats.select_emotion(task)

    return survey_descriptives


# %%
def make_survey_table(
    proj_dir, sur_online, sur_scanner, qualtrics_token, redcap_token
):
    """Generate tables from REDCap, Qualtrics, and task survey data.

    Trigger workflows.CalcRedcapQualtricsStats and workflows.calc_task_stats
    to generate tables of survey responses. Figures are also drawn.

    Parameters
    ----------
    proj_dir : path
        Location of project's experiment directory
    sur_online : list
        All abbreviations for surveys done remotely
    sur_scanner : list
        All abbreviations for task responses

    """
    # Trigger Visit 1-3 methods
    calc_rcq = CalcRedcapQualtricsStats(proj_dir)
    calc_rcq.gen_stats_plots(
        sur_online,
        False,
        False,
        qualtrics_token=qualtrics_token,
        redcap_token=redcap_token,
    )
    data_rcq = calc_rcq.survey_descriptives

    # Organize generated dataframe into table, write out
    df_all = pd.DataFrame(
        columns=["Title", "num", "mean", "SD", "skewness", "kurtosis"]
    )
    for main_key in data_rcq:
        for sub_key in data_rcq[main_key]:
            df_all = pd.concat(
                [df_all, pd.DataFrame(data_rcq[main_key][sub_key], index=[0])],
                ignore_index=True,
            )
    out_stats = os.path.join(
        proj_dir,
        "analyses/metrics_surveys",
        "table_redcap_qualtrics.csv",
    )
    df_all.to_csv(out_stats, index=False)

    # Trigger task methods
    _ = calc_task_stats(
        proj_dir, sur_scanner, draw_plot=True, qualtrics_token=qualtrics_token
    )


# %%
