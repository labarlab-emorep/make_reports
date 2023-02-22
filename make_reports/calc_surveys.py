"""Methods for describing survey responses.

Generate descriptive statistics and plots for REDCap, Qualtrics,
and rest-rating surveys.

Output files are written to:
    experiment2/EmoRep/Exp2_Compute_Emotion/analyses/surveys_stats_descriptive

"""
# %%
import os
import json
import glob
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


# %%
class _DescStat:
    """Title."""

    def __init__(self, df):
        """Title."""
        print("\tInitializing _DescStat")
        self.df = df

    def calc_total_stats(self, df: pd.DataFrame = None) -> dict:
        """Title."""
        df_calc = df.copy() if isinstance(df, pd.DataFrame) else self.df.copy()
        df_calc["total"] = df_calc[self.col_data].sum(axis=1)
        mean = round(df_calc["total"].mean(), 2)
        std = round(df_calc["total"].std(), 2)
        skew = round(df_calc["total"].skew(), 2)
        kurt = round(df_calc["total"].kurt(), 2)
        num = df_calc.shape[0]
        return {
            "num": num,
            "mean": mean,
            "SD": std,
            "skewness": skew,
            "kurtosis": kurt,
        }

    def calc_factor_stats(
        self, fac_col: str, fac_a: str, fac_b: str, df: pd.DataFrame = None
    ) -> dict:
        """Title."""
        df_calc = df.copy() if isinstance(df, pd.DataFrame) else self.df.copy()
        mask_a = df_calc[fac_col] == fac_a
        mask_b = df_calc[fac_col] == fac_b
        out_dict = {}
        out_dict[fac_a] = self.calc_total_stats(df=df_calc[mask_a])
        out_dict[fac_b] = self.calc_total_stats(df=df_calc[mask_b])
        return out_dict

    def draw_single_boxplot(self, main_title: str, out_path: str):
        """Title."""
        #
        stat_dict = self.calc_total_stats()
        df_plot = self.df.copy()
        df_plot["total"] = df_plot[self.col_data].sum(axis=1)

        fig, ax = plt.subplots()
        plt.boxplot(df_plot["total"])
        plt.ylabel("Participant Total")
        plt.tick_params(
            axis="x", which="both", bottom=False, top=False, labelbottom=False
        )
        _, ub = ax.get_ylim()
        plt.text(
            0.55,
            ub - 0.1 * ub,
            f"mean={stat_dict['mean']}\nSD={stat_dict['SD']}",
            horizontalalignment="left",
        )
        plt.title(main_title)
        plt.savefig(out_path)
        plt.close()
        print(f"\t\tDrew boxplot : {out_path}")

    def draw_double_boxplot(
        self,
        fac_col: str,
        fac_a: str,
        fac_b: str,
        main_title: str,
        out_path: str,
    ):
        """Title."""
        stat_dict = self.calc_factor_stats(fac_col, fac_a, fac_b)
        df_plot = self.df.copy()
        df_plot["total"] = df_plot[self.col_data].sum(axis=1)

        #
        df_a = df_plot.loc[df_plot["visit"] == fac_a, "total"]
        df_b = df_plot.loc[df_plot["visit"] == fac_b, "total"]
        fig, ax = plt.subplots()
        plt.boxplot([df_a, df_b])
        plt.ylabel("Participant Total")
        ax.set_xticklabels([fac_a, fac_b])
        ax.get_xaxis().tick_bottom()
        # plt.xlabel("X-label")
        _, ub = ax.get_ylim()
        plt.text(
            0.55,
            ub - 0.1 * ub,
            f"mean={stat_dict[fac_a]['mean']}\nSD={stat_dict[fac_a]['SD']}",
            horizontalalignment="left",
        )
        plt.text(
            2.15,
            ub - 0.1 * ub,
            f"mean={stat_dict[fac_b]['mean']}\nSD={stat_dict[fac_b]['SD']}",
            horizontalalignment="left",
        )
        plt.title(main_title)
        plt.savefig(out_path)
        print(f"\t\tDrew boxplot : {out_path}")
        plt.close()

    def calc_long_stats(self, grp_a: str, grp_b: str) -> pd.DataFrame:
        """Title."""
        df_mean = self.df.groupby([grp_a, grp_b]).mean()
        df_mean.columns = [*df_mean.columns[:-1], "mean"]
        df_std = self.df.groupby([grp_a, grp_b]).std()
        df_skew = self.df.groupby([grp_a, grp_b]).skew(numeric_only=True)
        df_kurt = self.df.groupby([grp_a, grp_b]).apply(
            pd.DataFrame.kurt
        )  # Throws the FutureWarning

        df_mean["std"] = df_std.iloc[:, -1:]
        df_mean["skew"] = df_skew.iloc[:, -1:]
        df_mean["kurt"] = df_kurt.iloc[:, -1:]
        df_mean.iloc[:, -4:] = round(df_mean.iloc[:, -4:], 2)
        return df_mean

    def draw_long_boxplot(
        self,
        x_col: str,
        x_lab: str,
        y_col: str,
        y_lab: str,
        hue_order: list,
        hue_col: str,
        main_title: str,
        out_path: str,
    ):
        """Title."""
        for _col in [x_col, y_col, hue_col]:
            if _col not in self.df.columns:
                raise KeyError(f"Missing expected column in df : {_col}")
        if not is_numeric_dtype(self.df[y_col]):
            raise TypeError("df[y_col] should be numeric type")
        if not is_string_dtype(self.df[x_col]) or not is_string_dtype(
            self.df[hue_col]
        ):
            raise TypeError("df[x_col] and df[hue_col] should be string type")

        #
        df_plot = self.df.copy()
        df_plot[y_col] = df_plot[y_col].astype(float)
        fig, ax = plt.subplots()
        plt.figure().set_figwidth(10)
        sns.boxplot(
            x=x_col,
            y=y_col,
            hue=hue_col,
            data=df_plot,
            hue_order=hue_order,
        )
        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)

        plt.ylabel(y_lab)
        plt.xlabel(x_lab)
        plt.xticks(rotation=45, ha="right")
        plt.title(main_title)
        plt.savefig(out_path, bbox_inches="tight")
        print(f"\tDrew boxplot : {out_path}")
        plt.close()

    def confusion_matrix(
        self,
        emo_list: list,
        subj_col: str,
        num_exp: int,
        emo_col: str = "emotion",
        resp_col: str = "response",
    ) -> pd.DataFrame:
        """Generate a confusion matrix of emotion endorsements."""
        for _col in [subj_col, emo_col, resp_col]:
            if _col not in self.df.columns:
                raise KeyError(f"Missing expected column in df : {_col}")
        if len(emo_list) != 15:
            raise ValueError(
                f"Expected emo_list to have len == 15, found : {len(emo_list)}"
            )
        if not is_string_dtype(self.df[emo_col]) or not is_string_dtype(
            self.df[resp_col]
        ):
            raise TypeError("df[y_col] and df[hue_col] should be string type")
        if not isinstance(num_exp, int):
            raise TypeError("Expected type int for num_exp")

        # Set denominator for proportion calc
        max_total = num_exp * len(self.df[subj_col].unique())

        # Calc proportion each emotion is endorsed as every emotion
        count_dict = {}
        for emo in emo_list:
            count_dict[emo] = {}
            df_emo = self.df[self.df[emo_col] == emo]
            for sub_emo in emo_list:
                count_emo = len(df_emo[df_emo[resp_col].str.contains(sub_emo)])
                count_dict[emo][sub_emo] = round(count_emo / max_total, 2)
        del df_emo

        # Generate dataframe and transponse for intuitive
        # stimulus-response axes.
        df_corr = pd.DataFrame.from_dict(
            {i: count_dict[i] for i in count_dict.keys()},
            orient="index",
        )
        df_trans = df_corr.transpose()
        return df_trans

    def confusion_heatmap(
        self,
        df_conf: pd.DataFrame,
        main_title: str,
        out_path: str,
        x_lab: str = "Stimulus Category",
        y_lab: str = "Participant Endorsement",
    ):
        """Draw a heatmap from a confusion matrix.

        Parameters
        ----------
        df_conf : pd.DataFrame, _DescStat.confusion_matrix

        """
        # Draw and write
        ax = sns.heatmap(df_conf)
        ax.set(xlabel=x_lab, ylabel=y_lab)
        ax.set_title(main_title)
        plt.savefig(out_path, bbox_inches="tight")
        print(f"\t\tDrew heat-prob plot : {out_path}")
        plt.close()


# %%
class Visit1Stats(_DescStat):
    """Title.

    Attributes
    ----------
    df : pd.DataFrame
        Survey-specific dataframe
    col_data : list
        Column names of df relevant to survey

    Methods
    -------


    """

    def __init__(self, csv_path, survey_name):
        """Initialize.

        Setup and construct dataframe of survey responses.

        Parameters
        ----------
        csv_path : path
            Location of cleaned survey CSV, requires columns
            "study_id" and "<survey_name>_*".
        survey_name : str
            Short name of survey, found in column names of CSV

        Attributes
        ----------
        df : pd.DataFrame
            Survey-specific dataframe

        Raises
        ------
        FileNotFoundError
            File missing at csv_path
        KeyError
            Missing column names

        """
        print("Initializing Visit1Stats")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Missing file : {csv_path}")

        # Validate, make dataframe
        df = pd.read_csv(csv_path)
        col_names = df.columns
        if "study_id" not in col_names:
            raise KeyError("Expected dataframe to have column : study_id")
        comb_names = "\t".join(col_names)
        if survey_name not in comb_names:
            raise KeyError(
                f"Expected dataframe column that contains : {survey_name}"
            )
        df = df.set_index("study_id")
        super().__init__(df)
        self._prep_df(survey_name)

    def _prep_df(self, name: str):
        """Title."""
        self.df = self.df.drop(labels=["datetime"], axis=1)
        self.col_data = [x for x in self.df.columns if name in x]
        self.df[self.col_data] = self.df[self.col_data].astype("Int64")

    def write_stats(self, out_path, title):
        """Title.

        Parameters
        ----------

        """
        out_ext = out_path.split(".")[-1]
        if out_ext != "json":
            raise ValueError("Expected output file extension json")

        # Get desired mean/std
        stats = self.calc_total_stats()
        report_dict = {"Title": title}
        report_dict.update(stats)
        with open(out_path, "w") as jf:
            json.dump(report_dict, jf)
        print(f"\t\tSaved descriptive stats : {out_path}")
        return report_dict


# %%
class Visit23Stats(_DescStat):
    """Title."""

    def __init__(self, day2_csv_path, day3_csv_path, survey_name):
        """Title."""
        #
        print("Initializing Visit23Stats")
        self.survey_name = survey_name
        df2 = pd.read_csv(day2_csv_path)
        df3 = pd.read_csv(day3_csv_path)
        if not self._valid_df(df2) or not self._valid_df(df3):
            raise ValueError("Missing expected columns in dataframes")

        # TODO Check that column names are equal between df2, df3
        self.col_data = [x for x in df2.columns if self.survey_name in x]
        df_day2 = self._prep_df(df2, "Visit 2")
        df_day3 = self._prep_df(df3, "Visit 3")
        df = pd.concat([df_day2, df_day3], ignore_index=True)
        super().__init__(df)
        del df2, df3, df_day2, df_day3

    def _valid_df(self, df: pd.DataFrame) -> bool:
        """Title."""
        col = df.columns
        comb = "\t".join(col)
        out_bool = (
            True if "study_id" in col and self.survey_name in comb else False
        )
        return out_bool

    def _prep_df(self, df: pd.DataFrame, fac: str) -> pd.DataFrame:
        """Title."""
        df = df.drop(labels=["datetime"], axis=1)
        df[self.col_data] = df[self.col_data].astype("Int64")
        df["visit"] = fac
        return df

    def write_stats(self, out_path, title):
        """Title.

        Parameters
        ----------

        """
        out_ext = out_path.split(".")[-1]
        if out_ext != "json":
            raise ValueError("Expected output file extension json")

        # Get desired mean/std
        stats = self.calc_factor_stats("visit", "Visit 2", "Visit 3")
        report_dict = {"Title": title}
        report_dict.update(stats)
        with open(out_path, "w") as jf:
            json.dump(report_dict, jf)
        print(f"\t\tSaved descriptive stats : {out_path}")
        return report_dict


# %%
class RestRatings(_DescStat):
    """Title."""

    def __init__(self, proj_dir):
        """Title."""
        self._proj_dir = proj_dir
        df_long = self._get_data()
        super().__init__(df_long)

    def _get_data(self):
        """Title."""
        # Identify rest-ratings dataframes
        day2_path = os.path.join(
            self._proj_dir,
            "data_survey/visit_day2/data_clean",
            "df_rest-ratings.csv",
        )
        day3_path = os.path.join(
            self._proj_dir,
            "data_survey/visit_day3/data_clean",
            "df_rest-ratings.csv",
        )
        for day in [day2_path, day3_path]:
            if not os.path.exists(day):
                raise FileNotFoundError(f"Expected to find file : {day}")

        # Make master dataframe
        df_day2 = pd.read_csv(day2_path, na_values=88)
        df_day3 = pd.read_csv(day3_path, na_values=88)
        df_rest_all = pd.concat([df_day2, df_day3], ignore_index=True)
        df_rest_all = df_rest_all.sort_values(
            by=["study_id", "visit", "resp_type"]
        ).reset_index(drop=True)
        df_rest_all["task"] = df_rest_all["task"].str.title()
        df_rest_all["task"] = df_rest_all["task"].replace("Movies", "Videos")
        del df_day2, df_day3

        # Subset df for integer responses
        df_rest_int = df_rest_all[
            df_rest_all["resp_type"] == "resp_int"
        ].copy()
        df_rest_int = df_rest_int.drop(["datetime", "resp_type"], axis=1)
        excl_list = ["study_id", "visit", "task"]
        emo_list = [x for x in df_rest_int.columns if x not in excl_list]
        df_rest_int[emo_list] = df_rest_int[emo_list].astype("Int64")

        # Convert to long form, organize columns
        df_long = pd.melt(
            df_rest_int,
            id_vars=["study_id", "visit", "task"],
            value_vars=emo_list,
            var_name="emotion",
            value_name="rating",
        )
        df_long["emotion"] = df_long["emotion"].str.title()
        df_long["rating"] = df_long["rating"].astype("Int64")
        for str_col in ["emotion", "visit", "task"]:
            df_long[str_col] = df_long[str_col].astype(pd.StringDtype())
        df_long = df_long.dropna(axis=0)
        return df_long

    def write_stats(self):
        """Title."""
        df_stats = self.calc_long_stats("task", "emotion")
        out_stats = os.path.join(
            self._proj_dir,
            "analyses/surveys_stats_descriptive",
            "stats_rest-ratings.csv",
        )
        df_stats.to_csv(out_stats)
        print(f"\tWrote csv : {out_stats}")
        return df_stats


class StimRatings(_DescStat):
    """Generate descriptives for stimulus ratings survey.

    Calculate descriptive statistics and draw plots for
    endorsement, arousal, and valence responses.

    Attributes
    ----------
    out_dir : path
        Output destination for generated files
    df_all : pd.DataFrame
        Day2, day3 stimulus ratings
    emo_list : list
        Emotion categories of stimuli

    Methods
    -------
    endorsement(stim_type)
        Generate stats and plots for endorsement responses
    arousal_valence(stim_type, prompt_name)
        Generate stats and plots for arousal and valence responses

    """

    def __init__(self, proj_dir):
        """Initialize.

        Parameters
        ----------
        proj_dir : path
            Location of project's experiment directory

        Attributes
        ----------
        out_dir : path
            Output destination for generated files

        """
        print("\nInitializing DescriptStimRatings")
        self._proj_dir = proj_dir
        self.out_dir = os.path.join(
            proj_dir, "analyses/surveys_stats_descriptive"
        )
        df = self._get_data()
        super().__init__(df)

    def _get_data(self) -> pd.DataFrame:
        """Make a dataframe of stimulus ratings.

        Attributes
        ----------
        emo_list : list
            Emotion categories of stimuli

        """
        day2_path = os.path.join(
            self._proj_dir,
            "data_survey/visit_day2/data_clean",
            "df_post_scan_ratings.csv",
        )
        day3_path = os.path.join(
            self._proj_dir,
            "data_survey/visit_day3/data_clean",
            "df_post_scan_ratings.csv",
        )
        for day in [day2_path, day3_path]:
            if not os.path.exists(day):
                raise FileNotFoundError(f"Expected to find : {day}")

        # Combine visit data
        df_day2 = pd.read_csv(day2_path)
        df_day3 = pd.read_csv(day3_path)
        df_all = pd.concat([df_day2, df_day3], ignore_index=True)
        df_all = df_all.sort_values(
            by=["study_id", "session", "type", "emotion", "prompt"]
        ).reset_index(drop=True)
        df_all["emotion"] = df_all["emotion"].str.title()
        for col_name in ["session", "type", "emotion", "prompt"]:
            df_all[col_name] = df_all[col_name].astype(pd.StringDtype())
        self.emo_list = df_all["emotion"].unique().tolist()
        self.df_all = df_all
        return df_all

    def endorsement(self, stim_type):
        """Generate descriptive info for emotion endorsements.

        Caculate proportion of ratings for each emotion category, save
        dataframe and draw confusion matrix.

        Parameters
        ----------
        stim_type : str
            [Videos | Scenarios]
            Stimulus modality of session

        Returns
        -------
        pd.DataFrame
            Columns = participant endorsement proportions
            Rows = emotion category

        Raises
        ------
        ValueError
            Unexpected stimulus type

        """
        if stim_type not in ["Videos", "Scenarios"]:
            raise ValueError(f"Unexpected stimulus type : {stim_type}")
        print(f"\tGenerating descriptives of endorsement for : {stim_type}")

        # Get endorsement data for stimulus type
        df_end = self.df_all[
            (self.df_all["type"] == stim_type)
            & (self.df_all["prompt"] == "Endorsement")
        ].copy()

        # Generate confusion matrix of endorsement probabilities
        out_stat = os.path.join(
            self.out_dir,
            f"stats_stim-ratings_endorsement_{stim_type.lower()}.csv",
        )
        self.df = df_end
        df_conf = self.confusion_matrix(self.emo_list, "study_id", 5)
        df_conf.to_csv(out_stat, index=False)
        print(f"\t\tWrote dataset : {out_stat}")

        # Draw heatmap
        out_plot = os.path.join(
            self.out_dir,
            f"plot_heatmap_stim-ratings_endorsement_{stim_type.lower()}.png",
        )
        self.confusion_heatmap(
            df_conf,
            main_title=f"Post-Scan {stim_type[:-1]} Endorsement Proportion",
            out_path=out_plot,
        )
        return df_conf

    def arousal_valence(self, stim_type):
        """Generate descriptive info for emotion valence and arousal ratings.

        Parameters
        ----------
        stim_type : str
            [Videos | Scenarios]
            Stimulus modality of session

        Returns
        -------
        pd.DataFrame
            Columns = participant
            Rows = emotion category

        Raises
        ------
        ValueError
            Unexpected stimulus type
            Unexpected prompt type

        """
        if stim_type not in ["Videos", "Scenarios"]:
            raise ValueError(f"Unexpected stimulus type : {stim_type}")

        # Get relevant data
        print(f"\tGenerating descriptives for {stim_type}: Arousal, Valence")
        df = self.df_all[
            (self.df_all["type"] == stim_type)
            & (self.df_all["prompt"] != "Endorsement")
        ].copy()
        df["response"] = df["response"].astype("Int64")

        # Calculate descriptive stats
        self.df = df
        df_stats = self.calc_long_stats("prompt", "emotion")

        # Write stats
        out_path = os.path.join(
            self.out_dir,
            f"stats_stim-ratings_{stim_type.lower()}.csv",
        )
        df_stats.to_csv(out_path)
        print(f"\t\tWrote dataset : {out_path}")

        #
        out_plot = os.path.join(
            self.out_dir,
            f"plot_boxplot-long_stim-ratings_{stim_type.lower()}.png",
        )
        self.draw_long_boxplot(
            x_col="emotion",
            x_lab="Emotion Category",
            y_col="response",
            y_lab="Rating",
            hue_order=["Arousal", "Valence"],
            hue_col="prompt",
            main_title=f"Post-Scan {stim_type[:-1]} Ratings",
            out_path=out_plot,
        )
        return df_stats


# %%
class EmorepTask(_DescStat):
    """Generate descriptive stats and plots for EmoRep task data.

    Use all available BIDS events files to generate dataframes of
    emotion and intensity selection data. Then generate descriptive
    stats and plots.

    Output plots and dataframes are written to:
        <out_dir>/[plot*|stats]_task-[emotion|intensity]*

    Attributes
    ----------
    df_int : pd.DataFrame
        Long-formatted dataframe of participant intensity selections
    df_emo : pd.DataFrame
        Long-formatted dataframe of participant emotion selections
    out_dir : path
        Location of output directory

    Methods
    -------
    desc_intensity()
        Generate descriptive stats and plots for intensity selection trials
    desc_emotion()
        Generate descriptive stats and plots for emotion selection trials

    """

    def __init__(self, proj_dir):
        """Initialize.

        Find all participant BIDS events files, trigger construction
        of dataframe from events files.

        Parameters
        ----------
        proj_dir : path
            Location of project's experiment directory

        Attributes
        ----------
        out_dir : path
            Location of output directory
        _events_all : list
            All participant events files
        _task_list : list
            Session stimulus types

        Raises
        ------
        ValueError
            Events files were not detected

        """
        print("\nInitializing DescriptTask")
        self._task_list = ["movies", "scenarios"]
        self.out_dir = os.path.join(
            proj_dir, "analyses/surveys_stats_descriptive"
        )

        # Find all events files, make dataframe
        mri_rawdata = os.path.join(proj_dir, "data_scanner_BIDS", "rawdata")
        self._events_all = sorted(
            glob.glob(f"{mri_rawdata}/**/*_events.tsv", recursive=True)
        )
        if not self._events_all:
            raise ValueError(
                f"Expected to find BIDS events files in : {mri_rawdata}"
            )
        self._mk_master_df()

    def _mk_master_df(self):
        """Combine all events files into dataframe.

        Aggregate all events data info dataframe, then remove trials
        and columns not pertaining to stimulus responses. Generate
        dataframes for intensity and emotion selection trials.

        Attributes
        ----------
        df_int : pd.DataFrame
            Long-formatted dataframe of participant intensity selections
        df_emo : pd.DataFrame
            Long-formatted dataframe of participant emotion selections

        """
        print("\tBuilding dataframe of all participant events.tsv")

        # Build dataframe of all events data
        df_all = pd.DataFrame(
            columns=[
                "onset",
                "duration",
                "trial_type",
                "stim_info",
                "response",
                "response_time",
                "accuracy",
                "emotion",
                "subj",
                "sess",
                "task",
                "run",
            ]
        )
        for event_path in self._events_all:
            subj, sess, task, run, _ = os.path.basename(event_path).split("_")
            df = pd.read_csv(event_path, sep="\t")
            df["subj"] = subj.split("-")[-1]
            df["sess"] = sess.split("-")[-1]
            df["task"] = task.split("-")[-1]
            df["run"] = int(run[-1])
            df_all = pd.concat([df_all, df], ignore_index=True)
            del df

        # Reduce dataframe to relevant rows and columns. Clean dataframe.
        df_resp = df_all.loc[
            df_all["trial_type"].isin(
                ["movie", "scenario", "emotion", "intensity"]
            )
        ].reset_index(drop=True)
        del df_all
        df_resp["emotion"] = df_resp["emotion"].fillna(method="ffill")
        df_resp = df_resp.loc[
            ~df_resp["trial_type"].isin(["movie", "scenario"])
        ].reset_index(drop=True)
        df_resp = df_resp.drop(
            ["onset", "duration", "accuracy", "stim_info"], axis=1
        )
        df_resp["emotion"] = df_resp["emotion"].str.title()
        df_resp["task"] = df_resp["task"].str.title()
        df_resp["task"] = df_resp["task"].replace("Movies", "Videos")
        df_resp["run"] = df_resp["run"].astype("Int64")
        super().__init__(df_resp)
        self.df_all = df_resp

    def desc_intensity(self):
        """Generate descriptive stats and plots for intensity selection.

        Output dataframe written to:
            <out_dir>/stats_task-intensity.csv

        Output plots written to:
            <out_dir>/plot_violin_task-intensity_*.png

        Returns
        -------
        pd.DataFrame
            Descriptive stats of task, emotion

        """
        # Generate and clean dataframe for intensity selection
        df_int = self.df_all.loc[
            self.df_all["trial_type"] == "intensity"
        ].copy()
        df_int["response"] = df_int["response"].replace("NONE", np.nan)
        df_int = df_int.reset_index(drop=True)
        df_int = df_int.drop(
            ["response_time", "run", "trial_type", "sess"], axis=1
        )
        df_int["task"] = df_int["task"].str.title()
        df_int["response"] = df_int["response"].astype("Int64")
        for str_col in ["emotion", "task"]:
            df_int[str_col] = df_int[str_col].astype(pd.StringDtype())
        df_int = df_int.sort_values(by=["subj", "emotion", "task"])

        #
        self.df = df_int
        df_stats = self.calc_long_stats(grp_a="task", grp_b="emotion")
        out_csv = os.path.join(self.out_dir, "stats_task-intensity.csv")
        df_stats.to_csv(out_csv)
        print(f"\tWrote csv : {out_csv}")

        # Make violin plots
        out_plot = os.path.join(
            self.out_dir,
            "plot_boxplot-long_task-intensity.png",
        )
        self.draw_long_boxplot(
            x_col="emotion",
            x_lab="Emotion",
            y_col="response",
            y_lab="Intensity",
            hue_order=["Scenarios", "Videos"],
            hue_col="task",
            main_title="In-Scan Stimulus Ratings",
            out_path=out_plot,
        )
        return df_stats

    def desc_emotion(self, task):
        """Generate descriptive stats and plots for emotion selection.

        Output dataframe written to:
            <out_dir>/stats_task-emotion_<task>.csv

        Output plots written to:
            <out_dir>/plot_heat-prob_task-emotion_<task>.png

        Parameters
        ----------
        task

        Returns
        -------
        dict
            pd.DataFrames of confusion matrices for each task

        """
        # Generate and clean dataframe for emotion selection
        df_emo = self.df_all.loc[
            (self.df_all["trial_type"] == "emotion")
            & (self.df_all["task"] == task)
        ].copy()
        df_emo["response"] = df_emo["response"].str.title()
        df_emo = df_emo.reset_index(drop=True)

        #
        emo_all = df_emo["emotion"].unique().tolist()
        emo_all.sort()
        out_data = os.path.join(
            self.out_dir, f"stats_task-emotion_{task.lower()}.csv"
        )
        self.df = df_emo
        df_conf = self.confusion_matrix(emo_all, "subj", 2)
        df_conf.to_csv(out_data, index=False)
        print(f"\t\tWrote dataset : {out_data}")

        # Draw heatmap
        out_plot = os.path.join(
            self.out_dir,
            f"plot_heatmap_task-emotion_{task.lower()}.png",
        )
        self.confusion_heatmap(
            df_conf,
            main_title=f"In-Scan {task[:-1]} Endorsement Proportion",
            out_path=out_plot,
        )
        return df_conf


# %%
