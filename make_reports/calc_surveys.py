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
class DescriptRedcapQualtrics:
    """Describe survey responses.

    Generate descriptive stats and plots of
    REDCap and Qualtrics survey data.

    Attributes
    ----------
    df : pd.DataFrame
        Survey-specific dataframe
    df_col : list
        Column names of df relevant to survey

    Methods
    -------
    write_mean_std(out_path, title, total_avg="total")
        Get and write mean, std for survey responses
    violin_plot(out_path, title, total_avg="total")
        Generate violin plot of survey responses

    """

    def __init__(self, proj_dir, csv_path, survey_name):
        """Initialize.

        Setup and construct dataframe of survey responses.

        Parameters
        ----------
        proj_dir : path
            Location of project's experiment directory
        csv_path : path
            Location of cleaned survey CSV, requires columns
            "study_id" and "<survey_name>_*".
        survey_name : str
            Short name of survey, found in column names of CSV

        Raises
        ------
        FileNotFoundError
            File missing at csv_path
        KeyError
            Missing column names

        """
        # Check for file
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
        self._prep_df(df, survey_name)

    def _prep_df(self, df, name):
        """Make dataframe of survey responses.

        Extract survey responses from larger, aggregate dataframe.

        Parameters
        ----------
        df : pd.DataFrame
            Survey dataset
        name : str
            Column substring, for subsetting dataframe

        Attributes
        ----------
        df : pd.DataFrame
            Survey-specific dataframe
        df_col : list
            Column names of df relevant to survey

        """
        df = df.drop(labels=["datetime"], axis=1)
        col_list = [x for x in df.columns if name in x]
        df[col_list] = df[col_list].astype("Int64")
        self.df = df
        self.df_col = col_list

    def _calc_df_stats(self):
        """Calculate descriptive stats for entire dataframe."""
        self._mean = round(self.df.stack().mean(), 2)
        self._std = round(self.df.stack().std(), 2)

    def _calc_row_stats(self):
        """Calculate descriptive stats for participant totals."""
        self.df["total"] = self.df[self.df_col].sum(axis=1)
        self._mean = round(self.df["total"].mean(), 2)
        self._std = round(self.df["total"].std(), 2)

    def _validate_total_avg(self, total_avg):
        """Check input parameter."""
        if total_avg not in ["total", "avg"]:
            raise ValueError(f"Unexpected total_avg values : {total_avg}")

    def write_mean_std(self, out_path, title, total_avg="total"):
        """Write mean and std to json.

        Parameters
        ----------
        out_path : path
            Output path, including file name
        title : str
            Survey name
        total_avg : str, optional
            [total | avg]
            Toggle reporting metrics of group average or
            participant totals.

        Returns
        -------
        dict
            Descriptive stats

        Raises
        ------
        ValueError
            Unexpected total_avg parameter

        """
        self._validate_total_avg(total_avg)

        # Get desired mean/std
        if total_avg == "total":
            self._calc_row_stats()
        elif total_avg == "avg":
            self._calc_df_stats()

        # Setup json content, write
        report = {
            "Title": title,
            "n": self.df.shape[0],
            "mean": self._mean,
            "std": self._std,
        }
        with open(out_path, "w") as jf:
            json.dump(report, jf)
        print(f"\t\tSaved descriptive stats : {out_path}")
        return report

    def violin_plot(self, out_path, title, total_avg="total"):
        """Make violin plot of survey responses.

        Parameters
        ----------
        out_path : path
            Output path, including file name
        title : str
            Survey name
        total_avg : str, optional
            [total | avg]
            Toggle reporting metrics of group average or
            participant totals.

        Returns
        -------
        path
            Location and name of violin plot

        """
        self._validate_total_avg(total_avg)

        # Get required metrics
        if total_avg == "total":
            self._calc_row_stats()
            x_label = "Total"
        elif total_avg == "avg":
            self._calc_df_stats()
            x_label = "Average"

        # Setup plotting column
        df_work = self.df.copy()
        if total_avg == "total":
            df_work["plot"] = df_work["total"]
        elif total_avg == "avg":
            df_work["plot"] = df_work[self.df_col].mean(axis=1)

        # Draw violin
        lb = self._mean - (3 * self._std)
        ub = self._mean + (3 * self._std)
        fig, ax = plt.subplots()
        sns.violinplot(x=df_work["plot"])
        ax.collections[0].set_alpha(0.5)
        ax.set_xlim(lb, ub)
        plt.title(title)
        plt.ylabel("Response Density")
        plt.xlabel(f"Participant {x_label}")
        plt.text(
            lb + (0.2 * self._std),
            -0.42,
            f"mean(sd) = {self._mean}({self._std})",
            horizontalalignment="left",
        )

        # Save and return
        plt.savefig(out_path)
        plt.close(fig)
        print(f"\t\tDrew violin plot : {out_path}")
        return out_path


# %%
def _split_violin_plots(
    emo_all,
    df,
    sub_col,
    x_col,
    x_lab,
    y_col,
    y_lab,
    hue_col,
    hue_order,
    title,
    out_path,
):
    """Make violin plots for groups of emotions.

    Split emo_all into 4 groups and draw violin plots for each. The
    file name in out_path will have an appended suffix for the plot
    number (file_name.png -> file_name_1.png).

    Parameters
    ----------
    emo_all : list
        All emotion categories
    df : pd.DataFrame
        Long-formatted dataframe
    sub_col : str
        df column name for identifying subjects
    x_col : str
        df column name to be used as plot x-axis
    x_lab : str
        Plot x-axis label
    y_col : str
        df column name to be used as plot y-axis
    y_lab : str
        Plot y-axis label
    hue_col : str
        df column name to be used as group factor
    hue_order : str
        Group legend order
    title : str
        Plot title
    out_path : path
        Output location and name of file

    Raises
    ------
    KeyError
        Missing required keys in df
    TypeError
        df columns improper type
    ValueError
        Incorrect number of emotions

    """
    # Validate
    for _col in [sub_col, x_col, y_col, hue_col]:
        if _col not in df.columns:
            raise KeyError(f"Missing expected column in df : {_col}")
    if len(emo_all) != 15:
        raise ValueError(
            f"Expected emo_all to have len == 15, found : {len(emo_all)}"
        )
    if not is_numeric_dtype(df[y_col]):
        raise TypeError("df[y_col] should be numeric type")
    if not is_string_dtype(df[x_col]) or not is_string_dtype(df[hue_col]):
        raise TypeError("df[x_col] and df[hue_col] should be string type")

    # Divide emotion list
    emo_a = emo_all[:4]
    emo_b = emo_all[4:8]
    emo_c = emo_all[8:12]
    emo_d = emo_all[12:]

    # Make each subplot
    for cnt, emo in enumerate([emo_a, emo_b, emo_c, emo_d]):
        df_plot = df[df[sub_col].isin(emo)].copy()
        df_plot[y_col] = df_plot[y_col].astype(float)

        # Draw violin plots
        fix, ax = plt.subplots()
        sns.violinplot(
            x=x_col,
            y=y_col,
            hue=hue_col,
            data=df_plot,
            hue_order=hue_order,
        )
        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
        plt.title(title)
        plt.ylabel(y_lab)
        plt.xlabel(x_lab)
        plt.xticks(rotation=45, ha="right")

        # Save and close
        out_plot = out_path.replace(".png", f"_{cnt+1}.png")
        plt.savefig(out_plot, bbox_inches="tight")
        plt.close()
        print(f"\tDrew violin plot : {out_plot}")


# %%
def _confusion_matrix(
    df,
    emo_list,
    subj_col,
    num_exp,
    out_path,
    emo_col="emotion",
    resp_col="response",
):
    """Generate a confusion matrix of emotion endorsements.

    Determine how likely (proportion) each emotion is endorsed
    as itself and others.

    Parameters
    ----------
    df : pd.DataFrame
        Long-formatted dataframe
    emo_list : list
        All emotion categories
    subj_col : str
        df column name for identifying subjects
    num_exp : int
        Number of expected emotion endorsements from each subejct
    out_path : path
        Output location and file name
    emo_col : str, optional
        df column name for identifying emotion (stimulus)
    resp_col : str, optional
        df column name for identifying participant endorsements

    Returns
    -------
    pd.DataFrame
        Confusion matrix

    Raises
    ------
    KeyError
        Missing columns in df
    TypeError
        df columns wrong type
        num_exp wrong type
    ValueError
        Unexpected number of emotions

    """
    # Validate
    for _col in [subj_col, emo_col, resp_col]:
        if _col not in df.columns:
            raise KeyError(f"Missing expected column in df : {_col}")
    if len(emo_list) != 15:
        raise ValueError(
            f"Expected emo_list to have len == 15, found : {len(emo_list)}"
        )
    if not is_string_dtype(df[emo_col]) or not is_string_dtype(df[resp_col]):
        raise TypeError("df[y_col] and df[hue_col] should be string type")
    if not isinstance(num_exp, int):
        raise TypeError("Expected type int for num_exp")

    # Set denominator for proportion calc
    max_total = num_exp * len(df[subj_col].unique())

    # Calc proportion each emotion is endorsed as every emotion
    count_dict = {}
    for emo in emo_list:
        count_dict[emo] = {}
        df_emo = df[df[emo_col] == emo]
        for sub_emo in emo_list:
            count_emo = len(df_emo[df_emo[resp_col].str.contains(sub_emo)])
            count_dict[emo][sub_emo] = round(count_emo / max_total, 2)
    del df_emo

    # Generate dataframe and transponse for intuitive stimulus-response axes
    df_corr = pd.DataFrame.from_dict(
        {i: count_dict[i] for i in count_dict.keys()},
        orient="index",
    )
    df_trans = df_corr.transpose()
    df_trans.to_csv(out_path, index=False)
    print(f"\t\tWrote dataset : {out_path}")
    return df_trans


# %%
def _confusion_heatmap(
    df_conf,
    title,
    out_path,
    x_lab="Stimulus Category",
    y_lab="Participant Endorsement",
):
    """Draw a heatmap from a confusion matrix.

    Parameters
    ----------
    df_conf : pd.DataFrame, from _confusion_matrix
        Confusion matrix
    title : str
        Plot title
    out_path : path
        Output location and name
    x_lab : str, optional
        Plot label for x-axis
    y_lab : str, optional
        Plot label for y-axis

    """
    # Draw and write
    ax = sns.heatmap(df_conf)
    ax.set(xlabel=x_lab, ylabel=y_lab)
    ax.set_title(title)
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    print(f"\t\tDrew heat-prob plot : {out_path}")


# %%
def descript_rest_ratings(proj_dir):
    """Generate descriptive stats for resting state ratings.

    Calculate average responses for each emotion by day, then
    produce violin plots.

    Stats are written to:
        <proj_dir>/analyses/surveys_stats_descriptive/stats_rest-ratings.csv

    Plots are written to:
        <proj_dir>/analyses/surveys_stats_descriptive/plot_violin_rest-ratings_*.png

    Parameters
    ----------
    proj_dir : path
        Location of project's experiment directory

    Returns
    -------
    pd.DataFrame
        Descriptive stats organized by day and emotion

    Raises
    ------
    FileNotFoundError
        Missing cleaned rest-rating dataframe

    """
    # Identify rest-ratings dataframes
    day2_path = os.path.join(
        proj_dir, "data_survey/visit_day2/data_clean", "df_rest-ratings.csv"
    )
    day3_path = os.path.join(
        proj_dir, "data_survey/visit_day3/data_clean", "df_rest-ratings.csv"
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
    del df_day2, df_day3

    # Get integer responses
    df_rest_int = df_rest_all[df_rest_all["resp_type"] == "resp_int"].copy()
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
    df_long = df_long.dropna(axis=0)
    emo_title = [x.title() for x in emo_list]

    # Make dataframe of descriptives for reporting, write out
    df_mean = df_long.groupby(["task", "emotion"]).mean()
    df_mean = df_mean.rename(columns={"rating": "mean"})
    df_mean["mean"] = round(df_mean["mean"], 2)

    df_std = df_long.groupby(["task", "emotion"]).std()
    df_std = df_std.rename(columns={"rating": "std"})
    df_std["std"] = round(df_std["std"], 2)
    df_mean["std"] = df_std["std"]

    out_dir = os.path.join(proj_dir, "analyses/surveys_stats_descriptive")
    out_csv = os.path.join(out_dir, "stats_rest-ratings.csv")
    df_mean.to_csv(out_csv)
    print(f"\tWrote csv : {out_csv}")

    # Draw plots
    out_path = os.path.join(
        out_dir,
        "plot_violin_rest-ratings.png",
    )
    _split_violin_plots(
        emo_title,
        df=df_long,
        sub_col="emotion",
        x_col="emotion",
        x_lab="Emotion",
        y_col="rating",
        y_lab="Frequency",
        hue_col="task",
        hue_order=["movies", "scenarios"],
        title="Emotion Frequence During Rest",
        out_path=out_path,
    )
    return df_mean


class DescriptStimRatings:
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
        self._get_data()

    def _get_data(self):
        """Make a dataframe of stimulus ratings.

        Attributes
        ----------
        df_all : pd.DataFrame
            Day2, day3 stimulus ratings
        emo_list : list
            Emotion categories of stimuli

        Raises
        ------
        FileNotFoundError
            Missing expected stimulus ratings CSV

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
        self.emo_list = df_all["emotion"].unique().tolist()
        self.df_all = df_all

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
        df_conf = _confusion_matrix(
            df_end, self.emo_list, "study_id", 5, out_stat
        )

        # Draw heatmap
        out_plot = os.path.join(
            self.out_dir,
            f"plot_heat-prob_stim-ratings_endorsement_{stim_type.lower()}.png",
        )
        title = f"Post-Scan {stim_type[:-1]} Endorsement Proportion"
        _confusion_heatmap(df_conf, title, out_plot)
        return df_conf

    def arousal_valence(self, stim_type, prompt_name):
        """Generate descriptive info for emotion valence and arousal ratings.

        Parameters
        ----------
        stim_type : str
            [Videos | Scenarios]
            Stimulus modality of session
        prompt_name : str
            [Arousal | Valence]
            Response prompt type

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
        if prompt_name not in ["Arousal", "Valence"]:
            raise ValueError(f"Unexpected prompt type : {prompt_name}")

        # Get relevant data
        print(f"\tGenerating descriptives of {prompt_name} for : {stim_type}")
        df = self.df_all[
            (self.df_all["type"] == stim_type)
            & (self.df_all["prompt"] == prompt_name)
        ].copy()
        df["response"] = df["response"].astype("Int64")

        # Calculate descriptive stats
        response_dict = {}
        for emo in self.emo_list:
            _mean = df.loc[df["emotion"] == emo, "response"].mean()
            _std = df.loc[df["emotion"] == emo, "response"].std()
            response_dict[emo] = {
                "mean": round(_mean, 2),
                "std": round(_std, 2),
            }
        df_stat = pd.DataFrame.from_dict(response_dict).transpose()

        # Write stats
        out_path = os.path.join(
            self.out_dir,
            "stats_stim-ratings_"
            + f"{prompt_name.lower()}_{stim_type.lower()}.csv",
        )
        df_stat.to_csv(out_path)
        print(f"\t\tWrote dataset : {out_path}")

        # Draw violin plot
        df["response"] = df["response"].astype("float")
        fig, ax = plt.subplots()
        sns.violinplot(x="emotion", y="response", data=df)
        plt.title(f"{stim_type[:-1]} {prompt_name} Ratings")
        plt.ylabel(f"{prompt_name} Rating")
        plt.xlabel("Emotion")
        plt.xticks(rotation=45, horizontalalignment="right")

        # Write violin plot
        out_path = os.path.join(
            self.out_dir,
            "plot_violin_stim-ratings_"
            + f"{prompt_name.lower()}_{stim_type.lower()}.png",
        )
        plt.subplots_adjust(bottom=0.25, left=0.1)
        plt.savefig(out_path)
        plt.close(fig)
        print(f"\t\tDrew violin plot : {out_path}")
        return df_stat


# %%
class DescriptTask:
    """Generate descriptive stats and plots for EmoRep task data.

    Use all available BIDS events files to generate dataframes of
    emotion and intensity selection data. Then generate descriptive
    stats and plots.

    Output plots and dataframes are written to:
        <out_dir>/[plot*|stats]_task-[emotion|intensity]*

    Attributes
    ----------
    df_intensity : pd.DataFrame
        Long-formatted dataframe of participant intensity selections
    df_emotion : pd.DataFrame
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
        df_intensity : pd.DataFrame
            Long-formatted dataframe of participant intensity selections
        df_emotion : pd.DataFrame
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
        df_resp["run"] = df_resp["run"].astype("Int64")

        # Generate and clean dataframe for intensity selection
        df_intensity = df_resp.loc[df_resp["trial_type"] == "intensity"].copy()
        df_intensity["response"] = df_intensity["response"].replace(
            "NONE", np.nan
        )
        df_intensity = df_intensity.reset_index(drop=True)
        df_intensity["response"] = df_intensity["response"].astype("Int64")
        self.df_intensity = df_intensity

        # Generate and clean dataframe for emotion selection
        df_emotion = df_resp.loc[df_resp["trial_type"] == "emotion"].copy()
        df_emotion["response"] = df_emotion["response"].str.title()
        df_emotion = df_emotion.reset_index(drop=True)
        self.df_emotion = df_emotion

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
        # Populate a dataframe of descriptive stats for each task and emotion
        emo_all = self.df_intensity["emotion"].unique().tolist()
        df_avg = pd.DataFrame(columns=["emotion", "mean", "std", "task"])
        for task in self._task_list:
            response_dict = {}
            for emo in emo_all:
                df = self.df_intensity[
                    (self.df_intensity["emotion"] == emo)
                    & (self.df_intensity["task"] == task)
                ]
                response_dict[emo] = {
                    "mean": round(df.response.mean(), 2),
                    "std": round(df.response.std(), 2),
                }
            df_tmp = pd.DataFrame.from_dict(response_dict).transpose()
            df_tmp.index = df_tmp.index.set_names(["emotion"])
            df_tmp = df_tmp.reset_index()
            df_tmp["task"] = task
            df_avg = pd.concat([df_avg, df_tmp], ignore_index=True)

        # Organize and save dataframe
        col_task = df_avg.pop("task")
        df_avg.insert(0, "task", col_task)
        out_csv = os.path.join(self.out_dir, "stats_task-intensity.csv")
        df_avg.to_csv(out_csv, index=False)
        print(f"\tWrote csv : {out_csv}")

        # Make violin plots
        out_path = os.path.join(
            self.out_dir,
            "plot_violin_task-intensity.png",
        )
        _split_violin_plots(
            emo_all,
            df=self.df_intensity,
            sub_col="emotion",
            x_col="emotion",
            x_lab="Emotion",
            y_col="response",
            y_lab="Intensity Rating",
            hue_col="task",
            hue_order=["movies", "scenarios"],
            title="Scan Stimulus Intensity",
            out_path=out_path,
        )
        return df_avg

    def desc_emotion(self):
        """Generate descriptive stats and plots for emotion selection.

        Output dataframe written to:
            <out_dir>/stats_task-emotion_<task>.csv

        Output plots written to:
            <out_dir>/plot_heat-prob_task-emotion_<task>.png

        Returns
        -------
        dict
            pd.DataFrames of confusion matrices for each task

        """
        emo_all = self.df_emotion["emotion"].unique().tolist()
        out_dict = {}
        for task in self._task_list:
            df_task = self.df_emotion.loc[self.df_emotion["task"] == task]
            out_data = os.path.join(
                self.out_dir, f"stats_task-emotion_{task}.csv"
            )
            df_conf = _confusion_matrix(
                df=df_task,
                emo_list=emo_all,
                subj_col="subj",
                num_exp=2,
                out_path=out_data,
            )
            out_dict[task] = df_conf
            out_plot = os.path.join(
                self.out_dir, f"plot_heat-prob_task-emotion_{task}.png"
            )
            title = f"Scan {task.title()[:-1]} Endorsement Proportion"
            _confusion_heatmap(df_conf, title, out_plot)
        return out_dict


# %%
