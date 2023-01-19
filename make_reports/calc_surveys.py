"""Methods for describing survey responses.

Generate descriptive statistics and plots for REDCap, Qualtrics,
and rest-rating surveys.

Output files are written to:
    experiment2/EmoRep/Exp2_Compute_Emotion/analyses/surveys_stats_descriptive

"""
import os
import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


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
    emo_list = [
        x for x in df_rest_int.columns if x != "study_id" and x != "visit"
    ]
    df_rest_int[emo_list] = df_rest_int[emo_list].astype("Int64")

    # Convert to long form, organize columns
    df_long = pd.melt(
        df_rest_int,
        id_vars=["study_id", "visit"],
        value_vars=emo_list,
        var_name="emotion",
        value_name="rating",
    )
    df_long["emotion"] = df_long["emotion"].str.title()
    df_long["visit"] = df_long["visit"].str.replace("ses-", "")
    df_long["rating"] = df_long["rating"].astype("Int64")
    df_long = df_long.dropna(axis=0)
    emo_title = [x.title() for x in emo_list]

    # Make dataframe of descriptives for reporting, write out
    df_mean = df_long.groupby(["visit", "emotion"]).mean()
    df_mean = df_mean.rename(columns={"rating": "mean"})
    df_mean["mean"] = round(df_mean["mean"], 2)
    df_std = df_long.groupby(["visit", "emotion"]).std()
    df_std = df_std.rename(columns={"rating": "std"})
    df_std["std"] = round(df_std["std"], 2)
    df_mean["std"] = df_std["std"]

    out_dir = os.path.join(proj_dir, "analyses/surveys_stats_descriptive")
    out_csv = os.path.join(out_dir, "stats_rest-ratings.csv")
    df_mean.to_csv(out_csv)
    print(f"\tWrote csv : {out_csv}")

    # Split plot into 4 subplots
    emo_a = emo_title[:4]
    emo_b = emo_title[4:8]
    emo_c = emo_title[8:12]
    emo_d = emo_title[12:]

    # Make each subplot
    for cnt, emo in enumerate([emo_a, emo_b, emo_c, emo_d]):
        df_sub = df_long[df_long["emotion"].isin(emo)]
        plot_dict = pd.DataFrame(df_sub.to_dict())

        # Draw violin plots
        fix, ax = plt.subplots()
        sns.violinplot(
            x="emotion",
            y="rating",
            hue="visit",
            data=plot_dict,
            hue_order=["day2", "day3"],
        )
        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
        plt.title("Emotion Frequence During Rest")
        plt.ylabel("Frequency")
        plt.xlabel("Emotion")
        plt.xticks(rotation=45, ha="right")

        # Save and close
        out_path = os.path.join(
            out_dir,
            f"plot_violin_rest-ratings_{cnt+1}.png",
        )
        plt.savefig(out_path, bbox_inches="tight")
        plt.close()
        print(f"\tDrew violin plot : {out_path}")

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

        # Set denominator for proportion calc
        num_subj = len(df_end["study_id"].unique())
        max_total = num_subj * 5

        # Calc proportion each emotion is endorsed as every emotion
        count_dict = {}
        for emo in self.emo_list:
            count_dict[emo] = {}
            df_emo = df_end[df_end["emotion"] == emo]
            for sub_emo in self.emo_list:
                count_emo = len(
                    df_emo[df_emo["response"].str.contains(sub_emo)]
                )
                count_dict[emo][sub_emo] = count_emo / max_total
        del df_end, df_emo
        df_corr = pd.DataFrame.from_dict(
            {i: count_dict[i] for i in count_dict.keys()},
            orient="index",
        )

        # Transpose for intuitive stimulus-response axes, write.
        df_trans = df_corr.transpose()
        out_path = os.path.join(
            self.out_dir,
            f"stats_stim-ratings_endorsement_{stim_type.lower()}.csv",
        )
        df_trans.to_csv(out_path)
        print(f"\t\tWrote dataset : {out_path}")

        # Draw and write
        ax = sns.heatmap(df_trans)
        ax.set(xlabel="Stimulus Category", ylabel="Participant Endorsement")
        ax.set_title(f"{stim_type[:-1]} Endorsement Proportion")
        out_path = os.path.join(
            self.out_dir,
            f"plot_heat-prob_stim-ratings_endorsement_{stim_type.lower()}.png",
        )
        plt.subplots_adjust(bottom=0.25, left=0.2)
        plt.savefig(out_path)
        plt.close()
        print(f"\t\tDrew heat-prob plot : {out_path}")
        return df_trans

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
