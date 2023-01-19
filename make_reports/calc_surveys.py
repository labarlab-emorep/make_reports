"""Title.

Calcualte survey stats:
    Scanner task metrics
    Rest rating metrics
    Stimulus rating metrics
    Survey metrics

"""
# %%
import os
import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# %%
class DescriptRedcapQualtrics:
    """Title.

    Desc.

    """

    def __init__(self, proj_dir, csv_path, survey_name):
        """Title.

        Desc.

        """
        #
        self.proj_dir = proj_dir
        df = pd.read_csv(csv_path, index_col="study_id")
        self._prep_df(df, survey_name)

    def _prep_df(self, df, name):
        """Title.

        Desc.

        """
        df = df.drop(labels=["datetime"], axis=1)
        col_list = [x for x in df.columns if name in x]
        df[col_list] = df[col_list].astype("Int64")
        self.df = df
        self.df_col = col_list

    def _calc_df_stats(self):
        """Title.

        Desc.

        """
        self.mean = round(self.df.stack().mean(), 2)
        self.std = round(self.df.stack().std(), 2)

    def _calc_row_stats(self):
        """Title.

        Desc.

        """
        self.df["total"] = self.df[self.df_col].sum(axis=1)
        self.mean = round(self.df["total"].mean(), 2)
        self.std = round(self.df["total"].std(), 2)

    def write_mean_std(self, out_path, title, total_avg="total"):
        """Title.

        Desc.

        """
        # validate df_row
        if total_avg not in ["total", "avg"]:
            raise ValueError(f"Unexpected total_avg values : {total_avg}")

        #
        if total_avg == "total":
            self._calc_row_stats()
        elif total_avg == "avg":
            self._calc_df_stats()

        #
        report = {
            "Title": title,
            "n": self.df.shape[0],
            "mean": self.mean,
            "std": self.std,
        }

        #
        with open(out_path, "w") as jf:
            json.dump(report, jf)
        print(f"\tSaved descriptive stats : {out_path}")
        return report

    def violin_plot(self, title, out_path, total_avg="total"):
        """Title.

        Desc.

        """
        #
        # validate df_row
        if total_avg not in ["total", "avg"]:
            raise ValueError(f"Unexpected total_avg values : {total_avg}")

        #
        if total_avg == "total":
            self._calc_row_stats()
            x_label = "Total"
        elif total_avg == "avg":
            self._calc_df_stats()
            x_label = "Average"

        #
        df_work = self.df.copy()
        if total_avg == "total":
            df_work["plot"] = df_work["total"]
        elif total_avg == "row":
            df_work["plot"] = df_work[self.df_col].mean(axis=1)

        #
        lb = self.mean - (3 * self.std)
        ub = self.mean + (3 * self.std)
        fig, ax = plt.subplots()
        sns.violinplot(x=df_work["plot"])
        ax.collections[0].set_alpha(0.5)
        ax.set_xlim(lb, ub)
        plt.title(title)
        plt.ylabel("Response Density")
        plt.xlabel(f"Participant {x_label}")
        plt.text(
            lb + (0.2 * self.std),
            -0.42,
            f"mean(sd) = {self.mean}({self.std})",
            horizontalalignment="left",
        )

        #
        plt.savefig(out_path)
        plt.close(fig)
        print(f"\tDrew violin plot : {out_path}")
        return out_path


# %%
def descript_rest_ratings(proj_dir):
    """Title.

    Desc.

    """

    #
    day2_path = os.path.join(
        proj_dir, "data_survey/visit_day2/data_clean", "df_rest-ratings.csv"
    )
    day3_path = os.path.join(
        proj_dir, "data_survey/visit_day3/data_clean", "df_rest-ratings.csv"
    )
    df_day2 = pd.read_csv(day2_path, na_values=88)
    df_day3 = pd.read_csv(day3_path, na_values=88)
    df_rest_all = pd.concat([df_day2, df_day3], ignore_index=True)
    df_rest_all = df_rest_all.sort_values(
        by=["study_id", "visit", "resp_type"]
    ).reset_index(drop=True)
    del df_day2, df_day3

    #
    df_rest_int = df_rest_all[df_rest_all["resp_type"] == "resp_int"].copy()
    df_rest_int = df_rest_int.drop(["datetime", "resp_type"], axis=1)
    emo_list = [
        x for x in df_rest_int.columns if x != "study_id" and x != "visit"
    ]
    df_rest_int[emo_list] = df_rest_int[emo_list].astype("Int64")

    #
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

    #
    df_mean = df_long.groupby(["visit", "emotion"]).mean()
    df_mean = df_mean.rename(columns={"rating": "mean"})
    df_mean["mean"] = round(df_mean["mean"], 2)
    df_std = df_long.groupby(["visit", "emotion"]).std()
    df_std = df_std.rename(columns={"rating": "std"})
    df_std["std"] = round(df_std["std"], 2)
    df_mean["std"] = df_std["std"]

    #
    out_dir = os.path.join(proj_dir, "analyses/surveys_stats_descriptive")
    out_csv = os.path.join(out_dir, "stats_rest-ratings.csv")
    df_mean.to_csv(out_csv)
    print(f"\tWrote csv : {out_csv}")

    #
    emo_a = emo_title[:4]
    emo_b = emo_title[4:8]
    emo_c = emo_title[8:12]
    emo_d = emo_title[12:]

    #
    for cnt, emo in enumerate([emo_a, emo_b, emo_c, emo_d]):
        df_sub = df_long[df_long["emotion"].isin(emo)]
        plot_dict = pd.DataFrame(df_sub.to_dict())

        #
        fix, ax = plt.subplots()
        sns.violinplot(
            x="emotion",
            y="rating",
            hue="visit",
            data=plot_dict,
            hue_order=["day2", "day3"],
        )
        plt.title("Emotion Frequence During Rest")
        plt.ylabel("Frequency")
        plt.xlabel("Emotion")
        plt.xticks(rotation=45, ha="right")

        #
        out_path = os.path.join(
            out_dir,
            f"plot_violin_rest-ratings_{cnt+1}.png",
        )
        plt.gcf().set_size_inches(10, 11)
        plt.savefig(out_path)
        plt.close()
        print(f"\tDrew violin plot : {out_path}")

    return df_mean


class DescriptStimRatings:
    """Title.

    Desc.

    """

    def __init__(self, proj_dir):
        """Title.

        Desc.

        """
        self.proj_dir = proj_dir
        self.out_dir = os.path.join(
            proj_dir, "analyses/surveys_stats_descriptive"
        )
        self._get_data()

    def _get_data(self):
        """Title.

        Desc.

        Attributes
        ----------
        df_all
        emo_list

        """
        day2_path = os.path.join(
            self.proj_dir,
            "data_survey/visit_day2/data_clean",
            "df_post_scan_ratings.csv",
        )
        df_day2 = pd.read_csv(day2_path)

        day3_path = os.path.join(
            self.proj_dir,
            "data_survey/visit_day3/data_clean",
            "df_post_scan_ratings.csv",
        )
        df_day3 = pd.read_csv(day3_path)

        #
        df_all = pd.concat([df_day2, df_day3], ignore_index=True)
        df_all = df_all.sort_values(
            by=["study_id", "session", "type", "emotion", "prompt"]
        ).reset_index(drop=True)
        df_all["emotion"] = df_all["emotion"].str.title()
        self.emo_list = df_all["emotion"].unique().tolist()
        self.df_all = df_all

    def endorsement(self, stim_type):
        """Title

        Desc.

        Parameters
        ----------
        stim_type : str
            [Videos | Scenarios]

        Raises
        ------

        """
        #
        if stim_type not in ["Videos", "Scenarios"]:
            raise ValueError(f"Unexpected stimulus type : {stim_type}")

        #
        df_end = self.df_all[
            (self.df_all["type"] == stim_type)
            & (self.df_all["prompt"] == "Endorsement")
        ].copy()

        # Find concordance of endorsement (emotion=Calm, response=Calm)
        num_subj = len(df_end["study_id"].unique())
        max_total = num_subj * 5

        #
        count_dict = {}
        for emo in self.emo_list:
            count_dict[emo] = {}
            df_emo = df_end[df_end["emotion"] == emo]
            for sub_emo in self.emo_list:
                count_emo = len(
                    df_emo[df_emo["response"].str.contains(sub_emo)]
                )
                count_dict[emo][sub_emo] = count_emo / max_total

        #
        del df_end, df_emo
        df_corr = pd.DataFrame.from_dict(
            {i: count_dict[i] for i in count_dict.keys()},
            orient="index",
        )
        df_trans = df_corr.transpose()
        out_path = os.path.join(
            self.out_dir,
            f"stats_stim-ratings_endorsement_{stim_type.lower()}.csv",
        )
        df_trans.to_csv(out_path)
        print(f"\t Wrote dataset : {out_path}")

        #
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
        print(f"\tDrew heat-prob plot : {out_path}")
        return df_trans

    def arousal(self, stim_type):
        """Title.

        Desc.

        """
        if stim_type not in ["Videos", "Scenarios"]:
            raise ValueError(f"Unexpected stimulus type : {stim_type}")

        #
        df = self.df_all[
            (self.df_all["type"] == stim_type)
            & (self.df_all["prompt"] == "Arousal")
        ].copy()
        df["response"] = df["response"].astype("Int64")

        #
        response_dict = {}
        for emo in self.emo_list:
            _mean = df.loc[df["emotion"] == emo, "response"].mean()
            _std = df.loc[df["emotion"] == emo, "response"].std()
            response_dict[emo] = {
                "mean": round(_mean, 2),
                "std": round(_std, 2),
            }
        df_stat = pd.DataFrame.from_dict(response_dict).transpose()
        out_path = os.path.join(
            self.out_dir, f"stats_stim-ratings_arousal_{stim_type.lower()}.csv"
        )
        df_stat.to_csv(out_path)
        print(f"\t Wrote dataset : {out_path}")

        #
        df["response"] = df["response"].astype("float")
        fig, ax = plt.subplots()
        sns.violinplot(x="emotion", y="response", data=df)
        plt.title(f"{stim_type[:-1]} Arousal Ratings")
        plt.ylabel("Arousal Rating")
        plt.xlabel("Emotion")
        plt.xticks(rotation=45, horizontalalignment="right")

        #
        out_path = os.path.join(
            self.out_dir,
            f"plot_violin_stim-ratings_arousal_{stim_type.lower()}.png",
        )
        plt.subplots_adjust(bottom=0.25, left=0.1)
        plt.savefig(out_path)
        plt.close(fig)
        print(f"\tDrew violin plot : {out_path}")
        return df_stat


# %%
