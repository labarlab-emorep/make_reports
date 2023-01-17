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


def descript_stim_ratings(proj_dir):
    """Title.

    Desc.

    """
    day2_path = os.path.join(
        proj_dir,
        "data_survey/visit_day2/data_clean",
        "df_post_scan_ratings.csv",
    )
    df_day2 = pd.read_csv(day2_path)

    day3_path = os.path.join(
        proj_dir,
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
    emo_list = df_all["emotion"].unique().tolist()
    del df_day2, df_day3

    #
    df_movies = df_all[df_all["type"] == "Videos"]

    #
    # df_movies_aro = df_movies[df_movies["prompt"] == "Arousal"]
    # df_movies_val = df_movies[df_movies["prompt"] == "Valence"]

    # Find concordance of endorsement (emotion=Calm, response=Calm)
    df_movies_end = df_movies[df_movies["prompt"] == "Endorsement"]
    num_subj = len(df_movies["study_id"].unique())
    max_total = num_subj * 5

    count_dict = {}
    for emo in emo_list:
        count_dict[emo] = {}
        df_emo = df_movies_end[df_movies_end["emotion"] == emo]
        for sub_emo in emo_list:
            count_emo = len(df_emo[df_emo["response"].str.contains(sub_emo)])
            count_dict[emo][sub_emo] = count_emo / max_total

    #
    df_corr = pd.DataFrame.from_dict(
        {i: count_dict[i] for i in count_dict.keys()},
        orient="index",
    )
    df_t = df_corr.transpose()
    ax = sns.heatmap(df_t)
    ax.set(xlabel="Stimulus Category", ylabel="Participant Endorsement")
    ax.set_title("Video Endorsement Likelihood")


# %%
