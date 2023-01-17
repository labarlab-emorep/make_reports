"""Title.

Calcualte survey stats:
    Scanner task metrics
    Rest rating metrics
    Stimulus rating metrics
    Survey metrics

"""
# %%
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
        print(f"\tDrew violin plot : {out_path}")
        return out_path
