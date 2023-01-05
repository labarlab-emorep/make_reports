"""Title.

Calculate project stats:
    Demographic rates vs proposed
    Time between visits
    Retention rates over time
    Recruitment pace

Calcualte survey stats:
    Scanner task metrics
    Rest rating metrics
    Stimulus rating metrics
    Survey metrics

"""
# %%
import os
import json
import datetime
import textwrap
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import importlib.resources as pkg_resources
from make_reports import report_helper
from make_reports import reference_files

# import matplotlib.pyplot as plt


# %%
class _CalcProp:
    """Title.

    Desc.

    Parameters
    ----------
    df_plan
    final_demo

    """

    def __init__(self, final_demo):
        """Title.

        Desc.

        """
        self.final_demo = final_demo
        self.total_rec = final_demo.shape[0]
        self._planned_demo()

    def get_demo_props(self, names, values):
        """Title.

        Desc.

        Parameters
        ----------
        names : list
        values : list

        Attributes
        ----------
        prop_plan : float
        prop_actual : float

        """
        #
        if len(names) != len(values):
            raise ValueError("Lengths of names and values are not equal.")

        #
        self.names = names
        self.values = values

        #
        meth_dict = {1: "_one_fact", 2: "_two_fact"}
        meth_find = getattr(self, meth_dict[len(names)])
        idx_plan, idx_final = meth_find()

        #
        self.prop_plan = round(
            (self.df_plan.loc[idx_plan, "prop"].sum() / 100), 3
        )
        self.prop_actual = round((len(idx_final) / self.total_rec), 3)

    def _one_fact(self):
        """Title.

        Desc.

        """
        idx_plan = self.df_plan.index[
            self.df_plan[self.names[0]] == self.values[0]
        ]
        idx_final = self.final_demo.index[
            self.final_demo[self.names[0]] == self.values[0]
        ].tolist()
        return (idx_plan, idx_final)

    def _two_fact(self):
        """Title.

        Desc.

        """
        idx_plan = self.df_plan.index[
            (self.df_plan[self.names[0]] == self.values[0])
            & (self.df_plan[self.names[1]] == self.values[1])
        ]
        idx_final = self.final_demo.index[
            (self.final_demo[self.names[0]] == self.values[0])
            & (self.final_demo[self.names[1]] == self.values[1])
        ].tolist()
        return (idx_plan, idx_final)

    def _planned_demo(self):
        """Title.

        Desc.

        """
        #
        sex_list = (["Male"] * 12) + (["Female"] * 12)
        his_list = (
            (["Hispanic or Latino"] * 6) + (["Not Hispanic or Latino"] * 6)
        ) * 2
        race_list = [
            "American Indian or Alaska Native",
            "Asian",
            "Native Hawaiian or Other Pacific Islander",
            "Black or African-American",
            "White",
            "More than One Race",
        ] * 4
        prop_list = [
            0.26,
            0.07,
            0.02,
            0.43,
            4.83,
            0.2,
            0.13,
            3.23,
            0.02,
            10.82,
            26.72,
            1.03,
            0.23,
            0.07,
            0.02,
            0.43,
            4.39,
            0.21,
            0.15,
            3.51,
            0.03,
            12.86,
            29.21,
            1.13,
        ]
        demo_plan = {
            "sex": sex_list,
            "ethnicity": his_list,
            "race": race_list,
            "prop": prop_list,
        }
        self.df_plan = pd.DataFrame.from_dict(demo_plan)


# %%
def demographics(proj_dir, final_demo):
    """Title.

    Desc.

    Returns
    -------

    """
    #
    calc_props = _CalcProp(final_demo)

    #
    plot_plan_all = [
        ("sex", "Female"),
        ("race", "Asian"),
        ("race", "Black or African-American"),
        ("ethnicity", "Hispanic or Latino"),
        ("race", "White"),
    ]
    plot_dict = {}
    for h_col, h_val in plot_plan_all:
        calc_props.get_demo_props([h_col], [h_val])
        plot_dict[h_val] = {
            "Planned": calc_props.prop_plan,
            "Actual": calc_props.prop_actual,
        }

    #
    df_plot = pd.DataFrame.from_dict(plot_dict, orient="index")
    df_plot = df_plot.reset_index()
    df_plot = df_plot.rename(columns={"index": "Group"})
    df_plot_all = pd.melt(
        df_plot,
        id_vars="Group",
        value_vars=["Planned", "Actual"],
        var_name="Type",
        value_name="Proportion",
    )

    #
    plot_group_all = sns.catplot(
        data=df_plot_all, x="Group", y="Proportion", hue="Type", jitter=False
    ).set(title="Recruitment Demographics")
    plot_group_all.set_xticklabels(rotation=30, horizontalalignment="right")
    out_file = os.path.join(
        proj_dir, "analyses/metrics_recruit", "demo_recruit_all.png"
    )
    plot_group_all.savefig(out_file)
    print(f"\tWrote : {out_file}")

    #
    plot_plan_sex = [
        (["sex", "race"], ["Female", "Asian"]),
        (["sex", "race"], ["Male", "Asian"]),
        (["sex", "race"], ["Female", "Black or African-American"]),
        (["sex", "race"], ["Male", "Black or African-American"]),
        (["sex", "ethnicity"], ["Female", "Hispanic or Latino"]),
        (["sex", "ethnicity"], ["Male", "Hispanic or Latino"]),
    ]
    df_plot_sex = pd.DataFrame(columns=["Sex", "Group", "Type", "Proportion"])
    for h_col, h_val in plot_plan_sex:
        calc_props.get_demo_props(h_col, h_val)
        for h_prop, h_type in zip(
            [calc_props.prop_plan, calc_props.prop_actual],
            ["Planned", "Actual"],
        ):
            h_dict = {
                "Sex": h_val[0],
                "Group": h_val[1],
                "Type": h_type,
                "Proportion": h_prop,
            }
            h_row = pd.DataFrame(h_dict, index=[0])
            df_plot_sex = pd.concat([df_plot_sex.loc[:], h_row]).reset_index(
                drop=True
            )
            del h_dict, h_row

    plot_group_sex = sns.catplot(
        data=df_plot_sex,
        x="Group",
        y="Proportion",
        col="Sex",
        hue="Type",
        jitter=False,
        height=4,
        aspect=0.6,
    )
    plot_group_sex.set_xticklabels(rotation=30, horizontalalignment="right")
    out_file = os.path.join(
        proj_dir, "analyses/metrics_recruit", "demo_recruit_sex.png"
    )
    plot_group_sex.savefig(out_file)
    print(f"\tWrote : {out_file}")

    return {"all": df_plot_all, "sex": df_plot_sex}


# %%
def calc_pending(redcap_token):
    """Title.

    Desc.

    """
    today_date = datetime.date.today()

    #
    with pkg_resources.open_text(
        reference_files, "log_keys_redcap.json"
    ) as jf:
        report_keys = json.load(jf)

    df_complete = report_helper.pull_redcap_data(
        redcap_token, report_keys["completion_log"]
    )
    df_visit2 = report_helper.pull_redcap_data(
        redcap_token, report_keys["mri_visit2"]
    )

    #
    df_complete = df_complete[
        df_complete["day_2_fully_completed"].notna()
    ].reset_index(drop=True)
    idx_no3 = df_complete.index[
        (df_complete["day_3_fully_completed"] != 1)
        & (df_complete["completion_log_complete"] == 0)
    ]
    # idx_no3 = df_complete.index[
    #     (
    #         (df_complete["day_3_fully_completed"] == 0)
    #         | df_complete["day_3_fully_completed"].apply(np.isnan)
    #     )
    #     & (df_complete["completion_log_complete"] == 0)
    # ]
    subj_no3 = df_complete.loc[idx_no3, "record_id"].tolist()

    #
    df_visit2 = df_visit2[
        df_visit2["session_numberv3_v2"].notna()
    ].reset_index(drop=True)

    df_visit2["date_mriv3_v2"] = df_visit2["date_mriv3_v2"].astype(
        "datetime64[ns]"
    )

    # df_visit2["date_mriv3_v2"] = pd.to_datetime(
    #     df_visit2["date_mriv3_v2"], format="%Y-%m-%d"
    # )

    idx_subj_no3 = df_visit2.index[df_visit2["record_id"].isin(subj_no3)]
    scan2_dates = df_visit2.loc[idx_subj_no3, "date_mriv3_v2"].tolist()

    #
    pend_dict = {}
    for h_subj, h_date in zip(subj_no3, scan2_dates):
        h_delta = today_date - h_date.date()
        pend_dict[h_subj] = f"{h_delta.days} days"
    return pend_dict


# %%
def _prep_df(df, name, has_datetime=True):
    """Title.

    Desc.

    """
    if has_datetime:
        df = df.drop(labels=["datetime"], axis=1)
    val_list = [x for x in df.columns if name in x]
    df[val_list] = df[val_list].astype("Int64")
    return (df, val_list)


def _write_metrics(name, num, mean, std, proj_dir):
    """Title.

    Desc.

    Parameters
    ----------

    Returns
    -------

    """
    report = f"""\
    {name} Descriptive Stats

    n    = {num}
    mean = {mean}
    std  = {std}
    """
    report = textwrap.dedent(report)

    #
    out_txt = os.path.join(
        proj_dir,
        "analyses/stats_descriptive_surveys",
        f"stats_{name}.txt",
    )
    with open(out_txt, "w") as f:
        for _line in report:
            f.writelines(_line)
    print(f"\tWrote stats : {out_txt}")
    return report


def _draw_violin(df, lb, ub, mean, std, title, out_path):
    """Title.

    Desc.

    """
    #
    fig, ax = plt.subplots()
    sns.violinplot(x=df["mean"])
    ax.collections[0].set_alpha(0.5)
    ax.set_xlim(lb, ub)
    plt.title(title)
    plt.ylabel("Response Density")
    plt.xlabel("Participant Average")
    plt.text(
        lb + 0.2,
        -0.42,
        f"mean(sd) = {mean}({std})",
        horizontalalignment="left",
    )

    #
    plt.savefig(out_path)
    print(f"\tDrew plot : {out_path}")
    return out_path


# %%
class DescAim:
    """Title.

    Desc.

    """

    def __init__(self, proj_dir):
        """Title.

        Desc.

        """
        self.proj_dir = proj_dir
        df_aim = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_AIM.csv",
            ),
            index_col="study_id",
        )
        self.df_aim, self.data_col = _prep_df(df_aim, "AIM")

    def _calc_metrics(self):
        """Title.

        Desc.

        """
        self.mean = round(self.df_aim.stack().mean(), 2)
        self.std = round(self.df_aim.stack().std(), 2)

    def metrics(self):
        """Title.

        Desc.

        """
        self._calc_metrics()
        _ = _write_metrics(
            "AIM", self.df_aim.shape[0], self.mean, self.std, self.proj_dir
        )

    def violin_plot(self):
        """Title.

        Desc.

        """
        #
        if not hasattr(self, "mean") or not hasattr(self, "std"):
            self._calc_metrics()
        self.df_aim["mean"] = self.df_aim[self.data_col].mean(axis=1)

        #
        plot_path = os.path.join(
            self.proj_dir,
            "analyses/stats_descriptive_surveys",
            "plot_violin_AIM.png",
        )
        _ = _draw_violin(
            self.df_aim,
            1,
            6,
            self.mean,
            self.std,
            "Affective Intensity Measure",
            plot_path,
        )


# %%
class DescAls:
    """Title.

    Desc.

    """

    def __init__(self, proj_dir):
        """Title.

        Desc.

        """
        self.proj_dir = proj_dir
        df_als = pd.read_csv(
            os.path.join(
                self.proj_dir,
                "data_survey",
                "visit_day1/data_clean",
                "df_ALS.csv",
            ),
            index_col="study_id",
        )
        self.df_als, self.data_col = _prep_df(df_als, "ALS")

    def _calc_metrics(self, df):
        """Title.

        Desc.

        """
        mean = round(df.stack().mean(), 2)
        std = round(df.stack().std(), 2)
        return (mean, std)

    def metrics(self, sub_df=None, sub_name=None):
        """Title.

        Desc.

        """
        df = sub_df if isinstance(sub_df, pd.DataFrame) else self.df_als
        file_name = "ALS" if not sub_name else f"ALS_{sub_name}"
        mean, std = self._calc_metrics(df)
        _ = _write_metrics(file_name, df.shape[0], mean, std, self.proj_dir)

    def violin_plot(self, sub_df=None, sub_col=None, sub_name=None):
        """Title.

        Desc.

        """
        #
        df = sub_df if isinstance(sub_df, pd.DataFrame) else self.df_als
        col = self.data_col if not sub_col else sub_col
        title = "Affective Lability Scale -- 18"
        if sub_name:
            title = title + f", {sub_name}"

        #
        mean, std = self._calc_metrics(df)
        df["mean"] = df[col].mean(axis=1)

        #
        plot_name = (
            "plot_violin_ALS.png"
            if not sub_name
            else f"plot_violin_ALS_{sub_name}.png"
        )
        plot_path = os.path.join(
            self.proj_dir, "analyses/stats_descriptive_surveys", plot_name
        )
        _ = _draw_violin(
            df,
            1,
            5,
            mean,
            std,
            title,
            plot_path,
        )

    def subscales(self):
        """Title.

        ALS-18
        https://www.sciencedirect.com/science/article/pii/S0191886903004793

        Desc.
        """
        anx_dep = [f"ALS_{x}" for x in [1, 3, 5, 6, 7]]
        dep_ela = [f"ALS_{x}" for x in [2, 10, 12, 13, 15, 16, 17, 18]]
        anger = [f"ALS_{x}" for x in [4, 8, 9, 11, 14]]

        sub_dict = {
            "Anx-Dep": self.df_als[anx_dep].copy(),
            "Dep-Ela": self.df_als[dep_ela].copy(),
            "Anger": self.df_als[anger].copy(),
        }

        for name, df in sub_dict.items():
            self.metrics(sub_df=df, sub_name=name)
            _, sub_col_list = _prep_df(df, "ALS", has_datetime=False)
            self.violin_plot(sub_df=df, sub_col=sub_col_list, sub_name=name)
