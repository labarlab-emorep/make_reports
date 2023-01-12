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
import pandas as pd
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
class _DescStat:
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
        self.df, self.data_col = self._prep_df(df, survey_name)

    def _prep_df(self, df, name, has_datetime=True):
        """Title.

        Desc.

        """
        if has_datetime:
            df = df.drop(labels=["datetime"], axis=1)
        val_list = [x for x in df.columns if name in x]
        df[val_list] = df[val_list].astype("Int64")
        return (df, val_list)
        # self.df = df
        # self.data_col = val_list

    def mean_std(self, df):
        """Title.

        Desc.

        """
        mean = round(df.stack().mean(), 2)
        std = round(df.stack().std(), 2)
        return (mean, std)

    def metrics(self, name, df_sub=None):
        """Title.

        Desc.

        """
        #
        df_work = df_sub if isinstance(df_sub, pd.DataFrame) else self.df
        mean, std = self.mean_std(df_work)

        #
        report = {
            "Title": "Descriptive Stats",
            "n": df_work.shape[0],
            "mean": mean,
            "std": std,
        }

        #
        out_path = os.path.join(
            self.proj_dir,
            "analyses/surveys_stats_descriptive",
            f"stats_{name}.json",
        )
        with open(out_path, "w") as jf:
            json.dump(report, jf)
        print(f"\tSaved descriptive stats : {out_path}")
        return report

    def violin_plot(self, lb, ub, title, plot_name, df_sub=None, col_sub=None):
        """Title.

        Desc.

        """
        #
        df_work = df_sub if isinstance(df_sub, pd.DataFrame) else self.df
        df_col = col_sub if col_sub else self.data_col

        mean, std = self.mean_std(df_work)
        df_work["mean"] = df_work[df_col].mean(axis=1)

        #
        fig, ax = plt.subplots()
        sns.violinplot(x=df_work["mean"])
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
        out_path = os.path.join(
            self.proj_dir, "analyses/surveys_stats_descriptive", plot_name
        )
        plt.savefig(out_path)
        print(f"\tDrew violin plot : {out_path}")
        return out_path


# %%
class SurveyStats(_DescStat):
    """Title.

    Desc.

    """

    def __init__(self, proj_dir, csv_path, survey_name):
        """Title.

        Desc.

        """
        super().__init__(proj_dir, csv_path, survey_name)
        self.survey_name = survey_name

    def _survey_switch(self):
        """Title.

        Desc.

        """

        #
        violin_name = f"plot_violing_{self.survey_name}.png"
        _dict = {
            "AIM": (1, 6, "Affective Intensity Measure", violin_name),
            "ALS": (1, 5, "Affective Lability Scale -- 18", violin_name),
            "ERQ": (0, 8, "Emotion Regulation Questionnaire", violin_name),
        }

        # Validate survey name
        if self.survey_name not in _dict.keys():
            raise AttributeError(
                f"Unexpected survey name : {self.survey_name}"
            )

        #
        param_dict = {}
        for key, value in _dict.items():
            param_dict[key] = {}
            for c, val in enumerate(["lb", "ub", "title", "violin_file"]):
                param_dict[key][val] = value[c]

        #
        self.survey_dict = param_dict[self.survey_name]

    def _subscale_switch(self):
        """Title.

        Desc.

        """
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
        }
        return subscale_dict[self.survey_name]

    def stats_plot(self):
        """Title.

        Desc.

        """
        self._survey_switch()
        self.metrics(name=self.survey_name)
        self.violin_plot(
            lb=self.survey_dict["lb"],
            ub=self.survey_dict["ub"],
            title=self.survey_dict["title"],
            plot_name=self.survey_dict["violin_file"],
        )

    def stats_plot_subscale(self):
        """Title.

        Desc.

        """
        # Get columns for subscales
        sub_dict = self._subscale_switch()
        self._survey_switch()

        #
        for sub_name, sub_cols in sub_dict.items():
            sub_df = self.df[sub_cols].copy()
            stat_name = f"{self.survey_name}_{sub_name}"
            self.metrics(name=stat_name, df_sub=sub_df)

            #
            sub_title = f"{self.survey_dict['title']}, {sub_name}"
            sub_name = self.survey_dict["violin_file"].replace(
                ".png", f"_{sub_name}.png"
            )
            self.violin_plot(
                lb=self.survey_dict["lb"],
                ub=self.survey_dict["ub"],
                title=sub_title,
                plot_name=sub_name,
                df_sub=sub_df,
                col_sub=sub_cols,
            )
