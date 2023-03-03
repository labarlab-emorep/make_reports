"""Title.

Calculate project stats:
    Demographic rates vs proposed
    Time between visits
    Retention rates over time
    Recruitment pace

"""
# %%
import os
import json
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import importlib.resources as pkg_resources
from make_reports import report_helper
from make_reports import reference_files


# %%
class _CalcProp:
    """Title.

    Desc.

    Attributes
    ----------
    prop_actual
    prop_plan

    """

    def __init__(self, final_demo):
        """Title.

        Desc.

        """
        self._final_demo = final_demo
        self._total_rec = final_demo.shape[0]
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
        self._names = names
        self._values = values

        #
        meth_dict = {1: "_one_fact", 2: "_two_fact"}
        meth_find = getattr(self, meth_dict[len(names)])
        idx_plan, idx_final = meth_find()

        #
        self.prop_plan = round(
            (self._df_plan.loc[idx_plan, "prop"].sum() / 100), 3
        )
        self.prop_actual = round((len(idx_final) / self._total_rec), 3)

    def _one_fact(self):
        """Title.

        Desc.

        """
        idx_plan = self._df_plan.index[
            self._df_plan[self._names[0]] == self._values[0]
        ]
        idx_final = self._final_demo.index[
            self._final_demo[self._names[0]] == self._values[0]
        ].tolist()
        return (idx_plan, idx_final)

    def _two_fact(self):
        """Title.

        Desc.

        """
        idx_plan = self._df_plan.index[
            (self._df_plan[self._names[0]] == self._values[0])
            & (self._df_plan[self._names[1]] == self._values[1])
        ]
        idx_final = self._final_demo.index[
            (self._final_demo[self._names[0]] == self._values[0])
            & (self._final_demo[self._names[1]] == self._values[1])
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
        self._df_plan = pd.DataFrame.from_dict(demo_plan)


# %%
def demographics(proj_dir, final_demo):
    """Title.

    Desc.

    Returns
    -------

    """
    plot_plan_all = [
        ("sex", "Female"),
        ("race", "Asian"),
        ("race", "Black or African-American"),
        ("ethnicity", "Hispanic or Latino"),
        ("race", "White"),
    ]

    #
    plot_dict = {}
    calc_props = _CalcProp(final_demo)
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
    ax = sns.catplot(
        data=df_plot_all, x="Group", y="Proportion", hue="Type", jitter=False
    )
    ax.set(
        title="Planned vs Actual Participant Demographics",
        ylabel="Proportion of Sample",
        xlabel=None,
    )
    ax.set_xticklabels(rotation=30, horizontalalignment="right")

    out_file = os.path.join(
        proj_dir, "analyses/metrics_recruit", "demo_recruit_all.png"
    )
    ax.savefig(out_file)
    print(f"\tWrote : {out_file}")
    plt.close(ax.fig)

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

    #
    ax = sns.catplot(
        data=df_plot_sex,
        x="Group",
        y="Proportion",
        col="Sex",
        hue="Type",
        jitter=False,
        height=4,
        aspect=0.6,
    )
    ax.set(
        ylabel="Proportion of Sample",
        xlabel=None,
    )
    ax.set_xticklabels(rotation=30, horizontalalignment="right")
    out_file = os.path.join(
        proj_dir, "analyses/metrics_recruit", "demo_recruit_sex.png"
    )
    ax.savefig(out_file)
    print(f"\tWrote : {out_file}")
    plt.close(ax.fig)
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
def scan_pace(redcap_token, proj_dir):
    """Generate barplot of attempted scans per calender week.

    Mine REDCap Visit 2, 3 Logs (MRI) for timestamps, indicating a log was
    started. Calculate how many scans occured during each calendar week, by
    visit number, and then generate and save a stacked barplot.

    Plot written to:
        <proj_dir>/analyses/metrics_recruit/weekly_scan_attempts.png

    Parameters
    ----------
    redcap_token : str
        API token for RedCap project
    proj_dir : path
        Project's experiment directory

    Returns
    -------
    pd.DataFrame
        Weekly scan attempts

    """

    def _get_visit_log(rep_key: str, day: str) -> pd.DataFrame:
        """Return dataframe of MRI visit log datetimes."""
        # Manage differing column names
        col_switch = {
            "day2": ("date_mriv3_v2", "session_numberv3_v2"),
            "day3": ("date_mriv3", "session_numberv3"),
        }
        col_date = col_switch[day][0]
        col_value = col_switch[day][1]

        # Download dataframe, clean up
        df_visit = report_helper.pull_redcap_data(redcap_token, rep_key)
        df_visit = df_visit[df_visit[col_value].notna()].reset_index(drop=True)
        df_visit.rename(columns={col_date: "datetime"}, inplace=True)
        df_visit["datetime"] = df_visit["datetime"].astype("datetime64[ns]")

        # Extract values of interest
        df_out = df_visit[["datetime"]].copy()
        df_out["Visit"] = day
        return df_out

    # Access report keys, visit info
    with pkg_resources.open_text(
        reference_files, "log_keys_redcap.json"
    ) as jf:
        report_keys = json.load(jf)
    df2 = _get_visit_log(report_keys["mri_visit2"], "day2")
    df3 = _get_visit_log(report_keys["mri_visit3"], "day3")

    # Combine dataframes, ready for weekly totalling
    df = pd.concat([df2, df3], ignore_index=True)
    df = df.sort_values(by=["datetime"]).reset_index(drop=True)
    df["datetime"] = df["datetime"] - pd.to_timedelta(7, unit="d")
    df["count"] = 1

    # Determine scan attempts per week, by visit
    df_week = (
        df.groupby(["Visit", pd.Grouper(key="datetime", freq="W-SUN")])[
            "count"
        ]
        .sum()
        .reset_index()
        .sort_values("datetime")
    )

    # Find weeks where scans did not occur
    def _fill_weeks(day: str) -> pd.DataFrame:
        "Return dataframe including weeks without scans for visit."
        s = pd.date_range(
            "2022-04-17",
            datetime.date.today().strftime("%Y-%m-%d"),
            freq="W-SUN",
        )
        df_left = pd.DataFrame(
            data={"datetime": s, "Visit": np.nan, "count": np.nan}
        )
        df = pd.merge(
            df_left,
            df_week[df_week["Visit"] == day],
            how="left",
            on="datetime",
        )
        df["Visit_y"] = day
        df["count_y"] = df["count_y"].replace(np.nan, 0.0)
        df = df.drop(["Visit_x", "count_x"], axis=1)
        df = df.rename(columns={"Visit_y": "Visit", "count_y": "count"})
        return df

    df2 = _fill_weeks("day2")
    df3 = _fill_weeks("day3")
    df_all = pd.concat([df2, df3]).reset_index(drop=True)
    df_all = df_all.sort_values(by="datetime").reset_index(drop=True)

    # Make plotting df
    df_plot = pd.pivot(
        df_all, index=["datetime"], columns="Visit", values="count"
    )
    df_plot = df_plot.reset_index()
    df_plot["datetime"] = df_plot["datetime"].dt.strftime("%Y-%m-%d")

    # Draw and save plot
    df_plot.plot(
        x="datetime",
        kind="bar",
        stacked=True,
        title="Weekly Scan Attempts by Visit",
        figsize=(12, 6),
    )
    plt.axhline(y=4, color="black", linestyle="-")
    plt.ylabel("Total Attempts")
    plt.xlabel("Week Start Date")
    out_plot = os.path.join(
        proj_dir, "analyses/metrics_recruit", "weekly_scan_attempts.png"
    )
    plt.savefig(out_plot, bbox_inches="tight")
    print(f"\t\tDrew barplot : {out_plot}")
    plt.close()
    return df_week


# %%
