"""Calculate metrics for tracking data acquistion.

demographics    : compare proposed to actual demographic numbers
scan_pace       : plot number of attempted scans by week
censored_volumes    : plot proportion of volumes exceeding FD threshold
ParticipantFlow : generate PRISMA flowchart of participants in experiment

"""
# %%
import os
import json
import glob
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from graphviz import Digraph
from make_reports.resources import survey_download
from make_reports.resources import build_reports
from make_reports.resources import report_helper


# %%
class _CalcProp:
    """Calculate planned and actual demographic proportions.

    Actual demographic proportions are derived from REDCap's demographic
    report, and planned demographics have been hardcoded from the grant
    proposal.

    Attributes
    ----------
    prop_plan : float
        Planned sample proportion of demographic group
    prop_actual : float
        Actual sample proportion of demographic group

    Example
    -------
    final_demo = make_reports.build_reports.DemoAll.final_demo
    cp_obj = calc_metrics._CalcProp(final_demo)
    cp_obj.get_demo_props(["sex"], ["Male"])
    actual_proportion = cp_obj.prop_actual
    planned_proportion = cp_obj.prop_plan

    """

    def __init__(self, final_demo):
        """Initialize.

        Parameters
        ----------
        final_demo : make_reports.build_reports.DemoAll.final_demo
            pd.DataFrame, compiled demographic info

        """
        print("\tInitializing _CalcProp")
        self._final_demo = final_demo
        self._total_rec = final_demo.shape[0]
        self._planned_demo()

    def get_demo_props(self, names, values):
        """Determine planned and actual demographic proportions.

        Parameters
        ----------
        names : list
            [sex | race | ethnicity]
            Demographic column names of self._df_plan
        values : list
            [Female | Male | Asian | Black or African-American |
            Hispanic or Latino]
            Specified values of self._df_plan columns

        Attributes
        ----------
        prop_plan : float
            Planned sample proportion of demographic group
        prop_actual : float
            Actual sample proportion of demographic group

        Example
        -------
        final_demo = make_reports.build_reports.DemoAll.final_demo
        cp_obj = calc_metrics._CalcProp(final_demo)
        cp_obj.get_demo_props(["sex"], ["Male"])
        cp_obj.get_demo_props(["sex", "race"], ["Female", "Asian"])

        """
        # Validate
        if len(names) > 2:
            raise ValueError("Length of names, values must be 1 or 2")
        if len(names) != len(values):
            raise ValueError("Lengths of names and values are not equal.")
        for _name in names:
            if _name not in ["sex", "race", "ethnicity"]:
                raise ValueError(f"Improper names arg supplied : {_name}")
        for _value in values:
            if _value not in [
                "Female",
                "Male",
                "White",
                "Asian",
                "Black or African-American",
                "Hispanic or Latino",
            ]:
                raise ValueError(f"Improper values arg supplied : {_value}")

        # Trigger proper method based on length of list
        self._names = names
        self._values = values
        meth_dict = {1: "_one_fact", 2: "_two_fact"}
        meth_find = getattr(self, meth_dict[len(names)])
        idx_plan, idx_final = meth_find()

        # Convert counts (idxs) to proportions
        self.prop_plan = round(
            (self._df_plan.loc[idx_plan, "prop"].sum() / 100), 3
        )
        self.prop_actual = round((len(idx_final) / self._total_rec), 3)

    def _one_fact(self) -> tuple:
        """Return planned, actual counts for one column querries."""
        idx_plan = self._df_plan.index[
            self._df_plan[self._names[0]] == self._values[0]
        ]
        idx_final = self._final_demo.index[
            self._final_demo[self._names[0]] == self._values[0]
        ].tolist()
        return (idx_plan, idx_final)

    def _two_fact(self) -> tuple:
        """Return planned, actual counts for two column querries."""
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
        """Set pd.DataFrame attribute of planned demographics."""
        # Convert values from grant proposal to long-formatted dataframe,
        # make each column of long dataframe as array.
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

        # Organize arrays and make dict
        demo_plan = {
            "sex": sex_list,
            "ethnicity": his_list,
            "race": race_list,
            "prop": prop_list,
        }
        self._df_plan = pd.DataFrame.from_dict(demo_plan)


# %%
def demographics(proj_dir, final_demo):
    """Check on demographic recruitment.

    Currently only supports a subset of total planned demographics.

    Generate dataframes and plots with planned demographic
    proportions versus sample actual.

    Plots are written to:
        <proj_dir>/analyses/metrics_recruit/demo_recruit_[all|sex].png"

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory
    final_demo : make_reports.build_reports.DemoAll.final_demo
        pd.DataFrame, compiled demographic info

    Returns
    -------
    dict
        Planned versus actual demographic proportions for
        one and two factors.
        ["one_factor"] = pd.DataFrame

    """
    # Line up single factor querries
    plot_plan_all = [
        ("sex", "Female"),
        ("race", "Asian"),
        ("race", "Black or African-American"),
        ("ethnicity", "Hispanic or Latino"),
        ("race", "White"),
    ]

    # Make a single factor dataframe
    plot_dict = {}
    calc_props = _CalcProp(final_demo)
    for h_col, h_val in plot_plan_all:
        calc_props.get_demo_props([h_col], [h_val])
        plot_dict[h_val] = {
            "Planned": calc_props.prop_plan,
            "Actual": calc_props.prop_actual,
        }

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

    # Draw and save factor scatter plot
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
        proj_dir,
        "analyses/metrics_recruit",
        "plot_scatter_recruit-goals_all.png",
    )
    ax.savefig(out_file)
    print(f"\tWrote : {out_file}")
    plt.close(ax.fig)

    # Line up two factor querries
    plot_plan_sex = [
        (["sex", "race"], ["Female", "Asian"]),
        (["sex", "race"], ["Male", "Asian"]),
        (["sex", "race"], ["Female", "Black or African-American"]),
        (["sex", "race"], ["Male", "Black or African-American"]),
        (["sex", "ethnicity"], ["Female", "Hispanic or Latino"]),
        (["sex", "ethnicity"], ["Male", "Hispanic or Latino"]),
    ]

    # make a two factor dataframe
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

    # Draw and save plot
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
        proj_dir,
        "analyses/metrics_recruit",
        "plot_scatter_recruit-goals_sex.png",
    )
    ax.savefig(out_file)
    print(f"\tWrote : {out_file}")
    plt.close(ax.fig)
    return {"one_factor": df_plot_all, "two_factor": df_plot_sex}


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
    # Get data, ready for weekly totals
    df_log = survey_download.download_mri_log(redcap_token)
    df_log["datetime"] = df_log["datetime"] - pd.to_timedelta(7, unit="d")
    df_log["count"] = 1

    # Determine scan attempts per week, by visit
    df_week = (
        df_log.groupby(["Visit", pd.Grouper(key="datetime", freq="W-SUN")])[
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
        proj_dir,
        "analyses/metrics_recruit",
        "plot_barplot-wide_scan-attempts.png",
    )
    plt.savefig(out_plot, bbox_inches="tight")
    print(f"\t\tDrew barplot : {out_plot}")
    plt.close()
    return df_week


# %%
def censored_volumes(proj_dir):
    """Plot proportion of volumes exceeding framewise displacement threshold.

    Find all confounds_proportions/*json files generated by
    func_model.resources.fsl.model.confounds, extract CensorProp
    value (proportion of volumes censored) for each run, and
    plot by session.

    Plot written to:
        <proj_dir>/analyses/metrics_recruit/plot_boxplot-double_epi-motion.png

    Currently only includes task-based EPI files.

    Parameters
    ----------
    proj_dir : path
        Project's experiment directory

    Returns
    -------
    pd.DataFrame
        long-formatted proportions, organized by subject, session, and run

    """
    # Identify available participants
    deriv_dir = os.path.join(
        proj_dir, "data_scanner_BIDS/derivatives/model_fsl"
    )
    subj_list = [os.path.basename(x) for x in glob.glob(f"{deriv_dir}/sub-*")]

    # Capture all proportion data, organized by subject * session * run
    data_dict = {}
    for subj in subj_list:
        data_dict[subj] = {}
        for sess in ["ses-day2", "ses-day3"]:
            search_path = os.path.join(
                deriv_dir, subj, sess, "func/confounds_proportions"
            )
            run_list = [
                x
                for x in sorted(glob.glob(f"{search_path}/*proportion.json"))
                if "task-rest" not in x
            ]
            if not run_list:
                continue
            data_dict[subj][sess] = {}
            for run_path in run_list:
                run_num = (
                    os.path.basename(run_path).split("run-")[1].split("_")[0]
                )
                with open(run_path) as jf:
                    run_dict = json.load(jf)
                data_dict[subj][sess][run_num] = run_dict["CensorProp"]

    # Unpack data_dict and make long-formatted dataframe
    rec = {}
    for name in data_dict:
        rec[name] = pd.DataFrame.from_records(data_dict[name]).unstack()
    df = pd.DataFrame.from_records(rec)
    df = df.reset_index()
    df_long = pd.melt(
        df, id_vars=["level_0", "level_1"], value_vars=df.columns.to_list()[2:]
    )

    # Draw and save plot
    fig, ax = plt.subplots()
    sns.boxplot(
        x="level_0",
        y="value",
        data=df_long,
    )
    plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
    plt.ylabel("Proportion")
    plt.xlabel("Session")
    plt.title("Proportion of Volumes Exceeding FD Threshold")

    out_path = os.path.join(
        proj_dir,
        "analyses/metrics_recruit",
        "plot_boxplot-double_epi-motion.png",
    )
    plt.savefig(out_path, bbox_inches="tight")
    print(f"\t\tDrew boxplot : {out_path}")
    plt.close()
    return df_long


# %%
class ParticipantFlow:
    """Generate PRISMA flowchart of participants in study.

    Methods
    -------
    draw_prisma()
        Generate participant flow chart from REDCap records

    Example
    -------
    pf = ParticipantFlow("/path/to/proj/dir", "REDCAP_API_STR")
    pf.draw_prisma()

    """

    def __init__(self, proj_dir, redcap_token):
        """Initialize.

        Parameters
        ----------
        proj_dir : str, os.PathLike
            Project's experiment directory
        redcap_token : str
            API token for RedCap project

        """
        print("Initializing ParticipantFlow")
        self._proj_dir = proj_dir
        self._rc_token = redcap_token

    def draw_prisma(self):
        """Generate PRISMA flowchart of participants in study.

        Calculate number of participants at each Visit/step
        from REDCap records and stitch together a flowchart
        incorporating participants lost-to-follow-up, excluded,
        and withdrawn.

        Writes output to:
            <proj-dir>/analyses_metrics/plot_flow-participant.png

        """
        # Identify recruit, enroll, final numbers, and status changes
        num_recruit = self._get_recruit()
        num_enroll = self._get_enroll()
        num_final = self._dl_compl()
        change_dict = self._get_status_change()

        # Build Visit0, Visit1 sections
        flo = Digraph("participant_flow")
        flo.attr(label="Participant Flow", labelloc="t", fontsize="18")
        flo.node("0", f"Recruited Individuals: {num_recruit}", shape="box")
        with flo.subgraph() as c:
            c.attr(rank="same")
            c.node(
                "1",
                "Visit1\n"
                + f"Enrolled: {num_enroll}\l"  # noqa: W605
                + f"Surveys: {self._v1_comp()}\l",  # noqa: W605
                shape="box",
            )
            c.node(
                "2",
                f"Excluded: {change_dict['excluded']['visit1']}\l"  # noqa: W605, E501
                + f"Lost: {change_dict['lost']['visit1']}\l"  # noqa: W605
                + f"Withdrawn: {change_dict['withdrew']['visit1']}\l",  # noqa: W605, E501
                shape="box",
            )

        # Build Visit2, Visit3 sections
        count = 3
        for day in [2, 3]:

            _sur, _mri = self._v23_comp(day)
            _excl = change_dict["excluded"][f"visit{day}"]
            _lost = change_dict["lost"][f"visit{day}"]
            _with = change_dict["withdrew"][f"visit{day}"]

            with flo.subgraph() as c:
                c.attr(rank="same")
                c.node(
                    str(count),
                    f"Visit{day}\n"
                    + f"Surveys: {_sur}\l"  # noqa: W605
                    + f"MRI: {_mri}\l",  # noqa: W605
                    shape="box",
                )
                count += 1
                c.node(
                    str(count),
                    f"Excluded: {_excl}\l"  # noqa: W605
                    + f"Lost: {_lost}\l"  # noqa: W605
                    + f"Withdrawn: {_with}\l",  # noqa: W605
                    shape="box",
                )
                count += 1

        # Build final and draw edges, write out
        flo.node(str(count), f"Final: {num_final}", shape="box")
        flo.edges(["01", "12", "13", "34", "35", "56", "57"])
        flo.format = "png"
        out_plot = os.path.join(
            self._proj_dir,
            "analyses/metrics_recruit",
            "plot_flow-participant",
        )
        flo.render(out_plot)
        print(f"\tDrew plot : {out_plot}")

    def _get_recruit(self) -> int:
        """Determine number of participants recruited."""
        df_pre = survey_download.download_prescreening(self._rc_token)
        return df_pre.shape[0]

    def _get_enroll(self) -> int:
        """Determine number of participants enrolled."""
        rc_demo = build_reports.DemoAll(self._proj_dir)
        return rc_demo.final_demo.shape[0]

    def _dl_compl(self) -> int:
        """Download completion log, determine final number."""
        self._df_compl = survey_download.download_completion_log(
            self._rc_token
        )
        return len(
            self._df_compl.index[
                (self._df_compl["day_3_fully_completed"] == 1.0)
                & (self._df_compl["withdrew_flag___1"] == 0.0)
            ].tolist()
        )

    def _get_status_change(self) -> dict:
        """Return number of participants lost/excl/with by visit."""
        part_comp = report_helper.ParticipantComplete()
        out_dict = {}
        for stat in ["lost", "excluded", "withdrew"]:
            out_dict[stat] = {}
            part_comp.status_change(stat)
            if stat == "lost":
                out_dict[stat]["visit1"] = len(part_comp.visit1)
                out_dict[stat]["visit2"] = len(part_comp.visit2)
                out_dict[stat]["visit3"] = len(part_comp.visit3)
            else:
                out_dict[stat]["visit1"] = self._unpack(part_comp.visit1)
                out_dict[stat]["visit2"] = self._unpack(part_comp.visit2)
                out_dict[stat]["visit3"] = self._unpack(part_comp.visit3)
        return out_dict

    def _unpack(self, in_dict) -> int:
        """Return total length of dict value lists."""
        run_tot = 0
        for _, value in in_dict.items():
            run_tot += len(value)
        return run_tot

    def _v1_comp(self) -> int:
        """Return number of visit1 complete."""
        return len(
            self._df_compl.index[
                (self._df_compl["day_1_fully_completed"] == 1.0)
                | (self._df_compl["emotion_quest_completed"] == 1.0)
            ].tolist()
        )

    def _v23_comp(self, day: int) -> tuple:
        """Return tuple of visit2/3 survey, MRI counts."""
        num_sur = len(
            self._df_compl.index[
                (self._df_compl[f"day_{day}_fully_completed"] == 1.0)
                | (self._df_compl[f"bdi_day{day}_completed"] == 1.0)
            ].tolist()
        )
        num_mri = len(
            self._df_compl.index[
                (self._df_compl[f"day_{day}_fully_completed"] == 1.0)
                | (self._df_compl[f"imaging_day{day}_completed"] == 1.0)
            ].tolist()
        )
        return (num_sur, num_mri)


# %%
