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
import pandas as pd

# import matplotlib.pyplot as plt
import seaborn as sns


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
