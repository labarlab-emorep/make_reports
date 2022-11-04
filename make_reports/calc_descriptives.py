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
import pandas as pd


def demographics(proj_dir, final_demo):
    """Title.

    Desc.

    """

    #
    sex_list = (["Male"] * 12) + (["Female"] * 12)
    his_list = ((["Hispanic"] * 6) + (["Not Hispanic"] * 6)) * 2
    race_list = [
        "American Indian or Alaska Native",
        "Asian",
        "Native Hawaiian or Other Pacific Islander",
        "Black",
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
        "hispanic": his_list,
        "race": race_list,
        "prop": prop_list,
    }
    df_demo_plan = pd.DataFrame.from_dict(demo_plan)

    #
    idx_plan_female = df_demo_plan.index[
        df_demo_plan["sex"] == "Female"
    ].tolist()
    idx_plan_hispanic = df_demo_plan.index[
        df_demo_plan["hispanic"] == "Hispanic"
    ].tolist()
    idx_plan_white = df_demo_plan.index[
        df_demo_plan["race"] == "White"
    ].tolist()
    idx_plan_black = df_demo_plan.index[
        df_demo_plan["race"] == "Black"
    ].tolist()

    prop_plan_female = round(
        (df_demo_plan.loc[idx_plan_female, "prop"].sum() / 100), 3
    )
    prop_plan_hispanic = round(
        (df_demo_plan.loc[idx_plan_hispanic, "prop"].sum() / 100), 3
    )
    prop_plan_white = round(
        (df_demo_plan.loc[idx_plan_white, "prop"].sum() / 100), 3
    )
    prop_plan_black = round(
        (df_demo_plan.loc[idx_plan_black, "prop"].sum() / 100), 3
    )

    #
    total_subj = final_demo.shape[0]

    idx_rec_female = final_demo.index[final_demo["sex"] == "Female"].tolist()
    idx_rec_hispanic = final_demo.index[
        final_demo["ethnicity"] == "Hispanic or Latino"
    ].tolist()
    idx_rec_white = final_demo.index[final_demo["race"] == "White"].tolist()
    idx_rec_black = final_demo.index[
        final_demo["race"] == "Black  or African-American"
    ].tolist()

    prop_rec_female = round((len(idx_rec_female) / total_subj), 3)
    prop_rec_hispanic = round((len(idx_rec_hispanic) / total_subj), 3)
    prop_rec_white = round((len(idx_rec_white) / total_subj), 3)
    prop_rec_black = round((len(idx_rec_black) / total_subj), 3)
