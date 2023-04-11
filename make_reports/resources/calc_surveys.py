"""Generate descriptive statistics for participant responses.

Manage and report on participant responses to REDCap, Qualtrics,
and EmoRep tasks. Methods are organized according to their visit
for REDCap and Qualtrics surveys, or task type. The post-scan
stimulus rating task is treated as an EmoRep task instead of a
Qualtrics survey.

"""
# %%
import os
import glob
from typing import Tuple
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


# %%
class _DescStat:
    """Supply statistic and plotting methods.

    Each method supplies their own examples.

    Attributes
    ----------
    col_data : list
        List of df columns containing numeric type data
    df : pd.DataFrame
        Wide- or long-formatted survey dataframe

    Methods
    -------
    calc_factor_stats(fac_col, fac_a, fac_b)
        Wrap calc_row_stats for multiple factors e.g. visit
    calc_long_stats(grp_a, grp_b)
        Calculate descriptive stats from long-formatted dataframe
        that has two grouping factors
    calc_row_stats()
        Generate descriptive stats for df rows i.e. subjects
    confusion_heatmap(**args)
        Generate heatmap from confusion matrix
    confusion_matrix(emo_list, subj_col, num_exp)
        Generate confusion matrices of participant stimulus
        endorsement proportions
    draw_double_boxplot(fac_col, fac_a, fac_b, main_title, out_path)
        Generate and write a boxplot with two factors
    draw_long_boxplot(**args)
        Generate wide boxplot for long-formatted dataframe using
        two grouping factors
    draw_single_boxplot(main_title, out_path)
        Generate and write a boxplot of row totals

    """

    def __init__(self, df: pd.DataFrame, col_data: list = None):
        """Initialize."""
        print("\tInitializing _DescStat")
        self.df = df
        if col_data:
            self.col_data = col_data

    def calc_row_stats(self):
        """Calculate descriptive stats for dataframe rows.

        Returns
        -------
        dict
            key, value pairs for number of participants, mean,
            standard deviation, skewness, kurtosis.

        Raises
        ------
        AttributeError
            col_data not set
        KeyError
            Columns from col_data not found in df

        Examples
        --------
        stat_obj = _DescStat(
            pd.DataFrame, col_data=["col_a", "col_b", "col_c"]
        )
        stat_dict = stat_obj.calc_row_stats()

        stat_obj.df = df_new
        stat_obj.col_data = ["col_x", "col_y"]
        stat_dict_new = stat_obj.calc_row_stats()

        """
        # Validate attrs
        if not hasattr(self, "col_data"):
            raise AttributeError("Missing required col_data attr.")
        for col in self.col_data:
            if col not in self.df.columns:
                raise KeyError(f"Missing expected column in df : {col}")

        # Total row values, avoid editing original df
        df_calc = self.df.copy()
        df_calc["total"] = df_calc[self.col_data].sum(axis=1)

        # Calc, return stats
        mean = round(df_calc["total"].mean(), 2)
        std = round(df_calc["total"].std(), 2)
        skew = round(df_calc["total"].skew(), 2)
        kurt = round(df_calc["total"].kurt(), 2)
        num = df_calc.shape[0]
        return {
            "num": num,
            "mean": mean,
            "SD": std,
            "skewness": skew,
            "kurtosis": kurt,
        }

    def calc_factor_stats(self, fac_col, fac_a, fac_b):
        """Calculate descriptive stats for dataframe rows, by factor column.

        Subset dataframe by fac_col, for rows matching fac_a|b, and then
        generate row descriptive stats for subset df.

        Parameters
        ----------
        fac_col : str
            Column name of pd.DataFrame containing fac_a, fac_b.
        fac_a : str
            Factor A name, e.g. "Visit 2"
        fac_b : str
            Factor B name, e.g. "Visit 3"

        Returns
        -------
        dict
            key = fac_a|b
            value = dict of descriptive stats

        Example
        -------
        stat_obj = _DescStat(
            pd.DataFrame, col_data=["col_a", "col_b", "col_c"]
        )
        stat_dict = stat_obj.calc_factor_stats("visit", "Visit 2", "Visit 3")

        """
        # Make bool masks for factors
        df_calc = self.df.copy()
        mask_a = df_calc[fac_col] == fac_a
        mask_b = df_calc[fac_col] == fac_b

        # Get subset descriptive stats
        out_dict = {}
        for fac, mask in zip([fac_a, fac_b], [mask_a, mask_b]):
            self.df = df_calc[mask]
            out_dict[fac] = self.calc_row_stats()
        self.df = df_calc.copy()
        return out_dict

    def draw_single_boxplot(self, main_title, out_path):
        """Draw boxplot for single factor.

        Parameters
        ----------
        main_title : str
            Main title of boxplot
        out_path : path
            Output location and file name of figure

        Examples
        --------
        stat_obj = _DescStat(
            pd.DataFrame, col_data=["col_a", "col_b", "col_c"]
        )
        stat_obj.draw_single_boxplot("Title", "/path/to/output/fig.png")

        stat_obj.df = df_subscale
        stat_obj.col_data = ["col_x", "col_y"]
        stat_obj.draw_single_boxplot(
            "Sub title", "/path/to/output/fig_sub.png"
        )

        """
        # Get needed data and setup dataframe
        stat_dict = self.calc_row_stats()
        df_plot = self.df.copy()
        df_plot["total"] = df_plot[self.col_data].sum(axis=1)

        # Draw plot
        fig, ax = plt.subplots()
        plt.boxplot(df_plot["total"])
        plt.ylabel("Participant Total")
        plt.tick_params(
            axis="x", which="both", bottom=False, top=False, labelbottom=False
        )
        _, ub = ax.get_ylim()
        plt.text(
            0.55,
            ub - 0.1 * ub,
            f"mean={stat_dict['mean']}\nSD={stat_dict['SD']}",
            horizontalalignment="left",
        )
        plt.title(main_title)

        # Write and close plot
        plt.savefig(out_path)
        plt.close()
        print(f"\t\tDrew boxplot : {out_path}")

    def draw_double_boxplot(
        self,
        fac_col: str,
        fac_a: str,
        fac_b: str,
        main_title: str,
        out_path: str,
    ):
        """Draw a boxplot with two factors.

        Parameters
        ----------
        fac_col : str
            Column name of pd.DataFrame containing fac_a, fac_b.
        fac_a : str
            Factor A name, e.g. "Visit 2"
        fac_b : str
            Factor B name, e.g. "Visit 3"
        main_title : str
            Main title of boxplot
        out_path : path
            Output location and file name of figure

        Example
        -------
        stat_obj = _DescStat(
            pd.DataFrame, col_data=["col_a", "col_b", "col_c"]
        )
        stat_obj.draw_double_boxplot(
            "visit", "Visit 2", "Visit 3", "Title", "/path/to/output/fig.png"
        )

        """
        # Get needed data and setup dataframes
        stat_dict = self.calc_factor_stats(fac_col, fac_a, fac_b)
        df_plot = self.df.copy()
        df_plot["total"] = df_plot[self.col_data].sum(axis=1)
        df_a = df_plot.loc[df_plot[fac_col] == fac_a, "total"]
        df_b = df_plot.loc[df_plot[fac_col] == fac_b, "total"]

        # Draw boxplots
        fig, ax = plt.subplots()
        plt.boxplot([df_a, df_b])
        plt.ylabel("Participant Total")
        ax.set_xticklabels([fac_a, fac_b])
        ax.get_xaxis().tick_bottom()
        # plt.xlabel("X-label")
        _, ub = ax.get_ylim()
        plt.text(
            0.55,
            ub - 0.1 * ub,
            f"mean={stat_dict[fac_a]['mean']}\nSD={stat_dict[fac_a]['SD']}",
            horizontalalignment="left",
        )
        plt.text(
            2.15,
            ub - 0.1 * ub,
            f"mean={stat_dict[fac_b]['mean']}\nSD={stat_dict[fac_b]['SD']}",
            horizontalalignment="left",
        )
        plt.title(main_title)

        # Write and close
        plt.savefig(out_path)
        print(f"\t\tDrew boxplot : {out_path}")
        plt.close()

    def calc_long_stats(self, grp_a, grp_b):
        """Caculate descriptive stats from long-formatted dataframe.

        Subset calculation by two grouping columns, e.g. "task", "emotion"
        if self.df.columns contains "task" and "emotion". Grouping columns
        can have multiple values i.e. movies, scenarios for "task" or
        all 15 emotions for "emotion".

        Parameters
        ----------
        grp_a : str
            Grouping column name
        grp_b : str
            Grouping column name

        Returns
        -------
        pd.DataFrame
            Long-formatted dataframe with stats for each combination
            of grp_a * grp_b.

        Raises
        ------
        KeyError
            Column name grp_a|b not found in df

        Example
        -------
        stat_obj = _DescStat(pd.DataFrame)
        stat_dict = stat_obj.calc_long_stats("task", "emotion")

        """
        # Check df for columns
        for col in [grp_a, grp_b]:
            if col not in self.df.columns:
                raise KeyError(f"Missing expected column in df : {col}")

        # Calc stats
        df_mean = self.df.groupby([grp_a, grp_b]).mean()
        df_mean.columns = [*df_mean.columns[:-1], "mean"]
        df_std = self.df.groupby([grp_a, grp_b]).std()
        df_skew = self.df.groupby([grp_a, grp_b]).skew(numeric_only=True)
        df_kurt = self.df.groupby([grp_a, grp_b]).apply(
            pd.DataFrame.kurt
        )  # Throws the FutureWarning

        # Combine relevant values, round
        df_mean["std"] = df_std.iloc[:, -1:]
        df_mean["skew"] = df_skew.iloc[:, -1:]
        df_mean["kurt"] = df_kurt.iloc[:, -1:]
        df_mean.iloc[:, -4:] = round(df_mean.iloc[:, -4:], 2)
        return df_mean

    def draw_long_boxplot(
        self,
        x_col,
        x_lab,
        y_col,
        y_lab,
        hue_order,
        hue_col,
        main_title,
        out_path,
    ):
        """Generate a wide plot with multiple boxplots.

        Using a long-formatted dataframe, generate a figure with
        multiple boxplots organized by two factors (x_col, hue_col).

        Parameters
        ----------
        x_col : str
            Column name for x-axis, values should by string dtype
        x_lab : str
            X-axis label
        y_col : str
            Column name for y-axis, values should by numeric dtype
        y_lab : str
            Y-axis label
        hue_order : list
            Order of factors
        hue_col : str
            Column for hue ordering, values should by string dtype
        main_title : str
            Main title of plot
        out_path : path
            Output location and file name

        Raises
        ------
        KeyError
            Specified columns (x|y|hue_col) not found in df
        TypeError
            Incorrect dtype of columns

        Example
        -------
        stat_obj = _DescStat(pd.DataFrame)
        stat_obj.draw_long_boxplot(
            x_col="emotion",
            x_lab="Emotion Category",
            y_col="rating",
            y_lab="Frequency",
            hue_order=["Scenarios", "Videos"],
            hue_col="task",
            main_title="In-Scan Resting Emotion Frequency",
            out_path="/some/path/file.png",
        )

        """
        # Validate arguments and types
        for _col in [x_col, y_col, hue_col]:
            if _col not in self.df.columns:
                raise KeyError(f"Missing expected column in df : {_col}")
        if not is_numeric_dtype(self.df[y_col]):
            raise TypeError("df[y_col] should be numeric type")
        if not is_string_dtype(self.df[x_col]) or not is_string_dtype(
            self.df[hue_col]
        ):
            raise TypeError("df[x_col] and df[hue_col] should be string type")

        # Draw and write plot
        df_plot = self.df.copy()
        df_plot[y_col] = df_plot[y_col].astype(float)
        fig, ax = plt.subplots()
        plt.figure().set_figwidth(10)
        sns.boxplot(
            x=x_col,
            y=y_col,
            hue=hue_col,
            data=df_plot,
            hue_order=hue_order,
        )
        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
        plt.ylabel(y_lab)
        plt.xlabel(x_lab)
        plt.xticks(rotation=45, ha="right")
        plt.title(main_title)
        plt.savefig(out_path, bbox_inches="tight")
        print(f"\t\tDrew boxplot : {out_path}")
        plt.close()

    def confusion_matrix(
        self,
        emo_list,
        subj_col,
        num_exp,
        emo_col="emotion",
        resp_col="response",
    ):
        """Generate a confusion matrix of emotion endorsements.

        Calculate the proportion of emotion endorsements for each emotion,
        and supply data as a confusion matrix. Dataframe should be
        long-formatted.

        Parameters
        ----------
        emo_list : list
            All emotions presented,
            len == 15
        subj_col : str
            Dataframe column identifer for subject IDs,
            dtype = string
        num_exp : int
            Number of possible endorsements each subject
            could have given an emotion.
        emo_col : str, optional
            Dataframe column identifier for emotion stimulus
        resp_col : str, optional
            Dataframe column identifier for participant endorsement,
            dtype = string

        Returns
        -------
        tuple of pd.DataFrame
            [0] = proportion confusion matrix
            [1] = count confusion matrix

        Raises
        ------
        KeyError
            Specified column string missing in df.columns
        TypeError
            Incorrect df dtypes or parameter types
        ValueError
            Unexpected number of emotions

        Example
        -------
        stat_obj = _DescStat(pd.DataFrame)
        df_prop, df_count = stat_obj.confusion_matrix(
            ["emo1", ... "emo15"], "subj_id", 5
        )

        """
        # Validate user input, dataframe types
        for _col in [subj_col, emo_col, resp_col]:
            if _col not in self.df.columns:
                raise KeyError(f"Missing expected column in df : {_col}")
        if len(emo_list) != 15:
            raise ValueError(
                f"Expected emo_list to have len == 15, found : {len(emo_list)}"
            )
        if not is_string_dtype(self.df[emo_col]) or not is_string_dtype(
            self.df[resp_col]
        ):
            raise TypeError("df[y_col] and df[hue_col] should be string type")
        if not isinstance(num_exp, int):
            raise TypeError("Expected type int for num_exp")

        # Set denominator for proportion calc
        max_total = num_exp * len(self.df[subj_col].unique())

        # Calc proportion each emotion is endorsed as every emotion
        count_dict = {}
        prop_dict = {}
        for emo in emo_list:
            count_dict[emo] = {}
            prop_dict[emo] = {}
            df_emo = self.df[self.df[emo_col] == emo]
            for sub_emo in emo_list:
                count_emo = len(df_emo[df_emo[resp_col].str.contains(sub_emo)])
                count_dict[emo][sub_emo] = count_emo
                prop_dict[emo][sub_emo] = round(count_emo / max_total, 2)
        del df_emo

        # Generate dataframe
        df_prop = pd.DataFrame.from_dict(
            {i: prop_dict[i] for i in prop_dict.keys()},
            orient="index",
        )
        df_count = pd.DataFrame.from_dict(
            {i: count_dict[i] for i in count_dict.keys()},
            orient="index",
        )
        return (df_prop, df_count)

    def confusion_heatmap(
        self,
        df_conf,
        main_title,
        out_path,
        y_lab="Stimulus Category",
        x_lab="Participant Endorsement",
    ):
        """Draw a heatmap from a confusion matrix.

        Parameters
        ----------
        df_conf : pd.DataFrame, _DescStat.confusion_matrix
            Dataframe of confusion matrix
        main_title : str
            Main title of plot
        out_path : path
            Output location and file name
        x_lab : str, optional
            X-axis label
        y_lab : str, optional
            Y-axis label

        Example
        -------
        stat_obj = _DescStat(pd.DataFrame)
        df_conf, _ = stat_obj.confusion_matrix(
            ["emo1", ... "emo15"], "subj_id", 5
        )
        stat_obj.confusion_heatmap(df_conf, "Title", "/some/path/file.png")

        """
        # Draw and write
        ax = sns.heatmap(df_conf)
        ax.set(xlabel=x_lab, ylabel=y_lab)
        ax.set_title(main_title)
        plt.savefig(out_path, bbox_inches="tight")
        print(f"\t\tDrew heatmap plot : {out_path}")
        plt.close()


# %%
class Visit1Stats(_DescStat):
    """Get Visit 1 data and supply statistic, plotting methods.

    Construct pd.DataFrame from cleaned Visit 1 survey data for a specific
    survey (e.g. AIM, TAS), and supply methods for generating descriptive
    statistics and figures.

    Inherits _DescStat.

    Example
    -------
    stat_obj = Visit1Stats("/path/to/AIM.csv", "AIM")
    stat_dict = stat_obj.calc_row_stats()
    stat_obj.draw_single_boxplot("Title", "/path/to/output/fig.png")

    """

    def __init__(self, csv_path, survey_name):
        """Initialize.

        Triggers construction of survey dataframe.

        Parameters
        ----------
        csv_path : path
            Location of cleaned survey CSV, requires columns
            "study_id" and "<survey_name>_*".
        survey_name : str
            Short name of survey, found in column names of CSV

        Raises
        ------
        FileNotFoundError
            File missing at csv_path

        """
        print("Initializing Visit1Stats")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Missing file : {csv_path}")
        df, col_data = self._make_df(csv_path, survey_name)
        super().__init__(df, col_data)

    def _make_df(
        self, csv_path: str, survey_name: str
    ) -> Tuple[pd.DataFrame, list]:
        """Return survey data as dataframe and data column names."""
        # Read in data, check column names
        df = pd.read_csv(csv_path)
        if "study_id" not in df.columns:
            raise KeyError("Expected dataframe to have column : study_id")
        col_str = "\t".join(df.columns)
        if survey_name not in col_str:
            raise KeyError(
                f"Expected dataframe column name that contains {survey_name}"
            )

        # Organize df for use by _DescStat
        df = df.set_index("study_id")
        df = df.drop(labels=["datetime"], axis=1)
        col_data = [x for x in df.columns if survey_name in x]
        df[col_data] = df[col_data].astype("Int64")
        return (df, col_data)


# %%
class Visit23Stats(_DescStat):
    """Get Visit 2, 3 data and supply statistic, plotting methods.

    Construct pd.DataFrame from cleaned Visit 2 and 3 survey data
    for a specific survey (e.g. BDI), and supply methods for
    generating descriptive statistics and figures.

    Inherits _DescStat.

    Example
    -------
    fac_col = "visit"
    fac_a = "Visit 2"
    fac_b = "Visit 3"
    stat_obj = Visit23Stats(
        "/path/to/day2/BDI.csv",
        "/path/to/day3/BDI.csv",
        "BDI",
        fac_col,
        fac_a,
        fac_b,
    )
    stat_dict = stat_obj.calc_factor_stats(fac_col, fac_a, fac_b)
    stat_obj.draw_double_boxplot(
        fac_col, fac_a, fac_b, "Title", "/path/to/output/fig.png"
    )

    """

    def __init__(
        self, day2_csv_path, day3_csv_path, survey_name, fac_col, fac_a, fac_b
    ):
        """Initialize.

        Triggers construction of concatenated survey dataframe, with
        added "visit" column.

        Parameters
        ----------
        day2_csv_path : path
            Location of cleaned visit 2 survey CSV, requires columns
            "study_id" and "<survey_name>_*".
        day3_csv_path : path
            Location of cleaned visit 3 survey CSV, requires columns
            "study_id" and "<survey_name>_*".
        survey_name : str
            Short name of survey, found in column names of CSV
        fac_col : str
            Column name for writing fac_a|b
        fac_a : str
            Value for identifying day2 data
        fac_b : str
            Value for identifying day3 data

        Raises
        ------
        FileNotFoundError
            File missing at csv_path

        """
        print("Initializing Visit23Stats")

        # Validate
        for chk_path in [day2_csv_path, day3_csv_path]:
            if not os.path.exists(chk_path):
                raise FileNotFoundError(
                    f"Missing expected CSV file : {chk_path}"
                )

        # Get visit data and check column names
        df_day2, col_day2 = self._make_df(
            day2_csv_path, survey_name, fac_a, fac_col
        )
        df_day3, col_day3 = self._make_df(
            day3_csv_path, survey_name, fac_b, fac_col
        )
        if col_day2 != col_day3:
            raise ValueError("Dataframes do not have identical column names.")

        # Make single df
        df = pd.concat([df_day2, df_day3], ignore_index=True)
        super().__init__(df, col_day2)

    def _make_df(
        self, csv_path: str, survey_name: str, fac: str, fac_col: str
    ) -> Tuple[pd.DataFrame, list]:
        """Return survey data as dataframe and data column names."""
        # Read in data, check column names
        df = pd.read_csv(csv_path)
        if "study_id" not in df.columns:
            raise KeyError("Expected dataframe to have column : study_id")
        col_str = "\t".join(df.columns)
        if survey_name not in col_str:
            raise KeyError(
                f"Expected dataframe column name that contains {survey_name}"
            )

        # Organize df for use by _DescStat
        df = df.drop(labels=["datetime"], axis=1)
        col_data = [x for x in df.columns if survey_name in x]
        df[col_data] = df[col_data].astype("Int64")
        df[fac_col] = fac
        return (df, col_data)


# %%
class RestRatings(_DescStat):
    """Get resting frequency data and supply statistic, plotting methods.

    Construct pd.DataFrame from cleaned Visit 2 and 3 resting-state
    emotion frequency responses and supply methods for generating
    descriptive statistics and figures.

    Inherits _DescStat.

    Methods
    -------
    write_stats(out_path)
        Calculate descriptive stats and save as CSV table

    Example
    -------
    rest_stats = RestRatings("/path/to/project/dir")
    df_long = rest_stats.write_stats("/path/to/output/file.csv")
    rest_stats.draw_long_boxplot(**args)

    """

    def __init__(self, proj_dir):
        """Initialize.

        Trigger construction of long-formatted dataframe of
        resting state emotion frequency responses.

        Parameters
        ----------
        proj_dir : path
            Location of project's experiment directory

        """
        self._proj_dir = proj_dir
        df_long = self._get_data()
        super().__init__(df_long)

    def _get_data(self) -> pd.DataFrame:
        """Construct dataframe from clean visit 2, 3 data."""
        # Identify rest-ratings files
        day2_path = os.path.join(
            self._proj_dir,
            "data_survey/visit_day2/data_clean",
            "df_rest-ratings.csv",
        )
        day3_path = os.path.join(
            self._proj_dir,
            "data_survey/visit_day3/data_clean",
            "df_rest-ratings.csv",
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
        df_rest_all["task"] = df_rest_all["task"].str.title()
        df_rest_all["task"] = df_rest_all["task"].replace("Movies", "Videos")
        del df_day2, df_day3

        # Subset df for integer responses
        df_rest_int = df_rest_all[
            df_rest_all["resp_type"] == "resp_int"
        ].copy()
        df_rest_int = df_rest_int.drop(["datetime", "resp_type"], axis=1)
        excl_list = ["study_id", "visit", "task"]
        emo_list = [x for x in df_rest_int.columns if x not in excl_list]
        df_rest_int[emo_list] = df_rest_int[emo_list].astype("Int64")

        # Convert to long format, organize columns
        df_long = pd.melt(
            df_rest_int,
            id_vars=["study_id", "visit", "task"],
            value_vars=emo_list,
            var_name="emotion",
            value_name="rating",
        )
        df_long["emotion"] = df_long["emotion"].str.title()
        df_long["rating"] = df_long["rating"].astype("Int64")
        for str_col in ["emotion", "visit", "task"]:
            df_long[str_col] = df_long[str_col].astype(pd.StringDtype())
        df_long = df_long.dropna(axis=0)
        return df_long

    def write_stats(self, out_path):
        """Calculate and write descriptive statistics.

        Parameters
        ----------
        out_path : path
            Output location and name of statistic table

        Returns
        -------
        pd.DataFrame
            Long-formatted descriptive stats

        """
        df_stats = self.calc_long_stats("task", "emotion")
        df_stats.to_csv(out_path)
        print(f"\tWrote csv : {out_path}")
        return df_stats


class StimRatings(_DescStat):
    """Processs post-scan stimulus ratings data.

    Construct pd.DataFrame from cleaned Visit 2 and 3 post-scan
    stimulus ratings, then calculate descriptive statistics and
    draw plots for emotion endorsement, valence, and arousal
    responses.

    Inherits _DescStat.

    Attributes
    ----------
    out_dir : path
        Output destination for generated files

    Methods
    -------
    endorsement(stim_type)
        Generate stats and plots for endorsement responses
    arousal_valence(stim_type, prompt_name)
        Generate stats and plots for arousal and valence responses

    Example
    -------
    stim_stats = StimRatings("/path/to/project/dir", True)
    stim_stats.endorsement("Videos")
    stim_stats.arousal_valence("Videos")

    """

    def __init__(self, proj_dir, draw_plot):
        """Initialize.

        Trigger construction of long-formatted dataframe of
        resting state emotion frequency responses.

        Parameters
        ----------
        proj_dir : path
            Location of project's experiment directory
        draw_plot : bool
            Whether to draw figures

        Attributes
        ----------
        out_dir : path
            Output destination for generated files
        _emo_list : list
            Emotions categories presented in task

        Raises
        ------
        TypeError
            Incorrect user-input parameter types

        """
        # Validate user input
        if not isinstance(draw_plot, bool):
            raise TypeError("Expected draw_plot type bool")

        # Set attrs
        print("\nInitializing DescriptStimRatings")
        self._draw_plot = draw_plot
        self._proj_dir = proj_dir
        self.out_dir = os.path.join(proj_dir, "analyses/metrics_surveys")

        # Trigger dataframe construction, initialize helper
        df, self._emo_list = self._get_data()
        super().__init__(df)

    def _get_data(self) -> Tuple[pd.DataFrame, list]:
        """Make a dataframe of stimulus ratings."""
        # Check for cleaned files
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

        # Read-in, combine visit data
        df_day2 = pd.read_csv(day2_path)
        df_day3 = pd.read_csv(day3_path)
        df_all = pd.concat([df_day2, df_day3], ignore_index=True)
        df_all = df_all.sort_values(
            by=["study_id", "session", "type", "emotion", "prompt"]
        ).reset_index(drop=True)

        # Manage column types and get list of emos
        df_all["emotion"] = df_all["emotion"].str.title()
        for col_name in ["session", "type", "emotion", "prompt"]:
            df_all[col_name] = df_all[col_name].astype(pd.StringDtype())
        emo_list = df_all["emotion"].unique().tolist()
        return (df_all, emo_list)

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
        tuple of pd.DataFrame
            [0] = proportion confusion matrix
            [1] = count confusion matrix

        Raises
        ------
        ValueError
            Unexpected stimulus type

        """
        if stim_type not in ["Videos", "Scenarios"]:
            raise ValueError(f"Unexpected stimulus type : {stim_type}")
        print(f"\tGenerating descriptives of endorsement for : {stim_type}")

        # Get endorsement data for stimulus type
        df_all = self.df.copy()
        df_end = df_all[
            (df_all["type"] == stim_type) & (df_all["prompt"] == "Endorsement")
        ]

        # Generate confusion matrix of endorsement probabilities
        out_prop = os.path.join(
            self.out_dir,
            f"table_stim-ratings_endorsement-prop_{stim_type.lower()}.csv",
        )
        out_count = out_prop.replace("-prop", "-count")
        self.df = df_end
        df_prop, df_count = self.confusion_matrix(
            self._emo_list, "study_id", 5
        )
        df_prop.to_csv(out_prop)
        df_count.to_csv(out_count)
        print(f"\t\tWrote dataset : {out_prop}")
        print(f"\t\tWrote dataset : {out_count}")

        # Draw heatmap
        if self._draw_plot:
            out_plot = os.path.join(
                self.out_dir,
                "plot_heatmap-prop_stim-ratings_endorsement_"
                + f"{stim_type.lower()}.png",
            )
            self.confusion_heatmap(
                df_prop,
                main_title=f"Post-Scan {stim_type[:-1]} "
                + "Endorsement Proportion",
                out_path=out_plot,
            )
        self.df = df_all.copy()
        return (df_prop, df_count)

    def arousal_valence(self, stim_type):
        """Generate descriptive info for emotion valence and arousal ratings.

        Caculate descriptive stats for each emotion category, save
        dataframe and draw boxplots.

        Parameters
        ----------
        stim_type : str
            [Videos | Scenarios]
            Stimulus modality of task

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
        # Validate
        if stim_type not in ["Videos", "Scenarios"]:
            raise ValueError(f"Unexpected stimulus type : {stim_type}")

        # Get relevant data
        print(f"\tGenerating descriptives for {stim_type}: Arousal, Valence")
        df_all = self.df.copy()
        df_av = df_all[
            (df_all["type"] == stim_type) & (df_all["prompt"] != "Endorsement")
        ].copy()
        df_av["response"] = df_av["response"].astype("Int64")

        # Calculate descriptive stats, write out
        self.df = df_av
        df_stats = self.calc_long_stats("prompt", "emotion")
        out_path = os.path.join(
            self.out_dir,
            f"table_stim-ratings_{stim_type.lower()}.csv",
        )
        df_stats.to_csv(out_path)
        print(f"\t\tWrote dataset : {out_path}")

        # Draw boxplot
        if self._draw_plot:
            out_plot = os.path.join(
                self.out_dir,
                f"plot_boxplot-long_stim-ratings_{stim_type.lower()}.png",
            )
            self.draw_long_boxplot(
                x_col="emotion",
                x_lab="Emotion Category",
                y_col="response",
                y_lab="Rating",
                hue_order=["Arousal", "Valence"],
                hue_col="prompt",
                main_title=f"Post-Scan {stim_type[:-1]} Ratings",
                out_path=out_plot,
            )
        self.df = df_all.copy()
        return df_stats


# %%
class EmorepTask(_DescStat):
    """Generate descriptive stats and plots for EmoRep task data.

    Concatenate all data from all participants' events files into
    a long-formatted dataframe, and then generate descriptive
    statistics and plots for emotion and intensity selection trials.

    Inherits _DescStat.

    Attributes
    ----------
    out_dir : path
        Output destination for generated files

    Methods
    -------
    select_emotion()
        Generate descriptive stats and plots for emotion selection trials
    select_intensity()
        Generate descriptive stats and plots for intensity selection trials

    Example
    -------
    stim_stats = EmorepTask("/path/to/project/dir", True)
    stim_stats.select_intensity()
    stim_stats.select_emotion("Videos")

    """

    def __init__(self, proj_dir, draw_plot):
        """Initialize.

        Find all participant BIDS events files, trigger construction
        of dataframe from events files.

        Parameters
        ----------
        proj_dir : path
            Location of project's experiment directory
        draw_plot : bool
            Whether to draw figures

        Attributes
        ----------
        out_dir : path
            Output destination for generated files

        Raises
        ------
        TypeError
        ValueError
            Events files were not detected

        """
        if not isinstance(draw_plot, bool):
            raise TypeError("Expected draw_plot type bool")

        print("\nInitializing DescriptTask")
        self._draw_plot = draw_plot
        self.out_dir = os.path.join(proj_dir, "analyses/metrics_surveys")

        # Find all events files, make and initialize dataframe
        mri_rawdata = os.path.join(proj_dir, "data_scanner_BIDS", "rawdata")
        events_all = sorted(
            glob.glob(f"{mri_rawdata}/**/*_events.tsv", recursive=True)
        )
        if not events_all:
            raise ValueError(
                f"Expected to find BIDS events files in : {mri_rawdata}"
            )
        df = self._get_data(events_all)
        super().__init__(df)

    def _get_data(self, events_all: list) -> pd.DataFrame:
        """Combine all events files into dataframe."""
        print("\tBuilding dataframe of all participant events.tsv")
        df_all = pd.DataFrame(
            columns=[
                "onset",
                "duration",
                "trial_type",
                "stim_info",
                "response",
                "response_time",
                "accuracy",
                "emotion",
                "subj",
                "sess",
                "task",
                "run",
            ]
        )
        for event_path in events_all:
            subj, sess, task, run, _ = os.path.basename(event_path).split("_")
            df = pd.read_csv(event_path, sep="\t")
            df["subj"] = subj.split("-")[-1]
            df["sess"] = sess.split("-")[-1]
            df["task"] = task.split("-")[-1]
            df["run"] = int(run[-1])
            df_all = pd.concat([df_all, df], ignore_index=True)
            del df

        # Extract participant response rows
        df_resp = df_all.loc[
            df_all["trial_type"].isin(
                ["movie", "scenario", "emotion", "intensity"]
            )
        ].reset_index(drop=True)
        del df_all

        # Organize dataframe
        df_resp["emotion"] = df_resp["emotion"].fillna(method="ffill")
        df_resp = df_resp.loc[
            ~df_resp["trial_type"].isin(["movie", "scenario"])
        ].reset_index(drop=True)
        df_resp = df_resp.drop(
            ["onset", "duration", "accuracy", "stim_info"], axis=1
        )

        # Clean up column values and types
        df_resp["emotion"] = df_resp["emotion"].str.title()
        df_resp["task"] = df_resp["task"].str.title()
        df_resp["task"] = df_resp["task"].replace("Movies", "Videos")
        df_resp["run"] = df_resp["run"].astype("Int64")
        return df_resp

    def select_intensity(self):
        """Generate descriptive stats and plots for intensity selection.

        Caculate descriptive stats for each emotion category and
        task type, save dataframe and draw boxplots.

        Returns
        -------
        pd.DataFrame
            Descriptive stats of task, emotion

        """
        # Subset dataframe for intensity selection
        df_all = self.df.copy()
        df_int = df_all.loc[df_all["trial_type"] == "intensity"].copy()
        df_int["response"] = df_int["response"].replace("NONE", np.nan)
        df_int = df_int.reset_index(drop=True)
        df_int = df_int.drop(
            ["response_time", "run", "trial_type", "sess"], axis=1
        )

        # Organize dataframe for _DescStat.calc_long_stats
        df_int["task"] = df_int["task"].str.title()
        df_int["response"] = df_int["response"].astype("Int64")
        for str_col in ["emotion", "task"]:
            df_int[str_col] = df_int[str_col].astype(pd.StringDtype())
        df_int = df_int.sort_values(by=["subj", "emotion", "task"])

        # Calculate and write stats
        self.df = df_int
        df_stats = self.calc_long_stats(grp_a="task", grp_b="emotion")
        out_csv = os.path.join(self.out_dir, "table_task-intensity.csv")
        df_stats.to_csv(out_csv)
        print(f"\tWrote csv : {out_csv}")

        # Draw boxplot
        if self._draw_plot:
            out_plot = os.path.join(
                self.out_dir,
                "plot_boxplot-long_task-intensity.png",
            )
            self.draw_long_boxplot(
                x_col="emotion",
                x_lab="Emotion",
                y_col="response",
                y_lab="Intensity",
                hue_order=["Scenarios", "Videos"],
                hue_col="task",
                main_title="In-Scan Stimulus Ratings",
                out_path=out_plot,
            )
        self.df = df_all.copy()
        return df_stats

    def select_emotion(self, task):
        """Generate descriptive stats and plots for emotion selection.

        Caculate proportion of ratings for each emotion category, save
        dataframe and draw confusion matrix.

        Parameters
        ----------
        task : str
            [Videos | Scenarios]
            Stimulus modality of task

        Returns
        -------
        tuple of pd.DataFrame
            [0] = proportion confusion matrix
            [1] = count confusion matrix

        Raises
        ------
        ValueError
            Unexpected input parameter

        """
        if task not in ["Videos", "Scenarios"]:
            raise ValueError(f"Unexpected task value : {task}")

        # Subset dataframe for emotion selection
        df_all = self.df.copy()
        df_emo = df_all.loc[
            (df_all["trial_type"] == "emotion") & (df_all["task"] == task)
        ].copy()
        df_emo["response"] = df_emo["response"].str.title()
        df_emo = df_emo.reset_index(drop=True)

        # Determine emotions
        emo_all = df_emo["emotion"].unique().tolist()
        emo_all.sort()

        # Calculate confusion matrix, write out
        self.df = df_emo
        df_prop, df_count = self.confusion_matrix(emo_all, "subj", 2)
        out_prop = os.path.join(
            self.out_dir,
            f"table_task-emotion_endorsement-prop_{task.lower()}.csv",
        )
        out_count = out_prop.replace("-prop", "-count")
        df_prop.to_csv(out_prop)
        df_count.to_csv(out_count)
        print(f"\t\tWrote dataset : {out_prop}")
        print(f"\t\tWrote dataset : {out_count}")

        # Draw heatmap
        if self._draw_plot:
            out_plot = os.path.join(
                self.out_dir,
                f"plot_heatmap-prop_task-emotion_{task.lower()}.png",
            )
            self.confusion_heatmap(
                df_count,
                main_title=f"In-Scan {task[:-1]} Endorsement Proportion",
                out_path=out_plot,
            )
        self.df = df_all.copy()
        return (df_prop, df_count)


# %%
