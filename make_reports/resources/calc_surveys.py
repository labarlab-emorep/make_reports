"""Generate descriptive statistics for participant responses.

Manage and report on participant responses to REDCap, Qualtrics,
and EmoRep tasks. Methods are organized according to their visit
for REDCap and Qualtrics surveys, or task type. The post-scan
stimulus rating task is treated as an EmoRep task instead of a
Qualtrics survey.

Visit1Stats : generate stats, plots for visit1 surveys
Visit23Stats : generate stats, plots for visit2, 3 data and surveys
RestRatings : generate stats, plots for post-rest ratings task
StimRatings : generate stats, plots for stimulus ratings task
EmorepTask : generate stats, plots emorep task

"""

# %%
import os
from typing import Tuple
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from make_reports.resources import manage_data


# %%
class _DescStat:
    """Supply statistic and plotting methods.

    Parameters
    ----------
    df : pd.DataFrame
        Wide- or long-formatted survey dataframe
    col_data : list, optional
        List of df columns containing numeric type data

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

    def __init__(self, df, col_data=None):
        """Initialize."""
        self.df = df
        self.col_data = col_data

    def _valid_cols(self):
        """Validate supplied cols in df."""
        if not hasattr(self, "col_data"):
            raise AttributeError("Missing required col_data attr.")
        for col in self.col_data:
            if col not in self.df.columns:
                raise KeyError(f"Missing expected column in df : {col}")

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
        self._valid_cols()

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
        stat_obj = _DescStat(pd.DataFrame)
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
        self._valid_cols()
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
        plt.savefig(out_path, dpi=300)
        plt.close()
        print(f"\t\tDrew boxplot : {out_path}")

    def draw_single_histogram(self, main_title, out_path, bin_num=20):
        """Draw histogram for single factor.

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
        stat_obj.draw_single_histogram(
            "Sub title", "/path/to/output/fig_sub.png"
        )

        """
        # Get needed data and setup dataframe
        self._valid_cols()
        df_plot = self.df.copy()
        df_plot[self.col_data] = df_plot[self.col_data].astype("Int64")
        df_plot["sum"] = df_plot[self.col_data].sum(axis=1)

        # Draw plot
        plt.hist(
            df_plot["sum"],
            bins=range(0, int(df_plot["sum"].max())),
            color="skyblue",
            edgecolor="black",
        )
        plt.xlabel("Response Total")
        plt.ylabel("Frequency")
        plt.title(main_title)

        # Write and close plot
        plt.savefig(out_path, dpi=300)
        plt.close()
        print(f"\t\tDrew histogram : {out_path}")

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
        self._valid_cols()
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
        plt.savefig(out_path, dpi=300)
        print(f"\t\tDrew boxplot : {out_path}")
        plt.close()

    def draw_double_histogram(
        self,
        fac_col: str,
        fac_a: str,
        fac_b: str,
        main_title: str,
        out_path: str,
    ):
        """Draw histogram for two factors.

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
        out_path : str, os.PathLike
            Output location and file name of figure

        Example
        -------
        stat_obj = _DescStat(
            pd.DataFrame, col_data=["col_a", "col_b", "col_c"]
        )
        stat_obj.draw_double_histogram(
            "visit", "Visit 2", "Visit 3", "Title", "/path/to/output/fig.png"
        )

        """
        # Get needed data and setup dataframe
        self._valid_cols()
        df_plot = self.df.copy()
        df_plot[self.col_data] = df_plot[self.col_data].astype("Int64")
        df_plot["sum"] = df_plot[self.col_data].sum(axis=1)

        # Draw plot
        plt.hist(
            [
                df_plot.loc[df_plot[fac_col] == fac_a, "sum"],
                df_plot.loc[df_plot[fac_col] == fac_b, "sum"],
            ],
            bins=range(0, int(df_plot["sum"].max())),
            stacked=True,
            color=["cyan", "Purple"],
            edgecolor="black",
        )
        plt.xlabel("Response Total")
        plt.ylabel("Frequency")
        plt.title(main_title)

        # Write and close plot
        plt.savefig(out_path, dpi=300)
        plt.close()
        print(f"\t\tDrew histogram : {out_path}")

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
        plt.savefig(out_path, bbox_inches="tight", dpi=300)
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
        ax.set_title(main_title, weight="bold", fontsize=15)
        plt.savefig(out_path, bbox_inches="tight", dpi=300)
        print(f"\t\tDrew heatmap plot : {out_path}")
        plt.close()


# %%
class Visit1Stats(_DescStat):
    """Get Visit 1 data and supply statistic, plotting methods.

    Construct pd.DataFrame from cleaned Visit 1 survey data for a specific
    survey (e.g. AIM, TAS), and supply methods for generating descriptive
    statistics and figures.

    Inherits _DescStat.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned survey data, requires columns
        "study_id" and "<survey_name>_*".
    survey_name : str
        Short name of survey, found in column names of CSV

    Example
    -------
    stat_obj = Visit1Stats(pd.DataFrame, "AIM")
    stat_dict = stat_obj.calc_row_stats()
    stat_obj.draw_single_boxplot("Title", "/path/to/output/fig.png")

    """

    def __init__(self, df, survey_name):
        """Initialize.

        Triggers construction of survey dataframe.

        """
        print("Initializing Visit1Stats")

        # Validate df
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pd.DataFrame, got df : {type(df)}")
        if "study_id" not in df.columns:
            raise KeyError("Expected dataframe to have column : study_id")
        col_str = "\t".join(df.columns)
        if survey_name not in col_str:
            raise KeyError(
                f"Expected dataframe column name that contains {survey_name}"
            )
        self.df = df
        col_data = self._prep_df(survey_name)
        super().__init__(df, col_data=col_data)

    def _prep_df(self, survey_name: str) -> list:
        """Prep df for analyses, return relevant column names."""
        self.df = self.df.set_index("study_id")
        self.df = self.df.drop(labels=["datetime"], axis=1)
        col_data = [x for x in self.df.columns if survey_name in x]
        self.df[col_data] = self.df[col_data].astype("Int64")
        return col_data


# %%
class Visit23Stats(_DescStat):
    """Get Visit 2, 3 data and supply statistic, plotting methods.

    Construct pd.DataFrame from cleaned Visit 2 and 3 survey data
    for a specific survey (e.g. BDI), and supply methods for
    generating descriptive statistics and figures.

    Inherits _DescStat.

    Parameters
    ----------
    df_day2 : pd.DataFrame
        Cleaned visit 2 survey, requires columns
        "study_id" and "<survey_name>_*".
    df_day3 : pd.DataFrame
        Cleaned visit 3 survey, requires columns
        "study_id" and "<survey_name>_*".
    survey_name : str
        Short name of survey, found in column names of CSV
    fac_col : str
        Column name for writing fac_a|b
    fac_a : str
        Value for identifying day2 data
    fac_b : str
        Value for identifying day3 data

    Example
    -------
    fac_col = "visit"
    fac_a = "Visit 2"
    fac_b = "Visit 3"
    stat_obj = Visit23Stats(
        df_day2,
        df_day3,
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

    def __init__(self, df_day2, df_day3, survey_name, fac_col, fac_a, fac_b):
        """Initialize.

        Triggers construction of concatenated survey dataframe, with
        added "visit" column.

        """
        print("Initializing Visit23Stats")

        # Get visit data and check column names
        df_day2_clean, col_day2 = self._make_df(
            df_day2, survey_name, fac_a, fac_col
        )
        df_day3_clean, col_day3 = self._make_df(
            df_day3, survey_name, fac_b, fac_col
        )
        if col_day2 != col_day3:
            raise ValueError("Dataframes do not have identical column names.")

        # Make single df
        df = pd.concat([df_day2_clean, df_day3_clean], ignore_index=True)
        super().__init__(df, col_data=col_day2)

    def _make_df(
        self, df: pd.DataFrame, survey_name: str, fac: str, fac_col: str
    ) -> Tuple[pd.DataFrame, list]:
        """Return survey data as dataframe and data column names."""
        # Validate
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pd.DataFrame, got df : {type(df)}")
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
class RestRatings(manage_data.GetRest, _DescStat):
    """Get resting frequency data and supply statistic, plotting methods.

    Construct pd.DataFrame from cleaned Visit 2 and 3 resting-state
    emotion frequency responses and supply methods for generating
    descriptive statistics and figures.

    Inherits manage_data.GetRest, _DescStat.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project parent directory

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

        """
        super().__init__(proj_dir)
        self.get_rest()
        df = self._make_df()
        _DescStat.__init__(self, df)

    def _make_df(self) -> pd.DataFrame:
        """Construct dataframe from clean visit 2, 3 data."""
        # Make master dataframe
        df_day2 = self.clean_rest["study"]["visit_day2"]["rest_ratings"].copy(
            deep=True
        )
        df_day3 = self.clean_rest["study"]["visit_day3"]["rest_ratings"].copy(
            deep=True
        )
        df_day2 = df_day2.replace(88, np.NaN)
        df_day3 = df_day3.replace(88, np.NaN)
        df_rest_all = pd.concat([df_day2, df_day3], ignore_index=True)
        df_rest_all = df_rest_all.sort_values(
            by=["study_id", "visit", "resp_type"]
        ).reset_index(drop=True)
        df_rest_all["task"] = df_rest_all["task"].str.title()
        # df_rest_all["task"] = df_rest_all["task"].replace("Movies", "Videos")

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
        out_path : str, os.PathLike
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

    Parameters
    ----------
    proj_dir : path
        Location of project's experiment directory
    draw_plot : bool
        Whether to draw figures
    df_day2 : pd.DataFrame
        Cleaned visit 2 post-scan stim ratings responses
    df_day3 : pd.DataFrame
        Cleaned visit 3 post-scan stim ratings responses

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
    stim_stats.endorsement("Movies")
    stim_stats.arousal_valence("Movies")

    """

    def __init__(self, proj_dir, draw_plot, df_day2, df_day3):
        """Initialize.

        Trigger construction of long-formatted dataframe of
        resting state emotion frequency responses.

        Attributes
        ----------
        out_dir : path
            Output destination for generated files

        """
        # Validate user input
        if not isinstance(draw_plot, bool):
            raise TypeError("Expected draw_plot type bool")

        # Set attrs
        print("\nInitializing DescriptStimRatings")
        self._draw_plot = draw_plot
        self.out_dir = os.path.join(proj_dir, "analyses/metrics_surveys")

        # Trigger dataframe construction, initialize helper
        self.df, self._emo_list = self._get_data(df_day2, df_day3)
        super().__init__(self.df)

    def _get_data(
        self, df_day2: pd.DataFrame, df_day3: pd.DataFrame
    ) -> Tuple[pd.DataFrame, list]:
        """Make a dataframe of stimulus ratings."""
        # Combine visit data
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
            [Movies | Scenarios]
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
        if stim_type not in ["Movies", "Scenarios"]:
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
                "plot_stim-ratings_endorsement_heatmap-prop_"
                + f"{stim_type.lower()}.png",
            )
            self.confusion_heatmap(
                df_prop,
                main_title=f"Post-Scan {stim_type} "
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
            [Movies | Scenarios]
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
        if stim_type not in ["Movies", "Scenarios"]:
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
                f"plot_stim-ratings_{stim_type.lower()}_boxplot-long.png",
            )
            self.draw_long_boxplot(
                x_col="emotion",
                x_lab="Emotion Category",
                y_col="response",
                y_lab="Rating",
                hue_order=["Arousal", "Valence"],
                hue_col="prompt",
                main_title=f"Post-Scan {stim_type} Ratings",
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
    stim_stats.select_emotion("Movies")

    """

    def __init__(self, proj_dir, draw_plot):
        """Initialize.

        Find all participant BIDS events files, trigger construction
        of dataframe from events files.

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

        print("\nInitializing EmorepTask")
        self._draw_plot = draw_plot
        self.out_dir = os.path.join(proj_dir, "analyses/metrics_surveys")

        # Get and organize cleaned data
        gt = manage_data.GetTask(proj_dir)
        gt.get_task()
        df_day2 = gt.clean_task["study"]["visit_day2"]["in_scan_task"]
        df_day3 = gt.clean_task["study"]["visit_day3"]["in_scan_task"]
        self.df = pd.concat([df_day2, df_day3], axis=0, ignore_index=True)
        self.df["task"] = self.df["task"].str.title()
        self.df["block"] = self.df["block"].str.title()
        self.df["resp_emotion"] = self.df["resp_emotion"].str.title()

        # Initialize stat helper
        super().__init__(self.df)

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
        df_int = df_all.drop(["visit", "run", "resp_emotion"], axis=1)

        # Organize dataframe for _DescStat.calc_long_stats
        for str_col in ["task", "block"]:
            df_int[str_col] = df_int[str_col].astype(pd.StringDtype())
        df_int = df_int.sort_values(by=["study_id", "block", "task"])

        # Calculate and write stats
        self.df = df_int
        df_stats = self.calc_long_stats("task", "block")
        out_csv = os.path.join(self.out_dir, "table_task-intensity.csv")
        df_stats.to_csv(out_csv)
        print(f"\tWrote csv : {out_csv}")

        # Draw boxplot
        if self._draw_plot:
            out_plot = os.path.join(
                self.out_dir,
                "plot_task-intensity_boxplot-long.png",
            )
            self.draw_long_boxplot(
                x_col="block",
                x_lab="Emotion",
                y_col="resp_intensity",
                y_lab="Intensity",
                hue_order=["Scenarios", "Movies"],
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
            [Movies | Scenarios]
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
        if task not in ["Movies", "Scenarios"]:
            raise ValueError(f"Unexpected task value : {task}")

        # Subset dataframe for emotion selection
        df_all = self.df.copy()
        df_emo = df_all.loc[df_all["task"] == task].copy()
        df_emo = df_emo.drop(["resp_intensity", "visit"], axis=1)

        # Determine emotions
        emo_all = df_emo["block"].unique().tolist()
        emo_all.sort()

        # Calculate confusion matrix, write out
        self.df = df_emo
        df_prop, df_count = self.confusion_matrix(
            emo_all, "study_id", 2, emo_col="block", resp_col="resp_emotion"
        )
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
                f"plot_task-emotion_{task.lower()}_heatmap-prop.png",
            )
            self.confusion_heatmap(
                df_count,
                main_title=f"In-Scan {task} Endorsement Proportion",
                out_path=out_plot,
            )
        self.df = df_all.copy()
        return (df_prop, df_count)


# %%
