"""Resources for checking data completeness.

CheckMri : check for expected rawdata and derivatives
CheckEmorepComplete : determine which EmoRep participants
    are missing data

"""
import os
import glob
import time
import pandas as pd
import numpy as np
from typing import Union, Tuple
from make_reports.resources import build_reports
from make_reports.resources import report_helper


class CheckMri:
    """Conduct checks for pipeline steps.

    Test whether each subject has the expected data, and
    capture the results in a dataframe.

    Dataframe cell values:
        - timestamp : all expected data were found
        - int : only some of expected data were found
        - np.nan : no files were found

    Parameters
    ----------
    subj_list : list
        Subjects encountered in project sourcedata
    sess_list : list
        Session identifiers without BIDS formatting
    raw_dir : path
        Location of project rawdata
    deriv_dir : path
        Location of project derivatives

    Attributes
    ----------
    df_mri : pd.DataFrame
        Dataframe of subj, MRI encountered

    Methods
    -------
    check_emorep()
        Check for EmoRep pipeline files, fills df_mri
    check_archival()
        Check for archival pipeline files, fills df_mri

    Example
    -------
    check_data = CheckMri(*args)
    check_data.check_emorep()
    df_emorep = check_data.df_mri

    """

    def __init__(self, subj_list, sess_list, raw_dir, deriv_dir):
        """Initialize."""
        print("\tInitializing CheckData")
        self._subj_list = subj_list
        self._sess_list = sess_list
        self._raw_dir = raw_dir
        self._deriv_dir = deriv_dir

    def _get_time(self, mri_file: str) -> str:
        """Return datetime string of file maketime."""
        return time.strftime(
            "%Y-%m-%d",
            time.strptime(time.ctime(os.path.getmtime(mri_file))),
        )

    def _compare_count(self, search_path, search_str, num_exp, col_name):
        """Compare encountered files to desired number.

        Search through directories to make a list of files. Compare length
        of list against desired quantity, then make cell value according
        to (a) whether all are found (datetime), (b) only some are found (int),
        or (c) no files are found (np.nan).

        Parameters
        ----------
        search_path : path
            Parent directory, where subjects will be searched
        search_str : str
            File suffix and extension to be searched for, will be
            prepended by wildcard.
        num_exp : int
            Number of expected files
        col_name : str
            Column name of self.df_mri to update

        """
        # Start empty column, search each subject
        print(f"\t\tChecking {col_name} ...")
        col_out = []
        for subj in self._subj_list:
            # Search for files, count
            file_list = sorted(
                glob.glob(
                    f"{search_path}/sub-{subj}/ses-{self._sess}/{search_str}",
                    recursive=True,
                )
            )
            file_num = len(file_list)

            # Determine input value for col_out
            if file_num == num_exp:
                h_val = self._get_time(file_list[-1])
            elif file_num > 0:
                h_val = file_num
            else:
                h_val = np.nan
            col_out.append(h_val)
        self._update_df(col_out, col_name)

    def _update_df(self, in_val: list, col_name: str):
        """Update column of df_mri."""
        idx_sess = self.df_mri.index[
            self.df_mri["sess"] == self._sess
        ].tolist()
        self.df_mri.loc[idx_sess, col_name] = in_val

    def _check_bids(self):
        """Check for BIDS organization, update dataframe."""
        print("\t\tChecking bidsification ...")
        is_bids = []
        for subj in self._subj_list:
            # Find contents of session dir, get names
            sess_dirs = glob.glob(
                f"{self._raw_dir}/sub-{subj}/ses-{self._sess}/*"
            )
            sess_names = [os.path.basename(x) for x in sess_dirs]

            # Set value according to whether anat is found, update is_bids
            h_val = (
                self._get_time(sess_dirs[0])
                if "anat" in sess_names
                else np.nan
            )
            is_bids.append(h_val)
        self._update_df(is_bids, "bidsify")

    def _check_mriqc(self):
        """Check for MRIQC output, update dataframe."""
        print("\t\tChecking MRIQC ...")
        mriqc_found = []
        for subj in self._subj_list:
            # Set path str
            mriqc_file = os.path.join(
                self._deriv_dir,
                "mriqc",
                f"sub-{subj}_ses-{self._sess}_T1w.html",
            )

            # Set value, update mriqc_found according to whether file exists.
            h_val = (
                self._get_time(mriqc_file)
                if os.path.exists(mriqc_file)
                else np.nan
            )
            mriqc_found.append(h_val)
        self._update_df(mriqc_found, "mriqc")

    def _check_dcmnii(self):
        """Check for dcm2niix output."""
        nii_found = []
        for subj in self._subj_list:
            # Find files, get counts
            search_path = os.path.join(
                self._raw_dir, f"sub-{subj}", f"ses-{self._sess}"
            )
            nii_list = sorted(
                glob.glob(f"{search_path}/**/*.nii.gz", recursive=True)
            )
            num_nii = len(nii_list)
            num_anat = len([x for x in nii_list if "anat/" in x])
            num_fmap = len([x for x in nii_list if "fmap/" in x])
            num_func = len([x for x in nii_list if "func/" in x])

            # Compare to anticipated totals
            if (
                num_anat == 1
                and (num_fmap == 1 or num_fmap == 2)
                and num_func == 9
            ):
                nii_found.append(self._get_time(nii_list[-1]))
            elif num_nii == 0:
                nii_found.append(np.nan)
            elif num_nii > 0:
                nii_found.append(num_nii)
        self._update_df(nii_found, "dcm-nii")

    def check_emorep(self):
        """Check for expected EmoRep files.

        Checks for the existence, organization of the following files:
            -   dcm2niix output
            -   BIDS organization
            -   MRIQC output
            -   task, rest behavioral data
            -   physio data
            -   defaced anats
            -   fmriprep output
            -   fsl preprocessing output
            -   fsl first-level models of rest, task
            -   fsl second-level models of task

        Attributes
        ----------
        df_mri : pd.DataFrame
            Dataframe of subj, MRI encountered

        """
        # Get info for checking, start df
        col_names, check_info = self._info_emorep()
        self._start_df(col_names)

        # Check for each scan session
        for self._sess in self._sess_list:
            print(f"\tChecking session : {self._sess}")

            # Check for bids organization and mriqc
            self._check_bids()
            self._check_mriqc()
            self._check_dcmnii()

            # Check steps in check_info, add session to search string
            for step, trip in check_info.items():
                self._compare_count(trip[0], trip[1], trip[2], step)

    def check_archival(self):
        """Check for expected archival files.

        Checks for the existence, organization of the following files:
            -   existence of anat, func files
            -   fmriprep output
            -   fsl preprocessing output
            -   fsl first-level models of rest

        Attributes
        ----------
        df_mri : pd.DataFrame
            Dataframe of subj, MRI encountered

        """
        # Get info for checking, start df
        col_names, check_info = self._info_archival()
        self._start_df(col_names)

        # Check for each scan session
        for self._sess in self._sess_list:
            print(f"\tChecking session : {self._sess}")
            for step, trip in check_info.items():
                self._compare_count(trip[0], trip[1], trip[2], step)

    def _start_df(self, col_names: list):
        """Generate pd.DataFrame from subject, session values."""
        df_mri = pd.DataFrame(columns=col_names)
        df_mri["subid"] = self._subj_list * len(self._sess_list)
        df_mri = df_mri.sort_values(by=["subid"], ignore_index=True)
        self.df_mri = df_mri
        self.df_mri["sess"] = self._sess_list * len(self._subj_list)

    def _info_emorep(self) -> Tuple[list, dict]:
        """Return list of column names, dict of expected data."""
        # Setup info for checking EmoRep.
        #   Key is preprocessing step which will become a column name
        #       of self.df_mri.
        #   Value is triple of:
        #       [0] = Path of parent directory location
        #       [1] = Searchable path/string by glob for matching files, e.g.:
        #           "**/*.nii.gz"
        #           "func/*desc-preproc_bold.nii.gz"
        #           "*defaced.nii.gz"
        #       [2] = int, number of files glob is expected to match
        chk_dict = {
            "task-events": (
                self._raw_dir,
                "func/*events.tsv",
                8,
            ),
            "task-rest": (self._raw_dir, "beh/*rest-ratings*.tsv", 1),
            "physio": (self._raw_dir, "phys/*physio.acq", 9),
            "deface": (
                os.path.join(self._deriv_dir, "deface"),
                "*defaced.nii.gz",
                1,
            ),
            "fmriprep": (
                os.path.join(self._deriv_dir, "pre_processing/fmriprep"),
                "func/*desc-preproc_bold.nii.gz",
                9,
            ),
            "fsl-preproc": (
                os.path.join(self._deriv_dir, "pre_processing/fsl_denoise"),
                "func/*desc-scaled_bold.nii.gz",
                9,
            ),
            # "afni-univ": (
            #     os.path.join(self._deriv_dir, "model_afni"),
            #     "func/decon_univ_stats_REML+tlrc.HEAD",
            #     1,
            # ),
            # "afni-rest": (
            #     os.path.join(self._deriv_dir, "model_afni"),
            #     "func/decon_rest_anaticor+tlrc.HEAD",
            #     1,
            # ),
            "fsl-rest": (
                os.path.join(self._deriv_dir, "model_fsl"),
                "func/run-01_level-first_name-rest.feat/stats/cope1.nii.gz",
                1,
            ),
            "fsl-sep-first": (
                os.path.join(self._deriv_dir, "model_fsl"),
                "func/*level-first_name-sep.feat/stats/cope*.nii.gz",
                60,
            ),
            "fsl-sep-second": (
                os.path.join(self._deriv_dir, "model_fsl"),
                "func/level-second_name-sep.gfeat/"
                + "cope1.feat/stats/cope*.nii.gz",
                30,
            ),
            "fsl-tog-first": (
                os.path.join(self._deriv_dir, "model_fsl"),
                "func/*level-first_name-tog.feat/stats/cope*.nii.gz",
                30,
            ),
            "fsl-tog-second": (
                os.path.join(self._deriv_dir, "model_fsl"),
                "func/level-second_name-tog.gfeat/"
                + "cope1.feat/stats/cope*.nii.gz",
                15,
            ),
            "fsl-lss": (
                os.path.join(self._deriv_dir, "model_fsl-lss"),
                "func/*level-first_name-lss*.feat/stats/cope1.nii.gz",
                150,
            ),
        }

        col_names = [
            "subid",
            "sess",
            "dcm-nii",
            "bidsify",
            "mriqc",
        ]
        for key in chk_dict:
            col_names.append(key)
        return (col_names, chk_dict)

    def _info_archival(self) -> Tuple[list, dict]:
        """Return list of column names, dict of expected data."""
        # Setup info for checking, see _info_emorep for notes.
        chk_dict = {
            "anat": (self._raw_dir, "anat/*.nii.gz", 1),
            "func": (self._raw_dir, "func/*.nii.gz", 1),
            "fmriprep": (
                os.path.join(self._deriv_dir, "pre_processing/fmriprep"),
                "func/*desc-preproc_bold.nii.gz",
                1,
            ),
            "fsl-preproc": (
                os.path.join(self._deriv_dir, "pre_processing/fsl_denoise"),
                "func/*desc-scaled_bold.nii.gz",
                1,
            ),
            "fsl-rest": (
                os.path.join(self._deriv_dir, "model_fsl"),
                "func/run-01_level-first_name-rest.feat/stats/cope1.nii.gz",
                1,
            ),
        }

        col_names = [
            "subid",
            "sess",
        ]
        for key in chk_dict:
            col_names.append(key)
        return (col_names, chk_dict)


class CheckEmorepComplete:
    """Compare encountered EmoRep data to expected.

    Iterate through all data to determine which participant
    is missing what survey responses or scanner files. Compare
    encountered participant data with the number of exepcted
    files and survey responses.

    Generated dataframe is wide-formatted, using participant
    as row and expected file/survey as column. Each cell
    has a number of potential values:
        -   pilot : participant was a pilot
        -   comp : complete data were found for item
        -   np.NaN : no data were found for item
        -   excl : participant was excluded at visit
        -   lost : participant was lost-to-follow at visit
        -   int : number of files found, which did not match expected

    When a visit status changes from enrolled, the subsequent visit
    cells will be np.NaN, as no data is expected for these visits.

    Participants who withdrew from the study are not included in
    the output, as this serves to identiy data that are usable
    for analyses.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project parent directory

    Attributes
    ----------
    df_check : pd.DataFrame
        Contains values for each participant * data type

    Methods
    -------
    check_data()
        Condut data completion check

    Example
    -------
    cec = CheckEmorepComplete()
    cec.check_data()
    df_out = cec.df_check

    """

    def __init__(self, proj_dir):
        """Initialize."""
        print("Initializing CheckEmorepComplete")
        self._proj_dir = proj_dir
        self._setup()

    def _setup(self):
        """Setup for conducting completion check."""
        # Load participant info
        rc_demo = build_reports.DemoAll(self._proj_dir)
        rc_demo.remove_withdrawn()
        add_status = report_helper.AddStatus()
        self._df_demo = add_status.enroll_status(
            rc_demo.final_demo, "src_subject_id"
        )

        # Determine participants, data expected, start df
        self._subj_list = self._df_demo["src_subject_id"].to_list()
        self._expected()
        self._start_df()

    def _start_df(self):
        """Start a pd.DataFrame to update."""
        col_list = ["subid"]
        for visit in self._ref_dict:
            for data_type in self._ref_dict[visit]:
                for measure in self._ref_dict[visit][data_type]:
                    col_list.append(f"{visit}_{data_type}_{measure}")
        self.df_check = pd.DataFrame(columns=col_list)
        self.df_check["subid"] = self._subj_list

    def _expected(self):
        """Hardcode expected data and files."""
        self._ref_dict = {
            "visit1": {
                "surveys": [
                    "AIM",
                    "ALS",
                    "ERQ",
                    "PSWQ",
                    "RRS",
                    "STAI-Trait",
                    "TAS",
                ]
            },
            "visit2": {
                "surveys": [
                    "BDI",
                    "PANAS",
                    "STAI-State",
                    "post-scan-ratings",
                    "rest-ratings",
                ],
                "scanner": {
                    "anat": [1],
                    "func": [9],
                    "fmap": [1, 2],
                    "phys": [9],
                    "task": [8],
                },
            },
            "visit3": {
                "surveys": [
                    "BDI",
                    "PANAS",
                    "STAI-State",
                    "post-scan-ratings",
                    "rest-ratings",
                ],
                "scanner": {
                    "anat": [1],
                    "func": [9],
                    "fmap": [1, 2],
                    "phys": [9],
                    "task": [8],
                },
            },
        }

    def check_data(self):
        """Conduct completion check for all visits.

        Compare encountered data, files from each visit
        with expected numbers. Checks for the various
        Qualtrics and REDCap surveys, MRI data, EmoRep task
        data, and physio data.

        """
        self._compare_surveys()
        for visit in ["visit2", "visit3"]:
            self._compare_scanner(visit)

    def _compare_surveys(self):
        """Check that subject is found in each survey."""
        print("\tChecking survey files for visit : visit1")

        # Get surveys, check for subj in each survey
        self._load_surveys()
        for col_name, df in self._df_dict.items():
            for subj in self._subj_list:
                cell_value = (
                    "comp" if subj in df["study_id"].to_list() else np.NaN
                )

                # Account for subj status change, update df
                cell_value = self._status_check(
                    subj, col_name.split("_")[0], cell_value
                )
                idx_subj_check = self._idx_check(subj)
                self.df_check.loc[idx_subj_check, col_name] = cell_value

    def _load_surveys(self):
        """Load cleaned surveys as chunks in a dict."""
        # Orient to cleaned dir for each survey
        self._df_dict = {}
        for visit in self._ref_dict:
            clean_dir = os.path.join(
                self._proj_dir, f"data_survey/visit_day{visit[-1]}/data_clean"
            )

            # Load each survey into dict, account for naming
            for survey in self._ref_dict[visit]["surveys"]:
                file_name = (
                    survey.replace("-", "_")
                    if survey != "rest-ratings"
                    else survey
                )
                file_path = os.path.join(clean_dir, f"df_{file_name}.csv")
                self._df_dict[f"{visit}_surveys_{survey}"] = pd.read_csv(
                    file_path
                )

    def _idx_check(self, subj) -> int:
        """Return index of participant in df_check."""
        return self.df_check.index[self.df_check["subid"] == subj].to_list()[0]

    def _status_check(
        self, subj: str, visit: str, cell_value: Union[str, int, float]
    ) -> Union[str, int, float]:
        """Check if participant does not have enrolled status for visit."""
        # Get status
        idx_subj_demo = self._idx_demo(subj)
        visit_status = self._df_demo.loc[idx_subj_demo, f"{visit}_status"]

        # Update cell value if needed
        cell_value = (
            visit_status[:4]
            if visit_status in ["excluded", "lost", "withdrew"]
            else cell_value
        )

        # Account for pilot participants
        cell_value = self._pilot_check(subj, cell_value)
        return cell_value

    def _pilot_check(
        self, subj: str, cell_value: Union[str, int, float]
    ) -> Union[str, int, float]:
        """Determine which participants were pilots."""
        cell_value = (
            "pilot"
            if subj in ["ER0001", "ER0002", "ER0003", "ER0004", "ER0005"]
            else cell_value
        )
        return cell_value

    def _idx_demo(self, subj) -> int:
        """Return index of participant in df_demo."""
        return self._df_demo.index[
            self._df_demo["src_subject_id"] == subj
        ].to_list()[0]

    def _compare_scanner(self, visit: str):
        """Compare encountered data from scanner to expected."""
        if visit not in ["visit2", "visit3"]:
            raise ValueError("Unexpected visit value.")

        # Find existing scanner files
        print(f"\tChecking scanner files for visit : {visit}")
        scan_dict = self._find_scanner(visit)
        for data_type, ref_count in self._ref_dict[visit]["scanner"].items():
            key_name = f"{visit}_scanner_{data_type}"

            # Check that each participant has expected files
            for subj in self._subj_list:
                idx_subj_check = self._idx_check(subj)
                subj_count = sum(subj in x for x in scan_dict[key_name])
                if subj_count:
                    cell_value = (
                        "comp" if subj_count in ref_count else subj_count
                    )
                else:
                    cell_value = np.NaN

                # Account for enrollment status
                cell_value = self._status_check(subj, visit, cell_value)
                self.df_check.loc[idx_subj_check, key_name] = cell_value

    def _find_scanner(self, visit: str) -> dict:
        """Return dict of paths to scanner files."""
        if visit not in ["visit2", "visit3"]:
            raise ValueError("Unexpected visit value.")

        # Setup search string, path
        dt_switch = {
            "anat": "anat/*T1w.nii.gz",
            "func": "func/*bold.nii.gz",
            "fmap": "fmap/*epi.nii.gz",
            "phys": "phys/*physio.txt",
            "task": "func/*task*_events.tsv",
        }
        raw_dir = os.path.join(self._proj_dir, "data_scanner_BIDS/rawdata")

        # Find all scanner files, populate dict
        scan_dict = {}
        for data_type in self._ref_dict[visit]["scanner"]:
            search_file = dt_switch[data_type]
            scan_dict[f"{visit}_scanner_{data_type}"] = [
                os.path.basename(x)
                for x in sorted(
                    glob.glob(
                        f"{raw_dir}/sub-*/ses-day{visit[-1]}/{search_file}"
                    )
                )
            ]
        return scan_dict
