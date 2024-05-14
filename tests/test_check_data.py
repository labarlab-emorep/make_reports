import pytest
import os
import math
from make_reports.resources import check_data


class Test_ChkRsc:

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.chk_rsc = check_data._ChkRsc()
        self.raw_dir = os.path.join(
            os.environ["PROJ_DIR"],
            "Exp2_Compute_Emotion",
            "data_scanner_BIDS",
            "rawdata",
        )
        self.subj = "sub-ER0009"
        self.sess = "ses-day2"

    def _run_start_df(self):
        self.chk_rsc.start_df(
            ["dcm-nii", "bidsify"], ["day2", "day3"], ["ER0009", "ER0087"]
        )

    def test_start_df(self):
        self._run_start_df()
        assert hasattr(self.chk_rsc, "df_mri")
        assert (4, 4) == self.chk_rsc.df_mri.shape
        assert ["dcm-nii", "bidsify", "subid", "sess"] == list(
            self.chk_rsc.df_mri.columns
        )
        assert "ER0087" == self.chk_rsc.df_mri.loc[2, "subid"]
        assert "day2" == self.chk_rsc.df_mri.loc[2, "sess"]

    def test_get_time(self):
        anat_path = os.path.join(
            self.raw_dir,
            self.subj,
            self.sess,
            "anat",
            f"{self.subj}_{self.sess}_T1w.nii.gz",
        )
        mk_time = self.chk_rsc.get_time(anat_path)
        assert "2023-04-13" == mk_time

    def test_compare_count(self):
        self.chk_rsc._sess = "day2"
        self.chk_rsc._search_path = self.raw_dir
        self.chk_rsc._search_str = f"anat/{self.subj}_{self.sess}*"
        self.chk_rsc._num_exp = 2

        # Check finding all, some, or none
        subj_id = self.subj.split("-")[-1]
        assert "2023-04-13" == self.chk_rsc._compare_count(subj_id)
        self.chk_rsc._num_exp = 3
        assert 2 == self.chk_rsc._compare_count(subj_id)
        self.chk_rsc._search_str = f"anat/{self.subj}_{self.sess}_foo*"
        assert math.isnan(self.chk_rsc._compare_count(subj_id))

    def test_multi_chk(self):
        self._run_start_df()
        self.chk_rsc.multi_chk(
            self.sess.split("-")[-1],
            "dcm-nii",
            self.raw_dir,
            "anat/*T1w*",
            ["ER0009", "ER0087"],
            2,
            num_proc=2,
        )
        assert "2023-04-13" == self.chk_rsc.df_mri.loc[0, "dcm-nii"]
        assert "2022-10-05" == self.chk_rsc.df_mri.loc[2, "dcm-nii"]

    def test_update_df(self):
        in_val = ["one", "two"]
        col_name = "dcm-nii"
        with pytest.raises(AttributeError):
            self.chk_rsc.update_df(in_val, col_name)

        self._run_start_df()
        self.chk_rsc._sess = "day3"
        self.chk_rsc.update_df(in_val, col_name)
        assert in_val == [
            self.chk_rsc.df_mri.loc[1, col_name],
            self.chk_rsc.df_mri.loc[3, col_name],
        ]


class Test_CheckEmorep:

    @pytest.fixture(autouse=True)
    def _setup(self):
        emorep_dir = os.path.join(
            os.environ["PROJ_DIR"],
            "Exp2_Compute_Emotion",
            "data_scanner_BIDS",
        )
        self.raw_dir = os.path.join(
            emorep_dir,
            "rawdata",
        )
        self.deriv_dir = os.path.join(emorep_dir, "derivatives")
        subj_list = ["ER0009", "ER0087"]
        self.chk_emo = check_data._CheckEmorep(
            subj_list, self.raw_dir, self.deriv_dir
        )
        self.chk_emo.start_df(["foo", "bar"], ["day2", "day3"], subj_list)

    def test_check_bids(self):
        self.chk_emo.check_bids()
        assert "bidsify" in list(self.chk_emo.df_mri.columns)
        assert "2023-04-13" == self.chk_emo.df_mri.loc[0, "bidsify"]
        assert "2022-10-05" == self.chk_emo.df_mri.loc[2, "bidsify"]

    def test_check_mriqc(self):
        self.chk_emo.check_mriqc()
        assert "mriqc" in list(self.chk_emo.df_mri.columns)
        assert "2024-02-09" == self.chk_emo.df_mri.loc[0, "mriqc"]
        assert "2023-09-05" == self.chk_emo.df_mri.loc[2, "mriqc"]

    def test_check_dcm2nii(self):
        self.chk_emo.check_dcmnii()
        assert "dcm-nii" in list(self.chk_emo.df_mri.columns)
        assert "2023-04-13" == self.chk_emo.df_mri.loc[0, "dcm-nii"]
        assert 9 == self.chk_emo.df_mri.loc[2, "dcm-nii"]

    def test_info_emorep(self):
        col_names, chk_dict = self.chk_emo.info_emorep()

        # Check some column names
        for chk_col in [
            "subid",
            "sess",
            "mriqc",
            "task-events",
            "physio",
            "deface",
            "fsl-preproc",
        ]:
            assert chk_col in col_names

        # Check some dict values
        assert (self.raw_dir, "func/*events.tsv", 8) == chk_dict["task-events"]
        assert (self.raw_dir, "phys/*physio.acq", 9) == chk_dict["physio"]
        assert (
            os.path.join(self.deriv_dir, "deface"),
            "*defaced.nii.gz",
            1,
        ) == chk_dict["deface"]


def test_CheckMri_emorep():
    # Setup to instantiate CheckMri for EmoRep
    emorep_dir = os.path.join(
        os.environ["PROJ_DIR"],
        "Exp2_Compute_Emotion",
        "data_scanner_BIDS",
    )
    raw_dir = os.path.join(
        emorep_dir,
        "rawdata",
    )
    deriv_dir = os.path.join(emorep_dir, "derivatives")
    subj_list = ["ER0009", "ER0087"]
    chk_mri = check_data.CheckMri(
        subj_list, ["day2", "day3"], raw_dir, deriv_dir
    )
    chk_mri.check_emorep()

    # Check shape and certain column names
    assert (4, 15) == chk_mri.df_mri.shape
    for chk_col in [
        "task-rest",
        "fsl-rest",
        "fsl-sep-first",
        "fsl-sep-second",
        "fsl-lss",
        "dot-sep-stim",
    ]:
        assert chk_col in chk_mri.df_mri.columns.to_list()

    # Check certain fields
    assert 1 == chk_mri.df_mri.loc[0, "dot-sep-stim"]
    assert "2023-04-13" == chk_mri.df_mri.loc[1, "task-rest"]
    assert "2023-12-07" == chk_mri.df_mri.loc[2, "fsl-sep-first"]


class TestCheckMri:

    @pytest.fixture(autouse=True)
    def _setup(self):
        archival_dir = os.path.join(
            os.environ["PROJ_DIR"],
            "Exp3_Classify_Archival",
            "data_mri_BIDS",
        )
        self.raw_dir = os.path.join(
            archival_dir,
            "rawdata",
        )
        self.deriv_dir = os.path.join(archival_dir, "derivatives")
        subj_list = ["08326", "13809"]
        self.chk_mri = check_data.CheckMri(
            subj_list, ["BAS1"], self.raw_dir, self.deriv_dir
        )
        self.chk_mri.check_archival()

    def test_check_archival(self):
        # Check shape and column names
        assert (2, 8) == self.chk_mri.df_mri.shape
        for chk_col in [
            "subid",
            "sess",
            "anat",
            "func",
            "fmriprep",
            "fsl-preproc",
            "fsl-rest",
            "dot-sep-stim",
        ]:
            assert chk_col in self.chk_mri.df_mri.columns.to_list()

        # Check certain fields
        assert "13809" == self.chk_mri.df_mri.loc[1, "subid"]
        assert "BAS1" == self.chk_mri.df_mri.loc[1, "sess"]
        assert "2023-05-16" == self.chk_mri.df_mri.loc[0, "anat"]
        assert 2 == self.chk_mri.df_mri.loc[0, "fsl-preproc"]
        assert "2023-08-29" == self.chk_mri.df_mri.loc[1, "fsl-rest"]

    def test_info_archival(self):
        col_names, chk_dict = self.chk_mri._info_archival()

        # Check some column names
        for chk_col in [
            "subid",
            "sess",
            "anat",
            "func",
            "fmriprep",
            "fsl-preproc",
            "fsl-rest",
            "dot-sep-stim",
        ]:
            assert chk_col in col_names

        # Check some dict values
        assert (self.raw_dir, "anat/*.nii.gz", 1) == chk_dict["anat"]
        assert (self.raw_dir, "func/*.nii.gz", 1) == chk_dict["func"]
        assert (
            os.path.join(self.deriv_dir, "pre_processing/fmriprep"),
            "func/*desc-preproc_bold.nii.gz",
            1,
        ) == chk_dict["fmriprep"]
        assert (
            os.path.join(self.deriv_dir, "model_fsl"),
            "func/run-01_level-first_name-rest.feat/stats/cope1.nii.gz",
            1,
        ) == chk_dict["fsl-rest"]
