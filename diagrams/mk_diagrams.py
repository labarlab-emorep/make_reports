# %%
from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import DataPipeline
from diagrams.aws.compute import Compute
from diagrams.aws.compute import Batch
from diagrams.aws.database import Database
from diagrams.aws.devtools import CommandLineInterface
from diagrams.aws.general import General
from diagrams.programming.language import Bash
from diagrams.aws.storage import Storage

# %%
with Diagram("imports", direction="TB", show=False):

    with Cluster("cli"):
        cli_chk_data = General("chk_data")
        cli_gen_guids = General("gen_guids")
        cli_get_surveys = General("get_surveys")
        cli_rep_metrics = General("rep_metrics")
        cli_rep_ndar = General("rep_ndar")
        cli_rep_regular = General("rep_regular")
        cli_sur_stats = General("sur_stats")

    with Cluster("workflows"):
        wf_beh_reports = General("behavioral_reports")
        wf_data_metrics = General("data_metrics")
        wf_req_reports = General("required_reports")

    with Cluster("resources"):
        rsc_bld_ndar = General("build_ndar")
        rsc_bld_reports = General("build_reports")
        rsc_calc_metrics = General("calc_metrics")
        rsc_calc_surveys = General("calc_surveys")
        rsc_chk_data = General("check_data")
        rsc_mng_data = General("manage_data")
        rsc_rep_helper = General("report_helper")
        rsc_sql_database = General("sql_database")
        rsc_sur_clean = General("survey_clean")
        rsc_sur_download = General("survey_download")

    ref_files = General("reference_files")
    ref_dfs = General("data_frames")

    # CLI imports
    cli_chk_data << wf_data_metrics

    cli_gen_guids << rsc_rep_helper
    cli_gen_guids << wf_req_reports

    cli_get_surveys << rsc_mng_data
    cli_get_surveys << rsc_rep_helper
    cli_get_surveys << rsc_bld_reports

    cli_rep_metrics << rsc_rep_helper
    cli_rep_metrics << wf_data_metrics

    cli_rep_ndar << rsc_rep_helper
    cli_rep_ndar << wf_req_reports

    cli_rep_regular << rsc_rep_helper
    cli_rep_regular << wf_req_reports

    cli_sur_stats << wf_beh_reports
    cli_sur_stats << rsc_rep_helper

    # Workflow imports
    wf_beh_reports << rsc_mng_data
    wf_beh_reports << rsc_calc_surveys
    wf_data_metrics << rsc_calc_metrics
    wf_data_metrics << rsc_chk_data
    wf_req_reports << rsc_bld_ndar
    wf_req_reports << rsc_mng_data
    wf_req_reports << rsc_bld_reports

    # Resource imports
    rsc_bld_ndar << rsc_rep_helper

    rsc_bld_reports << rsc_mng_data
    rsc_bld_reports << rsc_rep_helper
    rsc_bld_reports << rsc_sql_database

    rsc_calc_metrics << rsc_bld_reports
    rsc_calc_metrics << rsc_rep_helper
    rsc_calc_metrics << rsc_sur_download

    rsc_calc_surveys << rsc_mng_data

    rsc_chk_data << rsc_bld_reports
    rsc_chk_data << rsc_rep_helper

    rsc_mng_data << rsc_sur_download
    rsc_mng_data << rsc_sur_clean
    rsc_mng_data << rsc_rep_helper
    rsc_mng_data << rsc_sql_database

    rsc_rep_helper << ref_files
    rsc_rep_helper << ref_dfs
    rsc_rep_helper << rsc_sur_download

    rsc_sql_database << rsc_rep_helper

    rsc_sur_clean << rsc_sql_database
    rsc_sur_clean << rsc_rep_helper
    rsc_sur_clean << ref_files

    rsc_sur_download << rsc_rep_helper
    rsc_sur_download << ref_files


# %%
graph_attr = {
    "layout": "dot",
    "compound": "true",
}


# %%
with Diagram("process chk_data", graph_attr=graph_attr, show=False):
    with Cluster("cli"):
        cli_chk_data = CommandLineInterface("chk_data")

    with Cluster("workflows.data_metrics"):
        with Cluster("CheckProjectMri"):
            wf_run_check = Compute("run_check")
            wf_write_csv = Storage("write CSV")

    with Cluster("resources.check_data"):
        with Cluster("CheckMri"):
            with Cluster("check_emorep"):
                rsc_chk_dcm = Compute("check_dcmnii")
                rsc_chk_bids = Compute("check_bids")
                rsc_chk_mriqc = Compute("check_mriqc")
            with Cluster("check_archival"):
                rsc_chk_arch = Compute()

            rsc_multiproc = Batch("multi_chk")

    (
        cli_chk_data
        >> Edge(lhead="cluster_workflows.data_metrics")
        >> wf_run_check
    )
    wf_run_check >> Edge(lhead="cluster_check_emorep") >> rsc_chk_dcm
    rsc_chk_dcm >> rsc_chk_bids >> rsc_chk_mriqc >> rsc_multiproc
    (
        wf_run_check
        >> Edge(lhead="cluster_check_archival")
        >> rsc_chk_arch
        >> rsc_multiproc
    )
    wf_run_check >> wf_write_csv


# %%
with Diagram("process gen_guids", graph_attr=graph_attr, show=False):
    with Cluster("cli"):
        cli_gen_guids = CommandLineInterface("gen_guids")

    with Cluster("workflows.required_reports"):
        wf_gen_guids = Compute("generate_guids")

    with Cluster("resources.build_reports"):
        with Cluster("GenerateGuids"):
            rsc_mk_guids = Compute("make_guids")
            rsc_chk_guids = Compute("check_guids")

    with Cluster("resources.manage_data"):
        with Cluster("GetRedcap.get_redcap"):
            rsc_get_demo = Compute("demographics")
            rsc_get_guid = Compute("guid")

    with Cluster("subshell"):
        sys_guid_tool = Bash("guid-tool")
        sys_write_txt = Storage("write TXT")

    cli_gen_guids >> wf_gen_guids
    wf_gen_guids >> rsc_mk_guids
    rsc_mk_guids << rsc_get_demo
    rsc_mk_guids >> sys_guid_tool
    sys_guid_tool >> sys_write_txt

    wf_gen_guids >> rsc_chk_guids
    rsc_chk_guids << rsc_get_guid
    rsc_chk_guids >> rsc_mk_guids
    rsc_chk_guids << sys_write_txt


# %%
with Diagram("process get_surveys", graph_attr=graph_attr, show=False):
    with Cluster("cli"):
        cli_get_surveys = CommandLineInterface("get_surveys")

    with Cluster("Keoki"):
        bids_files = Storage("BIDS files")

    with Cluster("resources"):
        with Cluster("manage_data"):
            with Cluster("GetRest"):
                rsc_get_rest = DataPipeline("get_rest")
            with Cluster("GetRedcap"):
                rsc_get_redcap = DataPipeline("get_redcap")
            with Cluster("GetQualtrics"):
                rsc_get_qual = DataPipeline("get_qualtrics")
            with Cluster("GetTask"):
                rsc_get_task = DataPipeline("get_task")

        with Cluster("survey_download"):
            rsc_sur_dl_rc = Compute("dl_redcap")
            rsc_sur_dl_qual = Compute("dl_qualtrics")

        with Cluster("survey_clean"):
            rsc_sur_cl_rr = Compute("clean_rest_ratings")
            with Cluster("CleanRedcap"):
                rsc_sur_cl_rc = Compute("clean surveys")
            with Cluster("CleanQualtrics"):
                rsc_sur_cl_qual = Compute("clean surveys")

        with Cluster("report_helper"):
            with Cluster("CheckStatus"):
                rsc_stat_change = Compute("status_change")
            rsc_pull_rc = Compute("pull_redcap_data")
            rsc_pull_qual = Compute("pull_qualtrics_data")

        with Cluster("sql_database"):
            rsc_db_connect = Compute("DbConnect")
            with Cluster("DbUpdate"):
                rsc_db_update = Compute("update_db")

        with Cluster("build_reports"):
            with Cluster("DemoAll"):
                rsc_mk_compl = DataPipeline("make_complete")

    with Cluster("Helper Files"):
        with Cluster("dataframes"):
            ref_dfs = Storage("track_status.csv")

        with Cluster("reference_files"):
            ref_file = Storage("report_keys")

    with Cluster("LaBarLab Databases"):
        rsc_db_emorep = Database("db_emorep")

    with Cluster("Survey Databases"):
        db_redcap = Database("REDCap")
        db_qualtrics = Database("Qualtrics")

    # GetRest
    (
        cli_get_surveys
        >> rsc_get_rest
        >> Edge(color="")
        << rsc_sur_cl_rr
        << rsc_stat_change
        << ref_dfs
    )
    rsc_sur_cl_rr << bids_files
    rsc_get_rest >> rsc_db_update >> rsc_db_connect >> rsc_db_emorep

    # GetRedcap
    (
        cli_get_surveys
        >> rsc_get_redcap
        >> Edge(color="")
        << rsc_sur_dl_rc
        << ref_file
    )
    rsc_sur_dl_rc >> Edge(color="") << rsc_pull_rc << db_redcap
    rsc_get_redcap >> Edge(color="") << rsc_sur_cl_rc
    rsc_get_redcap >> rsc_db_update

    # GetQualtrics
    (
        cli_get_surveys
        >> rsc_get_qual
        >> Edge(color="")
        << rsc_sur_dl_qual
        << ref_file
    )
    rsc_sur_dl_qual >> Edge(color="") << rsc_pull_qual << db_qualtrics
    rsc_get_qual >> Edge(color="") << rsc_sur_cl_qual << rsc_stat_change
    rsc_get_qual >> rsc_db_update

    # GetTask
    cli_get_surveys >> rsc_get_task << bids_files
    rsc_get_task >> rsc_db_update

    # GetDemo
    cli_get_surveys >> rsc_mk_compl >> Edge(color="") << rsc_get_redcap
    rsc_mk_compl << rsc_stat_change
    rsc_mk_compl >> rsc_db_update
