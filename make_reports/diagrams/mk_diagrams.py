# %%
from diagrams import Cluster, Diagram
from diagrams.aws.general import General

# %%
# , show=False
with Diagram("imports", direction="TB"):

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
