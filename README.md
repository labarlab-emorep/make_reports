# make_reports
This package contains workflows for managing survey and task participant responses, generating reports, tracking data, and calculating basic descriptive statistics. Generally, each workflow downloads or finds original raw data, aggregates and cleans the data as appropriate, updates the MySQL database `db_emorep`, and then generates the report.

Sub-package/workflow navigation:
- [rep_get](#rep_get) : Download, aggregate, and clean survey and task responses
- [rep_regular](#rep_regular) : Generate demographic reports submitted to Duke and NIH
- [rep_ndar](#rep_ndar) : Generate reports and dataset submitted to the NDA
- [rep_metrics](#rep_metrics) : Make demographic, progress snapshots to aid acquisition
- [chk_data](#chk_data) : Check EmoRep and Archival MRI analysis pipelines progress
- [sur_stats](#sur_stats) : Compute descriptve stats and generate plots for participant responses
- [gen_guids](#gen_guids) : Generate and check participant GUIDs


## General Usage
- Install into project environment on labarserv2 (see [here](https://github.com/labarlab/conda_labarserv2)) via `$ python setup.py install`.
- Trigger general package help and usage via entrypoint `$ make_reports`.

```
(emorep)[nmm51-vm: day2_movies]$make_reports

Version : 2.3.0

The package make_reports consists of several sub-packages
that can be accessed from their respective entrypoints (below).

    rep_get     : Download and clean RedCap and Qualtrics surveys,
                    aggregate task and rest ratings
    rep_regular : Generate regular reports submitted by lab manager
    rep_ndar    : Generate reports for NDAR submission
    rep_metrics : Generate snapshots of the data to aid acquisition
    chk_data    : Check EmoRep and Archival data completeness,
                    pipeline progress.
    sur_stats   : Get descriptive stats and plots for surveys and task
    gen_guids   : Generate and check GUIDs
```


## General Requirements
Various workflows will require one or more of the following:
- An API token for the REDCap project EmoRep_fMRI (PID 11938) accessible via `$PAT_REDCAP_EMOREP`
- An API token to Qualtrics accessible via `$PAT_QUALTRICS_EMOREP`
- User password to MySQL database `db_emorep` accessible via `$SQL_PASS`

Example:

```bash
$echo MY-PAT-FOR-REDCAP > ~/.ssh/pat_redcap_emorep
$echo 'export PAT_REDCAP_EMOREP=$(cat ~/.ssh/pat_redcap_emorep)' >> ~/.bashrc
```


## rep_get
This workflow downloads, aggregates, and cleans participant surveys and task responses. Data are then uploaded to their respective table in MySQL database `db_emorep`.

Additionally, dataframes are written to the EmoRep data structure at `/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/data_survey` and organized by visit. Original downloads from REDCap and Qualtrics are titled `raw_*.csv` while cleaned dataframes are titled `df_*.csv`. See below for directory tree. Cleaned data from tasks conducted in the scanner are stored in the BIDS structure at `/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/data_scanner_BIDS/rawdata`

```
data_survey
├── redcap
│   ├── df_guid.csv
│   └── raw_guid.csv
├── visit_day1
│   ├── df_AIM.csv
│   ├── df_ALS.csv
│   ├── df_ERQ.csv
│   ├── df_PSWQ.csv
│   ├── df_RRS.csv
│   ├── df_STAI_Trait.csv
│   ├── df_TAS.csv
│   └── raw_EmoRep_Session_1.csv
├── visit_day2
│   ├── df_BDI.csv
│   ├── df_in_scan_ratings.csv
│   ├── df_PANAS.csv
│   ├── df_post_scan_ratings.csv
│   ├── df_rest-ratings.csv
│   ├── df_STAI_State.csv
│   ├── raw_bdi_day2.csv
│   ├── raw_FINAL - EmoRep Stimulus Ratings - fMRI Study.csv
│   └── raw_Session 2 & 3 Survey.csv
└── visit_day3
    ├── df_BDI.csv
    ├── df_in_scan_ratings.csv
    ├── df_PANAS.csv
    ├── df_post_scan_ratings.csv
    ├── df_rest-ratings.csv
    ├── df_STAI_State.csv
    └── raw_bdi_day3.csv
```


### Setup
- Generate and store API tokens for REDCap and Qualtrics in user environment (see [above](#general-requirements)).
- Store MySQL password for `db_emorep` in user environment.


### Usage
Trigger sub-package help and usage via `$ rep_get`:

```
(emorep)[nmm51-vm: ~]$rep_get
usage: rep_get [-h] [--get-demographics] [--get-redcap] [--get-qualtrics] [--get-rest] [--get-task] [--proj-dir PROJ_DIR]

Download and clean survey data.

Download RedCap and Qualtrics data, and aggregate all rest-rating
and task responses. Clean dataframes, and write raw and clean dataframes
to <proj-dir>/data_survey according to visit. Also update relevant
tables in db_emorep.

Notes
-----
* requires global variable 'SQL_PASS' in user environment, which holds
    user password to mysql db_emorep database.
* --get-redcap requires global variable 'PAT_REDCAP_EMOREP' in user
    env, which holds the personal access token to the emorep REDCap database.
* --get-qulatrics requires global variable 'PAT_QUALTRICS_EMOREP' in
    user env, which holds the personal access token to the emorep
    Qualtrics database.

Example
-------
rep_get \
    --get-redcap \
    --get-qualtrics \
    --get-rest \
    --get-task \
    --get-demographics

optional arguments:
  -h, --help           show this help message and exit
  --get-demographics   Download and clean demographic info
  --get-redcap         Download and clean RedCap surveys
  --get-qualtrics      Download and clean Qualtrics surveys
  --get-rest           Clean and aggregate resting state ratings
  --get-task           Clean and aggregate task ratings
  --proj-dir PROJ_DIR  Path to project's experiment directory
                       (default : /mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion)
```


### Considerations
This workflow is not unique and the same processes are utilized by other workflows (e.g. [rep_ndar](#rep_ndar)) -- accordingly it is not necessary to trigger this workflow before running others. This was added merely as a way of updating cleaned data on Keoki and `db_emorep` without needing to generate reports.


### rep_get methods
Schematic mapping the `rep_get` workflow to specific modules and methods of `make_reports`.
![process_get_survey](diagrams/process_get_surveys.png)


## rep_regular

## rep_ndar

## rep_metrics

## chk_data

## sur_stats

## gen_guids

## Diagrams