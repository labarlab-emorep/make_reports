"""Print entrypoint help."""
import make_reports._version as ver


def main():
    print(
        f"""

    Version : {ver.__version__}

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

    """
    )


if __name__ == "__main__":
    main()
