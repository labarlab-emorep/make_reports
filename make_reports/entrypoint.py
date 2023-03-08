"""Print entrypoint help."""


def main():
    print(
        """

    The package make_reports consists of several sub-packages
    that can be accessed from their respective entrypoints (below).

        rep_dl      : Download RedCap and Qualtrics surveys
        rep_cl      : Clean RedCap and Qualtrics surveys, aggregate rest ratings
        rep_manager : Generate regular reports submitted by lab manager
        rep_ndar    : Generate reports for NDAR submission
        rep_metrics : Generate snapshots of the data to aid acquisition
        sur_stats   : Get descriptive stats and plots for surveys and task
        gen_guids   : Generate and check GUIDs

    """
    )


if __name__ == "__main__":
    main()
