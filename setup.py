from setuptools import setup, find_packages

exec(open("make_reports/_version.py").read())

setup(
    name="make_reports",
    version=__version__,  # noqa: F821
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "make_reports=make_reports.entrypoint:main",
            "rep_get=make_reports.cli.get_surveys:main",
            "rep_regular=make_reports.cli.rep_regular:main",
            "rep_metrics=make_reports.cli.rep_metrics:main",
            "rep_ndar=make_reports.cli.rep_ndar:main",
            "chk_data=make_reports.cli.chk_data:main",
            "gen_guids=make_reports.cli.gen_guids:main",
            "sur_stats=make_reports.cli.sur_stats:main",
        ]
    },
    include_package_data=True,
    package_data={
        "": [
            "reference_files/*template.csv",
            "reference_files/*.json",
            "dataframes/track_*.csv",
        ]
    },
    install_requires=[
        "graphviz>=0.20.1",
        "matplotlib>=3.6.1",
        "numpy>=1.23.1",
        "pandas>=1.4.3",
        "pydicom>=2.3.1",
        "python_dateutil>=2.8.2",
        "requests>=2.22.0",
        "seaborn>=0.12.2",
        "setuptools>=65.5.1",
    ],
)
