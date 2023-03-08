from setuptools import setup, find_packages

setup(
    name="make_reports",
    version="0.3.0",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "make_reports=make_reports.entrypoint:main",
            "rep_dl=make_reports.cli.dl_surveys:main",
            "rep_cl=make_reports.cli.cl_surveys:main",
            "rep_manager=make_reports.cli.rep_manager:main",
            "rep_metrics=make_reports.cli.rep_metrics:main",
            "rep_ndar=make_reports.cli.rep_ndar:main",
            "gen_guids=make_reports.cli.gen_guids:main",
            "sur_stats=make_reports.cli.sur_stats:main",
        ]
    },
    include_package_data=True,
    package_data={
        "": ["reference_files/*template.csv", "reference_files/*.json"]
    },
    install_requires=[
        "numpy>=1.23.1",
        "pandas>=1.4.3",
        "pydicom>=2.3.1",
        "requests>=2.22.0",
        "seaborn>=0.12.2",
        "setuptools>=45.2.0",
    ],
)
