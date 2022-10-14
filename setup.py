from setuptools import setup, find_packages

setup(
    name="make_reports",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "make_reports=make_reports.entrypoint:main",
            "rep_dl=make_reports.cli.dl_surveys:main",
            "rep_cl=make_reports.cli.cl_surveys:main",
            "rep_manager=make_reports.cli.rep_manager:main",
            "rep_metrics=make_reports.cli.rep_metrics:main",
            "rep_ndar=make_reports.cli.rep_ndar:main",
        ]
    },
    include_package_data=True,
    package_data={
        "": ["reference_files/*template.csv", "reference_files/*.json"]
    },
    install_requires=[
        "numpy==1.23.1",
        "pandas==1.4.3",
        "requests==2.22.0",
        "setuptools==45.2.0",
    ],
)
