from setuptools import setup, find_packages

setup(
    name="make_reports",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "make_reports=make_reports.cli:main",
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
