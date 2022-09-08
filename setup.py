from setuptools import setup, find_packages

setup(
    name="nda_upload",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "nda_upload=nda_upload.cli:main",
        ]
    },
    include_package_data=True,
    package_data={
        "": ["reference_files/*template.csv", "reference_files/*.json"]
    },
)
