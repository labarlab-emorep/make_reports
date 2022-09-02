"""Title.

Desc.
"""
import importlib.resources as pkg_resources
from nda_upload import dataset_definitions


def demographics():
    """Title.

    Desc.
    """
    test_demo = pkg_resources.open_text(
        dataset_definitions, "demo_info01_definitions.csv"
    )
    df_demo = pd.read_csv(test_demo)
