import pytest
from make_reports.workflows import data_metrics


class SupplyVars:
    pass


@pytest.fixture(scope="session")
def fixt_setup():
    sup_vars = SupplyVars()
    sup_vars.proj_name1 = "emorep"
    sup_vars.proj_name2 = "archival"
    sup_vars.do_chk = data_metrics.CheckProjectMri()

    yield sup_vars
