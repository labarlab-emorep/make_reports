import pytest
from make_reports.resources import sql_database


@pytest.fixture(scope="module")
def fixt_database():
    db_con = sql_database.DbConnect(db_name="db_emorep_unittest")
    yield db_con
    db_con.close_con()


@pytest.mark.rep_get
class TestDbConnect:

    @pytest.fixture(autouse=True)
    def _setup(self, fixt_database):
        self.db_con = fixt_database

    def _unpack_rows(self, rows: list) -> dict:
        return {x[0]: x[1] for x in rows}

    @pytest.mark.dependency()
    def test_con_cursor(self):
        with self.db_con._con_cursor() as cur:
            cur.execute("select * from ref_emo where emo_id=1")
            rows = cur.fetchall()
        assert (1, "amusement") == rows[0]

    @pytest.mark.dependency(depends=["TestDbConnect::test_con_cursor"])
    def test_fetch_rows(self):
        rows = self.db_con.fetch_rows("select * from ref_emo")
        assert isinstance(rows, list)
        assert isinstance(rows[0], tuple)

        emo_dict = self._unpack_rows(rows)
        assert emo_dict[3] == "anxiety"
        assert emo_dict[15] == "surprise"

    @pytest.mark.dependency(depends=["TestDbConnect::test_fetch_rows"])
    def test_exec_many(self):
        # Add records
        sql_cmd = (
            "insert ignore into ref_emo (emo_id, emo_name) values (%s, %s)"
        )
        value_list = [(16, "foo"), (17, "foobar")]
        self.db_con.exec_many(sql_cmd, value_list)

        # Pull data
        sql_cmd = "select * from ref_emo where emo_name like 'foo%'"
        rows = self.db_con.fetch_rows(sql_cmd)
        emo_dict = self._unpack_rows(rows)
        assert emo_dict[16] == "foo"
        assert emo_dict[17] == "foobar"

        # Clean up
        with self.db_con._con_cursor() as cur:
            cur.execute("delete from ref_emo where emo_name like 'foo%'")
            self.db_con.con.commit()


def test_DbConnect():
    pass


def test_DfManip():
    pass


def test_Recipes():
    pass


def test_TaskMaps():
    pass


def test_PrepPsr():
    pass


def test_DbUpdate():
    pass
