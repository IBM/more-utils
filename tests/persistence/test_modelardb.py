"""Test class for ModelarDB connection and session"""

import types
from more_utils.persistence import ModelarDB, ModelarDBSession
from pymodelardb.cursors import ArrowCursor


class TestModelarDB:
    def test_db_connect(self):
        conn_obj = ModelarDB.connect(hostname="localhost", interface="arrow")
        session = conn_obj.create_session()
        assert isinstance(session, ModelarDBSession)
        session.close()
        conn_obj.close()

    def test_db_session(self):
        conn_obj = ModelarDB.connect(hostname="localhost", interface="arrow")
        session = conn_obj.create_session()
        assert isinstance(session._cursor, ArrowCursor)
        session.close()
        conn_obj.close()

    def test_execute_query(self, mocker, data_points):
        mocker.patch(
            "more_utils.persistence.ModelarDBSession.execute", return_value=1
        )
        mocker.patch(
            "more_utils.persistence.ModelarDBSession.result_set",
            return_value=data_points,
        )
        conn_obj = ModelarDB.connect(hostname="localhost", interface="arrow")
        session = conn_obj.create_session()
        assert session.execute("SELECT * FROM DataPoint WHERE TID = 1") == 1
        assert isinstance(session.result_set(), types.GeneratorType)
        assert len(list(session.result_set())) == 3
        session.close()
        conn_obj.close()
