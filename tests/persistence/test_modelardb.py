"""Test class for ModelarDB connection and session"""

import types
from more_utils.persistence import ModelarDBSession
from pymodelardb.cursors import ArrowCursor


class TestModelarDB:
    def test_db_connect(self, modelardb_conn):
        manager_session = modelardb_conn.create_arrow_session(conn_type="manager")
        assert isinstance(manager_session, ModelarDBSession)
        manager_session.close()

        edge_session = modelardb_conn.create_arrow_session(conn_type="edge")
        assert isinstance(edge_session, ModelarDBSession)
        edge_session.close()

        cloud_session = modelardb_conn.create_arrow_session(conn_type="cloud")
        assert isinstance(cloud_session, ModelarDBSession)
        cloud_session.close()

    def test_execute_query(self, modelardb_conn, mocker, data_points, columns):
        mocker.patch(
            "more_utils.persistence.ModelarDBSession.execute", return_value=1
        )
        mocker.patch(
            "more_utils.persistence.ModelarDBSession.result_set",
            return_value=data_points,
        )
        mocker.patch(
            "more_utils.persistence.ModelarDBSession.columns",
            return_value=columns,
        )

        session = modelardb_conn.create_session(conn_type="cloud")
        assert session.execute("SELECT * FROM WindMill LIMIT 3") == 1
        assert isinstance(session.result_set(), types.GeneratorType)
        assert len(list(session.result_set())) == 3
        session.close()
