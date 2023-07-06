from more_utils.persistence import ModelarDB
from more_utils.service import TimeseriesService


class TestTimeseriesService:
    def test_execute_query(self, mocker, data_points_tid_1):
        mocker.patch(
            "more_utils.persistence.ModelarDBSession.columns",
            new_callable=mocker.PropertyMock,
            return_value=[["TID"], ["TIMESTAMP"], ["VALUE"]],
        )
        mocker.patch(
            "more_utils.persistence.ModelarDBSession.result_set",
            new_callable=mocker.PropertyMock,
            return_value=data_points_tid_1[1],
        )

        conn_obj = ModelarDB.connect(hostname="localhost", interface="arrow")
        ts_service = TimeseriesService(source_db_conn=conn_obj)
        decompressed_ts = ts_service.get_time_series_data_from_ts_ids(
            ts_ids=[1],
            from_date="2019-01-01 00:00:02.0",
            to_date="2019-01-01 00:00:06.0",
            limit=3,
            value_column_labels=["POWER"],
        )
        assert len(decompressed_ts.fetch_all()) == 3
        conn_obj.close()

    def test_get_time_series_data_from_ts_ids_fetch_all(
        self, mocker, data_points_tid_1, data_points_tid_2, data_points_tid_3
    ):
        def ts_data_side_effect(*args, **kwargs):
            if 1 == args[0]["TS_ID"]:
                return data_points_tid_1
            if 2 == args[0]["TS_ID"]:
                return data_points_tid_2
            if 3 == args[0]["TS_ID"]:
                return data_points_tid_3

        mocker.patch(
            "more_utils.service.time_series.BaseService.execute",
            side_effect=ts_data_side_effect,
        )

        conn_obj = ModelarDB.connect(hostname="localhost", interface="arrow")
        ts_service = TimeseriesService(source_db_conn=conn_obj)
        decompressed_ts = ts_service.get_time_series_data_from_ts_ids(
            ts_ids=[1, 2, 3],
            from_date="2019-01-01 00:00:02.0",
            to_date="2019-01-01 00:00:06.0",
            limit=3,
            value_column_labels=["active power", "rotor speed", "wind speed"],
        )
        assert len(decompressed_ts.fetch_all(fetch_type="pandas")) == 9
        assert decompressed_ts.columns == [
            "TID",
            "TIMESTAMP",
            "active power",
            "rotor speed",
            "wind speed",
        ]
        conn_obj.close()

    def test_get_time_series_data_from_ts_ids_fetch_next(
        self, mocker, data_points_tid_1, data_points_tid_2, data_points_tid_3
    ):
        def ts_data_side_effect(*args, **kwargs):
            if 1 == args[0]["TS_ID"]:
                return data_points_tid_1
            if 2 == args[0]["TS_ID"]:
                return data_points_tid_2
            if 3 == args[0]["TS_ID"]:
                return data_points_tid_3

        mocker.patch(
            "more_utils.service.time_series.BaseService.execute",
            side_effect=ts_data_side_effect,
        )

        conn_obj = ModelarDB.connect(hostname="localhost", interface="arrow")
        ts_service = TimeseriesService(source_db_conn=conn_obj)
        decompressed_ts = ts_service.get_time_series_data_from_ts_ids(
            ts_ids=[1, 2, 3],
            from_date="2019-01-01 00:00:02.0",
            to_date="2019-01-01 00:00:06.0",
            limit=3,
            value_column_labels=["active power", "rotor speed", "wind speed"],
        )
        for ts_data in decompressed_ts.fetch_next(
            batch_size=1, fetch_type="pandas"
        ):
            assert len(ts_data) == 3
        assert decompressed_ts.columns == [
            "TID",
            "TIMESTAMP",
            "active power",
            "rotor speed",
            "wind speed",
        ]
        conn_obj.close()

    def test_get_time_series_data_models_from_ts_ids(
        self, mocker, data_models_tid_1, data_models_tid_2, data_model_columns
    ):
        def ts_data_side_effect(*args, **kwargs):
            if 1 == args[0]["TS_ID"]:
                return data_models_tid_1
            if 2 == args[0]["TS_ID"]:
                return data_models_tid_2

        mocker.patch(
            "more_utils.service.time_series.BaseService.execute",
            side_effect=ts_data_side_effect,
        )

        conn_obj = ModelarDB.connect(hostname="localhost", interface="arrow")
        ts_service = TimeseriesService(source_db_conn=conn_obj)
        ts_data_models = ts_service.get_time_series_data_models_from_ts_ids(
            [1, 2], limit=2
        )
        assert len(ts_data_models.fetch_all(fetch_type="pandas")) == 4
        assert ts_data_models.columns == data_model_columns
        conn_obj.close()
