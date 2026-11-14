#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import math
import pytest
import pandas as pd
from datalogger_to_ml import dpm_data
from datalogger_to_ml.dpm_data.dpm_data import (
    OUTPUT_FORMATS,
    IMPLEMENTED_OUTPUT_FORMATS,
    _open_output,
)

class TestClass:
    def test_local_to_utc_ms(self):
        local_now = datetime.datetime.now()
        local_now_iso = local_now.isoformat()
        local_datetime = datetime.datetime.fromisoformat(local_now_iso)
        ms_timestamp = dpm_data.local_to_utc_ms(local_datetime)
        local_now_ms = math.floor(local_now.timestamp() * 1000)
        assert local_now_ms == ms_timestamp

    def test_compare_hdf_device_list(self):
        device_list = ['G:AMANDA@e,12']
        status_replies = [True]
        data = {
            'Timestamps': [1612224000000],
            'Data': [17.543346]
        }
        data_frame = pd.DataFrame(data=data)

        with pd.HDFStore('test.h5') as hdf:
            hdf.append(device_list[0], data_frame)
            assert dpm_data.compare_hdf_device_list(hdf, device_list, status_replies)
            assert dpm_data.compare_hdf_device_list(hdf, [], status_replies) is False

    def test_compare_device_list(self):
        device_list = ['G:AMANDA@e,12']
        status_replies = [True]
        written_keys = ['/G:AMANDA@e,12']

        assert dpm_data.compare_device_list(written_keys, device_list, status_replies)
        assert dpm_data.compare_device_list([], device_list, status_replies) is False
        assert dpm_data.compare_device_list(written_keys, [], status_replies) is False

    def test_generate_data_source(self):
        input_start_time = datetime.datetime.fromisoformat('2021-02-01 19:00:00')
        input_end_time = datetime.datetime.fromisoformat('2021-02-01 20:00:00')
        output_start_time = dpm_data.local_to_utc_ms(input_start_time)
        output_end_time = dpm_data.local_to_utc_ms(input_end_time)

        data_source = dpm_data.generate_data_source(
            input_start_time,
            input_end_time,
            None
        )
        assert data_source == f'LOGGER:{output_start_time}:{output_end_time}'

    def test_output_formats_extensions(self):
        assert OUTPUT_FORMATS['hdf5'] == '.h5'
        assert OUTPUT_FORMATS['csv'] == '.csv'
        assert OUTPUT_FORMATS['parquet'] == '.parquet'

    def test_implemented_output_formats_subset(self):
        for fmt in IMPLEMENTED_OUTPUT_FORMATS:
            assert fmt in OUTPUT_FORMATS
            assert IMPLEMENTED_OUTPUT_FORMATS[fmt] == OUTPUT_FORMATS[fmt]

    def test_open_output_hdf5_valid(self, tmp_path):
        output_file = tmp_path / 'test.h5'
        with _open_output(output_file, 'hdf5') as store:
            assert isinstance(store, pd.HDFStore)

    def test_open_output_csv_not_implemented(self, tmp_path):
        output_file = tmp_path / 'test.csv'
        with pytest.raises(NotImplementedError):
            with _open_output(output_file, 'csv'):
                pass

    def test_open_output_parquet_not_implemented(self, tmp_path):
        output_file = tmp_path / 'test.parquet'
        with pytest.raises(NotImplementedError):
            with _open_output(output_file, 'parquet'):
                pass

    def test_open_output_unknown_format_raises_value_error(self, tmp_path):
        output_file = tmp_path / 'test.xyz'
        with pytest.raises(ValueError, match='Unknown output format'):
            with _open_output(output_file, 'xyz'):
                pass


def test_with_source():
    from datalogger_to_ml.dpm_data.dpm_data import _with_source
    assert _with_source('M:OUTTMP', 'LOGGER:1:2') == 'M:OUTTMP<-LOGGER:1:2'
    assert _with_source('M:OUTTMP<-LOGGERDURATION:60000', 'LOGGER:1:2') == 'M:OUTTMP<-LOGGERDURATION:60000'


def _timed(drf, data, micros):
    import numpy as np
    from pacsys.types import Reading, ValueType
    return Reading(drf=drf, value_type=ValueType.TIMED_SCALAR_ARRAY,
                   value={'data': np.asarray(data, dtype=float), 'micros': np.asarray(micros, dtype=np.int64)})


def test_reading_to_frame():
    from pacsys.types import Reading
    from datalogger_to_ml.dpm_data.dpm_data import _reading_to_frame
    scalar = _reading_to_frame(_timed('A', [1.0, 2.0, 3.0], [10, 20, 30]))
    assert list(scalar.columns) == ['Timestamps', 'Data']
    assert list(scalar['Data']) == [1.0, 2.0, 3.0] and list(scalar['Timestamps']) == [10, 20, 30]
    array = _reading_to_frame(_timed('B', [[1, 2], [3, 4]], [10, 20]))
    assert list(array.columns) == ['Timestamps', 'Data[0]', 'Data[1]'] and list(array['Data[1]']) == [2, 4]
    assert _reading_to_frame(Reading(drf='C', error_code=-6, message='no such device')) is None


def test_read_logger_writes_each_device(monkeypatch):
    """One batch read; good devices are written under their request name, failures are reported."""
    import pandas as pd
    from pacsys.types import Reading
    from datalogger_to_ml.dpm_data import dpm_data

    asked = {}

    class Backend:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def get_many(self, drfs, timeout=None):
            asked['drfs'], asked['timeout'] = drfs, timeout
            return [_timed(drfs[0], [1.0, 2.0], [10, 20]), Reading(drf=drfs[1], error_code=-6, message='no such device')]

    monkeypatch.setattr(dpm_data.pacsys, 'dpm', lambda host=None: Backend())
    calls = []
    monkeypatch.setattr(dpm_data, 'compare_device_list', lambda keys, devices, replies: calls.append((list(keys), replies)))

    class Store(dict):
        def append(self, key, frame):
            self[key] = pd.concat([self[key], frame]) if key in self else frame

    store = Store()
    dpm_data._read_logger(['A:GOOD', 'B:BAD'], store, 'LOGGER:1:2')
    assert asked['drfs'] == ['A:GOOD<-LOGGER:1:2', 'B:BAD<-LOGGER:1:2']
    assert asked['timeout'] == dpm_data.DEFAULT_TIMEOUT
    assert list(store) == ['A:GOOD'] and list(store['A:GOOD']['Data']) == [1.0, 2.0]
    assert calls == [(['A:GOOD'], [True, 'no such device'])]
