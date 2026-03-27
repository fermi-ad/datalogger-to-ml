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
