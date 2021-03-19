#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import math
import pandas as pd
import dpm_data

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
        data = {
            'Timestamps': [1612224000000],
            'Data': [17.543346]
        }
        data_frame = pd.DataFrame(data=data)

        with pd.HDFStore('test.h5') as hdf:
            hdf.append(device_list[0], data_frame)
            assert dpm_data.compare_hdf_device_list(hdf, device_list)
            assert dpm_data.compare_hdf_device_list(hdf, []) is False

    def test_generate_data_source(self):
        tests = [
            {
                'args': [
                    datetime.datetime.fromisoformat('2021-02-01 19:00:00'),
                    datetime.datetime.fromisoformat('2021-02-01 20:00:00'),
                    None
                ],
                'expected': 'LOGGER:1612227600000:1612231200000'
            }
        ]

        for test in tests:
            data_source = dpm_data.generate_data_source(
                test['args'][0],
                test['args'][1],
                test['args'][2]
            )
            assert data_source == test['expected']
