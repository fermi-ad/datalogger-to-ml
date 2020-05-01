#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 11:43:17 2020

@author: snag
"""
import pandas as pd
import DPM

DEVICE_LIMIT = 0
DEVICE_LIST = []

with open('DeviceList.csv') as f:
    DEVICE_LIST = [line.rstrip() for index, line in enumerate(f)
                   if index < DEVICE_LIMIT or DEVICE_LIMIT < 1]

dpm = DPM.Blocking()
for index, device in enumerate(DEVICE_LIST):
    dpm.add_entry(index, device)

data_done = [None] * len(DEVICE_LIST)

hdf = pd.HDFStore('data.h5')
for event_response in dpm.process('LOGGERDURATION:360000'):
    if hasattr(event_response, 'data'):
        d = {'Timestamps' : event_response.micros, 'Data' : event_response.data}
        df = pd.DataFrame(data=d)
        hdf.append(DEVICE_LIST[event_response.tag], df)

        if len(event_response.data) == 0:
            data_done[event_response.tag] = True

    #Status instead of actual data.
    else:
        #Want to make it status, but can't because of the bug
        data_done[event_response.tag] = False
        print(DEVICE_LIST[event_response.tag], event_response.status)

    if data_done.count(None) == 0:
        print(data_done)
        break

#READ THE HDF5 FILE
for k in list(hdf.keys()):
    df = hdf[k]
    print(k, ':\n', df)
