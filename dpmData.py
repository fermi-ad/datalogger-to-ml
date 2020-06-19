#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 13:20:08 2020

@author: snag
"""
import argparse
import datetime
import sys
import pandas as pd
import DPM

def main():
    parser = argparse.ArgumentParser()
    #Add positional/optional parameters
    parser.add_argument('-d', '--device_limit', help='Device Limit', type=int, default=0)
    parser.add_argument('-f', '--device_file', help='device file name')
    parser.add_argument('-o', '--output_file', help='output file name', default='data.h5')
    parser.add_argument('-n', '--node', help='node name', default=None, type=str)
        #Error when not using the default value
    #group 1
    parser.add_argument("-s", "--start_date", type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), help='Enter the start time/date. Do not use the duration tag', required=False)
    parser.add_argument("-e", "--end_date", type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), help='Enter the start time/date. Do not use the duration tag', required=False)
    #group 2
    parser.add_argument("-du", "--duration", help="Enter LOGGERDURATION in sec", required=False)
    #Parse the args
    args = parser.parse_args()
    sys.stdout.write(str(hdf_code(args)))

def hdf_code(args):
    START_DATE = args.start_date
    END_DATE = args.end_date
    DURATION = args.duration
    DEVICE_LIMIT = args.device_limit
    DEVICE_FILE = args.device_file
    OUTPUT_FILE = args.output_file
    #NODE = args.node #unused

    if DURATION and (START_DATE or END_DATE):
        print("-d and -s|-e are mutually exclusive! Exiting ...")
        sys.exit(2)
    elif(START_DATE and END_DATE):
        DURATION = END_DATE - START_DATE
        #print(int((END_DATE - START_DATE).total_seconds()))
        DURATION = str(DURATION)
        DURATION = 'LOGGERDURATION:' + DURATION
    elif(DURATION):
        #Transform duration from int to str, and then add "LOGGERDURATION:" before it.
        DURATION = str(DURATION)
        DURATION = 'LOGGERDURATION:' + DURATION

    DEVICE_LIST = []
    with open(DEVICE_FILE) as f:
        DEVICE_LIST = [line.rstrip() for index, line in enumerate(f)
            if index < DEVICE_LIMIT or DEVICE_LIMIT < 1]
    dpm = DPM.Blocking()
    for index, device in enumerate(DEVICE_LIST):
        dpm.add_entry(index, device)
    data_done = [None] * len(DEVICE_LIST)
    hdf = pd.HDFStore(OUTPUT_FILE)
    for event_response in dpm.process(DURATION):
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

if __name__ == '__main__':
    main()
