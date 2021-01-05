#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import datetime
import sys
import pandas as pd
import acsys.dpm
import os
from os import path
import helperMethods


def main():
    parser = argparse.ArgumentParser()
    # Add positional/optional parameters
    parser.add_argument('-d', '--device_limit',
                        help='Limit for number of devices. Default: 0. type=int', type=int, default=0)
    parser.add_argument('-f', '--device_file',
                        help='Filename containing the list of devices. Newline delimited. No default value. type=str', type=str)
    parser.add_argument('-o', '--output_file',
                        help='Name of the output file for the hdf5 file. Default: data.h5. type=str', default='data.h5', type=str)
    parser.add_argument(
        '-n', '--node', help='Name of the node. type=str', default=None, type=str)
    parser.add_argument(
        '-v', '--version', help='Version of the input device list. type=str', default=None, type=str)

    # group 1
    # Midnight to midnight currently.
    parser.add_argument("-s", "--start_date", type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'),
                        help='Enter the start time/date. Do not use the duration tag. type=datetime.datetime', required=False)
    parser.add_argument("-e", "--end_date", type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'),
                        help='Enter the end time/date. Do not use the duration tag. type=datetime.datetime', required=False)

    # group 2
    parser.add_argument(
        "-du", "--duration", help="Enter LOGGERDURATION in sec. type=str", required=False, type=str)

    # Run the program
    hdf_code(parser.parse_args())


def local_to_utc_ms(date):
    utc_timezone = datetime.timezone(datetime.timedelta(0))
    utc_datetime_obj = date.astimezone(utc_timezone)
    time_in_ms = int(utc_datetime_obj.timestamp() * 1000)
    return time_in_ms


def create_dpm_request(device_list, hdf, request_type=None):
    async def dpm_request(con):
        # Setup context
        async with acsys.dpm.DPMContext(con) as dpm:
            # Add acquisition requests
            for index, device in enumerate(device_list):
                await dpm.add_entry(index, device)

            # Start acquisition
            await dpm.start(request_type)

            # Track replies for each device
            data_done = [None] * len(device_list)

            # Process incoming data
            async for event_response in dpm:
                # This is a data response
                if hasattr(event_response, 'data'):
                    d = {'Timestamps': event_response.micros,
                         'Data': event_response.data}
                    df = pd.DataFrame(data=d)

                    hdf.append(device_list[event_response.tag], df)

                    if len(event_response.data) == 0:
                        data_done[event_response.tag] = True

                # Status instead of actual data.
                else:
                    # Want to make it status, but can't because of the bug
                    data_done[event_response.tag] = False

                    # TO DO: Generate an output file of devices with their statuses. Send it over to Charlie
                    print(device_list[event_response.tag],
                          event_response.status)

                # If all devices have a reply, we're done
                if data_done.count(None) == 0:
                    print(data_done)
                    break

    return dpm_request


def hdf_code(args):
    START_DATE = args.start_date
    END_DATE = args.end_date
    DURATION = args.duration
    DEVICE_LIMIT = args.device_limit
    DEVICE_FILE = args.device_file
    OUTPUT_FILE = args.output_file
    NODE = args.node

    request_string = ''  # Used later to provide input string for DPM

    if DURATION and (START_DATE or END_DATE):
        print("-d and -s|-e are mutually exclusive! Exiting ...")
        sys.exit(2)

    elif START_DATE and END_DATE:
        request_string = 'LOGGER:' + \
            str(local_to_utc_ms(START_DATE)) + \
            ':' + str(local_to_utc_ms(END_DATE))

    elif START_DATE and not END_DATE:
        END_DATE = datetime.datetime.now()  # This is not midnight time
        request_string = 'LOGGER:' + \
            str(local_to_utc_ms(START_DATE)) + \
            ':' + str(local_to_utc_ms(END_DATE))

    elif END_DATE and not START_DATE:
        print('Just entering end date is invalid. Please enter start date AND end date, or just start date.')
        sys.exit(2)

    elif DURATION:
        DURATION = int(DURATION) * 1000
        request_string = 'LOGGERDURATION:' + str(DURATION)

    if NODE:
        request_string += ':' + NODE

    # The input is line separated devices.
    DEVICE_LIST = []

    if DEVICE_FILE:
        with open(DEVICE_FILE) as f:
            DEVICE_LIST = [line.rstrip() for line in f if line]
    else:
        DEVICE_LIST = helperMethods.readURL(DEVICE_LIST)

    if DEVICE_LIMIT > 0:
        DEVICE_LIST = [line for index, line in enumerate(DEVICE_LIST)
                       if index < DEVICE_LIMIT]

    if path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    hdf = pd.HDFStore(OUTPUT_FILE)

    get_logger_data = create_dpm_request(DEVICE_LIST, hdf, request_string)

    acsys.run_client(get_logger_data)

    # READ THE HDF5 FILE
    for k in list(hdf.keys()):
        df = hdf[k]
        print(k, ':\n', df)


if __name__ == '__main__':
    main()
