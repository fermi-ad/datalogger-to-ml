#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import datetime
import sys
import warnings
import os
from os import path
import pandas as pd
import acsys.dpm
import pytz
from backports.datetime_fromisoformat import MonkeyPatch
import helper_methods

MonkeyPatch.patch_fromisoformat()


def main(raw_args=None):
    parser = argparse.ArgumentParser()
    # Add positional/optional parameters
    parser.add_argument('-d', '--device_limit', type=int, default=0,
                        help='Limit for number of devices. Default: 0. type=int')
    parser.add_argument('-f', '--device_file', type=str,
                        help=('Filename containing the list of devices. '
                              'Newline delimited. No default value. type=str'))
    parser.add_argument('-o', '--output_file', default='data.h5', type=str,
                        help=('Name of the output file for the hdf5 file. '
                              'Default: data.h5. type=str'))
    parser.add_argument('-n', '--node', default=None, type=str,
                        help='Name of the node. type=str')
    parser.add_argument('-v', '--version', default=None, type=str,
                        help='Version of the input device list. type=str')
    parser.add_argument('--debug', default=False, type=bool,
                        help='Enable all messages. type=bool')

    # group 1
    # Midnight to midnight currently.
    parser.add_argument("-s", "--start_date",
                        type=datetime.datetime.fromisoformat,
                        help=('Enter the start time/date. '
                              'Do not use the duration tag. '
                              'type=datetime.datetime'),
                        required=False)
    parser.add_argument("-e", "--end_date",
                        type=datetime.datetime.fromisoformat,
                        help=('Enter the end time/date. '
                              'Do not use the duration tag. '
                              'type=datetime.datetime'),
                        required=False)

    # group 2
    parser.add_argument(
        "-du", "--duration", help="Enter LOGGERDURATION in sec. type=str", required=False, type=str)

    # Run the program
    hdf_code(parser.parse_args(raw_args))


def local_to_utc_ms(date):
    utc_datetime_obj = date.astimezone(pytz.utc)
    time_in_ms = int(utc_datetime_obj.timestamp() * 1000)
    return time_in_ms


def create_dpm_request(device_list, hdf, request_type=None, debug=False):
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
                    dpm_data = {'Timestamps': event_response.micros,
                                'Data': event_response.data}
                    data_frame = pd.DataFrame(data=dpm_data)

                    hdf.append(device_list[event_response.tag], data_frame)

                    if len(event_response.data) == 0:
                        data_done[event_response.tag] = True

                # Status instead of actual data.
                else:
                    # Want to make it status, but can't because of the bug
                    data_done[event_response.tag] = False

                    # TODO: Generate an output file of devices with their
                    # statuses. Send it over to Charlie
                    if debug:
                        print(device_list[event_response.tag],
                              event_response.status)

                # If all devices have a reply, we're done
                if data_done.count(None) == 0:
                    if debug:
                        print(data_done)
                    break

    return dpm_request


def hdf_code(args):
    start_date = args.start_date
    end_date = args.end_date
    duration = args.duration
    device_limit = args.device_limit
    device_file = args.device_file
    output_file = args.output_file
    node = args.node
    debug = args.debug

    if not debug:
        warnings.simplefilter("ignore")

    request_string = ''  # Used later to provide input string for DPM

    if duration and (start_date or end_date):
        print("-d and -s|-e are mutually exclusive! Exiting ...")
        sys.exit(2)

    elif start_date and end_date:
        request_string = 'LOGGER:' + \
            str(local_to_utc_ms(start_date)) + \
            ':' + str(local_to_utc_ms(end_date))

    elif start_date and not end_date:
        end_date = datetime.datetime.now()  # This is not midnight time
        request_string = 'LOGGER:' + \
            str(local_to_utc_ms(start_date)) + \
            ':' + str(local_to_utc_ms(end_date))

    elif end_date and not start_date:
        print(('Just entering end date is invalid. '
               'Please enter start date AND end date, or just start date.'))
        sys.exit(2)

    elif duration:
        duration = int(duration) * 1000
        request_string = 'LOGGERDURATION:' + str(duration)

    if node:
        request_string += ':' + node

    # The input is line separated devices.
    device_list = []

    if device_file:
        with open(device_file) as file_handle:
            device_list = [line.rstrip() for line in file_handle if line]
    else:
        device_list = helper_methods.get_latest_device_list()

    if device_limit > 0:
        device_list = [line for index, line in enumerate(device_list)
                       if index < device_limit]

    if path.exists(output_file):
        os.remove(output_file)

    with pd.HDFStore(output_file) as hdf:

        get_logger_data = create_dpm_request(
            device_list, hdf, request_string, debug=debug)

        acsys.run_client(get_logger_data)

        if debug:
            # READ THE HDF5 FILE
            for keys in list(hdf.keys()):
                data_frame = hdf[keys]
                print(f'{keys}:\n{data_frame}')


if __name__ == '__main__':
    main()
