#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import datetime
import logging
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

# Set the acsys log to DEBUG
acsys_log = logging.getLogger('acsys')
acsys_log.setLevel(logging.DEBUG)

# Local logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def local_to_utc_ms(date):
    utc_datetime_obj = date.astimezone(pytz.utc)
    time_in_ms = int(utc_datetime_obj.timestamp() * 1000)
    return time_in_ms


def compare_hdf_device_list(hdf, device_list):
    hdf_keys = hdf.keys()

    if len(hdf_keys) != len(device_list):
        logger.error((
            'Empty DAQ for certain devices. All devices from '
            'device list are not present in the HDF5.'
        ))
        logger.debug(
            '%s devices are missing from the requested list of %s',
            len(device_list) - len(hdf_keys),
            len(device_list)
        )

        for device in device_list:
            if f'/{device}' not in hdf_keys:
                logger.error('Device not in HDF5 output: %s', device)

        return False

    return True


def create_data_processor(device_list, hdf):
    data_done = [None] * len(device_list)

    def run(event_response):
        # This is a data response
        if isinstance(event_response, acsys.dpm.ItemData):
            dpm_data = {
                'Timestamps': event_response.micros,
                'Data': event_response.data
            }
            data_frame = pd.DataFrame(data=dpm_data)

            hdf.append(device_list[event_response.tag], data_frame)

            # DPM tells us there is no more data with an empty list
            if len(event_response.data) == 0:
                data_done[event_response.tag] = True

        # Status instead of actual data.
        elif isinstance(event_response, acsys.dpm.ItemStatus):
            # Want to make it status, but can't because of the bug
            data_done[event_response.tag] = False
            logger.warning(
                'Returned status message %s for %s',
                event_response.status,
                device_list[event_response.tag]
            )

        else:
            logger.debug(
                'Unknown data response type: %s',
                event_response
            )

        # If all devices have a reply, we're done
        if data_done.count(None) == 0:
            logger.info('Data received for all devices')
            return data_done

        return False

    return run


def create_dpm_request(
    device_list,
    hdf,
    request_type=None
):
    async def dpm_request(con):
        # Setup context
        async with acsys.dpm.DPMContext(con) as dpm:
            # Add acquisition requests
            for index, device in enumerate(device_list):
                await dpm.add_entry(index, device)

            # Start acquisition
            logger.debug('Starting DAQ...')
            await dpm.start(request_type)

            # Track replies for each device
            process_data = create_data_processor(device_list, hdf)
            data_done = []

            # Process incoming data
            async for event_response in dpm:
                data_done = process_data(event_response)

                if data_done:
                    logger.info('Data acquisition complete')
                    for index, data in enumerate(data_done):
                        if data is None:
                            logger.debug(
                                'No response from: %s',
                                device_list[index]
                            )
                    break

            compare_hdf_device_list(hdf, device_list)

    return dpm_request


def generate_data_source(start_date, end_date, duration):
    result = ''

    if duration and (start_date or end_date):
        print('-d and -s|-e are mutually exclusive! Exiting ...')
        # Unix uses exit status 2 to indicate CLI syntax error
        sys.exit(2)

    elif end_date and not start_date:
        print(('Just entering end date is invalid. '
               'Please enter start date AND end date, '
               'or just start date.'))
        # Unix uses exit status 2 to indicate CLI syntax error
        sys.exit(2)

    elif start_date:
        if not end_date:
            end_date = datetime.datetime.now()  # This is not midnight time

        result = (
            'LOGGER:'
            f'{local_to_utc_ms(start_date)}:'
            f'{local_to_utc_ms(end_date)}'
        )

    elif duration:
        duration = int(duration) * 1000
        result = 'LOGGERDURATION: {duration}'

    return result


def generate_device_list(device_limit, device_file=None):
    # The input is line separated devices.
    result = []

    if device_file is not None:
        logger.debug('Opening device file: %s', device_file)
        with open(device_file) as file_handle:
            result = [line.rstrip() for line in file_handle if line]
    else:
        logger.debug('Fetching latest device list')
        result = helper_methods.get_latest_device_list()

    if device_limit > 0:
        logger.info('Limiting device list to %s device(s)', device_limit)
        result = [line for index, line in enumerate(result)
                  if index < device_limit]

    return result


def hdf_code(args):
    start_date = args.start_date
    end_date = args.end_date
    duration = args.duration
    device_limit = args.device_limit
    device_file = args.device_file
    output_file = args.output_file
    debug = args.debug

    # Silence STDOUT warnings
    warnings.simplefilter('ignore')

    if debug:
        logger.setLevel(logging.DEBUG)

    logger.debug(
        (
            'start_date: %s, end_date: %s, duration: %s, '
            'device_file: %s, device_limit: %s, output_file: %s, debug: %s'
        ),
        start_date,
        end_date,
        duration,
        device_file,
        device_limit,
        output_file,
        debug
    )

    if path.exists(output_file):
        os.remove(output_file)

    device_list = generate_device_list(device_limit, device_file)
    logger.debug('device_list: %s', device_list)
    data_source = generate_data_source(start_date, end_date, duration)
    logger.debug('data_source: %s', data_source)

    with pd.HDFStore(output_file) as hdf:
        get_logger_data = create_dpm_request(
            device_list,
            hdf,
            data_source
        )

        acsys.run_client(get_logger_data)

        if debug:
            # READ THE HDF5 FILE
            for keys in list(hdf.keys()):
                data_frame = hdf[keys]
                print(f'{keys}:\n{data_frame}')


def main(raw_args=None):
    parser = argparse.ArgumentParser()
    # Add positional/optional parameters
    parser.add_argument(
        '-d',
        '--device_limit',
        type=int,
        default=0,
        help='Limit for number of devices.'
    )
    parser.add_argument(
        '-f',
        '--device_file',
        type=str,
        help=('Filename containing the list of devices. '
              'Newline delimited.')
    )
    parser.add_argument(
        '-o',
        '--output_file',
        default='data.h5',
        type=str,
        help='Name of the output file for the hdf5 file.'
    )
    parser.add_argument(
        '-v',
        '--version',
        default=None,
        type=str,
        help='Version of the input device list.'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable all messages.'
    )

    # group 1
    # Midnight to midnight currently.
    parser.add_argument(
        '-s',
        '--start_date',
        type=datetime.datetime.fromisoformat,
        help=('Enter the start time/date. '
              'Do not use the duration tag.'),
        required=False
    )
    parser.add_argument(
        '-e',
        '--end_date',
        type=datetime.datetime.fromisoformat,
        help=('Enter the end time/date. '
              'Do not use the duration tag.'),
        required=False
    )
    parser.add_argument(
        '-du',
        '--duration',
        help='Enter LOGGERDURATION in sec.',
        required=False,
        type=str
    )

    # Run the program with argparse arguments
    hdf_code(parser.parse_args(raw_args))


if __name__ == '__main__':
    main()
