#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import logging
import sys
import warnings
import os
from pathlib import Path
import signal
import pandas as pd
import acsys.dpm
import pytz
from backports.datetime_fromisoformat import MonkeyPatch
import requests

MonkeyPatch.patch_fromisoformat()

# Set the acsys log to DEBUG
acsys_log = logging.getLogger('acsys')
acsys_log.setLevel(logging.DEBUG)

# Local logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def _signal_handler(signal_num, _):
    logger.warning('Signal handler called with signal %s', signal_num)
    sys.exit(130)


def local_to_utc_ms(date):
    utc_datetime_obj = date.astimezone(pytz.utc)
    time_in_ms = int(utc_datetime_obj.timestamp() * 1000)
    return time_in_ms


def compare_hdf_device_list(hdf, device_list, status_replies):
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

        if status_replies.count(True) < len(status_replies):
            for index, reply in enumerate(status_replies):
                if reply is not True:
                    logger.error(
                        'Status reply %s for %s',
                        reply,
                        device_list[index]
                    )
        else:
            logger.debug('No status replies received!')

        return False

    return True


def _create_data_processor(device_list, hdf):
    data_done = [None] * len(device_list)
    data_store = {}

    def _run(event_response):
        # This is a data response
        if isinstance(event_response, acsys.dpm.ItemData):
            request = device_list[event_response.tag]
            dpm_data = {
                'Timestamps': event_response.micros,
                'Data': event_response.data
            }
            data_frame = pd.DataFrame(data=dpm_data)

            # If we think data is done and more arrives, write it to the file
            if data_done[event_response.tag]:
                logger.warning(
                    'Data received after final response for %s',
                    request
                )
                hdf.append(request, data_frame)
            else:
                if request in data_store.keys():
                    data_store[request] = data_store[request].append(
                        data_frame)
                else:
                    data_store[request] = data_frame

            # DPM tells us there is no more data with an empty list
            if len(event_response.data) == 0:
                # Write data to file
                hdf.append(request, data_store[request])
                data_done[event_response.tag] = True
                logger.debug(
                    '%s of %s requests still processing.',
                    data_done.count(None),
                    len(device_list)
                )

        # Status instead of actual data.
        elif isinstance(event_response, acsys.dpm.ItemStatus):
            # Want to make it status, but can't because of the bug
            data_done[event_response.tag] = event_response.status
            logger.warning(
                'Returned status message %s for %s',
                event_response.status,
                device_list[event_response.tag]
            )

        elif isinstance(event_response, acsys.dpm.DeviceInfo_reply):
            # We don't use DeviceInfo objects so we can ignore them
            pass

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

    return _run


def _create_dpm_request(
    device_list,
    hdf,
    request_type=None,
    dpm_node=None
):
    async def _dpm_request(con):
        # Setup context
        async with acsys.dpm.DPMContext(con, dpm_node=dpm_node) as dpm:
            drf_requests = []

            for index, device in enumerate(device_list):
                drf_requests.append((index, device))

            # Add acquisition requests
            await dpm.add_entries(drf_requests)

            # Start acquisition
            logger.debug('Starting DAQ...')
            await dpm.start(request_type)

            # Track replies for each device
            process_data = _create_data_processor(device_list, hdf)
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

            compare_hdf_device_list(hdf, device_list, data_done)

    return _dpm_request


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
        result = f'LOGGERDURATION:{duration}'

    else:
        duration = 3600 * 1000
        result = f'LOGGERDURATION:{duration}'

    return result


def _write_output(path, output):
    with open(path, 'w+') as file:
        for line in output:
            file.write(f'{line}\n')


def _get_latest_device_list(output_filename=None):
    url = ('https://github.com/fermi-controls/linac-logger-device-cleaner/'
           'releases/latest/download/linac_logger_drf_requests.txt')
    req = requests.get(url)

    if req.status_code == requests.codes.get('ok'):
        device_list = [line.strip()  # Trim whitespace
                       for line in req.text.split('\n')
                       if line]  # Remove blank lines

        if output_filename:
            _write_output(output_filename, device_list)

        return device_list

    print(f'Could not fetch {url}')

    return None


def _generate_device_list(device_limit, device_file=None):
    # The input is line separated devices.
    result = []

    if device_file is not None:
        logger.debug('Opening device file: %s', device_file)
        with open(device_file) as file_handle:
            result = [line.rstrip() for line in file_handle if line]
    else:
        logger.debug('Fetching latest device list')
        result = _get_latest_device_list()

    if device_limit > 0:
        logger.info('Limiting device list to %s device(s)', device_limit)
        result = [line for index, line in enumerate(result)
                  if index < device_limit]

    return result


def get_data(**kwargs):
    signal.signal(signal.SIGINT, _signal_handler)

    start_date = kwargs.get('start-date', kwargs.get('start_date', None))
    end_date = kwargs.get('end-date', kwargs.get('end_date', None))
    duration = kwargs.get('duration', None)
    device_limit = kwargs.get('device-limit', 0)
    device_file = kwargs.get('device-file', kwargs.get('device_file', None))
    output_file = kwargs.get(
        'output-file',
        kwargs.get('output_file', Path('data.h5'))
    )
    dpm_node = kwargs.get('dpm-node', kwargs.get('dpm_node', None))
    debug = kwargs.get('debug', False)

    # Silence STDOUT warnings
    warnings.simplefilter('ignore')

    if debug:
        logger.setLevel(logging.DEBUG)

    logger.debug(
        (
            'start_date: %s, end_date: %s, duration: %s, device_file: %s, '
            'dpm_node: %s, device_limit: %s, output_file: %s, debug: %s'
        ),
        start_date,
        end_date,
        duration,
        device_file,
        dpm_node,
        device_limit,
        output_file,
        debug
    )

    if Path.exists(output_file):
        os.remove(output_file)

    device_list = _generate_device_list(device_limit, device_file)
    data_source = generate_data_source(start_date, end_date, duration)
    logger.debug('data_source: %s', data_source)

    with pd.HDFStore(output_file) as hdf:
        get_logger_data = _create_dpm_request(
            device_list,
            hdf,
            data_source,
            dpm_node
        )

        acsys.run_client(get_logger_data)
