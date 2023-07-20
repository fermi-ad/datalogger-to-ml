#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from glob import glob
from pathlib import Path, PurePath
from os import makedirs
from os import remove
from datetime import datetime
from datetime import timedelta
import sys
import shutil
import logging
from logging.handlers import RotatingFileHandler
import isodate
import requests
import pandas as pd
from . import dpm_data


def init_logging(logging_level):
    file_name = 'old_data_collector_logs.log'
    if logging_level == 'DEBUG':
        logging.basicConfig(
            filename=file_name,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%d-%b-%y %H:%M:%S',
            level=logging.DEBUG
        )
    elif logging_level == 'INFO':
        logging.basicConfig(
            filename=file_name,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%d-%b-%y %H:%M:%S',
            level=logging.INFO
        )

    logger = logging.getLogger(__name__)
    handler = RotatingFileHandler(
        file_name,
        maxBytes=1073741824,
        backupCount=10  # maxBytes value = 1073741824 = 1 GB
    )
    logger.addHandler(handler)

    return logger


def write_output(file, output):
    with open(file, 'w+') as file_handle:
        for line in output:
            file_handle.write(line + '\n')


def get_latest_device_list_version(logger):
    url = ('https://api.github.com/repos/fermi-controls/'
           'linac-logger-device-cleaner/releases/latest')
    response = requests.get(url, allow_redirects=False)

    if response.status_code == requests.codes.get('ok'):
        logger.debug('Latest device list acquired successfully.')
        return response.json()['name']

    logger.error('Could not fetch %s', url)

    return None


def get_latest_device_list(output_path, logger):
    url = ('https://github.com/fermi-controls/linac-logger-device-cleaner/'
           'releases/latest/download/linac_logger_drf_requests.txt')
    response = requests.get(url, allow_redirects=False)

    if response.status_code == requests.codes.get('ok'):
        device_list = [line.strip()  # Trim whitespace
                       for line in response.text.split('\n')
                       if line]  # Remove blank lines

        write_output(output_path, device_list)
        logger.debug('Wrote device list successfully to %s.', output_path)

    else:
        logger.error('Could not fetch %s', url)


def get_start_time(output_path, logger):
    duration = timedelta(hours=1)
    h5_outputs = PurePath.joinpath(output_path, '**', '*.h5')
    # Glob allows the use of the * wildcard
    file_paths = glob(h5_outputs, recursive=True)
    files = list(map(PurePath.name, file_paths))
    # Sort modifies the list in place
    files.sort()

    while len(files) > 0:
        try:
            # Get the oldest file which follows the naming convention
            most_recent_filename = PurePath(files[0]).name
            date_time_duration_str = most_recent_filename.split('-')[0]
            date_time_str, duration_str = date_time_duration_str.split('P')
            end_time = isodate.parse_datetime(date_time_str)
            parsed_duration = isodate.parse_duration(f'P{duration_str}')
            start_time = end_time - parsed_duration
            logger.debug('Calculated start time and parsed duration is %s %s',
                         start_time, parsed_duration)
            return start_time, parsed_duration

        except ValueError:
            logger.exception(
                'Exception occurred in get_start_time. Message: %s',
                ValueError
            )
            files.pop(0)

    # Determine start_time and duration without existing filename
    end_time = datetime.now()
    start_time = end_time - duration
    logger.debug('Calculated start time and parsed duration is %s %s',
                 start_time, parsed_duration)
    return start_time, duration


def name_output_file(start_time, logger, duration=None):
    start_time_str = isodate.datetime_isoformat(start_time)
    if duration is not None:
        duration_str = isodate.duration_isoformat(duration)
        output = (start_time_str + duration_str)\
            .replace('-', '')\
            .replace(':', '')
        logger.debug('Named the output file: %s', output)
        return output

    output = (start_time_str).replace('-', '').replace(':', '')
    return output


def create_structured_path(outputs_directory, start_time, logger):
    # YYYYMM
    month_directory = f'{start_time.year}{start_time.month:02d}'
    # DD
    day_directory = f'{start_time.day:02d}'
    structured_path = PurePath(outputs_directory).joinpath(
        month_directory, day_directory)

    logger.debug('Structured path is: %s', structured_path)
    return structured_path


def main(args):
    logger = init_logging('DEBUG')

    drf_request_list = 'linac_logger_drf_requests.txt'
    outputs_directory = Path.resolve('.')

    if len(args) > 1:
        outputs_directory = Path.resolve(args[1])

    # Download latest device request list if it doesn't exist
    # This means that the file must be deleted to get a newer version
    latest_version = get_latest_device_list_version(logger)

    # This always overwrites the file at DRF_REQUESTS_LIST
    get_latest_device_list(drf_request_list, logger)

    while True:
        # get_start_time always returns
        start_time, duration = get_start_time(outputs_directory, logger)
        end_time = start_time + duration

        structured_outputs_directory = create_structured_path(
            outputs_directory, start_time, logger)
        iso_datetime_duration = name_output_file(
            start_time, logger, duration)
        request_list_version = latest_version.replace('.', '_')
        output_filename = f'{iso_datetime_duration}-{request_list_version}.h5'
        temp_path_and_filename = PurePath.joinpath('.', output_filename)
        output_path_and_filename = PurePath.joinpath(
            structured_outputs_directory, output_filename)

        logger.debug('Output path and filename is: %s',
                     output_path_and_filename)
        logger.debug('Calling dpm_data.main...')
        # Being data request writing to local file
        dpm_data.main([
            '-s', str(start_time),
            '-e', str(end_time),
            '-f', drf_request_list,
            '-o', temp_path_and_filename
        ])

        empty_file = False
        with pd.HDFStore(temp_path_and_filename, 'r') as hdf:
            if len(hdf.keys()) == 0:
                empty_file = True
                logger.debug(
                    ('%s is an empty HDF5 file. '
                     'Removing %s and stopping data collection.'),
                    temp_path_and_filename,
                    temp_path_and_filename
                )

        if empty_file:
            remove(temp_path_and_filename)
            break
        # Ensure that the folders exist
        if not Path.exists(structured_outputs_directory):
            makedirs(structured_outputs_directory, exist_ok=True)

        # Move local closed file to final destination
        shutil.move(temp_path_and_filename, output_path_and_filename)


if __name__ == '__main__':
    main(sys.argv)
