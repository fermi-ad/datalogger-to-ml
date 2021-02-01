#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from glob import glob
from os import path
import requests
import dpmData
import re
import isodate
from datetime import datetime
from datetime import timedelta
import sys
import shutil

DRF_REQUESTS_LIST = 'linac_logger_drf_requests.txt'
OUTPUTS_DIRECTORY = path.abspath('.')
DURATION = timedelta(hours=1)


def write_output(file, output):
    with open(file, 'w+') as f:
        for line in output:
            f.write(line + '\n')


def get_latest_device_list_version():
    url = 'https://api.github.com/repos/fermi-controls/linac-logger-device-cleaner/releases/latest'
    response = requests.get(url)

    if response.status_code == requests.codes.ok:
        return response.json()["name"]
    else:
        print('Could not fetch ' + url)

    return None


def get_latest_device_list():
    global DRF_REQUESTS_LIST

    url = 'https://github.com/fermi-controls/linac-logger-device-cleaner/releases/latest/download/linac_logger_drf_requests.txt'
    response = requests.get(url)

    if response.status_code == requests.codes.ok:
        device_list = [line.strip()  # Trim whitespace
                       for line in response.text.split('\n')
                       if line]  # Remove blank lines

        write_output(DRF_REQUESTS_LIST, device_list)
    else:
        print('Could not fetch ' + url)


def get_start_time():
    global DURATION
    global OUTPUTS_DIRECTORY

    # TODO: Are there existing data files at OUTPUTS_DIRECTORY
    # TODO: If not, use duration to calculate start time
    # TODO: If so, calculate the most recent data timestamp from OUTPUTS_DIRECTORY filenames
    # output filename convention => `20210101T130000PT10M-1_0_0.h5`
    h5_outputs = path.join(OUTPUTS_DIRECTORY, '*.h5')
    # Glob allows the use of the * wildcard
    files = glob(h5_outputs)
    # Sort modifies the list in place
    files.sort()
    # Flag which keeps the while loop running until a valid filename is found

    while len(files) > 0:
        try:
            most_recent_filename = path.basename(files[-1])
            date_time_duration_str = most_recent_filename.split('-')[0]
            date_time_str, duration_str = date_time_duration_str.split('P')
            start_time = isodate.parse_datetime(date_time_str)
            duration = isodate.parse_duration('P' + duration_str)
            end_time = start_time + duration
            return end_time, duration
        except:
            files.pop()

    # TODO: determine start_time and duration without existing filename
    end_time = datetime.now()
    start_time = end_time - DURATION
    return start_time, DURATION


def name_output_file(start_time, duration=None):
    start_time_str = isodate.datetime_isoformat(start_time)
    if duration is not None:
        duration_str = isodate.duration_isoformat(duration)
        output = (start_time_str + duration_str).replace("-", "").replace(":", "")
        return output

    output = (start_time_str).replace("-", "").replace(":", "")
    return output


def main(args):
    global DRF_REQUESTS_LIST
    global OUTPUTS_DIRECTORY

    if len(args) > 1:
        OUTPUTS_DIRECTORY = path.abspath(args[1])
    # Download latest device request list if it doesn't exist
    # This means that the file must be deleted to get a newer version
    latest_version = get_latest_device_list_version()

    # This always overwrites the file at DRF_REQUESTS_LIST
    get_latest_device_list()

    # get_start_time always returns
    start_time, duration = get_start_time()
    end_time = start_time + duration
    iso_datetime_duration = name_output_file(
        start_time, duration)
    request_list_version = latest_version.replace('.', '_')
    output_filename = f'{iso_datetime_duration}-{request_list_version}.h5'
    temp_path_and_filename = path.join('.', output_filename)
    output_path_and_filename = path.join(OUTPUTS_DIRECTORY, output_filename)

    dpmData.main([
        '-s', str(start_time),
        '-e', str(end_time),
        '-f', DRF_REQUESTS_LIST,
        '-o', temp_path_and_filename,
        # '-d', '1'  # debugging with only one device
    ])

    shutil.move(temp_path_and_filename, output_path_and_filename)


if __name__ == '__main__':
    main(sys.argv)
