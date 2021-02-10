#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from glob import glob
from os import path
from os import makedirs
from datetime import datetime
from datetime import timedelta
import sys
import shutil
import isodate
import requests
import dpm_data

def write_output(file, output):
    with open(file, 'w+') as file_handle:
        for line in output:
            file_handle.write(line + '\n')


def get_latest_device_list_version():
    url = ('https://api.github.com/repos/fermi-controls/'
           'linac-logger-device-cleaner/releases/latest')
    response = requests.get(url)

    if response.status_code == requests.codes.ok:
        return response.json()["name"]

    print('Could not fetch ' + url)

    return None


def get_latest_device_list(output_path):
    url = ('https://github.com/fermi-controls/linac-logger-device-cleaner/'
           'releases/latest/download/linac_logger_drf_requests.txt')
    response = requests.get(url)

    if response.status_code == requests.codes.ok:
        device_list = [line.strip()  # Trim whitespace
                       for line in response.text.split('\n')
                       if line]  # Remove blank lines

        write_output(output_path, device_list)
    else:
        print('Could not fetch ' + url)


def get_start_time(output_path):
    duration = timedelta(hours=1)
    h5_outputs = path.join(output_path, '**', '*.h5')
    # Glob allows the use of the * wildcard
    file_paths = glob(h5_outputs, recursive=True)
    files = list(map(path.basename, file_paths))
    # Sort modifies the list in place
    files.sort()

    while len(files) > 0:
        try:
            most_recent_filename = path.basename(files[-1])
            date_time_duration_str = most_recent_filename.split('-')[0]
            date_time_str, duration_str = date_time_duration_str.split('P')
            start_time = isodate.parse_datetime(date_time_str)
            parsed_duration = isodate.parse_duration('P' + duration_str)
            end_time = start_time + parsed_duration
            return end_time, parsed_duration
        except ValueError:
            files.pop()

    # Determine start_time and duration without existing filename
    end_time = datetime.now()
    start_time = end_time - duration
    return start_time, duration


def name_output_file(start_time, duration=None):
    start_time_str = isodate.datetime_isoformat(start_time)
    if duration is not None:
        duration_str = isodate.duration_isoformat(duration)
        output = (start_time_str + duration_str)\
            .replace("-", "")\
            .replace(":", "")
        return output

    output = (start_time_str).replace("-", "").replace(":", "")
    return output


def create_structured_path(outputs_directory, start_time):
    # YYYYMM
    month_directory = f'{start_time.year}{start_time.month:02d}'
    # DD
    day_directory = f'{start_time.day:02d}'

    return path.join(outputs_directory, month_directory, day_directory)


def main(args):
    drf_request_list = 'linac_logger_drf_requests.txt'
    outputs_directory = path.abspath('.')

    if len(args) > 1:
        outputs_directory = path.abspath(args[1])

    # Download latest device request list if it doesn't exist
    # This means that the file must be deleted to get a newer version
    latest_version = get_latest_device_list_version()

    # This always overwrites the file at DRF_REQUESTS_LIST
    get_latest_device_list(drf_request_list)

    # get_start_time always returns
    current_time = datetime.now()
    start_time, duration = get_start_time(outputs_directory)
    end_time = start_time + duration

    while current_time > end_time:
        structured_outputs_directory = create_structured_path(
            outputs_directory, start_time)
        iso_datetime_duration = name_output_file(
            start_time, duration)
        request_list_version = latest_version.replace('.', '_')
        output_filename = f'{iso_datetime_duration}-{request_list_version}.h5'
        temp_path_and_filename = path.join('.', output_filename)
        output_path_and_filename = path.join(
            structured_outputs_directory, output_filename)

        # Being data request writing to local file
        dpm_data.main([
            '-s', str(start_time),
            '-e', str(end_time),
            '-f', drf_request_list,
            '-o', temp_path_and_filename,
            #'-d', '10'  # debugging with only one device
        ])

        # Ensure that the folders exist
        if not path.exists(structured_outputs_directory):
            makedirs(structured_outputs_directory, exist_ok=True)

        # Move local closed file to final destination
        shutil.move(temp_path_and_filename, output_path_and_filename)
        start_time = end_time
        end_time = start_time + duration

if __name__ == '__main__':
    main(sys.argv)
