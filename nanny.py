#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
## TODO: Call dpmData.py with times
## TODO: Call dpmData.py with variable output name
# TODO: Use argparse with sane defaults
'''

from glob import glob
from os import path
import requests
import dpmData

DRF_REQUESTS_LIST = 'linac_logger_drf_requests.txt'
OUTPUTS_DIRECTORY = path.abspath('.')


def read_input(file):
    output = []

    with open(file) as f:
        output = f.read().splitlines()

    return output


def write_output(file, output):
    with open(file, 'w+') as f:
        for line in output:
            f.write(line + '\n')


def get_latest_device_list():
    url = 'https://github.com/fermi-controls/linac-logger-device-cleaner/releases/latest/download/linac_logger_drf_requests.txt'
    req = requests.get(url)

    if req.status_code == requests.codes.ok:
        device_list = [line.strip()  # Trim whitespace
                       for line in req.text.split('\n')
                       if line]  # Remove blank lines

        write_output(DRF_REQUESTS_LIST, device_list)

        return device_list
    else:
        print('Could not fetch ' + url)


def get_start_time():
    # TODO: Are there existing data files at OUTPUTS_DIRECTORY
    # TODO: If not, use duration to calculate start time
    # TODO: If so, calculate the most recent data timestamp from OUTPUTS_DIRECTORY filenames
    # output filename convention => `20210101T130000PT10M-1_0_0.h5`
    # TODO: Get all filenames from OUTPUTS_DIRECTORY
    # TODO: Sort filenames to get most recent start time
    # TODO: Calculate start time of most recent file
    # TODO: Calculate duration of most recent file
    # TODO: Calculate end time of from start time and duration
    # TODO: return end time

    # Find all h5 files in OUTPUTS_DIRECTORY
    # TODO: If a file not following convention is found
    # TODO: it could always be sorted first. The files should be filtered
    # TODO: to ensure we are looking at data.
    h5_outputs = path.join(OUTPUTS_DIRECTORY, '*.h5')

    # Glob allows the use of the * wildcard
    files = glob(h5_outputs)
    # Sort modifies the list in place
    files.sort()
    most_recent_filename = path.basename(files[-1])
    date_time = most_recent_filename.split('P')[0]
    date, time = date_time.split('T')
    print(date, time)

    return 'start_time'


def main():
    # Download latest device request list if it doesn't exist
    # This means that the file must be deleted to get a newer version
    # TODO: Detect the version to keep it up to date
    if path.exists(DRF_REQUESTS_LIST):
        read_input(DRF_REQUESTS_LIST)
    else:
        get_latest_device_list()

    # TODO: determine start time
    start_time = get_start_time()
    print(start_time)
    output_filename = ''
    duration = 600

    # dpmData.main([
    #     '-s', start_time,
    #     '-du', duration,
    #     '-f', DRF_REQUESTS_LIST,
    #     '-o', output_filename,
    #     '-d', '1'  # debugging with only one device
    # ])


if __name__ == '__main__':
    main()
