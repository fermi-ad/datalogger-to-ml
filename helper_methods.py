#!/usr/bin/env python3
import requests


def write_output(file, output):
    with open(file, 'w+') as f:
        for line in output:
            f.write(line + '\n')


def get_latest_device_list(output_filename=None):
    url = 'https://github.com/fermi-controls/linac-logger-device-cleaner/releases/latest/download/linac_logger_drf_requests.txt'
    req = requests.get(url)

    if req.status_code == requests.codes.ok:
        device_list = [line.strip()  # Trim whitespace
                       for line in req.text.split('\n')
                       if line]  # Remove blank lines

        if output_filename:
            write_output(output_filename, device_list)

        return device_list
    else:
        print('Could not fetch ' + url)

    return None
