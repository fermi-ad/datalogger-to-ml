#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests


def write_output(path, output):
    with open(path, 'w+') as file:
        for line in output:
            file.write(f'{line}\n')


def get_latest_device_list(output_filename=None):
    url = ('https://github.com/fermi-ad/linac-logger-device-cleaner/'
           'releases/latest/download/linac_logger_drf_requests.txt')
    req = requests.get(url, allow_redirects=False)

    if req.status_code == requests.codes.get('ok'):
        device_list = [line.strip()  # Trim whitespace
                       for line in req.text.split('\n')
                       if line]  # Remove blank lines

        if output_filename:
            write_output(output_filename, device_list)

        return device_list

    print(f'Could not fetch {url}')

    return None
