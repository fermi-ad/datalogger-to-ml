#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from glob import glob
from os import path
from os import makedirs
from datetime import datetime
from datetime import timedelta
import sys
import shutil
import logging
from logging.handlers import RotatingFileHandler
import isodate
import requests
import yaml
import dpm_data


logger = logging.getLogger(__name__)


def config_logging(logging_level):
    file_name = 'nanny.log'
    level = logging.WARNING

    if logging_level == 'CRITICAL':
        level = logging.CRITICAL
    elif logging_level == 'ERROR':
        level = logging.ERROR
    elif logging_level == 'WARNING':
        level = logging.WARNING
    elif logging_level == 'INFO':
        level = logging.INFO
    elif logging_level == 'DEBUG':
        level = logging.DEBUG
    elif logging_level == 'NOTSET':
        level = logging.NOTSET

    logging.basicConfig(
        filename=file_name,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=level
    )

    handler = RotatingFileHandler(
        file_name,
        maxBytes=1073741824,  # maxBytes value = 1073741824 = 1 GB
        backupCount=10
    )
    logger.addHandler(handler)


def write_output(file, output):
    with open(file, 'w+') as file_handle:
        for line in output:
            file_handle.write(line + '\n')


def get_latest_device_list_version(owner, repo):
    url = f'https://api.github.com/repos/{owner}/{repo}/releases/latest'
    response = requests.get(url)

    if response.status_code == requests.codes.get('ok'):
        return response.json()['name']

    return None


def get_latest_device_list(owner, repo, file_name):
    url = (f'https://github.com/{owner}/{repo}/'
           f'releases/latest/download/{file_name}')
    response = requests.get(url)

    if response.status_code == requests.codes.get('ok'):
        return [line.strip()  # Trim whitespace
                for line in response.text.split('\n')
                if line]  # Remove blank lines

    return None


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
            logger.debug('Calculated end time: %s', end_time)
            logger.debug('Parsed duration: %s', parsed_duration)
            return end_time, parsed_duration
        except ValueError:
            logger.exception(
                'Ignoring %s for calculating start time.',
                files[-1]
            )
            files.pop()

    # Determine start_time and duration without existing filename
    end_time = datetime.now()
    start_time = end_time - duration
    logger.debug('Calculated end time and parsed duration is %s %s',
                 str(end_time), str(parsed_duration))

    return start_time, duration


def name_output_file(start_time, duration=None):
    start_time_str = isodate.datetime_isoformat(start_time)
    if duration is not None:
        duration_str = isodate.duration_isoformat(duration)
        output = (start_time_str + duration_str)\
            .replace('-', '')\
            .replace(':', '')
        return output

    output = (start_time_str).replace('-', '').replace(':', '')
    return output


def create_structured_path(outputs_directory, start_time):
    month_directory = f'{start_time.year}{start_time.month:02d}'  # YYYYMM
    day_directory = f'{start_time.day:02d}'  # DD
    structured_path = path.join(
        outputs_directory,
        month_directory,
        day_directory
    )

    return structured_path


def load_config():
    try:
        with open('config.yaml') as file_handle:
            return yaml.full_load(file_handle)
    except FileNotFoundError:
        return {}


def parse_args(raw_args=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-r'
        '--requests_list',
        type=str,
        help='Input file with line separated DRF requests'
    )
    parser.add_argument(
        '-v'
        '--list_version',
        type=str,
        help='List version for tracking list changes'
    )
    parser.add_argument(
        '-o'
        '--output_path',
        type=path.abspath,
        help='Output directory for completed files to be moved to'
    )
    parser.add_argument(
        '--log_level',
        type=str,
        help='Set the detail of messages produced in the log'
    )

    return parser.parse_args(raw_args)


def get_log_level(args, config):
    log_level = None

    # Determine input to use for logging configuration
    if 'log_level' in args:
        log_level = args.log_level
    elif 'logging' in config.keys():
        try:
            log_level = config['logging']['level']
        except KeyError:
            logger.error('Logging config does not contain "level".')

    return log_level


def get_output_path(args, config):
    outputs_directory = None

    # Determine input to use for output configuration
    if 'output_path' in args:
        outputs_directory = args.output_path
    elif 'output' in config.keys():
        try:
            outputs_directory = path.abspath(config['output']['path'])
        except KeyError:
            logger.error('Output config does not contain "path".')

    return outputs_directory


def handle_device_list_version(config):
    device_list_version = None

    # Download latest device request list if it doesn't exist
    # This means that the file must be deleted to get a newer version
    try:
        device_list_version = get_latest_device_list_version(
            config['github']['owner'],
            config['github']['repo']
        )
    except KeyError:
        logger.exception('GitHub config does not contain "owner" or "repo".')

    # get_latest_device_list_version returns None if th fetch doesn't work
    if device_list_version is None:
        logger.error(
            'Could not fetch latest device list version. Exiting.'
        )
        sys.exit('Could not fetch latest device list version from GitHub.')
    else:
        logger.debug('Latest device list version identified successfully.')

    return device_list_version


def handle_device_list(config, requests_list):
    try:
        # This always overwrites the file at DRF_REQUESTS_LIST
        latest_device_list = get_latest_device_list(
            config['github']['owner'],
            config['github']['repo'],
            config['github']['file']
        )
    except KeyError:
        logger.error(
            'GitHub config does not contain "owner", "repo", or "file".'
        )

    if latest_device_list is None:
        logger.error('Could not fetch latest device list. Exiting.')
        sys.exit('Could not fetch latest device list from GitHub.')
    else:
        write_output(requests_list, latest_device_list)
        logger.debug(
            'Wrote device list successfully to %s.',
            requests_list
        )


def get_request_list(config):
    requests_list = 'requests.txt'
    device_list_version = 'v0'

    if 'github' in config.keys():
        device_list_version = handle_device_list_version(config)
        handle_device_list(config, requests_list)
    elif 'local' in config.keys():
        try:
            requests_list = config['local']['file']
            device_list_version = config['local']['file']
        except KeyError:
            logger.error('Local config does not contain "file".')

    return requests_list, device_list_version


def main(raw_args=None):
    # Load values from config file
    config = load_config()
    # Load values from CLI args
    args = parse_args(raw_args)

    # Set logging level
    config_logging(get_log_level(args, config) or 'DEBUG')

    outputs_directory = get_output_path(args, config) or path.abspath('.')

    requests_list, device_list_version = get_request_list(config)

    # get_start_time always returns
    start_time, duration = get_start_time(outputs_directory)
    end_time = start_time + duration

    while datetime.now() > end_time:
        structured_outputs_directory = create_structured_path(
            outputs_directory,
            start_time
        )
        logger.debug('Structured path is: %s', structured_outputs_directory)

        iso_datetime_duration = name_output_file(
            start_time,
            duration
        )
        logger.debug('Named the output file: %s', iso_datetime_duration)

        request_list_version = device_list_version.replace('.', '_')
        output_filename = f'{iso_datetime_duration}-{request_list_version}.h5'
        temp_path_and_filename = path.join('.', output_filename)
        output_path_and_filename = path.join(
            structured_outputs_directory,
            output_filename
        )
        logger.debug(
            'Output path and filename is: %s',
            output_path_and_filename
        )

        logger.debug('Calling dpm_data.main...')
        # Begin data request and writing to local file
        dpm_data.main([
            '-s', str(start_time),
            '-e', str(end_time),
            '-f', requests_list,
            '-o', temp_path_and_filename,
            '--debug'
        ])

        # Ensure that the folders exist
        if not path.exists(structured_outputs_directory):
            makedirs(structured_outputs_directory, exist_ok=True)

        # Move local closed file to final destination
        shutil.move(temp_path_and_filename, output_path_and_filename)
        start_time = end_time
        end_time = start_time + duration


if __name__ == '__main__':
    main()
