#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from glob import glob
from pathlib import Path
from pathlib import PurePath
from os import makedirs
from datetime import datetime
import sys
import shutil
import logging
from logging.handlers import RotatingFileHandler
import signal
from typing import Any
import isodate
import requests
import yaml
from . import dpm_data


logger = logging.getLogger(__name__)


def signal_handler(signal_num, _):
    logger.warning('Signal handler called with signal %s', signal_num)
    sys.exit(130)


def config_logging(logging_level):
    file_name = 'nanny.log'
    level = logging.WARNING

    if isinstance(logging_level, int):
        level = logging_level
    elif isinstance(logging_level, str):
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
    else:
        raise TypeError

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
    with open(file, 'w+', encoding='utf8') as file_handle:
        for line in output:
            file_handle.write(line + '\n')


def get_latest_device_list_version(owner, repo):
    url = f'https://api.github.com/repos/{owner}/{repo}/releases/latest'
    response = requests.get(url, allow_redirects=False)

    if response.status_code == requests.codes.get('ok'):
        return response.json()['name']

    return None


def get_latest_device_list(owner, repo, file_name):
    url = (f'https://github.com/{owner}/{repo}/'
           f'releases/latest/download/{file_name}')
    response = requests.get(url, allow_redirects=False)

    if response.status_code == requests.codes.get('ok'):
        return [line.strip()  # Trim whitespace
                for line in response.text.split('\n')
                if line]  # Remove blank lines

    return None


def parse_iso(date_time_duration_str):
    try:
        date_time_str, duration_str = date_time_duration_str.split('P')
    except ValueError:
        logger.warning(('No duration defined in iso string. '
                        'Using the default duration.'))
        date_time_str = date_time_duration_str
        duration_str = 'T1H'

    start_time = isodate.parse_datetime(date_time_str)
    duration = isodate.parse_duration(f'P{duration_str}')

    return start_time, duration

# Return a tuple (start_time, duration)
def get_start_time_config(args, config):
    # Try to get the keyword argument from CLI, first
    start_time = args.get('start-time', args.get('start_time', None))

    # Determine input to use for output configuration
    if start_time is None and 'start' in config.keys():
        try:
            start_time = config['start']
        except KeyError:
            logger.debug('Config does not contain "start".')
            return None

    return parse_iso(start_time)


def get_duration_config(args, config):
    # Try to get the keyword argument from CLI, first
    duration = args.get('duration', None)

    # Determine input to use for output configuration
    if duration is None and 'duration' in config.keys():
        try:
            duration = config["duration"]
        except KeyError:
            logger.debug('Config does not contain "duration".')
            return None

    return isodate.parse_duration(f'P{duration}')

# Paths for `get_start_time`:
#     No time constraints with no files
#     No time constraints with files
#     Time constraints with no files
#     Time constraints with files


def get_start_time(output_path, args, config):
    start_time, _ = get_start_time_config(args, config)
    duration = get_duration_config(args, config)

    h5_outputs = output_path.joinpath('**', '*.h5')
    # Glob allows the use of the * wildcard
    file_paths = glob(str(h5_outputs), recursive=True)
    files = list(map(lambda path: PurePath(path).name, file_paths))
    # Sort modifies the list in place
    files.sort()

    while len(files) > 0:
        try:
            most_recent_filename = PurePath(files[-1]).name
            date_time_duration_str = most_recent_filename.split('-')[0]
            file_start_time, file_duration = parse_iso(date_time_duration_str)
            logger.debug('Parsed duration: %s', file_duration)
            break
        except ValueError:
            logger.debug(
                'Ignoring %s for calculating start time.',
                files[-1]
            )
            files.pop()

    try:
        if file_start_time > start_time:
            start_time = file_start_time + file_duration
    except NameError:
        if start_time is None:
            logger.error('Cannot determine start time.')
            raise
        else:
            logger.debug('`file_start_time` is undefined')
    except TypeError:
        start_time = file_start_time

    # This means the config didn't have a duration specified.
    if duration is None:
        # First try to get duration from file
        try:
            duration = file_duration
        except NameError:
            logger.debug('`file_duration` is undefined')
            # Otherwise, get the duration from the `start_time` or default.
            _, duration = parse_iso(start_time)

    end_time = start_time + duration
    logger.debug('Calculated end time: %s', end_time)
    logger.debug('Start time, end time, and duration are %s, %s, and %s',
                 start_time, end_time, duration)

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
    structured_path = Path(outputs_directory).joinpath(
        month_directory,
        day_directory
    )

    return structured_path


def load_config():
    try:
        with open('config.yaml', encoding='utf8') as file_handle:
            return yaml.full_load(file_handle)
    except FileNotFoundError:
        return {}


def verbosity_to_log_level(verbosity=0):
    verbosity_levels = [logging.WARN, logging.INFO, logging.DEBUG]
    clamped_verbosity = 0 if verbosity is None else min(
        len(verbosity_levels) - 1, verbosity)
    return verbosity_levels[clamped_verbosity]


def get_log_level(args, config):
    # Try to get the keyword argument from CLI, first
    log_level = verbosity_to_log_level(args.get('verbose', 0))

    # Determine input to use for output configuration
    if log_level is None and 'logging' in config.keys():
        try:
            log_level = config['logging']['level']
        except KeyError:
            logger.error('Logging config does not contain "level".')

    return log_level


def get_output_path(args, config):
    # Try to get the keyword argument from CLI, first
    outputs_directory = args.get('output-path', args.get('output_path', None))

    # Determine input to use for output configuration
    if outputs_directory is None and 'output' in config.keys():
        try:
            outputs_directory = Path(config['output']['path'])
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


def get_request_list(args, config):
    # Try to get the keyword argument from CLI, first
    requests_list = args.get(
        'requests-list',
        args.get('requests_list', 'requests.txt')
    )
    device_list_version = args.get(
        'list-version',
        args.get('list_version', 'v0.1.0')
    )

    if 'github' in config.keys():
        device_list_version = handle_device_list_version(config)
        handle_device_list(config, requests_list)
    elif 'local' in config.keys():
        try:
            requests_list = config['local']['file']
            device_list_version = config['local']['version']
        except KeyError:
            logger.info('Local config does not contain "file" or "version".')

    return requests_list, device_list_version


def get_data(**kwargs):
    signal.signal(signal.SIGINT, signal_handler)
    # Load values from config file
    config = load_config()

    # Set logging level
    config_logging(get_log_level(kwargs, config) or 'DEBUG')

    outputs_directory = get_output_path(kwargs, config) or Path('.')

    requests_list, device_list_version = get_request_list(kwargs, config)

    # get_start_time always returns
    start_time, duration = get_start_time(outputs_directory, kwargs, config)
    end_time = start_time + duration

    continue_loop = True

    while datetime.now() > end_time and continue_loop:
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
        temp_path_and_filename = Path('.').joinpath(output_filename)
        output_path_and_filename = Path(
            structured_outputs_directory
        ).joinpath(output_filename)
        logger.debug(
            'Output path and filename is: %s',
            output_path_and_filename
        )

        logger.info('Calling dpm_data.main...')
        logger.debug(
            ('start_date=%s, end_date=%s, device_file=%s, '
             'output_file=%s, debug=True'),
            start_time,
            end_time,
            requests_list,
            temp_path_and_filename
        )
        # Begin data request and writing to local file
        dpm_data.get_data(
            start_date=start_time,
            end_date=end_time,
            device_file=requests_list,
            output_file=temp_path_and_filename,
            debug=True
        )

        # Ensure that the folders exist
        if not structured_outputs_directory.exists():
            makedirs(structured_outputs_directory, exist_ok=True)

        # Move local closed file to final destination
        shutil.move(temp_path_and_filename, output_path_and_filename)
        start_time = end_time
        end_time = start_time + duration

        # Check if should continue
        continue_loop = not kwargs.get('run-once', kwargs.get('run_once'))
