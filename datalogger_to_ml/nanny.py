#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from glob import glob
from pathlib import Path
from pathlib import PurePath
import errno
import fcntl
import os
from os import makedirs, replace
import shutil
from datetime import datetime, timedelta
import sys
import logging
from logging.handlers import RotatingFileHandler
import signal
from typing import Any
import isodate
import requests
import yaml
from . import dpm_data
from .dpm_data import OUTPUT_FORMATS


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


def get_duration_config(args, config):
    # Try to get the keyword argument from CLI, first
    duration = args.get('duration', None)

    # Determine input to use for output configuration
    if duration is None and 'duration' in config.keys():
        duration = config['duration']

    return isodate.parse_duration(f'P{duration}') if duration is not None else None


def get_configured_start(args, config):
    """The start from --start-time or the config file, or None. A config string may carry its own
    duration (20200101T000000PT1H); YAML may hand over a datetime already."""
    start = args.get('start-time', args.get('start_time', None))
    if start is None:
        start = config.get('start')
    if start is None or isinstance(start, datetime):
        return start, None
    if 'P' not in str(start):  # a plain start is normal here; parse_iso would warn about it
        return isodate.parse_datetime(str(start)), None
    return parse_iso(str(start))


def existing_windows(output_path, file_extension):
    """(start, duration) of every window that already has a file under output_path, in any
    request-list version. Unfinished files end in .partial, so they never count."""
    have = set()
    pattern = Path(output_path).joinpath('**', f'*{file_extension}')
    for path in glob(str(pattern), recursive=True):
        name = PurePath(path).name.split('-')[0]
        if 'P' not in name:
            continue
        try:
            have.add(parse_iso(name))
        except ValueError:
            logger.debug('Ignoring %s: not a nanny file name.', path)
    return have


def last_whole_window(now, duration):
    """Start of the newest window that has ended, counting windows from midnight."""
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight + ((now - midnight) // duration - 1) * duration


def missing_windows(start_time, duration, now, have, cutoff=None):
    """Every whole window from start_time to now without a file, newest first: during a long
    backfill the latest hours land first, so the archive stays current while older holes fill.
    With a cutoff, only windows that end by the cutoff (see get_recency)."""
    limit = now if cutoff is None else min(now, cutoff)
    windows = []
    window = start_time
    while window + duration <= limit:
        if (window, duration) not in have:
            windows.append(window)
        window += duration
    return windows[::-1]


def get_recency(args, config):
    """Recency cutoff for a backfill: an ISO 8601 duration (e.g. P2D or PT36H) from --recency or
    `recency` in the config, or None. A run with one pulls only windows that ended at least that long
    ago, leaving the newest to the keep-up run (README, "Backfill beside a keep-up run")."""
    recency = args.get('recency', config.get('recency'))
    if recency is None or isinstance(recency, timedelta):
        return recency
    text = str(recency)
    return isodate.parse_duration(text if text.startswith('P') else f'P{text}')


def get_claims(args, config):
    """Directory of per-window claim locks shared by every nanny writing to one output path, or None."""
    claims = args.get('claims', config.get('claims'))
    return None if claims is None else Path(claims)


@contextmanager
def claim(claims_dir, name):
    """Claim one window for this process; yields True when claimed, False when another holds it.

    The claim is an exclusive flock on <claims_dir>/<name>.lock, so it lapses by itself when its holder
    exits, even by a crash: no stale claims. The file is removed on release; a process that locked a
    file just removed finds its inode no longer at the path and gives the window up. claims_dir must be
    on a filesystem with working locks (local disk) and shared by all the nannies of one output path.
    Without claims_dir every window counts as claimed (a single nanny needs none).
    """
    if claims_dir is None:
        yield True
        return
    makedirs(claims_dir, exist_ok=True)
    path = Path(claims_dir).joinpath(f'{name}.lock')
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            held = os.stat(path).st_ino == os.fstat(fd).st_ino
        except FileNotFoundError:
            held = False
        if not held:
            yield False
            return
        try:
            yield True
        finally:
            path.unlink(missing_ok=True)
    finally:
        os.close(fd)


def window_present(outputs_directory, start_time, duration, file_extension):
    """True when the window has a delivered file, in any request-list version (as existing_windows)."""
    pattern = create_structured_path(outputs_directory, start_time).joinpath(
        f'{name_output_file(start_time, duration)}-*{file_extension}')
    return bool(glob(str(pattern)))


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


def get_workbench(args, config):
    """Local directory where each file is built before it moves to the output path (default ./workbench)."""
    workbench = args.get('workbench', None)
    if workbench is None:
        workbench = config.get('output', {}).get('workbench', 'workbench')
    return Path(workbench)


def deliver(work_file, destination):
    """Move a complete, closed file from the workbench to its destination.

    The output path may be write-once storage such as pnfs, where a file cannot be edited in place, so
    files are only ever built in the workbench. The file goes in under a .partial name, written once
    front to back, and is then renamed: a copy cut short never sits under a real name, where it would
    count as a window already pulled. Across filesystems the copy carries no metadata (no chmod or
    utime), which write-once storage may refuse.
    """
    partial = Path(f'{destination}.partial')
    if partial.exists():
        partial.unlink()  # left by an interrupted copy; write-once storage cannot overwrite it
    try:
        replace(work_file, partial)  # same filesystem: a rename
    except OSError as error:
        if error.errno != errno.EXDEV:
            raise
        shutil.copyfile(work_file, partial)
        Path(work_file).unlink()
    replace(partial, destination)


def get_output_format(args, config):
    # Try to get the keyword argument from CLI, first
    output_format = args.get('output-format', args.get('output_format', None))

    # Determine input to use for output configuration
    if output_format is None and 'output' in config.keys():
        try:
            output_format = config['output']['format']
        except KeyError:
            logger.debug('Output config does not contain "format".')

    return output_format or 'hdf5'


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

    output_format = get_output_format(kwargs, config)
    file_extension = OUTPUT_FORMATS[output_format]

    now = datetime.now()
    configured_start, start_duration = get_configured_start(kwargs, config)
    duration = get_duration_config(kwargs, config) or start_duration or timedelta(hours=1)
    have = existing_windows(outputs_directory, file_extension)
    # With no configured start, fill back to the earliest file already written, or else pull only
    # the newest whole window.
    start_time = configured_start or min(
        (start for start, length in have if length == duration),
        default=last_whole_window(now, duration)
    )
    recency = get_recency(kwargs, config)
    cutoff = None if recency is None else now - recency
    windows = missing_windows(start_time, duration, now, have, cutoff)
    logger.info('%s window(s) to pull since %s%s', len(windows), start_time,
                '' if cutoff is None else f', ending by {cutoff} (recency {recency})')
    claims_dir = get_claims(kwargs, config)
    if kwargs.get('run-once', kwargs.get('run_once')):
        windows = windows[:1]

    workbench = get_workbench(kwargs, config)
    makedirs(workbench, exist_ok=True)
    request_list_version = device_list_version.replace('.', '_')
    for start_time in windows:
        with claim(claims_dir, name_output_file(start_time, duration)) as claimed:
            if not claimed:
                logger.info('Skipping %s: another nanny holds its claim.', start_time)
                continue
            # The list above is from the start of the run; another nanny may have filled it since.
            if window_present(outputs_directory, start_time, duration, file_extension):
                logger.info('Skipping %s: delivered meanwhile.', start_time)
                continue
            pull_window(start_time, duration, outputs_directory, request_list_version, file_extension,
                        workbench, requests_list, output_format)


def pull_window(start_time, duration, outputs_directory, request_list_version, file_extension,
                workbench, requests_list, output_format):
    """Pull one window into the workbench and deliver it to the output path."""
    end_time = start_time + duration
    structured_outputs_directory = create_structured_path(
        outputs_directory,
        start_time
    )
    makedirs(structured_outputs_directory, exist_ok=True)

    iso_datetime_duration = name_output_file(start_time, duration)
    output_filename = f'{iso_datetime_duration}-{request_list_version}{file_extension}'
    output_path_and_filename = Path(structured_outputs_directory).joinpath(output_filename)
    # Built and closed in the local workbench, then delivered (see deliver()).
    work_file = workbench.joinpath(output_filename)
    logger.debug('Output path and filename is: %s', output_path_and_filename)

    logger.info('Calling dpm_data.main...')
    logger.debug(
        ('start_date=%s, end_date=%s, device_file=%s, '
         'output_file=%s, debug=True'),
        start_time,
        end_time,
        requests_list,
        work_file
    )
    dpm_data.get_data(
        start_date=start_time,
        end_date=end_time,
        device_file=requests_list,
        output_file=work_file,
        output_format=output_format,
        debug=True
    )
    deliver(work_file, output_path_and_filename)
