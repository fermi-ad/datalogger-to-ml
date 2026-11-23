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
from datetime import datetime, timedelta, timezone
import sys
import logging
from logging.handlers import RotatingFileHandler
import signal
import time
from typing import Any
import isodate
import numpy as np
import pandas as pd
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


def to_utc(moment):
    """An aware UTC datetime. A naive one is read as host local time, as file names before 1.9.0 were
    written; an hour that came twice (the autumn time change) is read as its first occurrence."""
    if moment is None:
        return None
    return moment.astimezone(timezone.utc)


def get_configured_start(args, config):
    """The start from --start-time or the config file, in UTC, or None. A config string may carry its own
    duration (20200101T000000PT1H); YAML may hand over a datetime already. A start without a zone is host
    local time; one ending in Z (20140101T000000Z) is UTC."""
    start = args.get('start-time', args.get('start_time', None))
    if start is None:
        start = config.get('start')
    if start is None or isinstance(start, datetime):
        return to_utc(start), None
    if 'P' not in str(start):  # a plain start is normal here; parse_iso would warn about it
        return to_utc(isodate.parse_datetime(str(start))), None
    start, duration = parse_iso(str(start))
    return to_utc(start), duration


def get_configured_end(args, config):
    """The end from --end-time or `end` in the config, in UTC, or None: windows must end by it (exclusive),
    so a backfill stops where a keep-up run starts. Zone rules as for the start."""
    end = args.get('end-time', args.get('end_time', None))
    if end is None:
        end = config.get('end')
    if end is None or isinstance(end, datetime):
        return to_utc(end)
    return to_utc(isodate.parse_datetime(str(end)))


def get_pace(args, config):
    """Pause between windows, as a multiple of the last pull's run time (e.g. 4: a pull of 16 s is
    followed by 64 s of rest), from --pace or `pace` in the config; 0 when unset."""
    pace = args.get('pace', config.get('pace'))
    return float(pace or 0)


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
            start, duration = parse_iso(name)
            have.add((to_utc(start), duration))
        except ValueError:
            logger.debug('Ignoring %s: not a nanny file name.', path)
    return have


def last_whole_window(now, duration):
    """Start of the newest window that has ended, counting windows from midnight (UTC for an aware now)."""
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight + ((now - midnight) // duration - 1) * duration


def missing_windows(start_time, duration, now, have, cutoff=None, split=None):
    """Every whole window from start_time to now without a file, newest first: during a long
    backfill the latest hours land first, so the archive stays current while older holes fill.
    With a cutoff (or several: None is ignored), only windows that end by the earliest (see get_recency
    and get_configured_end). With a split, a window counts as present when all its parts are."""
    cutoffs = cutoff if isinstance(cutoff, (list, tuple)) else [cutoff]
    limit = min([now] + [c for c in cutoffs if c is not None])
    windows = []
    window = start_time
    while window + duration <= limit:
        if not window_covered(have, window, duration, split):
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


def get_split(args, config):
    """File length when it differs from the pull's window length (e.g. T1H with duration 7D: each week is
    requested at once and delivered as 168 hour files), from --split or `split` in the config, or None."""
    split = args.get('split', config.get('split'))
    if split is None or isinstance(split, timedelta):
        return split
    text = str(split)
    return isodate.parse_duration(text if text.startswith('P') else f'P{text}')


def sub_windows(start_time, duration, split):
    """Starts of the files a window is delivered as: [start_time] without a split."""
    if split is None:
        return [start_time]
    return [start_time + k * split for k in range(duration // split)]


def window_covered(have, start_time, duration, split=None):
    """True when the window has its own file, or, with a split, every one of its parts has one."""
    if (start_time, duration) in have:
        return True
    return split is not None and all((s, split) in have for s in sub_windows(start_time, duration, split))


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
    """START[DURATION] of a file name: 20261123T200000ZPT1H for an aware start (in UTC, marked Z), or
    20261123T140000PT1H for a naive one (host local time, the form before 1.9.0)."""
    if start_time.tzinfo is not None:
        output = start_time.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    else:
        output = isodate.datetime_isoformat(start_time).replace('-', '').replace(':', '')
    if duration is not None:
        output += isodate.duration_isoformat(duration)
    return output


def create_structured_path(outputs_directory, start_time):
    """<outputs_directory>/YYYYMM/DD of the window's start, in the start's own zone (UTC since 1.9.0)."""
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
    """-v (INFO) or -vv (DEBUG) from the CLI first, else `logging.level` in the config, else WARNING.
    Before 1.9.0 the CLI's default always won, so the config's level was never used."""
    if args.get('verbose'):
        return verbosity_to_log_level(args['verbose'])
    return (config.get('logging') or {}).get('level') or logging.WARNING


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

    # Windows run in UTC (1.9.0): every hour exists once, so no file holds the autumn time change's
    # repeated hour and none is skipped in spring. Files written before in host local time still count.
    now = datetime.now(timezone.utc)
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
    end = get_configured_end(kwargs, config)
    split = get_split(kwargs, config)
    if split is not None and (duration % split or output_format != 'parquet'):
        sys.exit(f'split {split} must divide duration {duration} evenly, with output format parquet.')
    windows = missing_windows(start_time, duration, now, have, (cutoff, end), split)
    logger.info('%s window(s) to pull since %s%s%s', len(windows), start_time,
                '' if cutoff is None else f', ending by {cutoff} (recency {recency})',
                '' if end is None else f', ending by {end} (end)')
    pace = get_pace(kwargs, config)
    claims_dir = get_claims(kwargs, config)
    if kwargs.get('run-once', kwargs.get('run_once')):
        windows = windows[:1]

    workbench = get_workbench(kwargs, config)
    makedirs(workbench, exist_ok=True)
    request_list_version = device_list_version.replace('.', '_')
    pulled = None  # run time of the last pull, in seconds
    for start_time in windows:
        if pace and pulled is not None:
            logger.info('Pausing %.0f s (pace %s x the last pull, %.1f s).', pace * pulled, pace, pulled)
            time.sleep(pace * pulled)
            pulled = None
        with claim(claims_dir, name_output_file(start_time, duration)) as claimed:
            if not claimed:
                logger.info('Skipping %s: another nanny holds its claim.', start_time)
                continue
            # The list above is from the start of the run; another nanny may have filled it since.
            if all(window_present(outputs_directory, s, split or duration, file_extension)
                   for s in sub_windows(start_time, duration, split)):
                logger.info('Skipping %s: delivered meanwhile.', start_time)
                continue
            began = time.monotonic()
            pull_window(start_time, duration, outputs_directory, request_list_version, file_extension,
                        workbench, requests_list, output_format, split, claims_dir)
            pulled = time.monotonic() - began
            logger.info('Pulled %s in %.1f s.', name_output_file(start_time, duration), pulled)


def pull_window(start_time, duration, outputs_directory, request_list_version, file_extension,
                workbench, requests_list, output_format, split=None, claims_dir=None):
    """Pull one window into the workbench and deliver it to the output path (in parts, with a split)."""
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
    if split is None:
        deliver(work_file, output_path_and_filename)
    else:
        deliver_split(work_file, start_time, duration, split, outputs_directory, request_list_version,
                      file_extension, workbench, claims_dir)


def deliver_split(work_file, start_time, duration, split, outputs_directory, request_list_version,
                  file_extension, workbench, claims_dir):
    """Cut one pulled Parquet window into files of length split, and deliver each part that has no file yet.

    Each part is named and placed as if pulled on its own (20250303T010000ZPT1H-2_0_0.parquet), so readers
    of hour files need no change. Points stamped before the window go to the first part and points at or
    after its end to the last, so none is dropped; the catalog still counts them as outside their file's
    window. Each part is claimed and checked again just before delivery, so a part another nanny holds or
    has delivered is left to it.
    """
    table = pd.read_parquet(work_file)
    starts = sub_windows(start_time, duration, split)
    inner_edges = [round(s.timestamp() * 1e6) for s in starts[1:]]
    part = np.searchsorted(np.asarray(inner_edges, dtype='int64'), table['Timestamps'].to_numpy(), side='right')
    delivered = 0
    for k, start in enumerate(starts):
        name = name_output_file(start, split)
        with claim(claims_dir, name) as claimed:
            if not claimed or window_present(outputs_directory, start, split, file_extension):
                logger.info('Leaving part %s: another nanny holds or delivered it.', name)
                continue
            piece = Path(workbench).joinpath(f'{name}-{request_list_version}{file_extension}')
            table[part == k].to_parquet(piece, index=False)
            directory = create_structured_path(outputs_directory, start)
            makedirs(directory, exist_ok=True)
            deliver(piece, directory.joinpath(piece.name))
            delivered += 1
    Path(work_file).unlink()
    logger.info('Delivered %s of %s parts of %s.', delivered, len(starts), name_output_file(start_time, duration))
