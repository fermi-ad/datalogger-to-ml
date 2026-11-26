#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import fcntl
import logging
import os
from datetime import datetime, timedelta, timezone

import pandas as pd

from datalogger_to_ml import nanny, h5_validator
from datalogger_to_ml.dpm_data.dpm_data import _open_output

HOUR = timedelta(hours=1)
UTC = timezone.utc


def utcnow():
    return datetime.now(UTC)


def test_missing_windows_newest_first_holes_included():
    start = datetime(2026, 11, 13, 0, 0)
    have = {(start + HOUR, HOUR)}
    now = datetime(2026, 11, 13, 3, 30)
    assert nanny.missing_windows(start, HOUR, now, have) == [start + 2 * HOUR, start]


def test_last_whole_window():
    assert nanny.last_whole_window(datetime(2026, 11, 13, 3, 30), HOUR) == datetime(2026, 11, 13, 2, 0)
    assert nanny.last_whole_window(datetime(2026, 11, 13, 0, 20), HOUR) == datetime(2026, 11, 12, 23, 0)


def test_existing_windows_any_version_never_partial(tmp_path):
    day = tmp_path / '202611' / '13'
    day.mkdir(parents=True)
    (day / '20261113T000000PT1H-1_0_0.parquet').touch()
    (day / '20261113T010000PT1H-0_1_0.parquet').touch()
    (day / '20261113T020000PT1H-1_0_0.parquet.partial').touch()
    (day / 'notes.parquet').touch()
    start = nanny.to_utc(datetime(2026, 11, 13))  # names without a zone are host local time
    assert nanny.existing_windows(tmp_path, '.parquet') == {(start, HOUR), (start + HOUR, HOUR)}


def _run_nanny(tmp_path, monkeypatch, **extra):
    """Run nanny.get_data with the logger read replaced by one that just creates the file."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'requests.txt').write_text('M:OUTTMP\n')
    pulled = []

    def fake_get_data(**kwargs):
        # Files are built in the local workbench, never in the output path (which may be write-once pnfs).
        assert kwargs['output_file'].parent.resolve() == (tmp_path / 'workbench').resolve()
        pulled.append(kwargs['start_date'])
        kwargs['output_file'].write_bytes(b'x')

    monkeypatch.setattr(nanny.dpm_data, 'get_data', fake_get_data)
    nanny.get_data(output_path=tmp_path / 'out', requests_list='requests.txt', output_format='parquet', **extra)
    return pulled


def test_nanny_fills_holes_newest_first(tmp_path, monkeypatch):
    newest = nanny.last_whole_window(utcnow(), HOUR)
    start = newest - 2 * HOUR
    middle = nanny.create_structured_path(tmp_path / 'out', start + HOUR)
    middle.mkdir(parents=True)
    (middle / f'{nanny.name_output_file(start + HOUR, HOUR)}-1_0_0.parquet').touch()

    pulled = _run_nanny(tmp_path, monkeypatch, start_time=start)

    assert pulled == [newest, start]
    names = sorted(p.name for p in (tmp_path / 'out').rglob('*') if p.is_file())
    assert len(names) == 3 and not any(n.endswith('.partial') for n in names)
    assert list((tmp_path / 'workbench').iterdir()) == []


def test_nanny_run_once_takes_only_the_newest(tmp_path, monkeypatch):
    newest = nanny.last_whole_window(utcnow(), HOUR)
    pulled = _run_nanny(tmp_path, monkeypatch, start_time=newest - 3 * HOUR, run_once=True)
    assert pulled == [newest]


def test_parquet_output_is_long_and_splits_arrays(tmp_path):
    path = tmp_path / 'window.parquet'
    with _open_output(path, 'parquet') as store:
        store.append('A:SCALAR', pd.DataFrame({'Timestamps': [10, 20], 'Data': [1.0, 2.0]}))
        store.append('A:SCALAR', pd.DataFrame({'Timestamps': [30], 'Data': [3.0]}))
        store.append('B:ARRAY', pd.DataFrame({'Timestamps': [10], 'Data[0]': [5.0], 'Data[1]': [6.0]}))
    table = pd.read_parquet(path)
    assert list(table.columns) == ['device', 'Timestamps', 'Data']
    assert list(table[table.device == 'A:SCALAR'].Data) == [1.0, 2.0, 3.0]
    assert sorted(set(table.device)) == ['A:SCALAR', 'B:ARRAY[0]', 'B:ARRAY[1]']


def test_validate_reads_both_formats_in_subfolders(tmp_path, capsys):
    day = tmp_path / '202611' / '13'
    day.mkdir(parents=True)
    pd.DataFrame({'device': ['A'], 'Timestamps': [1], 'Data': [1.0]}).to_parquet(day / 'w.parquet')
    with pd.HDFStore(tmp_path / 'w.h5') as store:
        store.append('A', pd.DataFrame({'Timestamps': [1], 'Data': [1.0]}))
    h5_validator.validate(**{'validate-path': tmp_path})
    out = capsys.readouterr().out
    assert out.count('was successfully read') == 2


def test_config_output_format_is_not_overridden_by_the_cli_default(tmp_path, monkeypatch):
    """The nanny CLI used to pass --output-format hdf5 by default, which hid output.format in config.yaml."""
    import sys
    from datalogger_to_ml import __main__ as cli
    seen = {}
    monkeypatch.setattr(nanny, 'get_data', lambda **kw: seen.update(kw))
    monkeypatch.setattr(sys, 'argv', ['datalogger-to-ml', 'nanny'])
    cli.main()
    assert 'output_format' not in seen and 'output-format' not in seen
    assert nanny.get_output_format(seen, {'output': {'format': 'parquet'}}) == 'parquet'


def test_deliver_across_filesystems_replaces_a_stale_partial(tmp_path, monkeypatch):
    """Across filesystems (EXDEV) the file is copied in as .partial, then renamed; a .partial left by an
    interrupted copy is removed first, since write-once storage cannot overwrite it."""
    import errno
    work = tmp_path / 'workbench' / 'w.parquet'
    work.parent.mkdir()
    work.write_bytes(b'complete')
    dest = tmp_path / 'out' / 'w.parquet'
    dest.parent.mkdir()
    (dest.parent / 'w.parquet.partial').write_bytes(b'half')

    real_replace = nanny.replace

    def cross_device_first(src, dst):
        if str(src).startswith(str(work.parent)):
            raise OSError(errno.EXDEV, 'Invalid cross-device link')
        return real_replace(src, dst)
    monkeypatch.setattr(nanny, 'replace', cross_device_first)

    nanny.deliver(work, dest)
    assert dest.read_bytes() == b'complete'
    assert not work.exists() and not (dest.parent / 'w.parquet.partial').exists()


def test_missing_windows_stops_at_the_cutoff():
    start = datetime(2026, 11, 13, 0, 0)
    now = datetime(2026, 11, 13, 5, 30)
    assert nanny.missing_windows(start, HOUR, now, set(), cutoff=now - 3 * HOUR) == [start + HOUR, start]


def test_get_recency_from_cli_or_config():
    assert nanny.get_recency({}, {}) is None
    assert nanny.get_recency({}, {'recency': 'P2D'}) == timedelta(days=2)
    assert nanny.get_recency({'recency': 'PT36H'}, {'recency': 'P2D'}) == timedelta(hours=36)
    assert nanny.get_recency({}, {'recency': 'T6H'}) == timedelta(hours=6)


def test_backfill_leaves_the_recent_windows(tmp_path, monkeypatch):
    newest = nanny.last_whole_window(utcnow(), HOUR)
    pulled = _run_nanny(tmp_path, monkeypatch, start_time=newest - 5 * HOUR, recency='PT3H')
    # Windows ending within the last 3 h are the keep-up run's; the rest come newest first.
    assert pulled and all(w + HOUR <= utcnow() - 3 * HOUR for w in pulled)
    assert pulled == sorted(pulled, reverse=True) and pulled[-1] == newest - 5 * HOUR
    assert newest not in pulled and newest - HOUR not in pulled


def test_a_claimed_window_is_skipped_and_claims_are_cleaned_up(tmp_path, monkeypatch):
    newest = nanny.last_whole_window(utcnow(), HOUR)
    claims = tmp_path / 'claims'
    claims.mkdir()
    held = claims / f'{nanny.name_output_file(newest, HOUR)}.lock'
    fd = os.open(held, os.O_CREAT | os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)  # another nanny is pulling the newest window
    try:
        pulled = _run_nanny(tmp_path, monkeypatch, start_time=newest - HOUR, claims=claims)
    finally:
        os.close(fd)
    assert pulled == [newest - HOUR]
    assert [p.name for p in claims.iterdir()] == [held.name]  # only the other's claim remains


def test_a_window_delivered_meanwhile_is_not_pulled_again(tmp_path, monkeypatch):
    newest = nanny.last_whole_window(utcnow(), HOUR)
    day = nanny.create_structured_path(tmp_path / 'out', newest)
    day.mkdir(parents=True)
    (day / f'{nanny.name_output_file(newest, HOUR)}-1_0_0.parquet').touch()
    monkeypatch.setattr(nanny, 'existing_windows', lambda *a: set())  # the list predates that delivery
    pulled = _run_nanny(tmp_path, monkeypatch, start_time=newest - HOUR)
    assert pulled == [newest - HOUR]


def test_claim_is_exclusive_and_removed_on_release(tmp_path):
    with nanny.claim(tmp_path, 'w') as first:
        assert first
        with nanny.claim(tmp_path, 'w') as second:
            assert not second  # held by the first
    assert not (tmp_path / 'w.lock').exists()
    with nanny.claim(None, 'w') as anyone:
        assert anyone


def test_names_carry_utc_and_old_local_names_still_count(tmp_path, monkeypatch):
    monkeypatch.setenv('TZ', 'America/Chicago')
    __import__('time').tzset()
    try:
        start = datetime(2026, 11, 1, 6, tzinfo=UTC)
        assert nanny.name_output_file(start, HOUR) == '20261101T060000ZPT1H'
        assert nanny.name_output_file(start, timedelta(days=7)) == '20261101T060000ZP7D'
        assert nanny.parse_iso('20261101T060000ZP7D') == (start, timedelta(days=7))
        day = tmp_path / '202611' / '01'
        day.mkdir(parents=True)
        # Before 1.9.0 the autumn time change's two 01:00 hours had one local name: it stands for the first.
        (day / '20261101T010000PT1H-2_0_0.parquet').touch()
        (day / '20261101T080000ZPT1H-1_0_0.parquet').touch()
        have = nanny.existing_windows(tmp_path, '.parquet')
        assert have == {(start, HOUR), (start + 2 * HOUR, HOUR)}
        # The second 01:00 (CST, 07:00 UTC) is an hour of its own, missing until pulled.
        assert nanny.missing_windows(start, HOUR, start + 3 * HOUR, have) == [start + HOUR]
        assert nanny.get_configured_start({}, {'start': '20261105T000000'})[0] == datetime(2026, 11, 5, 6, tzinfo=UTC)
        assert nanny.get_configured_start({}, {'start': '20261105T000000Z'})[0] == datetime(2026, 11, 5, tzinfo=UTC)
    finally:
        monkeypatch.undo()
        __import__('time').tzset()


def test_utc_windows_cross_the_time_changes_without_doubles_or_gaps():
    # Chicago's 2026-11-01 has 25 hours and 2026-03-08 has 23; in UTC every hour is one window.
    for day, hours in (('2026-11-01', 25), ('2026-03-08', 23)):
        local = datetime.fromisoformat(day).replace(tzinfo=__import__('zoneinfo').ZoneInfo('America/Chicago'))
        first, last = local.astimezone(UTC), (local + timedelta(days=1)).astimezone(UTC)
        windows = nanny.missing_windows(first, HOUR, last, set())
        assert len(windows) == hours and len(set(windows)) == hours


def test_end_stops_the_windows():
    start = datetime(2026, 11, 13, tzinfo=UTC)
    now = start + 10 * HOUR
    end = nanny.get_configured_end({}, {'end': '20261113T040000Z'})
    assert end == start + 4 * HOUR
    assert nanny.missing_windows(start, HOUR, now, set(), (None, end)) == [start + 3 * HOUR, start + 2 * HOUR,
                                                                           start + HOUR, start]
    assert nanny.get_configured_end({'end_time': '20261113T020000Z'}, {'end': '20261113T040000Z'}) == start + 2 * HOUR
    assert nanny.get_configured_end({}, {}) is None


def test_pace_rests_a_multiple_of_each_pull(tmp_path, monkeypatch):
    clock = [0.0]
    rests = []
    monkeypatch.setattr(nanny.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(nanny.time, 'sleep', rests.append)
    real_pull = nanny.pull_window

    def slow_pull(*args):
        clock[0] += 16.0
        real_pull(*args)

    monkeypatch.setattr(nanny, 'pull_window', slow_pull)
    newest = nanny.last_whole_window(utcnow(), HOUR)
    pulled = _run_nanny(tmp_path, monkeypatch, start_time=newest - 2 * HOUR, pace=4)
    assert len(pulled) == 3 and rests == [64.0, 64.0]  # no rest before the first window or after the last
    assert nanny.get_pace({}, {}) == 0 and nanny.get_pace({}, {'pace': 4}) == 4.0


def test_log_level_from_cli_else_config():
    assert nanny.get_log_level({}, {'logging': {'level': 'INFO'}}) == 'INFO'
    assert nanny.get_log_level({'verbose': 2}, {'logging': {'level': 'INFO'}}) == logging.DEBUG
    assert nanny.get_log_level({'verbose': 0}, {}) == logging.WARNING


def test_split_delivers_parts_named_as_hours_and_keeps_strays(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'requests.txt').write_text('M:OUTTMP\n')
    start = datetime(2025, 3, 3, tzinfo=UTC)
    t0 = round(start.timestamp() * 1e6)
    us_h = 3600 * 10**6
    rows = [('A', t0 - 10**6, 0.0), ('A', t0, 1.0), ('B', t0 + us_h - 1, 2.0), ('A', t0 + us_h, 3.0),
            ('A', t0 + 3 * us_h + 10**6, 4.0)]  # one stray before the window and one after it

    def fake_get_data(**kwargs):
        pd.DataFrame(rows, columns=['device', 'Timestamps', 'Data']).to_parquet(kwargs['output_file'], index=False)

    monkeypatch.setattr(nanny.dpm_data, 'get_data', fake_get_data)
    out = tmp_path / 'out'
    third = nanny.create_structured_path(out, start + 2 * HOUR)
    third.mkdir(parents=True)
    (third / f'{nanny.name_output_file(start + 2 * HOUR, HOUR)}-1_0_0.parquet').touch()  # the keep-up's
    nanny.get_data(output_path=out, requests_list='requests.txt', output_format='parquet',
                   start_time=start, end_time='20250303T030000Z', duration='T3H', split='T1H')
    files = sorted(p.name for p in out.rglob('*.parquet'))
    assert files == ['20250303T000000ZPT1H-v0_1_0.parquet', '20250303T010000ZPT1H-v0_1_0.parquet',
                     '20250303T020000ZPT1H-1_0_0.parquet']
    first = pd.read_parquet(next(out.rglob('20250303T000000ZPT1H-v0_1_0.parquet')))
    assert first['Data'].tolist() == [0.0, 1.0, 2.0]
    assert pd.read_parquet(next(out.rglob('20250303T010000ZPT1H-v0_1_0.parquet')))['Data'].tolist() == [3.0]
    assert list((tmp_path / 'workbench').iterdir()) == []
    # All three parts present now: the window counts as present and is not pulled again.
    have = nanny.existing_windows(out, '.parquet')
    assert nanny.missing_windows(start, 3 * HOUR, start + 10 * HOUR, have, start + 3 * HOUR, split=HOUR) == []
    assert nanny.missing_windows(start, 3 * HOUR, start + 10 * HOUR, have, start + 3 * HOUR) == [start]


def _replies(monkeypatch, replies):
    """Replace the logger read with one that writes the given row lists in turn, the last one repeating."""
    calls = []

    def fake_get_data(**kwargs):
        rows = replies[min(len(calls), len(replies) - 1)]
        calls.append(kwargs['start_date'])
        pd.DataFrame(rows, columns=['device', 'Timestamps', 'Data']).astype(
            {'device': str, 'Timestamps': 'int64', 'Data': float}).to_parquet(kwargs['output_file'], index=False)

    monkeypatch.setattr(nanny.dpm_data, 'get_data', fake_get_data)
    return calls


def test_an_empty_reply_is_asked_again_and_a_full_one_delivered(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'requests.txt').write_text('M:OUTTMP\n')
    start = datetime(2026, 4, 14, 3, tzinfo=UTC)
    t0 = round(start.timestamp() * 1e6)
    calls = _replies(monkeypatch, [[], [('L:CPSI', t0, 1.0)]])
    rests = []
    monkeypatch.setattr(nanny.time, 'sleep', rests.append)
    out = tmp_path / 'out'
    nanny.get_data(output_path=out, requests_list='requests.txt', output_format='parquet',
                   start_time=start, end_time='20260414T040000Z', retry_pause=30)
    assert len(calls) == 2 and rests == [30.0]
    (f,) = out.rglob('*.parquet')
    assert len(pd.read_parquet(f)) == 1


def test_a_reply_still_empty_after_the_retries_is_delivered(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'requests.txt').write_text('M:OUTTMP\n')
    start = datetime(2026, 7, 26, tzinfo=UTC)
    calls = _replies(monkeypatch, [[]])
    monkeypatch.setattr(nanny.time, 'sleep', lambda s: None)
    out = tmp_path / 'out'
    nanny.get_data(output_path=out, requests_list='requests.txt', output_format='parquet',
                   start_time=start, end_time='20260726T010000Z', retry_empty=3)
    assert len(calls) == 4
    (f,) = out.rglob('*.parquet')
    assert len(pd.read_parquet(f)) == 0
    assert nanny.get_retry_empty({}, {}) == (2, 60.0)
    assert nanny.get_retry_empty({}, {'retry-empty': 0, 'retry-pause': 5}) == (0, 5.0)


def _empty_file(out, start, version='2_0_0'):
    d = nanny.create_structured_path(out, start)
    d.mkdir(parents=True, exist_ok=True)
    f = d / f'{nanny.name_output_file(start, HOUR)}-{version}.parquet'
    pd.DataFrame({'device': pd.Series(dtype=str), 'Timestamps': pd.Series(dtype='int64'),
                  'Data': pd.Series(dtype=float)}).to_parquet(f, index=False)
    return f


def test_refill_replaces_empty_files_only_with_data(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'requests.txt').write_text('M:OUTTMP\n')
    monkeypatch.setattr(nanny.time, 'sleep', lambda s: None)
    start = datetime(2026, 4, 14, 2, tzinfo=UTC)
    t0 = round(start.timestamp() * 1e6)
    out = tmp_path / 'out'
    empty_a = _empty_file(out, start)             # the reply will have data: replaced
    empty_b = _empty_file(out, start + HOUR)      # the reply stays empty: kept
    full = _empty_file(out, start + 2 * HOUR)
    pd.DataFrame([('A', t0 + 2 * 3600 * 10**6, 1.0)], columns=['device', 'Timestamps', 'Data']).to_parquet(full)
    assert nanny.empty_windows(out, '.parquet') == {(start, HOUR), (start + HOUR, HOUR)}

    def reply(**kwargs):
        s = kwargs['start_date']
        rows = [('A', round(s.timestamp() * 1e6), 2.0)] if s == start else []
        pd.DataFrame(rows, columns=['device', 'Timestamps', 'Data']).astype(
            {'Timestamps': 'int64', 'Data': float}).to_parquet(kwargs['output_file'], index=False)
        pulled.append(s)

    pulled = []
    monkeypatch.setattr(nanny.dpm_data, 'get_data', reply)
    # Without refill nothing is missing.
    nanny.get_data(output_path=out, requests_list='requests.txt', output_format='parquet',
                   start_time=start, end_time='20260414T050000Z')
    assert pulled == []
    nanny.get_data(output_path=out, requests_list='requests.txt', output_format='parquet',
                   start_time=start, end_time='20260414T050000Z', refill_empty=True, retry_empty=1)
    assert sorted(set(pulled)) == [start, start + HOUR] and pulled.count(start + HOUR) == 2
    assert not empty_a.exists() and empty_b.exists()
    (new,) = [f for f in out.rglob(f'{nanny.name_output_file(start, HOUR)}-*.parquet')]
    assert pd.read_parquet(new)['Data'].tolist() == [2.0]
    assert nanny.empty_windows(out, '.parquet') == {(start + HOUR, HOUR)}
    assert list((tmp_path / 'workbench').iterdir()) == []


def test_refill_with_split_replaces_only_the_parts_with_data(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'requests.txt').write_text('M:OUTTMP\n')
    start = datetime(2026, 4, 13, tzinfo=UTC)
    t0 = round(start.timestamp() * 1e6)
    out = tmp_path / 'out'
    for k in range(3):
        _empty_file(out, start + k * HOUR)
    _replies(monkeypatch, [[('A', t0 + 3600 * 10**6 + 5, 1.0)]])  # data in the middle hour only
    nanny.get_data(output_path=out, requests_list='requests.txt', output_format='parquet', start_time=start,
                   end_time='20260413T030000Z', duration='T3H', split='T1H', refill_empty=True)
    assert nanny.empty_windows(out, '.parquet') == {(start, HOUR), (start + 2 * HOUR, HOUR)}
    assert len(list(out.rglob('*.parquet'))) == 3


def test_config_logging_one_rotating_formatted_file(tmp_path, monkeypatch):
    """Each record lands once, formatted; lines below the level from loggers set lower (pacsys) stay out;
    a second call replaces the handler; the file rolls over at 1 GB, 1,000 old files kept (1.10.1)."""
    from logging.handlers import RotatingFileHandler
    monkeypatch.chdir(tmp_path)
    root = logging.getLogger()
    saved = (root.handlers[:], root.level)
    try:
        nanny.config_logging('INFO')
        handler = nanny.config_logging('INFO')
        mine = [h for h in root.handlers if isinstance(h, RotatingFileHandler)]
        assert mine == [handler]
        assert handler.maxBytes == 1073741824 and handler.backupCount == 1000
        noisy = logging.getLogger('pacsys.test')
        noisy.setLevel(logging.DEBUG)
        noisy.debug('chunk received')
        nanny.logger.info('Pulled %s in %.1f s.', 'X', 1.0)
        handler.flush()
        lines = (tmp_path / 'nanny.log').read_text().splitlines()
        assert len(lines) == 1
        assert lines[0].endswith(' - INFO - datalogger_to_ml.nanny - Pulled X in 1.0 s.')
    finally:
        for h in root.handlers[:]:
            if h not in saved[0]:
                root.removeHandler(h)
                h.close()
        root.setLevel(saved[1])
