#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import fcntl
import os
from datetime import datetime, timedelta

import pandas as pd

from datalogger_to_ml import nanny, h5_validator
from datalogger_to_ml.dpm_data.dpm_data import _open_output

HOUR = timedelta(hours=1)


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
    start = datetime(2026, 11, 13)
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
    newest = nanny.last_whole_window(datetime.now(), HOUR)
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
    newest = nanny.last_whole_window(datetime.now(), HOUR)
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
    newest = nanny.last_whole_window(datetime.now(), HOUR)
    pulled = _run_nanny(tmp_path, monkeypatch, start_time=newest - 5 * HOUR, recency='PT3H')
    # Windows ending within the last 3 h are the keep-up run's; the rest come newest first.
    assert pulled and all(w + HOUR <= datetime.now() - 3 * HOUR for w in pulled)
    assert pulled == sorted(pulled, reverse=True) and pulled[-1] == newest - 5 * HOUR
    assert newest not in pulled and newest - HOUR not in pulled


def test_a_claimed_window_is_skipped_and_claims_are_cleaned_up(tmp_path, monkeypatch):
    newest = nanny.last_whole_window(datetime.now(), HOUR)
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
    newest = nanny.last_whole_window(datetime.now(), HOUR)
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
