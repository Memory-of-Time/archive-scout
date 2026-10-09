from __future__ import annotations

import contextlib

import errno

import gzip

import os

import socket

import sqlite3

import tempfile

import threading

import time

import unittest

from pathlib import Path

from types import SimpleNamespace

from unittest import mock

import httpcore

from archive_scout.database.connection import open_database

from archive_scout.events import Stopped

from archive_scout.projects import backups

@contextlib.contextmanager
def windows_fsync_policy():
    """Emulate Windows refusing fsync on a read-only file descriptor."""
    original_open, original_sync = Path.open, os.fsync
    handles = {}
    calls = []
    def opened(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        handles[handle.fileno()] = handle
        return handle
    def synced(fd):
        handle = handles[fd]
        if not handle.writable():
            raise OSError(errno.EBADF, 'read-only descriptor cannot be committed')
        calls.append(handle.name)
        return original_sync(fd)
    with mock.patch.object(Path, 'open', opened), mock.patch.object(backups.os, 'fsync', synced):
        yield calls


def fake_pool(sock, entered):
    class Connection:
        def __init__(self):
            self.sock = sock
        def getresponse(self):
            entered.set()
            with self.sock.makefile('rb') as reader:
                return reader.read(6)
    class Pool:
        ConnectionCls = Connection
    manager = SimpleNamespace(pool_classes_by_scheme={'http': Pool})
    return manager


class PlatformFixTests(unittest.TestCase):
    def test_backup_and_restore_sync_writable_descriptors_without_losing_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            db = open_database(root)
            db.execute("INSERT INTO project_meta(key,value) VALUES('platform_probe','backup')")
            db.commit(); db.close()
            with windows_fsync_policy() as calls:
                archive = backups.create_project_backup(root)
                with gzip.open(archive, 'rb') as handle:
                    self.assertTrue(handle.read().startswith(b'SQLite format 3'))
                db = open_database(root)
                db.execute("UPDATE project_meta SET value='current' WHERE key='platform_probe'")
                db.commit(); db.close()
                safety = backups.restore_project_backup(root, archive)
                self.assertEqual(len(calls), 2)
            for path, expected in [(root / 'archive_scout.sqlite3', 'backup'), (safety, 'current')]:
                db = sqlite3.connect(path)
                try:
                    self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(), [('ok',)])
                    self.assertEqual(db.execute("SELECT value FROM project_meta WHERE key='platform_probe'").fetchone()[0], expected)
                finally:
                    db.close()

    def test_real_fsync_failure_never_publishes_or_prunes_a_backup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            open_database(root).close()
            valid = backups.create_project_backup(root, keep=1)
            previous = valid.read_bytes()
            with mock.patch.object(backups.os, 'fsync', side_effect=OSError('injected disk sync failure')):
                with self.assertRaisesRegex(OSError, 'disk sync'):
                    backups.create_project_backup(root, keep=1)
            self.assertEqual(backups.list_project_backups(root), [valid])
            self.assertEqual(valid.read_bytes(), previous)
            self.assertEqual(list((root / 'backups').glob('*.tmp')), [])





