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
from archive_scout.network import cancellation
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


class NoWakeSocket:
    """A real socket whose shutdown deliberately does not wake a blocked read."""
    def __init__(self, sock):
        self.sock = sock
        self._io_refs = 0
        self._closed = False
    def __getattr__(self, name):
        return getattr(self.sock, name)
    def shutdown(self, how):
        pass
    def makefile(self, *args, **kwargs):
        return socket.socket.makefile(self, *args, **kwargs)
    def close(self):
        self._closed = True
        if not self._io_refs:
            self.sock.close()
    def _decref_socketios(self):
        self._io_refs -= 1
        if self._closed:
            self.close()


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

    def test_httpcore_stop_does_not_depend_on_shutdown_waking_the_read(self):
        entered, release, stop = threading.Event(), threading.Event(), threading.Event()
        errors = []
        class Stream:
            def get_extra_info(self, info):
                return SimpleNamespace(shutdown=lambda how: None)
            def read(self, amount, timeout=None):
                entered.set()
                if release.wait(timeout):
                    return b'complete'
                raise httpcore.ReadTimeout('waiting')
        owner = cancellation.SocketCancellation()
        stream = cancellation.CancellableStream(Stream(), owner)
        def read():
            try:
                with owner.scope(stop):
                    stream.read(6, timeout=5)
            except Exception as exc:
                errors.append(type(exc))
        with mock.patch.object(cancellation, '_WINDOWS_READ_POLLING', True, create=True):
            worker = threading.Thread(target=read)
            worker.start()
            try:
                self.assertTrue(entered.wait(2))
                stop.set(); worker.join(1)
                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, [Stopped])
            finally:
                release.set(); worker.join(3); owner.close()

    def test_urllib3_stop_does_not_depend_on_shutdown_waking_the_read(self):
        raw, peer = socket.socketpair()
        raw.settimeout(5)
        sock = NoWakeSocket(raw)
        entered, stop = threading.Event(), threading.Event()
        owner = cancellation.SocketCancellation()
        errors = []
        manager = fake_pool(sock, entered)
        def read():
            try:
                with owner.scope(stop):
                    manager.pool_classes_by_scheme['http'].ConnectionCls().getresponse()
            except Exception as exc:
                errors.append(type(exc))
        with mock.patch.object(cancellation, '_WINDOWS_READ_POLLING', True, create=True):
            cancellation.install_urllib3(manager, owner)
            worker = threading.Thread(target=read)
            worker.start()
            try:
                self.assertTrue(entered.wait(2))
                stop.set(); worker.join(1)
                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, [Stopped])
            finally:
                peer.close(); worker.join(3); sock.close(); owner.close()

    def test_short_read_waits_do_not_poison_urllib3_buffer_or_close_reused_socket(self):
        raw, peer = socket.socketpair()
        raw.settimeout(2)
        sock = NoWakeSocket(raw)
        entered, owner = threading.Event(), cancellation.SocketCancellation()
        manager = fake_pool(sock, entered)
        def delayed_data():
            time.sleep(.25)
            peer.sendall(b'needle')
        writer = threading.Thread(target=delayed_data)
        with mock.patch.object(cancellation, '_WINDOWS_READ_POLLING', True):
            cancellation.install_urllib3(manager, owner)
            connection = manager.pool_classes_by_scheme['http'].ConnectionCls()
            writer.start()
            try:
                with owner.scope(threading.Event()):
                    self.assertEqual(connection.getresponse(), b'needle')
                    peer.sendall(b'archiv')
                    self.assertEqual(connection.getresponse(), b'archiv')
                self.assertEqual(sock.gettimeout(), 2)
                self.assertGreaterEqual(sock.fileno(), 0)
            finally:
                writer.join(3); peer.close(); connection.sock.close(); owner.close()

    def test_read_polling_honors_full_deadline_and_nonblocking_mode(self):
        now, waits = [0.0], []
        def timeout(wait):
            waits.append(wait); now[0] += wait
            raise socket.timeout('original read deadline')
        with mock.patch.object(cancellation, '_WINDOWS_READ_POLLING', True), mock.patch.object(cancellation.time, 'monotonic', side_effect=lambda: now[0]):
            with self.assertRaisesRegex(socket.timeout, 'original read deadline'):
                cancellation._poll_read(timeout, .35, threading.Event(), socket.timeout)
        self.assertAlmostEqual(sum(waits), .35)
        self.assertEqual(len(waits), 4)
        operation = mock.Mock(return_value=b'available')
        with mock.patch.object(cancellation, '_WINDOWS_READ_POLLING', True):
            self.assertEqual(cancellation._poll_read(operation, 0, threading.Event(), socket.timeout), b'available')
        operation.assert_called_once_with(0)
