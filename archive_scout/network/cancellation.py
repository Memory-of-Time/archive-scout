"""Abort only the socket currently blocked in a cancelled request's I/O."""
from __future__ import annotations

import contextlib
import socket
import threading

import httpcore
import urllib3

from ..events import Stopped


class SocketCancellation:
    def __init__(self):
        self.condition = threading.Condition()
        self.local = threading.local()
        self.active = {}
        self.closed = False
        self.thread = None

    @contextlib.contextmanager
    def scope(self, stop_event):
        previous = getattr(self.local, 'stop', None)
        self.local.stop = stop_event
        try:
            if stop_event.is_set():
                raise Stopped
            yield
            if stop_event.is_set():
                raise Stopped
        except Exception:
            if stop_event.is_set():
                raise Stopped from None
            raise
        finally:
            self.local.stop = previous

    @contextlib.contextmanager
    def io(self, socket_getter):
        stop = getattr(self.local, 'stop', None)
        if stop is None:
            yield
            return
        if stop.is_set():
            raise Stopped
        token = object()
        with self.condition:
            if self.closed:
                raise Stopped
            if self.thread is None:
                self.thread = threading.Thread(target=self._watch, name='archive-socket-cancel', daemon=True)
                self.thread.start()
            self.active[token] = (stop, socket_getter)
            self.condition.notify_all()
        try:
            yield
        finally:
            # Removal and shutdown share the lock. A late watcher can never
            # abort this socket after it has been returned to the connection pool.
            with self.condition:
                self.active.pop(token, None)
                self.condition.notify_all()

    def _watch(self):
        with self.condition:
            while not self.closed:
                if not self.active:
                    self.condition.wait()
                    continue
                for token, (stop, getter) in tuple(self.active.items()):
                    if stop.is_set():
                        try:
                            sock = getter()
                            if sock is not None:
                                sock.shutdown(socket.SHUT_RDWR)
                        except (OSError, ValueError, AttributeError):
                            pass
                self.condition.wait(.025)

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        if self.thread is not None:
            self.thread.join(timeout=1)


class CancellableStream(httpcore.NetworkStream):
    def __init__(self, stream, cancellation):
        self.stream = stream
        self.cancellation = cancellation

    def read(self, max_bytes, timeout=None):
        with self.cancellation.io(lambda: self.stream.get_extra_info('socket')):
            return self.stream.read(max_bytes, timeout=timeout)

    def write(self, buffer, timeout=None):
        with self.cancellation.io(lambda: self.stream.get_extra_info('socket')):
            return self.stream.write(buffer, timeout=timeout)

    def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        with self.cancellation.io(lambda: self.stream.get_extra_info('socket')):
            self.stream = self.stream.start_tls(ssl_context, server_hostname=server_hostname, timeout=timeout)
        return self

    def get_extra_info(self, info):
        return self.stream.get_extra_info(info)

    def close(self):
        self.stream.close()


class CancellableNetwork(httpcore.NetworkBackend):
    def __init__(self, backend, cancellation):
        self.backend = backend
        self.cancellation = cancellation

    def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        stream = self.backend.connect_tcp(host, port, timeout=timeout, local_address=local_address, socket_options=socket_options)
        return CancellableStream(stream, self.cancellation)

    def connect_unix_socket(self, path, timeout=None, socket_options=None):
        return CancellableStream(self.backend.connect_unix_socket(path, timeout=timeout, socket_options=socket_options), self.cancellation)


def install_httpx(client, cancellation):
    # HTTPX 0.28 does not expose a network_backend constructor argument. This
    # isolated adapter is checked against the pinned release and every proxy
    # mount; all stream behavior uses HTTPCore's public backend interface.
    transports = [client._transport, *client._mounts.values()]
    for transport in {item for item in transports if item is not None}:
        pool = getattr(transport, '_pool', None)
        if pool is None or not hasattr(pool, '_network_backend'):
            raise RuntimeError('HTTPX transport cannot install cancellation backend')
        pool._network_backend = CancellableNetwork(pool._network_backend, cancellation)


def install_urllib3(manager, cancellation):
    classes = {}
    for scheme, original in manager.pool_classes_by_scheme.items():
        connection = original.ConnectionCls
        class Connection(connection):
            def request(self, *args, **kwargs):
                with cancellation.io(lambda: self.sock):
                    return super().request(*args, **kwargs)

            def getresponse(self):
                with cancellation.io(lambda: self.sock):
                    return super().getresponse()

        class Pool(original):
            ConnectionCls = Connection
        classes[scheme] = Pool
    manager.pool_classes_by_scheme = classes
