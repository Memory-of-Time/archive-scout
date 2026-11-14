"""Operation-owned connections; direct downloader calls remain self-contained."""
from contextvars import ContextVar
from concurrent.futures import ThreadPoolExecutor

_clients = ContextVar('archive_scout_acquisition_clients', default=None)


def begin_session():
    return _clients.set({})


def end_session(token):
    clients = _clients.get()
    try:
        for client in reversed(list((clients or {}).values())):
            client.close()
    finally:
        _clients.reset(token)


def acquisition_client(key, factory):
    clients = _clients.get()
    if clients is None:
        return factory(), True
    if key not in clients:
        clients[key] = factory()
    return clients[key], False


class AcquisitionExecutor(ThreadPoolExecutor):
    def close(self):
        self.shutdown(wait=True, cancel_futures=True)
