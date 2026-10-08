from __future__ import annotations

import contextlib
import errno
import hashlib
import os
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

import httpx
import urllib3

from ..events import Stopped
from ..text_encoding import TextDecodingError
from ..runtime import ensure_frozen_bundle_available
from .cancellation import SocketCancellation, install_httpx, install_urllib3

try:
    import truststore
except ImportError:  # pragma: no cover - exercised in minimal source installs
    truststore = None


class BackendUnavailable(RuntimeError):
    pass


class LocalStorageError(OSError):
    """A transport reported a failure writing its local response file."""


class RequestAdmissionRejected(RuntimeError):
    """A shared service gate invalidated this attempt before bytes were sent."""


class InvalidRangeResponse(RuntimeError):
    """Replay cannot safely be appended; retry the complete representation."""


class PayloadValidationError(RuntimeError):
    """A response validator failed; keep the healthy transport eligible."""


def _validate_preview(validator, headers, data):
    try:
        return validator(headers, data)
    except (TextDecodingError, PreviewRejected):
        raise
    except Exception as exc:
        raise PayloadValidationError(f"Payload validator failed ({type(exc).__name__})") from exc


class PreviewRejected(RuntimeError):
    """Bounded replay prefix proved the payload belongs outside text capture."""

    def __init__(self, classification: str) -> None:
        self.classification = str(classification or "unknown")
        super().__init__(f"replay prefix classified as {self.classification}")


class BackendsCoolingDown(RuntimeError):
    """No transport backend is currently eligible for a new wire attempt."""

    def __init__(self, wait_seconds: float) -> None:
        self.wait_seconds = max(0.0, float(wait_seconds))
        super().__init__(f"all network backends are cooling down for about {self.wait_seconds:.1f}s")


class ServiceStatusResponse(RuntimeError):
    """A live Wayback service status was known from headers before body I/O."""

    def __init__(self, status: int, headers: dict[str, str], url: str, backend: str) -> None:
        self.status = int(status)
        self.headers = dict(headers)
        self.url = str(url)
        self.backend = str(backend)
        super().__init__(f"HTTP {self.status}: {self.url}")


class RedirectPolicyError(RuntimeError):
    """A redirect was rejected before contacting the destination."""

    def __init__(self, source: str, destination: str, category: str = "external_redirect_blocked") -> None:
        self.source = str(source)
        self.destination = str(destination)
        self.category = str(category)
        self.status = None
        super().__init__(f"{self.category}: {self.source} -> {self.destination}")


class TransportExhaustedError(RuntimeError):
    def __init__(self, url: str, failures: list[tuple[str, BaseException]]) -> None:
        self.url = url
        self.failures = failures
        self.timed_out = any(is_transport_timeout(exc) for _, exc in failures)
        self.read_timed_out = any(is_transport_read_timeout(exc) for _, exc in failures)
        self.connection_failed = bool(failures) and all(
            is_transport_connection_failure(exc)
            and urllib.parse.urlsplit(getattr(exc, "request_url", url)).netloc.casefold()
                == urllib.parse.urlsplit(url).netloc.casefold()
            for _, exc in failures
        )
        summary = "; ".join(f"{name}: {type(exc).__name__}: {exc}" for name, exc in failures)
        summary = re.sub(r"([a-zA-Z][\w+.-]*://)[^/\s@]+@", r"\1[redacted]@", summary)
        super().__init__(f"all network backends failed for {url}: {summary}")


@dataclass(slots=True)
class TransportResponse:
    status: int
    headers: dict[str, str]
    final_url: str
    data: bytes | bytearray
    backend: str
    elapsed: float


@dataclass(slots=True)
class TransportFileResponse:
    status: int
    headers: dict[str, str]
    final_url: str
    path: Path
    bytes_written: int
    content_hash: str
    preview: bytes
    backend: str
    elapsed: float


def is_transport_timeout(exc: BaseException) -> bool:
    current: BaseException | None = exc
    visited: set[int] = set()
    timeout_types = (
        TimeoutError,
        httpx.TimeoutException,
        urllib3.exceptions.TimeoutError,
        urllib3.exceptions.ReadTimeoutError,
        urllib3.exceptions.ConnectTimeoutError,
    )
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, timeout_types):
            return True
        reason = getattr(current, "reason", None)
        if isinstance(reason, BaseException) and reason is not current:
            current = reason
            continue
        current = current.__cause__ or current.__context__
    return False




def is_transport_connection_failure(exc: BaseException) -> bool:
    """Classify DNS, proxy, TLS, and socket setup failures across backends."""
    current: BaseException | None = exc
    visited: set[int] = set()
    connection_types = (
        ConnectionError,
        ConnectionRefusedError,
        socket.gaierror,
        ssl.SSLError,
        httpx.ConnectError,
        httpx.ConnectTimeout,
        httpx.ProxyError,
        urllib3.exceptions.NewConnectionError,
        urllib3.exceptions.NameResolutionError,
        urllib3.exceptions.ConnectTimeoutError,
        urllib3.exceptions.ProxyError,
    )
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, (
            httpx.ReadError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.PoolTimeout,
            urllib3.exceptions.ReadTimeoutError, urllib3.exceptions.ProtocolError,
            urllib3.exceptions.EmptyPoolError,
        )):
            return False  # A nested reset during body I/O is not connection setup.
        if isinstance(current, connection_types):
            return True
        if isinstance(current, OSError):
            text = str(current).casefold()
            if any(token in text for token in (
                "could not resolve", "name resolution", "failed to resolve",
                "connection refused", "connect call failed", "failed to connect",
                "connection timed out", "resolving timed out", "proxy",
                "certificate", "ssl", "tls", "network is unreachable",
                "no route to host",
            )):
                return True
        reason = getattr(current, "reason", None)
        if isinstance(reason, BaseException) and reason is not current:
            current = reason
            continue
        current = current.__cause__ or current.__context__
    return False


def is_transport_read_timeout(exc: BaseException) -> bool:
    current: BaseException | None = exc
    visited: set[int] = set()
    read_types = (
        httpx.ReadTimeout,
        urllib3.exceptions.ReadTimeoutError,
    )
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, read_types):
            return True
        reason = getattr(current, "reason", None)
        if isinstance(reason, BaseException) and reason is not current:
            current = reason
            continue
        current = current.__cause__ or current.__context__
    return False


def is_local_storage_error(exc: BaseException) -> bool:
    """Return True for local filesystem failures that must never rotate HTTP stacks."""
    current: BaseException | None = exc
    visited: set[int] = set()
    storage_errnos = {
        errno.ENOSPC, errno.EDQUOT, errno.EROFS, errno.EACCES, errno.EPERM,
        errno.ENAMETOOLONG, errno.ENOENT,
    }
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, LocalStorageError):
            return True
        if isinstance(current, OSError) and getattr(current, "errno", None) in storage_errnos:
            return True
        current = current.__cause__ or current.__context__
    return False


def is_response_failure(exc: BaseException) -> bool:
    """A slow/malformed body or local pool wait does not prove a broken stack."""
    if is_transport_connection_failure(exc):
        return False
    current = exc
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if is_transport_timeout(current) or isinstance(current, (
            httpx.ReadError, httpx.RemoteProtocolError, httpx.DecodingError,
            urllib3.exceptions.ProtocolError, urllib3.exceptions.DecodeError,
            urllib3.exceptions.EmptyPoolError,
        )):
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_archived_memento(headers: dict[str, str], url: str) -> bool:
    if "/web/" not in str(url):
        return False
    lowered = {str(k).casefold(): str(v) for k, v in headers.items()}
    return bool(lowered.get("memento-datetime"))


def _raise_live_service_status(status: int, headers: dict[str, str], url: str, backend: str) -> None:
    if int(status) in {429, 503} and not _is_archived_memento(headers, url):
        raise ServiceStatusResponse(status, headers, url, backend)


def _redirect_destination(
    current_url: str,
    location: str,
    request_headers: dict[str, str],
    redirect_validator: Callable[[str, str], None] | None,
) -> str:
    destination = urllib.parse.urljoin(current_url, location)
    if "Range" in request_headers or "range" in request_headers:
        # A saved prefix belongs to the original representation.  Until a
        # persisted validator proves otherwise, restart this capture from zero
        # before following any redirect so bytes from two identities can never
        # be appended together.
        raise InvalidRangeResponse("replay redirected while resuming a partial file; restarting complete representation")
    if redirect_validator is not None:
        redirect_validator(current_url, destination)
    return destination

def _ssl_context(trust_env: bool = True) -> ssl.SSLContext:
    context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT) if truststore else ssl.create_default_context()
    if trust_env:
        cafile, capath = os.environ.get("SSL_CERT_FILE"), os.environ.get("SSL_CERT_DIR")
        if cafile or capath:
            context.load_verify_locations(cafile=cafile, capath=capath)
    return context


def _validate_range(status: int, response_headers, request_headers, existing_size: int) -> bool:
    if status != 206:
        return False  # A full 200 replaces, never appends to, the saved prefix.
    fields = {str(k).lower(): str(v) for k, v in response_headers.items()}
    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", fields.get("content-range", "").strip())
    requested = request_headers.get("Range", "")
    if (not match or requested != f"bytes={existing_size}-" or existing_size <= 0
            or fields.get("content-encoding", "identity").lower() not in {"", "identity"}):
        raise InvalidRangeResponse("invalid replay range response; restarting complete file")
    start, end, total = map(int, match.groups())
    if start != existing_size or end < start or end + 1 != total:
        raise InvalidRangeResponse("mismatched replay range offsets; restarting complete file")
    return True


def _validate_range_size(status: int, headers, total: int) -> None:
    if status == 206:
        fields = {str(k).lower(): str(v) for k, v in headers.items()}
        if total != int(fields["content-range"].rsplit("/", 1)[1]):
            raise InvalidRangeResponse("incomplete replay range body; restarting complete file")


def _read_limited(chunks: Iterable[bytes], max_bytes: int, stop_event: threading.Event, progress=None) -> bytearray:
    data = bytearray()
    for chunk in chunks:
        if stop_event.is_set():
            raise Stopped
        if not chunk:
            continue
        data.extend(chunk)
        if len(data) > max_bytes:
            raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
        if progress is not None and len(data) - len(chunk) < 64 * 1024:
            progress(bytes(data[:8192]))
    # Returning the bytearray avoids a full-size bytes copy at the exact moment
    # the response buffer is largest. Consumers only require the bytes-like API.
    return data




def _write_limited(
    chunks: Iterable[bytes],
    destination: Path,
    max_bytes: int,
    stop_event: threading.Event,
    preview_bytes: int = 20000,
    *,
    append: bool = False,
    compute_hash: bool = True,
    preview_validator: Callable[[dict[str, str], bytes], str | None] | None = None,
    response_headers: dict[str, str] | None = None,
    progress=None,
) -> tuple[int, str, bytes]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256() if compute_hash else None
    preview = bytearray()
    prefix = destination.stat().st_size if append and destination.exists() else 0
    if prefix:
        with destination.open("rb") as existing:
            if digest is None:
                preview.extend(existing.read(preview_bytes))
            else:
                while True:
                    chunk = existing.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                    if len(preview) < preview_bytes:
                        preview.extend(chunk[: preview_bytes - len(preview)])
    total = prefix
    mode = "ab" if append and prefix else "wb"
    preview_checked = False
    headers = response_headers or {}
    try:
        with destination.open(mode) as handle:
            for chunk in chunks:
                if stop_event.is_set():
                    raise Stopped
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                handle.write(chunk)
                if digest is not None:
                    digest.update(chunk)
                if len(preview) < preview_bytes:
                    preview.extend(chunk[: preview_bytes - len(preview)])
                if preview_validator is not None and not preview_checked and len(preview) >= min(preview_bytes, 8192):
                    rejected = _validate_preview(preview_validator, headers, bytes(preview))
                    preview_checked = True
                    if rejected:
                        raise PreviewRejected(str(rejected))
                if progress is not None and (preview_checked or preview_validator is None):
                    progress(bytes(preview))
                    progress = None
            if preview_validator is not None and not preview_checked:
                rejected = _validate_preview(preview_validator, headers, bytes(preview))
                if rejected:
                    raise PreviewRejected(str(rejected))
        return total, digest.hexdigest() if digest is not None else "", bytes(preview)
    except Stopped:
        # A partially streamed replay is durable resume state. Leave it intact.
        raise
    except RuntimeError as exc:
        # A local size/classification rejection cannot become a valid Range resume.
        if isinstance(exc, PreviewRejected) or "response exceeds" in str(exc).casefold():
            destination.unlink(missing_ok=True)
        raise
    except Exception:
        # Network reads can fail after useful bytes reached disk. Preserve those
        # bytes so HttpClient can retry with Range or a later run can resume.
        raise


def _copy_headers(items: Iterable[tuple[object, object]]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for raw_key, raw_value in items:
        key = str(raw_key)
        value = str(raw_value)
        headers[key] = value
        headers[key.casefold()] = value
    return headers


@contextlib.contextmanager
def _origin_attempt(factory, url: str):
    """Attribute a redirected wire failure to the host actually contacted."""
    try:
        if factory is None:
            context = contextlib.nullcontext()
        else:
            try:
                context = factory(url=url)
            except TypeError as exc:
                if "unexpected keyword argument" not in str(exc):
                    raise
                context = factory()
        with context as progress:
            yield progress if callable(progress) else (lambda *args: None)
    except Exception as exc:
        exc.request_url = url
        raise


def _cancel_scope(backend, stop_event):
    cancellation = getattr(backend, 'cancellation', None)
    return cancellation.scope(stop_event) if cancellation is not None else contextlib.nullcontext()


def _cancel_io(backend, getter):
    cancellation = getattr(backend, 'cancellation', None)
    return cancellation.io(getter) if cancellation is not None else contextlib.nullcontext()


class HttpxBackend:
    name = "httpx"

    def __init__(self, pool_size: int, connect_timeout: float, read_timeout: float, trust_env: bool = True) -> None:
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.read_timeout = max(1.0, float(read_timeout))
        self.cancellation = SocketCancellation()
        self.client = httpx.Client(
            verify=_ssl_context(trust_env),
            follow_redirects=False,
            trust_env=bool(trust_env),
            http2=False,
            limits=httpx.Limits(
                max_connections=max(2, int(pool_size)),
                max_keepalive_connections=max(1, int(pool_size)),
                keepalive_expiry=90.0,
            ),
            timeout=httpx.Timeout(
                connect=self.connect_timeout,
                read=self.read_timeout,
                write=self.connect_timeout,
                pool=max(5.0, self.connect_timeout),
            ),
        )

        install_httpx(self.client, self.cancellation)

    def close(self) -> None:
        self.client.close()
        cancellation = getattr(self, "cancellation", None)
        if cancellation is not None:
            cancellation.close()

    @staticmethod
    def _attempt(factory):
        return factory() if factory is not None else contextlib.nullcontext()

    def request(
        self,
        url: str,
        headers: dict[str, str],
        max_bytes: int,
        stop_event: threading.Event,
        *,
        attempt_context_factory=None,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            if stop_event.is_set():
                raise Stopped
            with _cancel_scope(self, stop_event), _origin_attempt(attempt_context_factory, current_url) as progress:
                with self.client.stream("GET", current_url, headers=headers, follow_redirects=False) as response:
                    status = int(response.status_code)
                    copied_headers = _copy_headers(response.headers.items())
                    _raise_live_service_status(status, copied_headers, str(response.url), self.name)
                    progress(status, copied_headers, str(response.url))
                    if status in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location")
                        if location:
                            current_url = _redirect_destination(current_url, location, headers, redirect_validator)
                            continue
                    announced = response.headers.get("Content-Length")
                    if announced and announced.isdigit() and int(announced) > max_bytes:
                        raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                    data = _read_limited(response.iter_bytes(), max_bytes, stop_event, lambda prefix: progress(status, copied_headers, str(response.url), prefix))
                    return TransportResponse(
                        status=status,
                        headers=copied_headers,
                        final_url=str(response.url),
                        data=data,
                        backend=self.name,
                        elapsed=time.monotonic() - started,
                    )
        raise RuntimeError(f"too many redirects: {url}")

    def download(
        self,
        url: str,
        headers: dict[str, str],
        destination: Path,
        max_bytes: int,
        stop_event: threading.Event,
        compute_hash: bool = True,
        preview_validator: Callable[[dict[str, str], bytes], str | None] | None = None,
        *,
        attempt_context_factory=None,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            if stop_event.is_set():
                raise Stopped
            with _cancel_scope(self, stop_event), _origin_attempt(attempt_context_factory, current_url) as progress:
                with self.client.stream("GET", current_url, headers=headers, follow_redirects=False) as response:
                    status = int(response.status_code)
                    copied_headers = _copy_headers(response.headers.items())
                    _raise_live_service_status(status, copied_headers, str(response.url), self.name)
                    progress(status, copied_headers, str(response.url))
                    if status in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location")
                        if location:
                            current_url = _redirect_destination(current_url, location, headers, redirect_validator)
                            continue
                    announced = response.headers.get("Content-Length")
                    if announced and announced.isdigit() and int(announced) > max_bytes:
                        raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                    append = _validate_range(
                        status, response.headers, headers,
                        destination.stat().st_size if destination.exists() else 0,
                    )
                    total, content_hash, preview = _write_limited(
                        response.iter_bytes(), destination, max_bytes, stop_event,
                        preview_bytes=(64 * 1024 if preview_validator is not None else 20000),
                        append=append, compute_hash=compute_hash,
                        preview_validator=preview_validator, response_headers=copied_headers,
                        progress=lambda prefix: progress(status, copied_headers, current_url, prefix),
                    )
                    _validate_range_size(status, response.headers, total)
                    return TransportFileResponse(
                        status=status,
                        headers=copied_headers,
                        final_url=str(response.url),
                        path=destination,
                        bytes_written=total,
                        content_hash=content_hash,
                        preview=preview,
                        backend=self.name,
                        elapsed=time.monotonic() - started,
                    )
        raise RuntimeError(f"too many redirects: {url}")


class Urllib3Backend:
    name = "urllib3"

    def __init__(self, pool_size: int, connect_timeout: float, read_timeout: float, trust_env: bool = True) -> None:
        self.cancellation = SocketCancellation()
        self.timeout = urllib3.Timeout(connect=max(1.0, connect_timeout), read=max(1.0, read_timeout))
        self.pool_options = dict(
            num_pools=4,
            maxsize=max(2, int(pool_size)),
            block=True,
            ssl_context=_ssl_context(trust_env),
            retries=False,
        )
        self.pool = urllib3.PoolManager(**self.pool_options)
        install_urllib3(self.pool, self.cancellation)
        self.proxies = urllib.request.getproxies() if trust_env else {}
        self.proxy_pools: dict[str, object] = {}
        self.proxy_lock = threading.Lock()

    def _pool_for(self, url: str):
        parsed = urllib.parse.urlsplit(url)
        if not self.proxies or urllib.request.proxy_bypass_environment(parsed.netloc, self.proxies):
            return self.pool
        proxy = self.proxies.get(parsed.scheme) or self.proxies.get("all")
        if not proxy:
            return self.pool
        with self.proxy_lock:
            if proxy not in self.proxy_pools:
                parsed_proxy = urllib.parse.urlsplit(proxy)
                if parsed_proxy.scheme not in {"http", "https"}:
                    raise BackendUnavailable("urllib3 requires an HTTP(S) proxy; use httpx or curl for this proxy type")
                credentials = None
                if parsed_proxy.username is not None:
                    credentials = urllib3.make_headers(proxy_basic_auth=(
                        urllib.parse.unquote(parsed_proxy.username) + ":" + urllib.parse.unquote(parsed_proxy.password or "")
                    ))
                proxy_address = urllib.parse.urlunsplit(parsed_proxy._replace(netloc=parsed_proxy.netloc.rsplit("@", 1)[-1]))
                self.proxy_pools[proxy] = urllib3.ProxyManager(proxy_address, proxy_headers=credentials, **self.pool_options)
                install_urllib3(self.proxy_pools[proxy], self.cancellation)
            return self.proxy_pools[proxy]

    def close(self) -> None:
        self.pool.clear()
        for pool in self.proxy_pools.values():
            pool.clear()
        cancellation = getattr(self, "cancellation", None)
        if cancellation is not None:
            cancellation.close()

    @staticmethod
    def _discard(response) -> None:
        if response is None:
            return
        try:
            response.close()
            response.release_conn()
        except Exception:
            try:
                response.close()
            except Exception:
                pass

    @staticmethod
    def _attempt(factory):
        return factory() if factory is not None else contextlib.nullcontext()

    def request(
        self, url: str, headers: dict[str, str], max_bytes: int,
        stop_event: threading.Event, *, attempt_context_factory=None,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            response = None
            try:
                if stop_event.is_set():
                    raise Stopped
                with _cancel_scope(self, stop_event), _origin_attempt(attempt_context_factory, current_url) as progress:
                    response = self._pool_for(current_url).request(
                        "GET", current_url, headers=headers, preload_content=False,
                        redirect=False, retries=False, timeout=self.timeout,
                        pool_timeout=max(1.0, float(self.timeout.connect_timeout)),
                    )
                    status = int(response.status)
                    copied_headers = _copy_headers(response.headers.items())
                    _raise_live_service_status(status, copied_headers, current_url, self.name)
                    progress(status, copied_headers, current_url)
                    if status in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location")
                        if location:
                            self._discard(response)
                            response = None
                            current_url = _redirect_destination(current_url, str(location), headers, redirect_validator)
                            continue
                    announced = response.headers.get("Content-Length")
                    if announced and str(announced).isdigit() and int(announced) > max_bytes:
                        raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                    def chunks():
                        while True:
                            with _cancel_io(self, lambda: response.connection.sock if response.connection else None):
                                chunk = response.read1(64 * 1024, decode_content=True)
                            if not chunk:
                                return
                            yield chunk
                    data = _read_limited(chunks(), max_bytes, stop_event, lambda prefix: progress(status, copied_headers, current_url, prefix))
                    result = TransportResponse(
                        status=status, headers=copied_headers,
                        final_url=current_url, data=data, backend=self.name,
                        elapsed=time.monotonic() - started,
                    )
                    response.release_conn()
                    response = None
                    return result
            finally:
                if response is not None:
                    self._discard(response)
        raise RuntimeError(f"too many redirects: {url}")

    def download(
        self, url: str, headers: dict[str, str], destination: Path,
        max_bytes: int, stop_event: threading.Event, compute_hash: bool = True,
        preview_validator: Callable[[dict[str, str], bytes], str | None] | None = None,
        *, attempt_context_factory=None,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            response = None
            try:
                if stop_event.is_set():
                    raise Stopped
                with _cancel_scope(self, stop_event), _origin_attempt(attempt_context_factory, current_url) as progress:
                    response = self._pool_for(current_url).request(
                        "GET", current_url, headers=headers, preload_content=False,
                        redirect=False, retries=False, timeout=self.timeout,
                        pool_timeout=max(1.0, float(self.timeout.connect_timeout)),
                    )
                    status = int(response.status)
                    copied_headers = _copy_headers(response.headers.items())
                    _raise_live_service_status(status, copied_headers, current_url, self.name)
                    progress(status, copied_headers, current_url)
                    if status in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location")
                        if location:
                            self._discard(response)
                            response = None
                            current_url = _redirect_destination(current_url, str(location), headers, redirect_validator)
                            continue
                    announced = response.headers.get("Content-Length")
                    if announced and str(announced).isdigit() and int(announced) > max_bytes:
                        raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                    append = _validate_range(
                        status, response.headers, headers,
                        destination.stat().st_size if destination.exists() else 0,
                    )
                    def chunks():
                        # read1 returns available decoded bytes without waiting
                        # to fill a large application chunk. Small prefixes must
                        # reach disk before a later EOF/read timeout is raised.
                        while True:
                            with _cancel_io(self, lambda: response.connection.sock if response.connection else None):
                                chunk = response.read1(64 * 1024, decode_content=True)
                            if not chunk:
                                break
                            yield chunk
                    total, content_hash, preview = _write_limited(
                        chunks(),
                        destination, max_bytes, stop_event,
                        preview_bytes=(64 * 1024 if preview_validator is not None else 20000),
                        append=append, compute_hash=compute_hash,
                        preview_validator=preview_validator, response_headers=copied_headers,
                        progress=lambda prefix: progress(status, copied_headers, current_url, prefix),
                    )
                    _validate_range_size(status, response.headers, total)
                    result = TransportFileResponse(
                        status=status, headers=copied_headers, final_url=current_url,
                        path=destination, bytes_written=total, content_hash=content_hash,
                        preview=preview, backend=self.name, elapsed=time.monotonic() - started,
                    )
                    response.release_conn()
                    response = None
                    return result
            finally:
                if response is not None:
                    self._discard(response)
        raise RuntimeError(f"too many redirects: {url}")


class CurlBackend:
    name = "curl"

    def __init__(self, connect_timeout: float, read_timeout: float, trust_env: bool = True) -> None:
        executable = shutil.which("curl")
        if not executable:
            raise BackendUnavailable("curl executable was not found")
        self.executable = executable
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.read_timeout = max(1.0, float(read_timeout))
        self.trust_env = bool(trust_env)

    def _environment(self) -> dict[str, str]:
        excluded = {"http_proxy", "https_proxy", "all_proxy", "no_proxy", "ssl_cert_file", "ssl_cert_dir", "curl_ca_bundle"}
        return {key: value for key, value in os.environ.items()
                if self.trust_env or key.lower() not in excluded}

    def close(self) -> None:
        return

    @staticmethod
    def _parse_headers(raw: str) -> dict[str, str]:
        blocks = [block for block in raw.replace("\r\n", "\n").split("\n\n") if block.strip()]
        block = blocks[-1] if blocks else raw
        headers: dict[str, str] = {}
        for line in block.splitlines()[1:]:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            clean_key = key.strip()
            clean_value = value.strip()
            headers[clean_key] = clean_value
            headers[clean_key.casefold()] = clean_value
        return headers

    @classmethod
    def _origin_headers(cls, header_path: Path) -> tuple[int, dict[str, str]] | None:
        """Read only a complete final-origin header block, never proxy/interim headers."""
        try:
            raw = header_path.read_text(encoding="iso-8859-1", errors="replace").replace("\r\n", "\n")
        except FileNotFoundError:
            return None
        blocks = raw.split("\n\n")[:-1]
        for block in reversed(blocks):
            first = block.split("\n", 1)[0]
            match = re.match(r"HTTP/\S+\s+(\d{3})(?:\s+(.*))?$", first)
            if not match:
                continue
            status = int(match.group(1))
            if status < 200 or "connection established" in (match.group(2) or "").casefold():
                continue
            return status, cls._parse_headers(block)
        return None

    @staticmethod
    def _stop_process(proc) -> None:
        if proc.poll() is None:
            proc.kill()
        proc.communicate(timeout=5)

    @staticmethod
    def _attempt(factory):
        return factory() if factory is not None else contextlib.nullcontext()

    def _run_single(
        self, url: str, headers: dict[str, str], body_path: Path, header_path: Path,
        max_bytes: int, stop_event: threading.Event, *, attempt_context_factory=None,
        preview_validator=None,
    ) -> tuple[int, dict[str, str], str]:
        command = [
            self.executable, "--disable", "--http1.1", "--silent", "--show-error", "--no-buffer",
            "--connect-timeout", str(int(self.connect_timeout)),
            "--max-time", str(int(self.connect_timeout + self.read_timeout)),
            "--max-filesize", str(int(max_bytes)), "--dump-header", str(header_path),
            "--output", str(body_path), "--write-out", "%{http_code}\n%{url_effective}",
        ]
        # Compression is safe for full requests. Range callers already force
        # identity encoding in HttpClient and that explicit header wins.
        command.append("--compressed")
        for key, value in headers.items():
            command.extend(["--header", f"{key}: {value}"])
        command.extend(["--", url])
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        with _origin_attempt(attempt_context_factory, url) as progress:
            if stop_event.is_set():
                raise Stopped
            proc = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                creationflags=creationflags, env=self._environment(),
            )
            preview_checked = False
            progress_checked = False
            try:
                while True:
                    if stop_event.is_set():
                        raise Stopped
                    origin = self._origin_headers(header_path)
                    if origin is not None:
                        status, response_headers = origin
                        _raise_live_service_status(status, response_headers, url, self.name)
                        progress(status, response_headers, url)
                        if status in {301, 302, 303, 307, 308} and response_headers.get("location"):
                            self._stop_process(proc)
                            return status, response_headers, url
                        if (not preview_checked and preview_validator is not None
                                and status == 200 and body_path.exists()
                                and body_path.stat().st_size >= 8192):
                            with body_path.open("rb") as handle:
                                rejected = _validate_preview(preview_validator, response_headers, handle.read(64 * 1024))
                            preview_checked = True
                            if rejected:
                                raise PreviewRejected(str(rejected))
                        if (not progress_checked and status == 200 and body_path.exists()
                                and body_path.stat().st_size >= 8192
                                and (preview_validator is None or preview_checked)):
                            with body_path.open("rb") as handle:
                                progress(status, response_headers, url, handle.read(8192))
                            progress_checked = True
                    if proc.poll() is not None:
                        break
                    stop_event.wait(0.05)
                stdout, stderr = proc.communicate()
            except BaseException:
                self._stop_process(proc)
                raise
            # The process may finish between the last poll and header read.
            origin = self._origin_headers(header_path)
            if origin is not None:
                _raise_live_service_status(origin[0], origin[1], url, self.name)
            if proc.returncode != 0:
                message = (stderr or stdout or f"curl exited {proc.returncode}").strip()
                if proc.returncode == 28:
                    raise TimeoutError(message)
                if proc.returncode == 23:
                    raise LocalStorageError(message)
                if proc.returncode in {18, 56} and self._origin_headers(header_path) is not None:
                    raise httpx.RemoteProtocolError(message)
                raise OSError(message)
        lines = stdout.splitlines()
        status = int(lines[-2]) if len(lines) >= 2 and lines[-2].isdigit() else 0
        final_url = lines[-1] if lines else url
        raw_headers = header_path.read_text(encoding="iso-8859-1", errors="replace") if header_path.exists() else ""
        return status, self._parse_headers(raw_headers), final_url

    def request(
        self, url: str, headers: dict[str, str], max_bytes: int,
        stop_event: threading.Event, *, attempt_context_factory=None,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        with tempfile.TemporaryDirectory(prefix="archive-scout-curl-") as temp_dir:
            temp = Path(temp_dir)
            for hop in range(11):
                body_path = temp / f"body-{hop}.bin"
                header_path = temp / f"headers-{hop}.txt"
                status, response_headers, final_url = self._run_single(
                    current_url, headers, body_path, header_path, max_bytes, stop_event,
                    attempt_context_factory=attempt_context_factory,
                )
                _raise_live_service_status(status, response_headers, final_url or current_url, self.name)
                if status in {301, 302, 303, 307, 308} and response_headers.get("location"):
                    current_url = _redirect_destination(current_url, response_headers["location"], headers, redirect_validator)
                    continue
                data = body_path.read_bytes() if body_path.exists() else b""
                if len(data) > max_bytes:
                    raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                return TransportResponse(
                    status=status, headers=response_headers, final_url=final_url or current_url,
                    data=data, backend=self.name, elapsed=time.monotonic() - started,
                )
        raise RuntimeError(f"too many redirects: {url}")

    def download(
        self, url: str, headers: dict[str, str], destination: Path,
        max_bytes: int, stop_event: threading.Event, compute_hash: bool = True,
        preview_validator: Callable[[dict[str, str], bytes], str | None] | None = None,
        *, attempt_context_factory=None,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        destination.parent.mkdir(parents=True, exist_ok=True)
        existing_size = destination.stat().st_size if destination.exists() else 0
        current_url = url
        with tempfile.TemporaryDirectory(prefix="archive-scout-curl-") as temp_dir:
            temp = Path(temp_dir)
            for hop in range(11):
                body_path = temp / f"body-{hop}.bin"
                header_path = temp / f"headers-{hop}.txt"
                try:
                    status, response_headers, final_url = self._run_single(
                        current_url, headers, body_path, header_path, max_bytes, stop_event,
                        attempt_context_factory=attempt_context_factory,
                        preview_validator=preview_validator,
                    )
                except (OSError, httpx.RemoteProtocolError, Stopped) as failure:
                    # Preserve a validated identity prefix before the temporary
                    # curl directory is removed. No HTTP error/redirect body may
                    # become Range state, and a resumed response must match it.
                    if is_local_storage_error(failure):
                        raise
                    origin = self._origin_headers(header_path)
                    if (origin and origin[0] in {200, 206} and body_path.exists()
                            and body_path.stat().st_size > 0):
                        status, response_headers = origin
                        encoding = response_headers.get("content-encoding", "identity").casefold()
                        if encoding in {"", "identity"}:
                            append = _validate_range(status, response_headers, headers, existing_size)
                            with body_path.open("rb") as handle:
                                def partial_chunks():
                                    while chunk := handle.read(64 * 1024):
                                        yield chunk
                                _write_limited(
                                    partial_chunks(), destination, max_bytes, threading.Event(),
                                    append=append, compute_hash=False,
                                    preview_validator=preview_validator,
                                    response_headers=response_headers,
                                )
                    raise
                _raise_live_service_status(status, response_headers, final_url or current_url, self.name)
                if status in {301, 302, 303, 307, 308} and response_headers.get("location"):
                    current_url = _redirect_destination(current_url, response_headers["location"], headers, redirect_validator)
                    continue
                append = _validate_range(status, response_headers, headers, existing_size)
                with body_path.open("rb") as handle:
                    def chunks():
                        while True:
                            chunk = handle.read(1024 * 1024)
                            if not chunk:
                                break
                            yield chunk
                    total, content_hash, preview = _write_limited(
                        chunks(), destination, max_bytes, stop_event,
                        preview_bytes=(64 * 1024 if preview_validator is not None else 20000),
                        append=append, compute_hash=compute_hash,
                        preview_validator=preview_validator, response_headers=response_headers,
                    )
                _validate_range_size(status, response_headers, total)
                return TransportFileResponse(
                    status=status, headers=response_headers, final_url=final_url or current_url,
                    path=destination, bytes_written=total, content_hash=content_hash,
                    preview=preview, backend=self.name, elapsed=time.monotonic() - started,
                )
        raise RuntimeError(f"too many redirects: {url}")


@dataclass
class _BackendHealth:
    cooldown_until: dict[str, float] = field(default_factory=dict)
    last_success: str | None = None
    probing: set[str] = field(default_factory=set)


class ResilientTransport:
    """Persistent multi-backend HTTP transport.

    Auto mode prefers httpx because it honors operating-system proxy settings,
    falls back to urllib3 for a second independent Python stack, and finally uses
    the operating system's curl implementation when available. Network failures
    temporarily cool down only the failing backend; HTTP status responses are
    returned to the caller so Wayback-specific retry policy remains centralized.
    """

    def __init__(
        self,
        *,
        pool_size: int,
        connect_timeout: float,
        read_timeout: float,
        mode: str = "auto",
        trust_env: bool = True,
        callback: Callable[[str], None] | None = None,
    ) -> None:
        requested = mode.strip().casefold() or "auto"
        if requested not in {"auto", "httpx", "urllib3", "curl"}:
            raise ValueError("network backend must be auto, httpx, urllib3, or curl")
        self.callback = callback
        self.attempt_context_factory = None
        self._probe_local = threading.local()
        self.lock = threading.Lock()
        self.cooldown_until: dict[str, float] = {}
        self.last_success: str | None = None
        factories = {
            "httpx": lambda: HttpxBackend(pool_size, connect_timeout, read_timeout, trust_env=trust_env),
            "urllib3": lambda: Urllib3Backend(pool_size, connect_timeout, read_timeout, trust_env=trust_env),
            "curl": lambda: CurlBackend(connect_timeout, read_timeout, trust_env=trust_env),
        }
        self._factories = factories
        self._active = {}
        self._renew_pending = set()
        self._failure_streak = {}
        self._generations = {}
        self.backends: dict[str, object] = {}
        # A broken optional backend must not prevent the selected one starting.
        # In auto mode an unavailable SOCKS extra, for example, need not prevent
        # curl from using the user's configured SOCKS proxy.
        for name in factories if requested == "auto" else (requested,):
            try:
                self.backends[name] = factories[name]()
            except Exception as exc:
                if requested != "auto":
                    raise BackendUnavailable(f"{name} initialization failed ({type(exc).__name__}); check network settings") from exc
                if callback:
                    callback(f"Network backend {name} unavailable ({type(exc).__name__}); checking remaining backends")
        self.order = list(self.backends)
        if not self.backends:
            raise BackendUnavailable("No network backend could start; check proxy and certificate settings")


    def set_attempt_context_factory(self, factory) -> None:
        """Install a context factory invoked for every actual backend/hop attempt."""
        self.attempt_context_factory = factory

    @contextlib.contextmanager
    def recovery_probe(self, url: str, enabled: bool = True):
        """Let one host-gate-admitted probe test cooled connection methods.

        Backend penalties describe earlier connection failures, not server
        deadlines. The shared host gate has already enforced those deadlines
        before this scope is entered. Penalties remain intact until real I/O
        succeeds, and unrelated workers/origins keep their normal eligibility.
        """
        with self.lock:
            if not hasattr(self, "_probe_local"):
                self._probe_local = threading.local()
        previous = getattr(self._probe_local, "origin", "")
        self._probe_local.origin = urllib.parse.urlsplit(url).netloc.casefold() if enabled else ""
        try:
            yield
        finally:
            self._probe_local.origin = previous

    def _is_recovery_probe(self, url: str) -> bool:
        origin = urllib.parse.urlsplit(url).netloc.casefold()
        return bool(origin and getattr(getattr(self, "_probe_local", None), "origin", "") == origin)

    def backend_ready(self, url: str) -> bool:
        """Whether an ordinary retry can use a backend without a local wait."""
        with self.lock:
            state = self._health_locked(url)
            now = time.monotonic()
            return any(state.cooldown_until.get(name, 0.0) <= now and name not in state.probing
                       for name in self.order)

    def _request_backend(self, backend, url, headers, max_bytes, stop_event, redirect_validator=None):
        try:
            return backend.request(
                url, headers, max_bytes, stop_event,
                attempt_context_factory=getattr(self, "attempt_context_factory", None),
                redirect_validator=redirect_validator,
            )
        except TypeError as exc:
            if "attempt_context_factory" not in str(exc) and "redirect_validator" not in str(exc):
                raise
            factory = getattr(self, "attempt_context_factory", None)
            with (factory() if factory is not None else contextlib.nullcontext()):
                return backend.request(url, headers, max_bytes, stop_event)

    def _download_backend(self, backend, url, headers, destination, max_bytes, stop_event, compute_hash, preview_validator, redirect_validator=None):
        try:
            return backend.download(
                url, headers, destination, max_bytes, stop_event,
                compute_hash=compute_hash, preview_validator=preview_validator,
                attempt_context_factory=getattr(self, "attempt_context_factory", None),
                redirect_validator=redirect_validator,
            )
        except TypeError as exc:
            if "attempt_context_factory" not in str(exc) and "redirect_validator" not in str(exc):
                raise
            factory = getattr(self, "attempt_context_factory", None)
            with (factory() if factory is not None else contextlib.nullcontext()):
                return backend.download(
                    url, headers, destination, max_bytes, stop_event,
                    compute_hash=compute_hash, preview_validator=preview_validator,
                )

    @property
    def backend_names(self) -> tuple[str, ...]:
        return tuple(self.order)

    def close(self) -> None:
        for backend in self.backends.values():
            backend.close()

    def _health_locked(self, url: str) -> _BackendHealth:
        # Proxy/TLS policy is fixed for this transport. Origins must not share
        # backend penalties (an allowed external redirect can have other needs).
        key = urllib.parse.urlsplit(url).netloc.casefold()
        if not hasattr(self, "_health_states"):
            self._health_states = {}
        if key not in self._health_states:
            self._health_states[key] = _BackendHealth()
        return self._health_states[key]

    def _claim_backend(self, url: str, name: str) -> bool:
        with self.lock:
            state = self._health_locked(url)
            if name in state.probing or (state.cooldown_until.get(name, 0.0) > time.monotonic()
                                         and not self._is_recovery_probe(url)):
                return False
            if name in state.cooldown_until:
                state.probing.add(name)
            if hasattr(self, "_active"):
                self._active[name] = self._active.get(name, 0) + 1
            return True

    def _release_backend(self, url: str, name: str) -> None:
        retired = None
        with self.lock:
            self._health_locked(url).probing.discard(name)
            if hasattr(self, '_active'):
                self._active[name] = max(0, self._active.get(name, 1) - 1)
                if name in self._renew_pending and self._active[name] == 0:
                    # No older request may still own a response or .part path.
                    # Keep origin/service cooldowns unchanged when renewing pools.
                    try:
                        replacement = self._factories[name]()
                    except Exception:
                        replacement = None
                    if replacement is not None:
                        retired, self.backends[name] = self.backends[name], replacement
                        self._generations[name] = self._generations.get(name, 0) + 1
                    self._renew_pending.discard(name)
                    self._failure_streak[name] = 0
        if retired is not None:
            retired.close()
            if self.callback:
                self.callback(f'Network backend {name}: renewed drained connection pool')

    def metrics_snapshot(self):
        with self.lock:
            return {'pool_renewals': sum(getattr(self, '_generations', {}).values()),
                    'backend_generations': dict(getattr(self, '_generations', {})),
                    'active_backend_requests': sum(getattr(self, '_active', {}).values())}

    def _backend_succeeded(self, url: str, name: str) -> None:
        with self.lock:
            state = self._health_locked(url)
            previous = state.last_success
            if (previous is None or self.order.index(name) <= self.order.index(previous)
                    or state.cooldown_until.get(previous, 0.0) > time.monotonic()):
                state.last_success = name
            changed = previous != state.last_success
            state.cooldown_until.pop(name, None)
            if hasattr(self, "_failure_streak"):
                self._failure_streak[name] = 0
                self._renew_pending.discard(name)
            self.last_success = state.last_success  # Legacy diagnostic surface.
        if changed and self.callback:
            self.callback(f"Network backend: {self.last_success}")

    def _backend_failed(self, url: str, name: str, exc: BaseException) -> None:
        actual_url = str(getattr(exc, "request_url", url))
        with self.lock:
            state = self._health_locked(actual_url)
            if is_response_failure(exc):
                state.cooldown_until.pop(name, None)
            else:
                state.cooldown_until[name] = time.monotonic() + 30.0
            if hasattr(self, "_failure_streak") and (is_transport_connection_failure(exc) or isinstance(exc, httpx.RemoteProtocolError)):
                self._failure_streak[name] = self._failure_streak.get(name, 0) + 1
                if self._failure_streak[name] >= 2:
                    self._renew_pending.add(name)

    def _ordered_names(self, url: str = "") -> list[str]:
        now = time.monotonic()
        with self.lock:
            state = self._health_locked(url)
            preferred = state.last_success
            available = [name for name in self.order
                         if (state.cooldown_until.get(name, 0.0) <= now or self._is_recovery_probe(url))
                         and name not in state.probing]
            eligible_at = min((state.cooldown_until.get(name, now) for name in self.order), default=now)
            recovered = [name for name in available if name in state.cooldown_until]
        if not available:
            raise BackendsCoolingDown(max(0.05, eligible_at - now))
        if preferred in available:
            available.remove(preferred)
            available.insert(0, preferred)
            # One real queued request requalifies a recovered higher-priority
            # pooled backend. Other workers keep the functioning fallback.
            for name in recovered:
                if self.order.index(name) < self.order.index(preferred):
                    available.remove(name)
                    available.insert(0, name)
                    break
        return available

    def request(
        self,
        url: str,
        headers: dict[str, str],
        max_bytes: int,
        stop_event: threading.Event,
        *,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportResponse:
        failures: list[tuple[str, BaseException]] = []
        for name in self._ordered_names(url):
            if stop_event.is_set():
                raise Stopped
            if not self._claim_backend(url, name):
                continue
            backend = self.backends[name]
            try:
                response = self._request_backend(backend, url, headers, max_bytes, stop_event, redirect_validator)
                self._backend_succeeded(url, name)
                return response
            except Stopped:
                raise
            except (ServiceStatusResponse, RedirectPolicyError, BackendsCoolingDown, TextDecodingError, PayloadValidationError):
                raise
            except RuntimeError as exc:
                # Size limits and other deterministic local validation failures
                # must not be retried using another backend.
                if isinstance(exc, (InvalidRangeResponse, PreviewRejected, RequestAdmissionRejected)) or str(exc).startswith("response exceeds") or "too many redirects" in str(exc):
                    raise
                failures.append((name, exc))
                self._backend_failed(url, name, exc)
            except Exception as exc:
                if is_local_storage_error(exc):
                    raise
                failures.append((name, exc))
                self._backend_failed(url, name, exc)
            finally:
                self._release_backend(url, name)
            last_error = failures[-1][1]
            if (is_response_failure(last_error)
                    or urllib.parse.urlsplit(getattr(last_error, "request_url", url)).netloc.casefold()
                        != urllib.parse.urlsplit(url).netloc.casefold()):
                # Once a server has accepted the connection and stalled while
                # returning a CDX body, changing Python HTTP stacks normally
                # repeats the same long wait. Let the indexer retry or requeue
                # the page instead of multiplying one timeout by every backend.
                if self.callback:
                    self.callback(f"Network backend {name}: {type(last_error).__name__}; retrying this response without disabling the connection pool…")
                break
            if self.callback:
                if is_transport_connection_failure(last_error):
                    message = f"Network backend {name} failed during connection setup; trying another connection method…"
                elif is_transport_timeout(last_error):
                    message = f"Network backend {name} timed out; trying another connection method…"
                else:
                    message = f"Network backend {name} failed with {type(last_error).__name__}; trying another connection method…"
                self.callback(message)
        if not failures:
            raise BackendsCoolingDown(0.05)
        raise TransportExhaustedError(url, failures)

    def download(
        self,
        url: str,
        headers: dict[str, str],
        destination: Path,
        max_bytes: int,
        stop_event: threading.Event,
        compute_hash: bool = True,
        preview_validator: Callable[[dict[str, str], bytes], str | None] | None = None,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportFileResponse:
        failures: list[tuple[str, BaseException]] = []
        names = self._ordered_names(url)
        # Python streaming remains preferred by the backend order. Curl still
        # applies the same bounded validator after its file-oriented transfer;
        # genuine connection failures must not remove that final fallback.
        for name in names:
            if stop_event.is_set():
                raise Stopped
            if not self._claim_backend(url, name):
                continue
            backend = self.backends[name]
            try:
                response = self._download_backend(
                    backend, url, headers, destination, max_bytes, stop_event,
                    compute_hash, preview_validator, redirect_validator,
                )
                self._backend_succeeded(url, name)
                return response
            except Stopped:
                raise
            except (ServiceStatusResponse, RedirectPolicyError, BackendsCoolingDown, TextDecodingError, PayloadValidationError):
                raise
            except RuntimeError as exc:
                if isinstance(exc, (InvalidRangeResponse, PreviewRejected, RequestAdmissionRejected)) or str(exc).startswith("response exceeds") or "too many redirects" in str(exc):
                    raise
                failures.append((name, exc))
                self._backend_failed(url, name, exc)
            except Exception as exc:
                if is_local_storage_error(exc):
                    raise
                failures.append((name, exc))
                self._backend_failed(url, name, exc)
            finally:
                self._release_backend(url, name)
            # A failure can extend a valid .part prefix. Never send the stale
            # Range header to another backend: the client must calculate the
            # new offset for its next bounded retry.
            if destination.exists() and destination.stat().st_size:
                break
            last_error = failures[-1][1]
            if (is_response_failure(last_error)
                    or urllib.parse.urlsplit(getattr(last_error, "request_url", url)).netloc.casefold()
                        != urllib.parse.urlsplit(url).netloc.casefold()):
                if self.callback:
                    self.callback(
                        f"Network backend {name}: {type(last_error).__name__}; "
                        "retrying this response without disabling the connection pool…"
                    )
                break
            if self.callback:
                if is_transport_connection_failure(last_error):
                    message = f"Network backend {name} failed during connection setup; trying another connection method…"
                elif is_transport_timeout(last_error):
                    message = f"Network backend {name} timed out; trying another connection method…"
                else:
                    message = f"Network backend {name} failed with {type(last_error).__name__}; trying another connection method…"
                self.callback(message)
        if not failures:
            raise BackendsCoolingDown(0.05)
        raise TransportExhaustedError(url, failures)
