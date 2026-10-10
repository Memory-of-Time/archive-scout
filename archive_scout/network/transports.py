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
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import httpx
import urllib3

from ..events import Stopped
from ..text_encoding import TextDecodingError
from ..runtime import ensure_frozen_bundle_available

try:
    import truststore
except ImportError:  # pragma: no cover - exercised in minimal source installs
    truststore = None


class LocalStorageError(OSError):
    """A transport reported a failure writing its local response file."""


class PayloadValidationError(RuntimeError):
    """A response validator failed; keep the healthy transport eligible."""


def _validate_preview(validator, headers, data):
    try:
        return validator(headers, data)
    except (TextDecodingError, PreviewRejected):
        raise
    except Exception as exc:
        raise PayloadValidationError(f"Payload validator failed ({type(exc).__name__})") from exc


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



class RedirectPolicyError(RuntimeError):
    """A redirect was rejected before contacting the destination."""

    def __init__(self, source: str, destination: str, category: str = "external_redirect_blocked") -> None:
        self.source = str(source)
        self.destination = str(destination)
        self.category = str(category)
        self.status = None
        super().__init__(f"{self.category}: {self.source} -> {self.destination}")


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


class BackendUnavailable(RuntimeError):
    pass


class RequestAdmissionRejected(RuntimeError):
    """A shared service gate invalidated this attempt before bytes were sent."""


class InvalidRangeResponse(RuntimeError):
    """Replay cannot safely be appended; retry the complete representation."""


class PreviewRejected(RuntimeError):
    """Bounded replay prefix proved the payload belongs outside text capture."""

    def __init__(self, classification: str) -> None:
        self.classification = str(classification or "unknown")
        super().__init__(f"replay prefix classified as {self.classification}")


class TransportExhaustedError(RuntimeError):
    def __init__(self, url: str, failures: list[tuple[str, BaseException]]) -> None:
        self.url = url
        self.failures = failures
        self.timed_out = any(is_transport_timeout(exc) for _, exc in failures)
        self.read_timed_out = any(is_transport_read_timeout(exc) for _, exc in failures)
        self.connection_failed = bool(failures) and all(
            is_transport_connection_failure(exc) for _, exc in failures
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


def _read_limited(chunks: Iterable[bytes], max_bytes: int, stop_event: threading.Event) -> bytearray:
    data = bytearray()
    for chunk in chunks:
        if stop_event.is_set():
            raise Stopped
        if not chunk:
            continue
        data.extend(chunk)
        if len(data) > max_bytes:
            raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
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


class HttpxBackend:
    name = "httpx"

    def __init__(self, pool_size: int, connect_timeout: float, read_timeout: float, trust_env: bool = True) -> None:
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.read_timeout = max(1.0, float(read_timeout))
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

    def close(self) -> None:
        self.client.close()

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
        redirect_validator=None,
    ) -> TransportResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            if stop_event.is_set():
                raise Stopped
            with self._attempt(attempt_context_factory):
                with self.client.stream("GET", current_url, headers=headers, follow_redirects=False) as response:
                    status = int(response.status_code)
                    if status in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location")
                        if location:
                            current_url = _redirect_destination(current_url, str(location), headers, redirect_validator)
                            continue
                    announced = response.headers.get("Content-Length")
                    if announced and announced.isdigit() and int(announced) > max_bytes:
                        raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                    data = _read_limited(response.iter_bytes(1024 * 1024), max_bytes, stop_event)
                    return TransportResponse(
                        status=status,
                        headers=_copy_headers(response.headers.items()),
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
        redirect_validator=None,
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            if stop_event.is_set():
                raise Stopped
            with self._attempt(attempt_context_factory):
                with self.client.stream("GET", current_url, headers=headers, follow_redirects=False) as response:
                    status = int(response.status_code)
                    if status in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location")
                        if location:
                            current_url = _redirect_destination(current_url, str(location), headers, redirect_validator)
                            continue
                    announced = response.headers.get("Content-Length")
                    if announced and announced.isdigit() and int(announced) > max_bytes:
                        raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                    copied_headers = _copy_headers(response.headers.items())
                    append = _validate_range(
                        status, response.headers, headers,
                        destination.stat().st_size if destination.exists() else 0,
                    )
                    chunk_size = 64 * 1024 if preview_validator is not None else 1024 * 1024
                    total, content_hash, preview = _write_limited(
                        response.iter_bytes(chunk_size), destination, max_bytes, stop_event,
                        preview_bytes=(64 * 1024 if preview_validator is not None else 20000),
                        append=append, compute_hash=compute_hash,
                        preview_validator=preview_validator, response_headers=copied_headers,
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
        self.timeout = urllib3.Timeout(connect=max(1.0, connect_timeout), read=max(1.0, read_timeout))
        self.pool_options = dict(
            num_pools=4,
            maxsize=max(2, int(pool_size)),
            block=True,
            ssl_context=_ssl_context(trust_env),
            retries=False,
        )
        self.pool = urllib3.PoolManager(**self.pool_options)
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
            return self.proxy_pools[proxy]

    def close(self) -> None:
        self.pool.clear()
        for pool in self.proxy_pools.values():
            pool.clear()

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
    ) -> TransportResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            response = None
            try:
                if stop_event.is_set():
                    raise Stopped
                with self._attempt(attempt_context_factory):
                    response = self._pool_for(current_url).request(
                        "GET", current_url, headers=headers, preload_content=False,
                        redirect=False, retries=False, timeout=self.timeout,
                    )
                    status = int(response.status)
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
                    data = _read_limited(response.stream(amt=1024 * 1024, decode_content=True), max_bytes, stop_event)
                    result = TransportResponse(
                        status=status, headers=_copy_headers(response.headers.items()),
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
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            response = None
            try:
                if stop_event.is_set():
                    raise Stopped
                with self._attempt(attempt_context_factory):
                    response = self._pool_for(current_url).request(
                        "GET", current_url, headers=headers, preload_content=False,
                        redirect=False, retries=False, timeout=self.timeout,
                    )
                    status = int(response.status)
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
                    copied_headers = _copy_headers(response.headers.items())
                    append = _validate_range(
                        status, response.headers, headers,
                        destination.stat().st_size if destination.exists() else 0,
                    )
                    chunk_size = 64 * 1024 if preview_validator is not None else 1024 * 1024
                    total, content_hash, preview = _write_limited(
                        response.stream(amt=chunk_size, decode_content=True),
                        destination, max_bytes, stop_event,
                        preview_bytes=(64 * 1024 if preview_validator is not None else 20000),
                        append=append, compute_hash=compute_hash,
                        preview_validator=preview_validator, response_headers=copied_headers,
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

    @staticmethod
    def _attempt(factory):
        return factory() if factory is not None else contextlib.nullcontext()

    def _run_single(
        self, url: str, headers: dict[str, str], body_path: Path, header_path: Path,
        max_bytes: int, stop_event: threading.Event, *, attempt_context_factory=None,
    ) -> tuple[int, dict[str, str], str]:
        command = [
            self.executable, "--disable", "--http1.1", "--silent", "--show-error",
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
        with self._attempt(attempt_context_factory):
            proc = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                creationflags=creationflags, env=self._environment(),
            )
            while proc.poll() is None:
                if stop_event.wait(0.2):
                    proc.kill()
                    proc.wait(timeout=5)
                    raise Stopped
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                message = (stderr or stdout or f"curl exited {proc.returncode}").strip()
                if proc.returncode == 28:
                    raise TimeoutError(message)
                raise OSError(message)
        lines = stdout.splitlines()
        status = int(lines[-2]) if len(lines) >= 2 and lines[-2].isdigit() else 0
        final_url = lines[-1] if lines else url
        raw_headers = header_path.read_text(encoding="iso-8859-1", errors="replace") if header_path.exists() else ""
        return status, self._parse_headers(raw_headers), final_url

    def request(
        self, url: str, headers: dict[str, str], max_bytes: int,
        stop_event: threading.Event, *, attempt_context_factory=None,
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
                status, response_headers, final_url = self._run_single(
                    current_url, headers, body_path, header_path, max_bytes, stop_event,
                    attempt_context_factory=attempt_context_factory,
                )
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
        self.lock = threading.Lock()
        self.cooldown_until: dict[str, float] = {}
        self.last_success: str | None = None
        self._fallback_successes = 0
        factories = {
            "httpx": lambda: HttpxBackend(pool_size, connect_timeout, read_timeout, trust_env=trust_env),
            "urllib3": lambda: Urllib3Backend(pool_size, connect_timeout, read_timeout, trust_env=trust_env),
            "curl": lambda: CurlBackend(connect_timeout, read_timeout, trust_env=trust_env),
        }
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

    def _ordered_names(self) -> list[str]:
        now = time.monotonic()
        with self.lock:
            preferred = self.last_success
            available = [name for name in self.order if self.cooldown_until.get(name, 0.0) <= now]
            # After a backend fallback, periodically try the primary pooled
            # transport on a real request. A healthy primary resumes normal
            # reuse without a separate probe request or a forced connection.
            if (getattr(self, "_fallback_successes", 0) >= 32 and self.order
                    and self.order[0] in available):
                preferred = self.order[0]
        if not available:
            # All backends are cooling down. Try all of them instead of blocking
            # forever; the caller owns retry/backoff and can save progress.
            available = list(self.order)
        if preferred in available:
            available.remove(preferred)
            available.insert(0, preferred)
        return available

    def request(
        self,
        url: str,
        headers: dict[str, str],
        max_bytes: int,
        stop_event: threading.Event,
        redirect_validator=None,
    ) -> TransportResponse:
        failures: list[tuple[str, BaseException]] = []
        for name in self._ordered_names():
            if stop_event.is_set():
                raise Stopped
            backend = self.backends[name]
            try:
                response = self._request_backend(backend, url, headers, max_bytes, stop_event, redirect_validator)
                with self.lock:
                    changed = self.last_success != name
                    self.last_success = name
                    self._fallback_successes = (0 if name == self.order[0]
                                                else min(32, getattr(self, "_fallback_successes", 0) + 1))
                    self.cooldown_until.pop(name, None)
                if changed and self.callback:
                    self.callback(f"Network backend: {name}")
                return response
            except Stopped:
                raise
            except RuntimeError as exc:
                # Size limits and other deterministic local validation failures
                # must not be retried using another backend.
                if isinstance(exc, (InvalidRangeResponse, PreviewRejected, RequestAdmissionRejected, PayloadValidationError, TextDecodingError, RedirectPolicyError)) or str(exc).startswith("response exceeds") or "too many redirects" in str(exc):
                    raise
                failures.append((name, exc))
            except Exception as exc:
                if is_local_storage_error(exc):
                    raise
                failures.append((name, exc))
            with self.lock:
                self.cooldown_until[name] = time.monotonic() + 1.0
                if name == self.order[0]:
                    self._fallback_successes = 0
            last_error = failures[-1][1]
            if is_transport_read_timeout(last_error):
                # Once a server has accepted the connection and stalled while
                # returning a CDX body, changing Python HTTP stacks normally
                # repeats the same long wait. Let the indexer retry or requeue
                # the page instead of multiplying one timeout by every backend.
                if self.callback:
                    self.callback(f"Network backend {name} reached Wayback but the response timed out; requeueing without repeating the full timeout on every backend…")
                break
            if self.callback:
                self.callback(f"Network backend {name} failed during connection setup; trying another connection method…")
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
        redirect_validator=None,
    ) -> TransportFileResponse:
        failures: list[tuple[str, BaseException]] = []
        names = self._ordered_names()
        if preview_validator is not None and len(names) > 1:
            # Curl's file-oriented fallback cannot reject a response until its
            # transfer has completed. Keep it available for ordinary media and
            # replay downloads, but prefer streaming Python transports for the
            # text-validation path so known binary payloads stop near the
            # bounded prefix instead of downloading megabytes before rejection.
            names = [name for name in names if name != "curl"] or names
        for name in names:
            if stop_event.is_set():
                raise Stopped
            backend = self.backends[name]
            try:
                response = self._download_backend(
                    backend, url, headers, destination, max_bytes, stop_event,
                    compute_hash, preview_validator, redirect_validator,
                )
                with self.lock:
                    changed = self.last_success != name
                    self.last_success = name
                    self._fallback_successes = (0 if name == self.order[0]
                                                else min(32, getattr(self, "_fallback_successes", 0) + 1))
                    self.cooldown_until.pop(name, None)
                if changed and self.callback:
                    self.callback(f"Network backend: {name}")
                return response
            except Stopped:
                raise
            except RuntimeError as exc:
                if isinstance(exc, (InvalidRangeResponse, PreviewRejected, RequestAdmissionRejected, PayloadValidationError, TextDecodingError, RedirectPolicyError)) or str(exc).startswith("response exceeds") or "too many redirects" in str(exc):
                    raise
                failures.append((name, exc))
            except Exception as exc:
                if is_local_storage_error(exc):
                    raise
                failures.append((name, exc))
            # A failure can extend a valid .part prefix. Never send the stale
            # Range header to another backend: the client must calculate the
            # new offset for its next bounded retry.
            if destination.exists() and destination.stat().st_size:
                with self.lock:
                    if name == self.order[0]:
                        self._fallback_successes = 0
                break
            with self.lock:
                self.cooldown_until[name] = time.monotonic() + 1.0
                if name == self.order[0]:
                    self._fallback_successes = 0
            last_error = failures[-1][1]
            if is_transport_read_timeout(last_error):
                if self.callback:
                    self.callback(
                        f"Network backend {name} reached Wayback but the response timed out; "
                        "requeueing without repeating the full timeout on every backend…"
                    )
                break
            if self.callback:
                self.callback(
                    f"Network backend {name} failed during connection setup; trying another connection method…"
                )
        raise TransportExhaustedError(url, failures)
