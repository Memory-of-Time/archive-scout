from __future__ import annotations

import hashlib
import json
import re
import base64
import zlib
import itertools
import os
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
from ..runtime import ensure_frozen_bundle_available

try:
    import truststore
except ImportError:  # pragma: no cover - exercised in minimal source installs
    truststore = None


class BackendUnavailable(RuntimeError):
    pass


class PermitRevoked(RuntimeError):
    """A shared host pause began while awaiting the physical network slot."""


class RedirectPolicyError(RuntimeError):
    """A redirect rejected before the destination is contacted."""

    def __init__(self, source: str, destination: str, category: str):
        self.source = source
        self.destination = destination
        self.category = category
        self.status = None
        super().__init__(f"{category}: {source} -> {destination}")


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

def _ssl_context() -> ssl.SSLContext:
    return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT) if truststore else ssl.create_default_context()


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
    preview_bytes: int = 20_000,
    preserve_on_stream_error: bool = False,
) -> tuple[int, str, bytes]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    preview = bytearray()
    total = 0
    try:
        with destination.open("wb") as handle:
            for chunk in chunks:
                if stop_event.is_set():
                    raise Stopped
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                handle.write(chunk)
                digest.update(chunk)
                if len(preview) < preview_bytes:
                    preview.extend(chunk[: preview_bytes - len(preview)])
        return total, digest.hexdigest(), bytes(preview)
    except (Stopped, httpx.HTTPError, urllib3.exceptions.HTTPError, OSError, TimeoutError):
        if not preserve_on_stream_error:
            destination.unlink(missing_ok=True)
        raise
    except Exception:
        destination.unlink(missing_ok=True)
        raise


class IncompleteRepresentationError(RuntimeError):
    """The received bytes do not establish a complete HTTP representation."""


class ContentEncodingMismatch(IncompleteRepresentationError):
    """Declared compression contradicts the raw received representation."""


class PayloadDigestMismatch(IncompleteRepresentationError):
    """A proposed header repair did not match the selected archived payload."""


def _check_full_range(status: int, headers: dict[str, str]) -> None:
    """A standalone 206 prefix must never be published as an entire document."""
    if status != 206:
        return
    value = headers.get("content-range", headers.get("Content-Range", ""))
    match = re.fullmatch(r"bytes\s+(\d+)-(\d+)/(\d+)", value.strip(), re.I)
    if not match:
        raise IncompleteRepresentationError("partial HTTP 206 without a verifiable complete range")
    start, end, total = map(int, match.groups())
    if start != 0 or end + 1 != total:
        raise IncompleteRepresentationError("HTTP 206 contains only part of the selected representation")


def _decoded_verified_chunks(chunks: Iterable[bytes], headers: dict[str, str],
                             stop_event: threading.Event, *,
                             expected_digest: str = "") -> Iterable[bytes]:
    """Bounded raw-wire decoder with gzip/deflate EOF and CRC verification.

    Libraries' convenience decoded iterators may accept an incomplete gzip
    stream with a valid HTTP Content-Length. Decode raw chunks ourselves so
    every backend uses the same framing requirements, including multiple
    concatenated gzip members. Ordinary identity bodies take the fast path.
    """
    encoding = headers.get("content-encoding", "identity").split(",", 1)[0].strip().casefold()
    if encoding not in {"", "identity", "gzip", "x-gzip", "deflate"}:
        raise IncompleteRepresentationError(f"unsupported HTTP content encoding: {encoding}")
    repair_digest = ""
    repair_hash = None
    if encoding in {"gzip", "x-gzip"}:
        iterator = iter(chunks)
        prefix = bytearray()
        while len(prefix) < 2:
            if stop_event.is_set():
                raise Stopped
            try:
                prefix.extend(next(iterator))
            except StopIteration:
                break
        chunks = itertools.chain((bytes(prefix),), iterator)
        if prefix and not prefix.startswith(b"\x1f\x8b"):
            repair_digest = _valid_cdx_digest(expected_digest)
            if not repair_digest:
                raise ContentEncodingMismatch("gzip header contradicts payload; no comparable capture digest for recovery")
            # Never guess from HTML appearance. The complete candidate must
            # match this capture's CDX payload digest before it can publish.
            repair_hash = hashlib.sha1()
            encoding = "identity"
    advertised = headers.get("content-length", "")
    expected_wire = int(advertised) if advertised.isdigit() else None
    received_wire = 0
    if encoding in {"", "identity"}:
        for chunk in chunks:
            if stop_event.is_set():
                raise Stopped
            received_wire += len(chunk)
            if repair_hash is not None:
                repair_hash.update(chunk)
            yield chunk
    else:
        gzip_mode = encoding in {"gzip", "x-gzip"}
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS if gzip_mode else zlib.MAX_WBITS)
        started = False
        for chunk in chunks:
            if stop_event.is_set():
                raise Stopped
            received_wire += len(chunk)
            pending = chunk
            while pending:
                started = True
                try:
                    expanded = decoder.decompress(pending, 64 * 1024)
                except zlib.error as exc:
                    raise IncompleteRepresentationError(f"invalid {encoding} stream: {exc}") from exc
                if expanded:
                    yield expanded
                if decoder.eof:
                    pending = decoder.unused_data
                    if pending:
                        if not gzip_mode:
                            raise IncompleteRepresentationError("unexpected bytes after deflate stream")
                        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
                    continue
                pending = decoder.unconsumed_tail
        if not started or not decoder.eof:
            raise IncompleteRepresentationError(f"incomplete {encoding} stream; end marker or checksum missing")
    if expected_wire is not None and expected_wire != received_wire:
        raise IncompleteRepresentationError(
            f"HTTP Content-Length mismatch: received {received_wire}, expected {expected_wire}")
    if repair_hash is not None:
        actual = base64.b32encode(repair_hash.digest()).decode("ascii").rstrip("=")
        if actual != repair_digest:
            raise PayloadDigestMismatch("encoding-header recovery failed the selected capture's CDX digest")
        headers["x-archive-scout-encoding-recovery"] = "gzip header mismatch; CDX SHA-1 verified: " + actual


def _partial_metadata_path(destination: Path) -> Path:
    return destination.with_name(destination.name + ".resume.json")


def _valid_cdx_digest(value: str) -> str:
    value = str(value or "").upper().rstrip("=")
    return value if re.fullmatch(r"[A-Z2-7]{32}", value) else ""


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _save_partial_metadata(destination: Path, url: str, final_url: str,
                           headers: dict, expected_total: int) -> None:
    """Commit a resumable prefix record after a failed full transfer only."""
    size = destination.stat().st_size if destination.exists() else 0
    encoding = str(headers.get("content-encoding", "identity")).casefold()
    if not (0 < size < expected_total and final_url == url
            and encoding in {"", "identity"}):
        destination.unlink(missing_ok=True)
        return
    metadata = {
        "url": url, "final_url": final_url, "size": size, "total": expected_total,
        "etag": str(headers.get("etag") or ""),
        "last_modified": str(headers.get("last-modified") or ""),
        "sha256": _file_sha256(destination),
    }
    metadata_path = _partial_metadata_path(destination)
    staging = metadata_path.with_name(metadata_path.name + ".tmp")
    try:
        staging.write_text(json.dumps(metadata, separators=(",", ":")), encoding="utf-8")
        os.replace(staging, metadata_path)
    finally:
        staging.unlink(missing_ok=True)


def _discard_partial(destination: Path) -> None:
    destination.unlink(missing_ok=True)
    _partial_metadata_path(destination).unlink(missing_ok=True)


def _resume_partial_httpx(backend, url: str, headers: dict, destination: Path,
                          max_bytes: int, stop_event: threading.Event,
                          expected_digest: str = "") -> TransportFileResponse | None:
    """Failure-only Range continuation. Never append if the identity is uncertain.

    Return None to start a fresh full request. Preserve the prefix on temporary
    transfer failures. All accepted 206 bodies match the saved offset and total.
    """
    meta_path = _partial_metadata_path(destination)
    if not meta_path.is_file() or not destination.is_file():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        size = destination.stat().st_size
        total = int(meta["total"])
        if (meta["url"] != url or meta["final_url"] != url or
                size != int(meta["size"]) or not 0 < size < total <= max_bytes or
                _file_sha256(destination) != meta["sha256"]):
            _discard_partial(destination)
            return None
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        _discard_partial(destination)
        return None
    digest_expected = _valid_cdx_digest(expected_digest)
    etag = str(meta.get("etag") or "")
    strong_etag = etag if etag and not etag.startswith("W/") else ""
    if not strong_etag and not digest_expected:
        # No reliable identity validator; repeating a complete request is safer.
        _discard_partial(destination)
        return None
    request_headers = dict(headers)
    request_headers["Range"] = f"bytes={size}-"
    request_headers["Accept-Encoding"] = "identity"
    if strong_etag:
        request_headers["If-Range"] = strong_etag
    started = time.monotonic()
    with backend.client.stream("GET", url, headers=request_headers, follow_redirects=False) as response:
        status = int(response.status_code)
        if status != 206:
            if status in {429, 503}:
                raise RuntimeError(f"Range recovery returned HTTP {status}; prefix preserved")
            _discard_partial(destination)
            if status != 200 or str(response.url) != url:
                return None
            # Some Wayback replay endpoints ignore Range and send the entire
            # representation. Consume that same 200 response as a clean full
            # download rather than immediately issuing another GET.
            full_headers = _copy_headers(response.headers.items())
            announced = full_headers.get("content-length", "")
            if announced.isdigit() and int(announced) > max_bytes:
                raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
            try:
                written, sha256, preview = _write_limited(
                    response.iter_bytes(64 * 1024), destination, max_bytes, stop_event,
                    preserve_on_stream_error=True,
                )
            except (Stopped, httpx.HTTPError, TimeoutError, OSError):
                if announced.isdigit() and int(announced) <= max_bytes:
                    _save_partial_metadata(destination, url, url, full_headers, int(announced))
                else:
                    _discard_partial(destination)
                raise
            return TransportFileResponse(
                status=200, headers=full_headers, final_url=url, path=destination,
                bytes_written=written, content_hash=sha256, preview=preview,
                backend="httpx-full-range-ignored", elapsed=time.monotonic() - started,
            )
        fields = _copy_headers(response.headers.items())
        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", fields.get("content-range", "").strip())
        if (not match or fields.get("content-encoding", "identity").casefold() not in {"", "identity"}
                or str(response.url) != url):
            _discard_partial(destination)
            return None
        start, end, length = map(int, match.groups())
        if start != size or end + 1 != length or length != total:
            _discard_partial(destination)
            return None
        new_etag = fields.get("etag", "")
        if strong_etag and new_etag and new_etag != strong_etag:
            _discard_partial(destination)
            return None
        # Derive the complete-file digests on this exceptional path only.
        sha256 = hashlib.sha256()
        sha1 = hashlib.sha1()
        preview = bytearray()
        with destination.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                sha256.update(chunk)
                sha1.update(chunk)
                if len(preview) < 20_000:
                    preview.extend(chunk[:20_000 - len(preview)])
        written = size
        try:
            with destination.open("ab") as stream:
                for chunk in response.iter_bytes(1024 * 1024):
                    if stop_event.is_set():
                        raise Stopped
                    written += len(chunk)
                    if written > total:
                        raise ValueError("Range body exceeds the advertised total")
                    stream.write(chunk)
                    sha256.update(chunk)
                    sha1.update(chunk)
                    if len(preview) < 20_000:
                        preview.extend(chunk[:20_000 - len(preview)])
        except (Stopped, httpx.HTTPError, TimeoutError, OSError):
            # The last bytes came from a validated range of this exact body.
            # Persist the newly accumulated prefix for a later retry.
            _save_partial_metadata(destination, url, url, fields, total)
            raise
        if written != total:
            _save_partial_metadata(destination, url, url, fields, total)
            raise httpx.RemoteProtocolError("Incomplete 206 response; prefix preserved")
        if digest_expected and base64.b32encode(sha1.digest()).decode("ascii").rstrip("=") != digest_expected:
            _discard_partial(destination)
            raise ValueError("Range-completed media disagrees with indexed CDX digest; never publish")
        meta_path.unlink(missing_ok=True)
        return TransportFileResponse(
            status=200, headers=fields, final_url=url, path=destination,
            bytes_written=written, content_hash=sha256.hexdigest(),
            preview=bytes(preview), backend="httpx-range", elapsed=time.monotonic() - started,
        )


def _copy_headers(items: Iterable[tuple[object, object]]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for raw_key, raw_value in items:
        key = str(raw_key)
        value = str(raw_value)
        headers[key] = value
        headers[key.casefold()] = value
    return headers


def _pace_redirect_hop(backend, stop_event: threading.Event) -> None:
    """Each additional HTTP GET must respect the same shared physical pacer.

    The initial GET is admitted by ResilientTransport. Direct backend callers
    intentionally remain unaffected unless an explicit pacer is provided.
    """
    pacer = getattr(backend, "redirect_pacer", None)
    if pacer is not None:
        pacer.wait(stop_event)
    checker = getattr(backend, "redirect_permit_checker", None)
    if checker is not None and not checker():
        raise PermitRevoked("host permit expired before a redirected physical request")
    if stop_event.is_set():
        raise Stopped


class HttpxBackend:
    name = "httpx"

    def __init__(self, pool_size: int, connect_timeout: float, read_timeout: float, trust_env: bool = True) -> None:
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.read_timeout = max(1.0, float(read_timeout))
        self.client = httpx.Client(
            verify=_ssl_context(),
            follow_redirects=True,
            max_redirects=10,
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

    def request(
        self,
        url: str,
        headers: dict[str, str],
        max_bytes: int,
        stop_event: threading.Event,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            options = {"follow_redirects": False} if (redirect_validator is not None or (getattr(self, "redirect_pacer", None) or getattr(self, "redirect_permit_checker", None))) else {}
            with self.client.stream("GET", current_url, headers=headers, **options) as response:
                location = response.headers.get("Location")
                if response.status_code in {301,302,303,307,308} and not location and redirect_validator:
                    redirect_validator(current_url, current_url)
                    raise IncompleteRepresentationError("redirect response has no Location header")
                if response.status_code in {301, 302, 303, 307, 308} and location and (
                    redirect_validator is not None or (getattr(self, "redirect_pacer", None) or getattr(self, "redirect_permit_checker", None))
                ):
                    destination = urllib.parse.urljoin(current_url, location)
                    if redirect_validator is not None:
                        redirect_validator(current_url, destination)
                    _pace_redirect_hop(self, stop_event)
                    current_url = destination
                    continue
                if response.status_code in {429, 503}:
                    return TransportResponse(int(response.status_code),
                        _copy_headers(response.headers.items()), str(response.url), b"",
                        self.name, time.monotonic()-started)
                announced = response.headers.get("Content-Length")
                if announced and announced.isdigit() and int(announced) > max_bytes:
                    raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
                data = _read_limited(response.iter_bytes(1024 * 1024), max_bytes, stop_event)
                return TransportResponse(
                    status=int(response.status_code), headers=_copy_headers(response.headers.items()),
                    final_url=str(response.url), data=data, backend=self.name,
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
        *,
        retain_partial: bool = False,
        expected_digest: str = "",
        redirect_validator: Callable[[str, str], None] | None = None,
        _redirect_depth: int = 0,
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        if retain_partial and destination.exists() and _partial_metadata_path(destination).exists():
            resumed = _resume_partial_httpx(
                self, url, headers, destination, max_bytes, stop_event, expected_digest
            )
            if resumed is not None:
                return resumed
        started = time.monotonic()
        # Range requires byte-identity: never retain compressed representations.
        request_headers = dict(headers)
        if retain_partial:
            request_headers["Accept-Encoding"] = "identity"
        if _redirect_depth > 10:
            raise RuntimeError(f"too many redirects: {url}")
        with self.client.stream("GET", url, headers=request_headers,
                                **({"follow_redirects": False} if redirect_validator or (getattr(self, "redirect_pacer", None) or getattr(self, "redirect_permit_checker", None)) else {})) as response:
            if (redirect_validator or (getattr(self, "redirect_pacer", None) or getattr(self, "redirect_permit_checker", None))) and int(response.status_code) in {301,302,303,307,308}:
                location = response.headers.get("location")
                if not location:
                    if redirect_validator:
                        redirect_validator(url, url)
                    raise IncompleteRepresentationError("redirect response has no Location header")
                if location:
                    next_url = urllib.parse.urljoin(url, location)
                    if redirect_validator:
                        redirect_validator(url, next_url)
                    response.close()  # release pool slot before following another hop
                    _pace_redirect_hop(self, stop_event)
                    return self.download(next_url, headers, destination, max_bytes, stop_event,
                        retain_partial=False, expected_digest="", redirect_validator=redirect_validator,
                        _redirect_depth=_redirect_depth+1)
            announced = response.headers.get("Content-Length")
            if announced and announced.isdigit() and int(announced) > max_bytes:
                raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
            fields = _copy_headers(response.headers.items())
            if not retain_partial:
                _check_full_range(int(response.status_code), fields)
            # Error bodies need not be decoded before the 429/503 gate observes
            # their status. Do not turn a malformed error page into a cooldown.
            if int(response.status_code) in {429, 503}:
                destination.unlink(missing_ok=True)
                return TransportFileResponse(int(response.status_code), fields,
                    str(response.url), destination, 0, hashlib.sha256(b"").hexdigest(),
                    b"", self.name, time.monotonic()-started)
            try:
                raw_chunks = response.iter_raw(64 * 1024) if not retain_partial else response.iter_bytes(64 * 1024)
                verified_chunks = (_decoded_verified_chunks(raw_chunks, fields, stop_event, expected_digest=expected_digest)
                                   if not retain_partial else raw_chunks)
                total, content_hash, preview = _write_limited(
                    verified_chunks,
                    destination, max_bytes, stop_event,
                    preserve_on_stream_error=retain_partial,
                )
            except (Stopped, httpx.HTTPError, OSError, TimeoutError):
                announced = response.headers.get("Content-Length", "")
                if (retain_partial and int(response.status_code) == 200 and
                        announced.isdigit() and int(announced) <= max_bytes):
                    _save_partial_metadata(destination, url, str(response.url),
                                           _copy_headers(response.headers.items()), int(announced))
                else:
                    _discard_partial(destination)
                raise
            return TransportFileResponse(
                status=int(response.status_code),
                headers=fields,
                final_url=str(response.url),
                path=destination,
                bytes_written=total,
                content_hash=content_hash,
                preview=preview,
                backend=self.name,
                elapsed=time.monotonic() - started,
            )


class Urllib3Backend:
    name = "urllib3"

    def __init__(self, pool_size: int, connect_timeout: float, read_timeout: float,
                 trust_env: bool = True) -> None:
        self.timeout = urllib3.Timeout(connect=max(1.0, connect_timeout), read=max(1.0, read_timeout))
        self.trust_env = bool(trust_env)
        self.pool_size = max(2, int(pool_size))
        self.pool = urllib3.PoolManager(
            num_pools=4, maxsize=self.pool_size, block=True,
            ssl_context=_ssl_context(), retries=False,
        )
        self.proxy_pools: dict[str, object] = {}
        self.proxy_lock = threading.Lock()

    def _pool_for(self, url: str):
        if not self.trust_env:
            return self.pool
        parsed = urllib.parse.urlsplit(url)
        if parsed.hostname and urllib.request.proxy_bypass(parsed.hostname):
            return self.pool
        proxy = urllib.request.getproxies().get(parsed.scheme)
        if not proxy:
            return self.pool
        # Proxy clients are created lazily and reused; credentials are never
        # included in diagnostics. SOCKS without support must fail closed.
        with self.proxy_lock:
            if proxy not in self.proxy_pools:
                if proxy.lower().startswith(("socks4://", "socks5://", "socks5h://")):
                    from urllib3.contrib.socks import SOCKSProxyManager
                    pool = SOCKSProxyManager(proxy, num_pools=4,
                        maxsize=self.pool_size, block=True, retries=False)
                else:
                    pool = urllib3.ProxyManager(proxy, num_pools=4,
                        maxsize=self.pool_size, block=True, retries=False)
                self.proxy_pools[proxy] = pool
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
            response.drain_conn()
            response.release_conn()
        except Exception:
            try:
                response.close()
            except Exception:
                pass

    def request(
        self,
        url: str,
        headers: dict[str, str],
        max_bytes: int,
        stop_event: threading.Event,
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        response = None
        try:
            for _ in range(11):
                response = self._pool_for(current_url).request(
                    "GET",
                    current_url,
                    headers=headers,
                    preload_content=False,
                    redirect=False,
                    retries=False,
                    timeout=self.timeout,
                )
                if int(response.status) not in {301, 302, 303, 307, 308}:
                    break
                location = response.headers.get("Location")
                if not location:
                    if redirect_validator:
                        redirect_validator(current_url, current_url)
                    raise IncompleteRepresentationError("redirect response has no Location header")
                destination = urllib.parse.urljoin(current_url, location)
                if redirect_validator is not None:
                    redirect_validator(current_url, destination)
                self._discard(response)
                response = None
                _pace_redirect_hop(self, stop_event)
                current_url = destination
            else:
                raise RuntimeError(f"too many redirects: {url}")

            assert response is not None
            if int(response.status) in {429, 503}:
                fields = _copy_headers(response.headers.items())
                status = int(response.status)
                response.close()  # never drain an arbitrarily slow error body
                response.release_conn()
                response = None
                return TransportResponse(status, fields, current_url, b"",
                    self.name, time.monotonic()-started)
            announced = response.headers.get("Content-Length")
            if announced and str(announced).isdigit() and int(announced) > max_bytes:
                raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
            data = _read_limited(response.stream(amt=1024 * 1024, decode_content=True), max_bytes, stop_event)
            result = TransportResponse(
                status=int(response.status),
                headers=_copy_headers(response.headers.items()),
                final_url=current_url,
                data=data,
                backend=self.name,
                elapsed=time.monotonic() - started,
            )
            self._discard(response)
            response = None
            return result
        finally:
            if response is not None:
                self._discard(response)



    def download(
        self,
        url: str,
        headers: dict[str, str],
        destination: Path,
        max_bytes: int,
        stop_event: threading.Event,
        redirect_validator: Callable[[str, str], None] | None = None,
        *, expected_digest: str = "",
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        response = None
        try:
            for _ in range(11):
                response = self._pool_for(current_url).request(
                    "GET", current_url, headers=headers, preload_content=False,
                    redirect=False, retries=False, timeout=self.timeout,
                )
                if int(response.status) not in {301, 302, 303, 307, 308}:
                    break
                location = response.headers.get("Location")
                if not location:
                    if redirect_validator:
                        redirect_validator(current_url, current_url)
                    raise IncompleteRepresentationError("redirect response has no Location header")
                destination_url = urllib.parse.urljoin(current_url, location)
                if redirect_validator:
                    redirect_validator(current_url, destination_url)
                self._discard(response)
                response = None
                _pace_redirect_hop(self, stop_event)
                current_url = destination_url
                expected_digest = ""  # source capture evidence does not identify a different replay
            else:
                raise RuntimeError(f"too many redirects: {url}")
            assert response is not None
            announced = response.headers.get("Content-Length")
            if announced and str(announced).isdigit() and int(announced) > max_bytes:
                raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
            fields = _copy_headers(response.headers.items())
            _check_full_range(int(response.status), fields)
            if int(response.status) in {429, 503}:
                status = int(response.status)
                response.close()  # do not wait through the error body
                response.release_conn()
                response = None
                return TransportFileResponse(status, fields,
                    current_url, destination, 0, hashlib.sha256(b"").hexdigest(),
                    b"", self.name, time.monotonic()-started)
            total, content_hash, preview = _write_limited(
                _decoded_verified_chunks(response.stream(amt=64 * 1024, decode_content=False),
                                         fields, stop_event, expected_digest=expected_digest),
                destination, max_bytes, stop_event,
            )
            result = TransportFileResponse(
                status=int(response.status),
                headers=fields,
                final_url=current_url,
                path=destination,
                bytes_written=total,
                content_hash=content_hash,
                preview=preview,
                backend=self.name,
                elapsed=time.monotonic() - started,
            )
            self._discard(response)
            response = None
            return result
        finally:
            if response is not None:
                self._discard(response)


class CurlBackend:
    name = "curl"

    def __init__(self, connect_timeout: float, read_timeout: float,
                 trust_env: bool = True) -> None:
        self.trust_env = bool(trust_env)
        executable = shutil.which("curl")
        if not executable:
            raise BackendUnavailable("curl executable was not found")
        self.executable = executable
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.read_timeout = max(1.0, float(read_timeout))

    def close(self) -> None:
        return

    def _curl_environment(self) -> dict[str, str] | None:
        if self.trust_env:
            return None
        return {key: value for key, value in os.environ.items()
                if key.casefold() not in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}}

    @staticmethod
    def _early_control_status(path: Path, *, redirects: bool = False) -> tuple[int, dict[str, str]] | None:
        """Read a bounded complete header block, never a potentially huge body.

        Curl writes `--dump-header` as soon as response headers arrive. This
        permits prompt handling of HTTP 429/503 even if their body stalls.
        """
        try:
            with path.open("rb") as stream:
                prefix = stream.read(64 * 1024)
        except FileNotFoundError:
            return None
        if not prefix or len(prefix) >= 64 * 1024:
            return None
        normalized = prefix.replace(b"\r\n", b"\n")
        blocks = [block for block in normalized.split(b"\n\n")[:-1] if block.startswith(b"HTTP/")]
        if not blocks:
            return None
        last = blocks[-1].decode("iso-8859-1", "replace")
        parts = last.splitlines()[0].split()
        accepted = {"429", "503"} | ({"301", "302", "303", "307", "308"} if redirects else set())
        if len(parts) < 2 or parts[1] not in accepted:
            return None
        fields = {}
        for line in last.splitlines()[1:]:
            if ":" in line:
                key, value = line.split(":", 1)
                fields[key.strip().casefold()] = value.strip()
        return int(parts[1]), fields

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

    def request(self, url: str, headers: dict[str, str], max_bytes: int,
                stop_event: threading.Event,
                redirect_validator: Callable[[str, str], None] | None = None) -> TransportResponse:
        if redirect_validator is None and not (getattr(self, "redirect_pacer", None) or getattr(self, "redirect_permit_checker", None)):
            return self._request_once(url, headers, max_bytes, stop_event)
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            response = self._request_once(current_url, headers, max_bytes, stop_event, False)
            location = response.headers.get("location")
            if response.status in {301,302,303,307,308} and not location:
                if redirect_validator:
                    redirect_validator(current_url, current_url)
                raise IncompleteRepresentationError("redirect response has no Location header")
            if response.status not in {301, 302, 303, 307, 308} or not location:
                response.elapsed = time.monotonic() - started
                return response
            destination = urllib.parse.urljoin(current_url, location)
            if redirect_validator is not None:
                redirect_validator(current_url, destination)
            _pace_redirect_hop(self, stop_event)
            current_url = destination
        raise RuntimeError(f"too many redirects: {url}")

    def _request_once(
        self,
        url: str,
        headers: dict[str, str],
        max_bytes: int,
        stop_event: threading.Event,
        follow_redirects: bool = True,
    ) -> TransportResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        with tempfile.TemporaryDirectory(prefix="archive-scout-curl-") as temp_dir:
            header_path = Path(temp_dir) / "headers.txt"
            body_path = Path(temp_dir) / "body.bin"
            command = [
                self.executable,
                "--disable", "--location",
                "--compressed",
                "--http1.1",
                "--silent",
                "--show-error",
                "--connect-timeout",
                str(int(self.connect_timeout)),
                "--max-time",
                str(int(self.connect_timeout + self.read_timeout)),
                "--max-filesize",
                str(int(max_bytes)),
                "--dump-header",
                str(header_path),
                "--output",
                str(body_path),
                "--write-out",
                "%{http_code}\n%{url_effective}",
            ]
            if not follow_redirects:
                command.remove("--location")
            for key, value in headers.items():
                command.extend(["--header", f"{key}: {value}"])
            command.extend(["--", url])
            creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            proc = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=creationflags, env=self._curl_environment(),
            )
            early_control = None
            while proc.poll() is None:
                early_control = self._early_control_status(header_path, redirects=not follow_redirects)
                if early_control is not None:
                    proc.kill()
                    break
                if stop_event.wait(0.2):
                    proc.kill()
                    proc.wait(timeout=5)
                    raise Stopped
            stdout, stderr = proc.communicate()
            if early_control is None:
                # A short redirect can finish (or close its irrelevant body)
                # between polls. Its headers still take priority over curl's
                # transfer/decompression error for that discarded body.
                early_control = self._early_control_status(header_path, redirects=not follow_redirects)
            if early_control is not None:
                status, fields = early_control
                return TransportResponse(status, fields, url, b"", self.name,
                    time.monotonic()-started)
            if proc.returncode != 0:
                message = (stderr or stdout or f"curl exited {proc.returncode}").strip()
                if proc.returncode == 28:
                    raise TimeoutError(message)
                raise OSError(message)
            lines = stdout.splitlines()
            status = int(lines[-2]) if len(lines) >= 2 and lines[-2].isdigit() else 0
            final_url = lines[-1] if lines else url
            data = body_path.read_bytes() if body_path.exists() else b""
            if len(data) > max_bytes:
                raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
            raw_headers = header_path.read_text(encoding="iso-8859-1", errors="replace") if header_path.exists() else ""
            return TransportResponse(
                status=status,
                headers=self._parse_headers(raw_headers),
                final_url=final_url,
                data=data,
                backend=self.name,
                elapsed=time.monotonic() - started,
            )



    def download(
        self,
        url: str,
        headers: dict[str, str],
        destination: Path,
        max_bytes: int,
        stop_event: threading.Event,
        redirect_validator: Callable[[str, str], None] | None = None,
        _redirect_depth: int = 0,
        *, expected_digest: str = "",
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.unlink(missing_ok=True)
        with tempfile.TemporaryDirectory(prefix="archive-scout-curl-") as temp_dir:
            header_path = Path(temp_dir) / "headers.txt"
            raw_path = Path(temp_dir) / "raw-response.bin"
            command = [
                self.executable, "--disable", "--location", "--http1.1",
                "--silent", "--show-error", "--connect-timeout", str(int(self.connect_timeout)),
                "--max-time", str(int(self.connect_timeout + self.read_timeout)),
                "--max-filesize", str(int(max_bytes)), "--dump-header", str(header_path),
                "--output", str(raw_path), "--write-out", "%{http_code}\n%{url_effective}",
            ]
            if redirect_validator or (getattr(self, "redirect_pacer", None) or getattr(self, "redirect_permit_checker", None)):
                command.remove("--location")
            for key, value in headers.items():
                command.extend(["--header", f"{key}: {value}"])
            command.extend(["--", url])
            creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            proc = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                creationflags=creationflags, env=self._curl_environment(),
            )
            early_control = None
            while proc.poll() is None:
                early_control = self._early_control_status(header_path,
                    redirects=bool(redirect_validator or getattr(self, "redirect_pacer", None)
                                   or getattr(self, "redirect_permit_checker", None)))
                if early_control is not None:
                    proc.kill()
                    break
                if stop_event.wait(0.2):
                    proc.kill()
                    proc.wait(timeout=5)
                    destination.unlink(missing_ok=True)
                    raise Stopped
            stdout, stderr = proc.communicate()
            if early_control is None:
                early_control = self._early_control_status(header_path,
                    redirects=bool(redirect_validator or getattr(self, "redirect_pacer", None)
                                   or getattr(self, "redirect_permit_checker", None)))
            if early_control is not None:
                status, fields = early_control
                if status in {301,302,303,307,308} and not fields.get("location"):
                    if redirect_validator:
                        redirect_validator(url, url)
                    raise IncompleteRepresentationError("redirect response has no Location header")
                if status in {301, 302, 303, 307, 308} and fields.get("location"):
                    if _redirect_depth >= 10:
                        raise RuntimeError(f"too many redirects: {url}")
                    next_url = urllib.parse.urljoin(url, fields["location"])
                    if redirect_validator:
                        redirect_validator(url, next_url)
                    _pace_redirect_hop(self, stop_event)
                    return self.download(next_url, headers, destination, max_bytes, stop_event,
                        redirect_validator=redirect_validator, _redirect_depth=_redirect_depth+1,
                        expected_digest="")
                return TransportFileResponse(status, fields, url, destination, 0,
                    hashlib.sha256(b"").hexdigest(), b"", self.name,
                    time.monotonic()-started)
            if proc.returncode != 0:
                destination.unlink(missing_ok=True)
                message = (stderr or stdout or f"curl exited {proc.returncode}").strip()
                if proc.returncode == 28:
                    raise TimeoutError(message)
                raise OSError(message)
            lines = stdout.splitlines()
            status = int(lines[-2]) if len(lines) >= 2 and lines[-2].isdigit() else 0
            final_url = lines[-1] if lines else url
            raw_headers = header_path.read_text(encoding="iso-8859-1", errors="replace") if header_path.exists() else ""
            parsed_headers = self._parse_headers(raw_headers)
            location = parsed_headers.get("location")
            if (redirect_validator or (getattr(self, "redirect_pacer", None) or getattr(self, "redirect_permit_checker", None))) and status in {301,302,303,307,308} and location:
                if _redirect_depth >= 10:
                    raise RuntimeError(f"too many redirects: {url}")
                next_url = urllib.parse.urljoin(url, location)
                if redirect_validator:
                    redirect_validator(url, next_url)
                _pace_redirect_hop(self, stop_event)
                return self.download(next_url, headers, destination, max_bytes, stop_event,
                    redirect_validator=redirect_validator, _redirect_depth=_redirect_depth+1,
                    expected_digest="")
            _check_full_range(status, parsed_headers)
            if status in {429, 503}:
                return TransportFileResponse(status, parsed_headers, final_url,
                    destination, 0, hashlib.sha256(b"").hexdigest(), b"", self.name,
                    time.monotonic()-started)
            with raw_path.open("rb") as raw:
                raw_chunks = iter(lambda: raw.read(64 * 1024), b"")
                size, content_hash, preview = _write_limited(
                    _decoded_verified_chunks(raw_chunks, parsed_headers, stop_event, expected_digest=expected_digest),
                    destination, max_bytes, stop_event)
            return TransportFileResponse(
                status=status, headers=parsed_headers, final_url=final_url,
                path=destination, bytes_written=size, content_hash=content_hash,
                preview=preview, backend=self.name, elapsed=time.monotonic() - started,
            )

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
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.recovery_probes: set[str] = set()
        self.backend_waiters = 0
        self.cooldown_until: dict[str, float] = {}
        self.fallback_pacer = None
        self.last_success: str | None = None
        # Explicit backends must not instantiate unused stacks. In particular,
        # a missing optional SOCKS dependency must not break urllib3-only mode.
        factories = {
            "httpx": lambda: HttpxBackend(pool_size, connect_timeout, read_timeout, trust_env=trust_env),
            "urllib3": lambda: Urllib3Backend(pool_size, connect_timeout, read_timeout, trust_env=trust_env),
            "curl": lambda: CurlBackend(connect_timeout, read_timeout, trust_env=trust_env),
        }
        self.backends: dict[str, object] = {}
        names = ("httpx", "urllib3", "curl") if requested == "auto" else (requested,)
        for name in names:
            try:
                self.backends[name] = factories[name]()
            except (BackendUnavailable, ImportError, ValueError, RuntimeError, OSError) as exc:
                if requested != "auto":
                    raise BackendUnavailable(f"requested network backend unavailable: {name}: {exc}") from exc
                if callback:
                    callback(f"Network backend {name} unavailable during initialization; using an available backend.")
        if not self.backends:
            raise BackendUnavailable("no network backend available")
        self.order = list(self.backends)

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
        if not available:
            # At most one scheduled recovery probe when all routes have failed.
            # Never bypass every backend cooldown for every waiting capture.
            available = [min(self.order, key=lambda name: self.cooldown_until.get(name, 0))]
        if preferred in available:
            available.remove(preferred)
            available.insert(0, preferred)
        return available

    def snapshot(self) -> dict[str, object]:
        """Bounded transport health counters for stall-only diagnostics."""
        with self.condition:
            now = time.monotonic()
            return {
                "backend_waiters": self.backend_waiters,
                "cooling_backends": tuple(name for name, until in self.cooldown_until.items() if until > now),
                "cooldown_seconds": round(max((until-now for until in self.cooldown_until.values()), default=0.0), 1),
                "recovery_probes": len(self.recovery_probes),
                "preferred_backend": self.last_success,
            }

    def _await_backend(self, name: str, stop_event: threading.Event,
                       alternatives: Iterable[str] = ()) -> str:
        """One recovery probe per cooling backend, without blocking healthy peers."""
        if not hasattr(self, "condition"):
            # Compatibility with legacy injected transports made via __new__.
            self.condition = threading.Condition(self.lock)
            self.recovery_probes = set()
        while True:
            if stop_event.is_set():
                raise Stopped
            with self.condition:
                until = self.cooldown_until.get(name)
                if until is None:
                    return name
                delay = max(0.0, until - time.monotonic())
                if delay > 0 or name in self.recovery_probes:
                    ready = next((other for other in alternatives
                        if other != name and other in self.backends
                        and other not in self.recovery_probes
                        and self.cooldown_until.get(other, 0.0) <= time.monotonic()), None)
                    if ready is not None:
                        name = ready
                        continue
                    self.backend_waiters += 1
                    try:
                        self.condition.wait(timeout=min(0.5, max(0.05, delay)))
                    finally:
                        self.backend_waiters -= 1
                    continue
                self.recovery_probes.add(name)
                return name

    def _backend_result(self, name: str, succeeded: bool) -> None:
        with self.condition:
            if succeeded:
                self.cooldown_until.pop(name, None)
            else:
                self.cooldown_until[name] = time.monotonic() + 30.0
            self.recovery_probes.discard(name)
            self.condition.notify_all()

    def request(
        self,
        url: str,
        headers: dict[str, str],
        max_bytes: int,
        stop_event: threading.Event,
        redirect_validator: Callable[[str, str], None] | None = None,
        permit_checker: Callable[[], bool] | None = None,
    ) -> TransportResponse:
        failures: list[tuple[str, BaseException]] = []
        # An alternate stack is a diagnosis/recovery trial, not three new
        # physical connections for every failed logical URL attempt.
        attempted: set[str] = set()
        for attempt_number, name in enumerate(self._ordered_names()[:2]):
            if name in attempted:
                continue
            name = self._await_backend(name, stop_event,
                [route for route in self.order if route not in attempted])
            if name in attempted:
                continue
            attempted.add(name)
            try:
                pacer = getattr(self, "physical_pacer", None) or getattr(self, "fallback_pacer", None)
                if pacer is not None and (getattr(self, "physical_pacer", None) is not None or attempt_number):
                    pacer.wait(stop_event)
                if permit_checker is not None and not permit_checker():
                    raise PermitRevoked("host permit expired before the physical request")
                if stop_event.is_set():
                    raise Stopped
                backend = self.backends[name]
                backend.redirect_pacer = pacer
                backend.redirect_permit_checker = permit_checker
            except BaseException:
                # The exclusive cooling-backend probe may already have been
                # reserved by _await_backend before admission/pacing failed.
                with self.condition:
                    self.recovery_probes.discard(name)
                    self.condition.notify_all()
                raise
            try:
                options = {"redirect_validator": redirect_validator} if redirect_validator is not None else {}
                response = backend.request(url, headers, max_bytes, stop_event, **options)
                with self.lock:
                    changed = self.last_success != name
                    self.last_success = name
                self._backend_result(name, True)
                if changed and self.callback:
                    self.callback(f"Network backend: {name}")
                return response
            except RedirectPolicyError:
                self._backend_result(name, True)  # successful response, policy refused redirect
                raise
            except PermitRevoked:
                # Host generation changed between redirect hops. Do not retry
                # through another backend while the shared service is paused.
                self._backend_result(name, True)
                raise
            except Stopped:
                with self.condition:
                    self.recovery_probes.discard(name)
                    self.condition.notify_all()
                raise
            except OSError as exc:
                if exc.errno in {28, 122} or getattr(exc, "winerror", None) in {39, 112}:
                    self._backend_result(name, True)
                    raise
                failures.append((name, exc))
            except RuntimeError as exc:
                # Size limits and other deterministic local validation failures
                # must not be retried using another backend.
                if str(exc).startswith("response exceeds") or "too many redirects" in str(exc):
                    # Local response-policy refusal means the route answered.
                    # Release an exclusive recovery probe before propagating.
                    self._backend_result(name, True)
                    raise
                failures.append((name, exc))
            except Exception as exc:
                failures.append((name, exc))
            last_error = failures[-1][1]
            # A read/first-byte timeout does not establish a broken backend.
            self._backend_result(name, not is_transport_connection_failure(last_error))
            if isinstance(last_error, IncompleteRepresentationError):
                break
            if is_transport_read_timeout(last_error):
                # Once a server has accepted the connection and stalled while
                # returning a CDX body, changing Python HTTP stacks normally
                # repeats the same long wait. Let the indexer retry or requeue
                # the page instead of multiplying one timeout by every backend.
                if self.callback:
                    self.callback(f"Network backend {name} reached Wayback but the response timed out; requeueing without repeating the full timeout on every backend…")
                break
            if self.callback:
                stage = ("connection setup" if is_transport_connection_failure(last_error)
                         else "response transfer or TLS handling")
                self.callback(f"Network backend {name} failed during {stage}; checking another connection method…")
        raise TransportExhaustedError(url, failures)


    def download(
        self,
        url: str,
        headers: dict[str, str],
        destination: Path,
        max_bytes: int,
        stop_event: threading.Event,
        *,
        retain_partial: bool = False,
        expected_digest: str = "",
        redirect_validator: Callable[[str, str], None] | None = None,
        permit_checker: Callable[[], bool] | None = None,
    ) -> TransportFileResponse:
        failures: list[tuple[str, BaseException]] = []
        names = (["httpx"] if retain_partial and destination.exists() and _partial_metadata_path(destination).is_file()
                 and "httpx" in self.backends else self._ordered_names()[:2])
        attempted: set[str] = set()
        for attempt_number, name in enumerate(names):
            if name in attempted:
                continue
            alternatives = [route for route in self.order if route not in attempted]
            if retain_partial and len(names) == 1:
                alternatives = names
            name = self._await_backend(name, stop_event, alternatives)
            if name in attempted:
                continue
            attempted.add(name)
            try:
                pacer = getattr(self, "physical_pacer", None) or getattr(self, "fallback_pacer", None)
                if pacer is not None and (getattr(self, "physical_pacer", None) is not None or attempt_number):
                    pacer.wait(stop_event)
                if permit_checker is not None and not permit_checker():
                    raise PermitRevoked("host permit expired before the physical request")
                if stop_event.is_set():
                    raise Stopped
                backend = self.backends[name]
                backend.redirect_pacer = pacer
                backend.redirect_permit_checker = permit_checker
            except BaseException:
                # The exclusive cooling-backend probe may already have been
                # reserved by _await_backend before admission/pacing failed.
                with self.condition:
                    self.recovery_probes.discard(name)
                    self.condition.notify_all()
                raise
            try:
                kwargs = ({"redirect_validator": redirect_validator}
                          if redirect_validator is not None else {})
                if _valid_cdx_digest(expected_digest):
                    kwargs["expected_digest"] = expected_digest
                if name == "httpx" and retain_partial:
                    kwargs.pop("expected_digest", None)
                    response = backend.download(
                        url, headers, destination, max_bytes, stop_event,
                        retain_partial=True, expected_digest=expected_digest, **kwargs,
                    )
                else:
                    response = backend.download(url, headers, destination, max_bytes, stop_event, **kwargs)
                with self.lock:
                    changed = self.last_success != name
                    self.last_success = name
                self._backend_result(name, True)
                if changed and self.callback:
                    self.callback(f"Network backend: {name}")
                return response
            except RedirectPolicyError:
                self._backend_result(name, True)  # successful response, policy refused redirect
                raise
            except PermitRevoked:
                # Host generation changed between redirect hops. Do not retry
                # through another backend while the shared service is paused.
                self._backend_result(name, True)
                raise
            except Stopped:
                with self.condition:
                    self.recovery_probes.discard(name)
                    self.condition.notify_all()
                raise
            except OSError as exc:
                if exc.errno in {28, 122} or getattr(exc, "winerror", None) in {39, 112}:
                    self._backend_result(name, True)
                    raise
                failures.append((name, exc))
            except RuntimeError as exc:
                if str(exc).startswith("response exceeds") or "too many redirects" in str(exc):
                    # Local response-policy refusal means the route answered.
                    # Release an exclusive recovery probe before propagating.
                    self._backend_result(name, True)
                    raise
                failures.append((name, exc))
            except Exception as exc:
                failures.append((name, exc))
            partial_ready = bool(retain_partial and _partial_metadata_path(destination).is_file()
                                 and destination.is_file())
            if not partial_ready:
                destination.unlink(missing_ok=True)
            if partial_ready:
                with self.condition:
                    self.recovery_probes.discard(name)
                    self.condition.notify_all()
            else:
                self._backend_result(name, not is_transport_connection_failure(failures[-1][1]))
            last_error = failures[-1][1]
            if partial_ready or isinstance(last_error, IncompleteRepresentationError) or is_transport_read_timeout(last_error):
                # Never overwrite identity-bound prefixes or repeat a remote
                # read/response failure via a different Python HTTP client.
                break
            if is_transport_read_timeout(last_error):
                if self.callback:
                    self.callback(
                        f"Network backend {name} reached Wayback but the response timed out; "
                        "requeueing without repeating the full timeout on every backend…"
                    )
                break
            if self.callback:
                stage = ("connection setup" if is_transport_connection_failure(last_error)
                         else "response transfer or TLS handling")
                self.callback(f"Network backend {name} failed during {stage}; checking another connection method…")
        raise TransportExhaustedError(url, failures)
