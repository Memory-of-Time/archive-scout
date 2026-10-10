from __future__ import annotations

import hashlib
import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.parse
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
    except Exception:
        destination.unlink(missing_ok=True)
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
            options = {"follow_redirects": False} if redirect_validator is not None else {}
            with self.client.stream("GET", current_url, headers=headers, **options) as response:
                location = response.headers.get("Location")
                if redirect_validator is not None and response.status_code in {301, 302, 303, 307, 308} and location:
                    destination = urllib.parse.urljoin(current_url, location)
                    redirect_validator(current_url, destination)
                    current_url = destination
                    continue
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
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        with self.client.stream("GET", url, headers=headers) as response:
            announced = response.headers.get("Content-Length")
            if announced and announced.isdigit() and int(announced) > max_bytes:
                raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
            total, content_hash, preview = _write_limited(
                response.iter_bytes(1024 * 1024), destination, max_bytes, stop_event
            )
            return TransportFileResponse(
                status=int(response.status_code),
                headers=_copy_headers(response.headers.items()),
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

    def __init__(self, pool_size: int, connect_timeout: float, read_timeout: float) -> None:
        self.timeout = urllib3.Timeout(connect=max(1.0, connect_timeout), read=max(1.0, read_timeout))
        self.pool = urllib3.PoolManager(
            num_pools=4,
            maxsize=max(2, int(pool_size)),
            block=True,
            ssl_context=_ssl_context(),
            retries=False,
        )

    def close(self) -> None:
        self.pool.clear()

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
                response = self.pool.request(
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
                    break
                destination = urllib.parse.urljoin(current_url, location)
                if redirect_validator is not None:
                    redirect_validator(current_url, destination)
                self._discard(response)
                response = None
                current_url = destination
            else:
                raise RuntimeError(f"too many redirects: {url}")

            assert response is not None
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
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        current_url = url
        response = None
        try:
            for _ in range(11):
                response = self.pool.request(
                    "GET", current_url, headers=headers, preload_content=False,
                    redirect=False, retries=False, timeout=self.timeout,
                )
                if int(response.status) not in {301, 302, 303, 307, 308}:
                    break
                location = response.headers.get("Location")
                if not location:
                    break
                self._discard(response)
                response = None
                current_url = urllib.parse.urljoin(current_url, location)
            else:
                raise RuntimeError(f"too many redirects: {url}")
            assert response is not None
            announced = response.headers.get("Content-Length")
            if announced and str(announced).isdigit() and int(announced) > max_bytes:
                raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
            total, content_hash, preview = _write_limited(
                response.stream(amt=1024 * 1024, decode_content=True),
                destination, max_bytes, stop_event,
            )
            result = TransportFileResponse(
                status=int(response.status),
                headers=_copy_headers(response.headers.items()),
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

    def __init__(self, connect_timeout: float, read_timeout: float) -> None:
        executable = shutil.which("curl")
        if not executable:
            raise BackendUnavailable("curl executable was not found")
        self.executable = executable
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.read_timeout = max(1.0, float(read_timeout))

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

    def request(self, url: str, headers: dict[str, str], max_bytes: int,
                stop_event: threading.Event,
                redirect_validator: Callable[[str, str], None] | None = None) -> TransportResponse:
        if redirect_validator is None:
            return self._request_once(url, headers, max_bytes, stop_event)
        started = time.monotonic()
        current_url = url
        for _ in range(11):
            response = self._request_once(current_url, headers, max_bytes, stop_event, False)
            location = response.headers.get("location")
            if response.status not in {301, 302, 303, 307, 308} or not location:
                response.elapsed = time.monotonic() - started
                return response
            destination = urllib.parse.urljoin(current_url, location)
            redirect_validator(current_url, destination)
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
                "--location",
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
                creationflags=creationflags,
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
    ) -> TransportFileResponse:
        ensure_frozen_bundle_available()
        started = time.monotonic()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.unlink(missing_ok=True)
        with tempfile.TemporaryDirectory(prefix="archive-scout-curl-") as temp_dir:
            header_path = Path(temp_dir) / "headers.txt"
            command = [
                self.executable, "--location", "--compressed", "--http1.1",
                "--silent", "--show-error", "--connect-timeout", str(int(self.connect_timeout)),
                "--max-time", str(int(self.connect_timeout + self.read_timeout)),
                "--max-filesize", str(int(max_bytes)), "--dump-header", str(header_path),
                "--output", str(destination), "--write-out", "%{http_code}\n%{url_effective}",
            ]
            for key, value in headers.items():
                command.extend(["--header", f"{key}: {value}"])
            command.extend(["--", url])
            creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            proc = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                creationflags=creationflags,
            )
            while proc.poll() is None:
                if stop_event.wait(0.2):
                    proc.kill()
                    proc.wait(timeout=5)
                    destination.unlink(missing_ok=True)
                    raise Stopped
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                destination.unlink(missing_ok=True)
                message = (stderr or stdout or f"curl exited {proc.returncode}").strip()
                if proc.returncode == 28:
                    raise TimeoutError(message)
                raise OSError(message)
            size = destination.stat().st_size if destination.exists() else 0
            if size > max_bytes:
                destination.unlink(missing_ok=True)
                raise RuntimeError(f"response exceeds {max_bytes:,} bytes")
            digest = hashlib.sha256()
            preview = bytearray()
            with destination.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    if stop_event.is_set():
                        destination.unlink(missing_ok=True)
                        raise Stopped
                    digest.update(chunk)
                    if len(preview) < 20_000:
                        preview.extend(chunk[: 20_000 - len(preview)])
            lines = stdout.splitlines()
            status = int(lines[-2]) if len(lines) >= 2 and lines[-2].isdigit() else 0
            final_url = lines[-1] if lines else url
            raw_headers = header_path.read_text(encoding="iso-8859-1", errors="replace") if header_path.exists() else ""
            return TransportFileResponse(
                status=status, headers=self._parse_headers(raw_headers), final_url=final_url,
                path=destination, bytes_written=size, content_hash=digest.hexdigest(),
                preview=bytes(preview), backend=self.name, elapsed=time.monotonic() - started,
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
        self.cooldown_until: dict[str, float] = {}
        self.last_success: str | None = None
        available: dict[str, object] = {
            "httpx": HttpxBackend(pool_size, connect_timeout, read_timeout, trust_env=trust_env),
            "urllib3": Urllib3Backend(pool_size, connect_timeout, read_timeout),
        }
        try:
            available["curl"] = CurlBackend(connect_timeout, read_timeout)
        except BackendUnavailable:
            pass
        if requested == "auto":
            self.backends = available
            self.order = [name for name in ("httpx", "urllib3", "curl") if name in available]
        else:
            if requested not in available:
                raise BackendUnavailable(f"requested network backend is unavailable: {requested}")
            self.backends = {requested: available[requested]}
            self.order = [requested]
            for name, backend in available.items():
                if name != requested:
                    backend.close()

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
        redirect_validator: Callable[[str, str], None] | None = None,
    ) -> TransportResponse:
        failures: list[tuple[str, BaseException]] = []
        for name in self._ordered_names():
            if stop_event.is_set():
                raise Stopped
            backend = self.backends[name]
            try:
                options = {"redirect_validator": redirect_validator} if redirect_validator is not None else {}
                response = backend.request(url, headers, max_bytes, stop_event, **options)
                with self.lock:
                    changed = self.last_success != name
                    self.last_success = name
                    self.cooldown_until.pop(name, None)
                if changed and self.callback:
                    self.callback(f"Network backend: {name}")
                return response
            except (Stopped, RedirectPolicyError):
                raise
            except RuntimeError as exc:
                # Size limits and other deterministic local validation failures
                # must not be retried using another backend.
                if str(exc).startswith("response exceeds") or "too many redirects" in str(exc):
                    raise
                failures.append((name, exc))
            except Exception as exc:
                failures.append((name, exc))
            with self.lock:
                self.cooldown_until[name] = time.monotonic() + 30.0
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
    ) -> TransportFileResponse:
        failures: list[tuple[str, BaseException]] = []
        for name in self._ordered_names():
            if stop_event.is_set():
                raise Stopped
            backend = self.backends[name]
            try:
                response = backend.download(url, headers, destination, max_bytes, stop_event)
                with self.lock:
                    changed = self.last_success != name
                    self.last_success = name
                    self.cooldown_until.pop(name, None)
                if changed and self.callback:
                    self.callback(f"Network backend: {name}")
                return response
            except Stopped:
                raise
            except RuntimeError as exc:
                if str(exc).startswith("response exceeds") or "too many redirects" in str(exc):
                    raise
                failures.append((name, exc))
            except Exception as exc:
                failures.append((name, exc))
            destination.unlink(missing_ok=True)
            with self.lock:
                self.cooldown_until[name] = time.monotonic() + 30.0
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
