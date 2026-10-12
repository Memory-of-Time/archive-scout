from __future__ import annotations

import contextlib
import threading
import time
from concurrent.futures import CancelledError, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Callable, Iterable, Iterator

from ..events import Stopped
from .client import CDXRow, HttpClient, RateLimitDeferred, request_cdx_json_rows, request_cdx_rows


@dataclass(slots=True)
class PageFetchResult:
    page: int
    rows: list[CDXRow]
    elapsed: float
    error: BaseException | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


def effective_page_workers(requested_workers: int, page_blocks: int) -> int:
    """Bound concurrent page bodies while favoring fewer, server-sized pages.

    ``page_blocks == 0`` in the explicit paged compatibility strategy means
    pageSize is omitted and Internet Archive chooses its normal page grouping.
    Those bodies can be much larger than old fixed-block pages, so three
    concurrent requests are enough to hide latency without multiplying memory
    use. Explicit smaller page sizes may still use more workers.
    """
    requested = max(1, int(requested_workers))
    blocks = int(page_blocks)
    if blocks <= 0:
        return min(requested, 3)
    if blocks <= 9:
        # The reference downloader uses pageSize=9 with ten concurrent Timemap
        # requests. Match that proven envelope while retaining the caller's cap.
        return min(requested, 10)
    memory_cap = max(2, 96 // max(1, blocks))
    return min(requested, memory_cap)


def iter_cdx_pages(
    client: HttpClient,
    endpoints: Iterable[str],
    pages: list[int],
    params_for_page: Callable[[int], list[tuple[str, str]]],
    stop_event: threading.Event,
    workers: int,
    max_bytes: int = 64 * 1024 * 1024,
    prefer_text: bool = True,
    json_only: bool = False,
) -> Iterator[PageFetchResult]:
    """Yield independent CDX pages as soon as each page completes.

    Older builds waited for every page in a batch and retained all parsed page
    dictionaries until the slowest sibling finished. Yielding completed compact
    pages lets the indexer write and release each result immediately.
    """
    if not pages:
        return
    worker_count = min(max(1, int(workers)), len(pages))
    endpoint_tuple = tuple(endpoints)
    pool_cancel = threading.Event()

    def fetch(page: int) -> PageFetchResult:
        if stop_event.is_set():
            raise Stopped
        started = time.monotonic()
        try:
            scope_factory = getattr(client, "cancellation_scope", None)
            scope = scope_factory(pool_cancel) if callable(scope_factory) else contextlib.nullcontext()
            with scope:
                if json_only:
                    result = request_cdx_json_rows(
                        client,
                        endpoint_tuple,
                        params_for_page(page),
                        max_bytes=max_bytes,
                    )
                else:
                    result = request_cdx_rows(
                        client,
                        endpoint_tuple,
                        params_for_page(page),
                        max_bytes=max_bytes,
                        prefer_text=prefer_text,
                    )
            return PageFetchResult(page, result.rows, time.monotonic() - started)
        except RateLimitDeferred as exc:
            # A quota/overload exhaustion is one pool-wide control signal, not a
            # thousand independent page failures.  Set the local cancel token in
            # the worker that first observes it so admission/gate waits in sibling
            # workers wake before the scheduler thread sees this result.
            pool_cancel.set()
            return PageFetchResult(page, [], time.monotonic() - started, exc)
        except Stopped:
            if pool_cancel.is_set() and not stop_event.is_set():
                return PageFetchResult(page, [], time.monotonic() - started, Stopped("paged request cancelled by service pause"))
            raise
        except Exception as exc:
            return PageFetchResult(page, [], time.monotonic() - started, exc)

    executor = ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="archive-scout-cdx")
    pending: dict[Future[PageFetchResult], int] = {}
    remaining = iter(pages)

    def fill() -> None:
        # Bound *results*, not merely worker threads. Submitting all 1,000 page
        # numbers lets completed bodies pile up while SQLite consumes a page.
        while len(pending) < worker_count * 2 and not stop_event.is_set() and not pool_cancel.is_set():
            page = next(remaining, None)
            if page is None:
                break
            pending[executor.submit(fetch, int(page))] = int(page)

    service_pause: RateLimitDeferred | None = None
    try:
        fill()
        while pending:
            if stop_event.is_set():
                raise Stopped
            done, _ = wait(tuple(pending), timeout=0.25, return_when=FIRST_COMPLETED)
            if not done:
                continue
            for future in done:
                pending.pop(future, None)
                try:
                    result = future.result()
                except CancelledError:
                    continue
                if isinstance(result.error, RateLimitDeferred):
                    if service_pause is None:
                        service_pause = result.error
                    pool_cancel.set()
                    for queued in pending:
                        queued.cancel()
                    continue
                if service_pause is not None:
                    # A success that had already reached a valid response before
                    # cancellation is still useful and is committed by the owner
                    # thread.  Any other sibling error belongs to the same global
                    # pause and must not become a per-page failure counter.
                    if result.succeeded:
                        yield result
                    continue
                yield result
            done.clear()
            if service_pause is None:
                fill()
        if service_pause is not None:
            raise service_pause
    except Exception:
        pool_cancel.set()
        for future in pending:
            future.cancel()
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


def fetch_cdx_pages(
    client: HttpClient,
    endpoints: Iterable[str],
    pages: list[int],
    params_for_page: Callable[[int], list[tuple[str, str]]],
    stop_event: threading.Event,
    workers: int,
    max_bytes: int = 64 * 1024 * 1024,
    prefer_text: bool = True,
    json_only: bool = False,
) -> list[PageFetchResult]:
    """Compatibility wrapper returning deterministic page order."""
    results = list(
        iter_cdx_pages(
            client,
            endpoints,
            pages,
            params_for_page,
            stop_event,
            workers,
            max_bytes=max_bytes,
            prefer_text=prefer_text,
            json_only=json_only,
        )
    )
    results.sort(key=lambda item: item.page)
    return results
