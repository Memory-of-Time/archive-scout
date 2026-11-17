from __future__ import annotations

import concurrent.futures as futures
import csv
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from ..constants import CDX_URL, VERSION
from ..database.lease import project_lease
from ..downloads.rate_limit import SharedFixedRateLimiter, shared_host_gate
from ..events import ConnectivityPaused, ProgressEvent, Stopped
from ..utils import normalize_cdx_date
from .client import HttpClient, PermanentRequestError, TransientRequestError, decode_cdx_encoded_body
from .parameters import parse_cdx

PAGE_SIZE = 100
IMPORT_BATCH = 4096
WORKERS = 8
HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
SCHEMA = """
CREATE TABLE IF NOT EXISTS input (
    id INTEGER PRIMARY KEY CHECK(id=1), offset INTEGER NOT NULL, eof INTEGER NOT NULL,
    rejected INTEGER NOT NULL
);
INSERT OR IGNORE INTO input VALUES (1, 0, 0, 0);
CREATE TABLE IF NOT EXISTS domains (
    id INTEGER PRIMARY KEY, domain TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '', resume_key TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS domain_work ON domains(state, id);
CREATE TABLE IF NOT EXISTS matches (
    url TEXT PRIMARY KEY, timestamp TEXT NOT NULL, status TEXT NOT NULL,
    domain TEXT NOT NULL
) WITHOUT ROWID;
"""


def _domain(line: bytes) -> str:
    value = line.decode("utf-8-sig").strip().rstrip(".").removeprefix("*.")
    host = value.encode("idna").decode("ascii").lower()
    if len(host) <= 253 and "." in host and all(HOST_LABEL.fullmatch(part) for part in host.split(".")):
        return host
    return ""


def _import_chunk(db, source, expected, stop):
    offset, eof, _ = db.execute("SELECT offset,eof,rejected FROM input WHERE id=1").fetchone()
    if eof:
        return False
    if _stamp(os.fstat(source.fileno())) != expected:
        raise ValueError("The domain list changed during discovery. Start a new run with the updated file.")
    source.seek(offset)
    hosts = []
    rejected = 0
    for _ in range(IMPORT_BATCH):
        if stop.is_set():
            raise Stopped
        line = source.readline(4097)
        if not line:
            eof = 1
            break
        if len(line) > 4096:
            while line and not line.endswith(b"\n"):
                if stop.is_set():
                    raise Stopped
                line = source.readline(4097)
            rejected += 1
            continue
        if not line.strip() or line.lstrip().startswith(b"#"):
            continue
        try:
            host = _domain(line)
        except UnicodeError:
            host = ""
        if host:
            hosts.append((host,))
        else:
            rejected += 1
    if _stamp(os.fstat(source.fileno())) != expected:
        raise ValueError("The domain list changed during discovery. Start a new run with the updated file.")
    with db:
        db.executemany("INSERT OR IGNORE INTO domains(domain) VALUES (?)", hosts)
        db.execute("UPDATE input SET offset=?, eof=?, rejected=rejected+? WHERE id=1",
                   (source.tell(), eof, rejected))
    return bool(hosts) or not eof


def _stamp(stat):
    return stat.st_size, stat.st_mtime_ns


def _lookup(client, domain, start, end, pattern):
    params = {
        "url": domain, "matchType": "domain", "from": start, "to": end,
        "filter": "original:" + pattern, "collapse": "urlkey", "limit": PAGE_SIZE,
        "showResumeKey": "true", "output": "json", "fl": "urlkey,timestamp,original,statuscode",
    }
    try:
        response = client.get(CDX_URL + "?" + urlencode(params), 2 * 1024 * 1024, "application/json")
        if response["status"] != 200:
            return "deferred", f'Unexpected HTTP status {response["status"]}', "", []
        try:
            rows, resume = parse_cdx(json.loads(decode_cdx_encoded_body(response["data"], max_bytes=2 * 1024 * 1024)))
        except (RuntimeError, ValueError) as exc:
            return "deferred", f"Invalid CDX response: {exc}"[:180], "", []
        matches = []
        for row in rows[:PAGE_SIZE]:
            stamp, url, status = row["timestamp"], row["original"], row.get("statuscode", "")
            if not isinstance(stamp, str) or not isinstance(url, str) or not isinstance(status, str):
                raise ValueError("Invalid CDX field type")
            parsed = urlsplit(url)
            host = (parsed.hostname or "").rstrip(".").encode("idna").decode("ascii").lower()
            if (parsed.scheme not in {"http", "https"} or not host or
                    (host != domain and not host.endswith("." + domain)) or
                    not re.fullmatch(r"[0-9]{14}", stamp) or not start <= stamp <= end or
                    any(ord(char) < 32 or ord(char) == 127 for char in url)):
                raise ValueError("CDX returned an invalid or out-of-scope capture")
            matches.append((url, stamp, status, domain))
        limited = bool(resume) or len(rows) >= PAGE_SIZE
        return ("limited" if limited else "searched"), ("Page limit" if limited else ""), resume or "", matches
    except PermanentRequestError as exc:
        if exc.status in {400, 414, 422}:
            raise ValueError(f"Wayback rejected the discovery query (HTTP {exc.status}). Check the URL regex and its length.") from exc
        return "deferred", f"HTTP {exc.status}", "", []
    except TransientRequestError as exc:
        reason = f"HTTP {exc.status}" if exc.status else type(exc).__name__
        return "deferred", reason, "", []
    except ValueError as exc:
        return "deferred", str(exc)[:180], "", []



def _replace_report(path, write):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8", newline="") as handle:
        write(handle)
    temp.replace(path)


def _write_reports(db, paths, counts):
    def evidence(handle):
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(("original_url", "earliest_returned_capture", "status_code", "matched_domain"))
        writer.writerows(db.execute("SELECT url,timestamp,status,domain FROM matches ORDER BY url"))

    def coverage(handle):
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(("domain", "state", "attempts", "reason", "resume_key"))
        writer.writerows(db.execute("""SELECT domain,state,attempts,reason,resume_key FROM domains
                                       WHERE state IN ('deferred','limited') ORDER BY id"""))

    _replace_report(paths["evidence"], evidence)
    _replace_report(paths["deferred"], coverage)
    _replace_report(paths["summary"], lambda handle: json.dump(counts, handle, indent=2))


def _save_results(db, completed, output):
    new = []
    with db:
        for domain_id, (state, reason, resume, matches) in completed:
            for match in matches:
                if db.execute("INSERT OR IGNORE INTO matches VALUES (?,?,?,?)", match).rowcount:
                    new.append(match[0] + "\n")
                else:
                    db.execute("UPDATE matches SET timestamp=?,status=?,domain=? WHERE url=? AND timestamp>?",
                               (*match[1:], match[0], match[1]))
            db.execute("UPDATE domains SET state=?,attempts=attempts+1,reason=?,resume_key=? WHERE id=?",
                       (state, reason, resume, domain_id))
    if new:
        output.writelines(new)
        output.flush()
    return len(new)


def _run_pass(db, source, stamp, client, start, end, pattern, stop, output, callback, *, retry=False):
    state = "deferred" if retry else "pending"
    after = 0
    active = {}
    done_count = hit_count = 0
    last_update = time.monotonic()
    exhausted = False
    with futures.ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="scout-url") as pool:
        while not exhausted or active:
            if stop.is_set():
                raise Stopped
            if client.host_gate.remaining() > 30:
                raise ConnectivityPaused("Wayback requested a longer cooldown. Start discovery again with the same settings to resume.")
            slots = WORKERS - len(active)
            if slots and not exhausted:
                rows = db.execute("""SELECT id,domain FROM domains WHERE state=? AND id>?
                    AND attempts<? ORDER BY id LIMIT ?""", (state, after, 2 if retry else 1, slots)).fetchall()
                if not rows:
                    exhausted = retry or not _import_chunk(db, source, stamp, stop)
                    if not active:
                        continue
                for domain_id, domain in rows:
                    active[pool.submit(_lookup, client, domain, start, end, pattern)] = domain_id
                    after = domain_id
            if active:
                ready, _ = futures.wait(active, timeout=0.25, return_when=futures.FIRST_COMPLETED)
                completed = []
                try:
                    for job in ready:
                        domain_id = active.pop(job)
                        try:
                            completed.append((domain_id, job.result()))
                        except Stopped:
                            if not stop.is_set():
                                raise
                finally:
                    hit_count += _save_results(db, completed, output) if completed else 0
                    done_count += len(completed)
            if callback and time.monotonic() - last_update >= 2.0:
                callback(ProgressEvent("discovery", f"{'Retry' if retry else 'First'} pass: "
                                       f"{done_count:,} domain requests finished, {hit_count:,} new matching URLs."))
                last_update = time.monotonic()


def run_fast_discovery(domain_file, output_dir, url_filter, from_date="2001", to_date="2003",
                       *, requests_per_second=1.0, stop_event=None, callback=None):
    source_path = Path(domain_file).expanduser().resolve(strict=True)
    if not source_path.is_file():
        raise ValueError("Select a text file with one domain per line.")
    start, end = normalize_cdx_date(from_date, end=False), normalize_cdx_date(to_date, end=True)
    pattern = url_filter.strip().removeprefix("&filter=").removeprefix("filter=").removeprefix("original:")
    if start > end or not pattern:
        raise ValueError("Enter a URL regex and a valid capture date range.")
    if not math.isfinite(requests_per_second) or not 0 < requests_per_second <= 8:
        raise ValueError("Requests per second must be above zero and at most 8.")
    if len(urlencode({"filter": pattern})) > 6000:
        raise ValueError("The URL regex is too long for this CDX request.")
    stop = stop_event or threading.Event()
    with source_path.open("rb") as source:
        stamp = _stamp(os.fstat(source.fileno()))
        identity = json.dumps([str(source_path), stamp, start, end, pattern])
        root = Path(output_dir).expanduser().resolve() / hashlib.sha256(identity.encode()).hexdigest()[:20]
        paths = {key: root / name for key, name in (
            ("urls", "matching_urls.txt"), ("evidence", "matches.tsv"),
            ("deferred", "deferred_domains.tsv"), ("summary", "summary.json"),
        )}
        with project_lease(root), closing(sqlite3.connect(root / "discovery.sqlite3")) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)
            if callback:
                callback(ProgressEvent("discovery", f"Searching Wayback URL metadata. Live results: {paths['urls']}"))

            def rebuild_urls(handle):
                for (url,) in db.execute("SELECT url FROM matches ORDER BY url"):
                    handle.write(url + "\n")

            _replace_report(paths["urls"], rebuild_urls)
            transport = HttpClient(
                SharedFixedRateLimiter(1 / requests_per_second), 1, 5,
                f"ArchiveScout/{VERSION} URL discovery", stop, connect_timeout=3, read_timeout=5,
                pool_size=WORKERS, host_gate=shared_host_gate(), network_backend="httpx",
                rate_limit_attempts=1, rate_limit_max_wait=30,
            )
            try:
                with paths["urls"].open("a", encoding="utf-8", newline="") as output:
                    _run_pass(db, source, stamp, transport, start, end, pattern, stop, output, callback)
                    _run_pass(db, source, stamp, transport, start, end, pattern, stop, output, callback, retry=True)
            finally:
                transport.close()
                _, eof, rejected = db.execute("SELECT offset,eof,rejected FROM input").fetchone()
                counts = dict(db.execute("SELECT state,COUNT(*) FROM domains GROUP BY state"))
                counts.update(matches=db.execute("SELECT COUNT(*) FROM matches").fetchone()[0],
                              invalid_input_lines=rejected, input_read_to_end=bool(eof))
                _write_reports(db, paths, counts)
    return paths, counts
