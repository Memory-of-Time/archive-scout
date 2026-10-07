from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import threading
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlsplit
from typing import Callable

from ..content import decode_bytes, looks_textual_bytes, parse_page
from ..events import ProgressEvent, Stopped
from ..storage import sha256_file
from ..utils import utc_now
from .normalization import normalize_search
from .automaton import LiteralAutomaton


def _normalized_keywords(values: list[str]) -> list[str]:
    unique: dict[str, str] = {}
    for raw in values:
        display = str(raw).strip()
        normalized = normalize_search(display)
        if normalized:
            unique.setdefault(normalized, display)
    return list(unique.values())


def hitlist_fingerprint(values: list[str]) -> str:
    normalized = sorted({normalize_search(value) for value in values if normalize_search(value)})
    return hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()


def load_hitlist(keywords: list[str] | None = None, file_path: str | Path = "") -> list[str]:
    values = list(keywords or [])
    if str(file_path or "").strip():
        path = Path(file_path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(path)
        values.extend(path.read_text(encoding="utf-8", errors="replace").splitlines())
    return _normalized_keywords(values)


def _count_matches(automaton: LiteralAutomaton, text: str) -> dict[str, int]:
    return automaton.count_non_overlapping(text)


def _resume_or_create_run(database: sqlite3.Connection, keywords: list[str]) -> tuple[int, int, int]:
    fingerprint = hitlist_fingerprint(keywords)
    row = database.execute(
        """SELECT id,last_capture_id,match_count,updated_at,coverage_version,capture_limit FROM quick_search_runs
           WHERE fingerprint=? AND status IN ('running','interrupted') ORDER BY id DESC LIMIT 1""",
        (fingerprint,),
    ).fetchone()
    now = utc_now()
    if row:
        run_id = int(row["id"])
        if int(row["coverage_version"] or 0) != 2:
            # Older checkpoints did not prove the identity of on-disk bytes.
            # Recheck them once, preserving an existing corpus boundary.
            database.execute("DELETE FROM quick_search_hits WHERE run_id=?", (run_id,))
            database.execute("DELETE FROM quick_search_coverage WHERE run_id=?", (run_id,))
            database.execute("""UPDATE quick_search_runs SET status='running',last_capture_id=0,indexed_checked=0,
                local_checked=0,unavailable_count=0,discarded_count=0,missing_count=0,non_text_count=0,
                incomplete_count=0,match_count=0,capture_limit=CASE WHEN coverage_version>0 THEN capture_limit ELSE (SELECT COALESCE(MAX(id),0) FROM captures) END,coverage_version=2,updated_at=? WHERE id=?""", (now,run_id))
            return run_id,0,0
        database.execute(
            "UPDATE quick_search_runs SET status='running',updated_at=? WHERE id=?", (now, run_id)
        )
        return run_id, int(row["last_capture_id"] or 0), int(row["match_count"] or 0)
    cursor = database.execute(
        """INSERT INTO quick_search_runs(fingerprint,keywords_json,status,started_at,updated_at,coverage_version,capture_limit)
           VALUES(?,?,'running',?,?,2,(SELECT COALESCE(MAX(id),0) FROM captures))""",
        (fingerprint, json.dumps(keywords, ensure_ascii=False), now, now),
    )
    return int(cursor.lastrowid), 0, 0


def _payload_source(row: sqlite3.Row, root: Path) -> tuple[Path | None, str]:
    local = str(row["local_path"] or row["document_path"] or "")
    path = Path(local) if local else None
    availability = str(row["payload_availability"] or "not_acquired")
    if availability == "discarded":
        return path, "discarded"
    if availability == "partial":
        return path, "partial"
    if str(row["resource_class"] or "unknown") in {"image", "video", "audio", "other_binary"}:
        return path, "non_text"
    try:
        if path and path.resolve().is_relative_to(root) and path.is_file():
            return path, ""
    except OSError:
        pass
    return path, "missing"


def _coverage_fingerprint(row: sqlite3.Row, body_identity: str) -> str:
    metadata = [str(row[name] or "") for name in (
        "original_url", "timestamp", "local_path", "document_path", "mimetype",
        "detected_encoding", "resource_class", "payload_availability",
    )]
    metadata.append(body_identity)
    return hashlib.sha256(json.dumps(metadata, ensure_ascii=False).encode("utf-8")).hexdigest()


def _verify_covered_prefix(database: sqlite3.Connection, run_id: int, last_id: int,
                           root: Path, stop_event: threading.Event,
                           callback: Callable[[ProgressEvent], None] | None,
                           batch_size: int) -> None:
    """Verify exact bytes once per resume; size/mtime are insufficient evidence.

    Hash streams are bounded to 1 MiB. Scoring/parsing only repeat for changed
    coverage, and no new captures are added to the saved corpus boundary.
    """
    cursor_id = checked = 0
    total = int(database.execute(
        "SELECT COUNT(*) FROM quick_search_coverage WHERE run_id=? AND capture_id<=?", (run_id, last_id)
    ).fetchone()[0])
    while True:
        if stop_event.is_set():
            raise Stopped
        rows = database.execute(
            """SELECT c.*,d.path AS document_path,q.content_fingerprint
               FROM quick_search_coverage q JOIN captures c ON c.id=q.capture_id
               LEFT JOIN documents d ON d.id=c.document_id
               WHERE q.run_id=? AND c.id>? AND c.id<=? ORDER BY c.id LIMIT ?""",
            (run_id, cursor_id, last_id, max(1, int(batch_size))),
        ).fetchall()
        if not rows:
            return
        changed = []
        for row in rows:
            if stop_event.is_set():
                raise Stopped
            path, identity = _payload_source(row, root)
            if not identity:
                digest = hashlib.sha256()
                try:
                    with path.open("rb") as handle:
                        while chunk := handle.read(1024 * 1024):
                            if stop_event.is_set():
                                raise Stopped
                            digest.update(chunk)
                    identity = digest.hexdigest()
                except OSError:
                    identity = "missing"
            if _coverage_fingerprint(row, identity) != str(row["content_fingerprint"] or ""):
                changed.append((run_id, int(row["id"])))
        with database:
            database.executemany(
                "UPDATE quick_search_coverage SET body_revision=-1 WHERE run_id=? AND capture_id=?", changed
            )
        checked += len(rows)
        cursor_id = int(rows[-1]["id"])
        if callback:
            callback(ProgressEvent("hitlist_verify", "Verifying saved Hitlist coverage", checked, total))


def _needs_rendered_fallback(
    raw: str,
    content_type: str,
    path: Path | None,
    original_url: str,
) -> bool:
    """Return whether missing literals should be checked in rendered text.

    Stored captures intentionally end in ``.txt``, so the local suffix alone is
    not a trustworthy content signal. We skip the DOM only for formats that are
    confidently plain/non-markup; ambiguous MIME, historical HTML suffixes,
    fragments, and late markup retain the rendered fallback for coverage.
    """
    del raw  # Classification intentionally avoids a short-prefix HTML heuristic.
    mime = (content_type or "").split(";", 1)[0].strip().casefold()
    if "html" in mime or "xhtml" in mime:
        return True

    parsed = urlsplit(original_url if "://" in original_url else "https://" + original_url)
    name = (parsed.path.rsplit("/", 1)[-1] if parsed else "").casefold()
    html_suffixes = (
        ".html", ".htm", ".shtml", ".shtm", ".xhtml", ".xhtm",
        ".php", ".php3", ".php4", ".php5", ".phtml", ".asp", ".aspx",
        ".jsp", ".cfm", ".cgi", ".dhtml", ".dhtm",
    )
    if name.endswith(html_suffixes):
        return True

    # These representations do not gain useful phrase adjacency from HTML DOM
    # rendering. Everything else remains conservative because old archives often
    # carry misleading or missing MIME metadata.
    if mime in {
        "text/plain", "text/css", "application/json", "text/json",
        "application/javascript", "text/javascript", "application/x-javascript",
    }:
        return False
    plain_suffixes = (".txt", ".text", ".css", ".js", ".json")
    if name.endswith(plain_suffixes) and mime not in {
        "", "application/octet-stream", "binary/octet-stream", "application/binary"
    }:
        return False

    # A local source imported outside normal capture naming may still carry a
    # meaningful HTML suffix. This is additive evidence only; `.txt` is never
    # treated as proof of plain text because Archive Scout appends it broadly.
    if path is not None and path.suffix.casefold() in {".html", ".htm", ".xhtml"}:
        return True
    return True


def _is_html_like(raw: str, content_type: str, path: Path | None) -> bool:
    """Backward-compatible helper retained for audit/integration tooling."""
    return _needs_rendered_fallback(raw, content_type, path, "")


def search_with_hitlist(
    root: Path,
    database: sqlite3.Connection,
    keywords: list[str],
    stop_event: threading.Event,
    callback: Callable[[ProgressEvent], None] | None = None,
    *,
    batch_size: int = 250,
) -> dict[str, object]:
    root = Path(root).resolve()
    keywords = _normalized_keywords(keywords)
    if not keywords:
        raise ValueError("Search with Hitlist requires at least one keyword")
    normalized_to_display = {normalize_search(value): value for value in keywords}
    automaton = LiteralAutomaton(normalized_to_display)
    run_id, last_id, matched_total = _resume_or_create_run(database, keywords)
    database.commit()
    capture_limit = int(database.execute("SELECT capture_limit FROM quick_search_runs WHERE id=?", (run_id,)).fetchone()[0])
    total = int(database.execute("SELECT COUNT(*) FROM captures WHERE id<=?",(capture_limit,)).fetchone()[0])
    indexed_checked = local_checked = unavailable = 0
    discarded = missing = non_text = incomplete = 0

    try:
        if last_id:
            _verify_covered_prefix(database, run_id, last_id, root, stop_event, callback, batch_size)
        while True:
            if stop_event.is_set():
                raise Stopped
            revisiting = False
            rows = database.execute(
                """SELECT c.id,c.original_url,c.timestamp,c.local_path,c.document_id,c.mimetype,c.detected_encoding,
                          c.resource_class,c.payload_availability,c.state,c.body_revision,d.path AS document_path,
                          0 AS old_mask
                   FROM captures c LEFT JOIN documents d ON d.id=c.document_id
                   WHERE c.id>? AND c.id<=? ORDER BY c.id LIMIT ?""",
                (last_id,capture_limit,max(1,int(batch_size))),
            ).fetchall()
            if not rows:
                # Hold the writer boundary while deciding that coverage is
                # current. Changes after this commit belong to a later search.
                with database:
                    database.execute("BEGIN IMMEDIATE")
                    rows = database.execute(
                        """SELECT c.id,c.original_url,c.timestamp,c.local_path,c.document_id,c.mimetype,c.detected_encoding,
                                  c.resource_class,c.payload_availability,c.state,c.body_revision,d.path AS document_path,
                                  q.coverage_mask AS old_mask
                           FROM quick_search_coverage q JOIN captures c ON c.id=q.capture_id
                           LEFT JOIN documents d ON d.id=c.document_id
                           WHERE q.run_id=? AND q.body_revision<>c.body_revision ORDER BY c.id LIMIT ?""",
                        (run_id,max(1,int(batch_size))),
                    ).fetchall()
                    if not rows:
                        database.execute("UPDATE quick_search_runs SET status='complete',completed_at=?,updated_at=?,match_count=? WHERE id=?", (utc_now(),utc_now(),matched_total,run_id))
                if not rows:
                    break
                revisiting = True
            coverage_rows = []
            old_matched = 0
            if revisiting:
                ids = [int(row['id']) for row in rows]
                marks = ','.join('?' for _ in ids)
                old_matched = int(database.execute(f"SELECT COUNT(DISTINCT capture_id) FROM quick_search_hits WHERE run_id=? AND capture_id IN ({marks})",(run_id,*ids)).fetchone()[0])
            hit_rows: list[tuple[int, int, str, str, int]] = []
            for row in rows:
                if stop_event.is_set():
                    raise Stopped
                capture_id = int(row["id"])
                if not revisiting:
                    last_id = capture_id
                    indexed_checked += 1
                old_mask = int(row['old_mask']) if revisiting else 0
                for index,name in enumerate(('local','unavailable','discarded','missing','non_text','incomplete')):
                    if old_mask & (1 << index):
                        if name == 'local': local_checked -= 1
                        elif name == 'unavailable': unavailable -= 1
                        elif name == 'discarded': discarded -= 1
                        elif name == 'missing': missing -= 1
                        elif name == 'non_text': non_text -= 1
                        else: incomplete -= 1
                before_counts = (local_checked,unavailable,discarded,missing,non_text,incomplete)
                fields_by_pattern: dict[str, set[str]] = defaultdict(set)
                counts_by_pattern: Counter[str] = Counter()

                url_counts = _count_matches(automaton, normalize_search(str(row["original_url"])))
                for pattern, count in url_counts.items():
                    counts_by_pattern[pattern] += count
                    fields_by_pattern[pattern].add("url")

                path, body_identity = _payload_source(row, root)
                data = None
                if body_identity == "discarded":
                    discarded += 1
                elif body_identity == "partial":
                    incomplete += 1
                elif body_identity == "non_text":
                    non_text += 1
                elif body_identity == "missing":
                    missing += 1
                else:
                    try:
                        data = path.read_bytes()
                        body_identity = hashlib.sha256(data).hexdigest()
                    except OSError:
                        body_identity = "missing"
                        missing += 1
                content_fingerprint = _coverage_fingerprint(row, body_identity)
                content_type = str(row["mimetype"] or "")
                if row["detected_encoding"]:
                    content_type += "; charset=" + str(row["detected_encoding"])
                if data is not None and looks_textual_bytes(data[:16384], content_type):
                    local_checked += 1
                    raw = decode_bytes(data, content_type)
                    del data
                    source_counts = _count_matches(automaton, normalize_search(raw))
                    for pattern, count in source_counts.items():
                        counts_by_pattern[pattern] += count
                        fields_by_pattern[pattern].add("source")
                    # Render only when at least one requested literal is still
                    # absent from source. Ambiguous historical text stays eligible
                    # for this fallback so tag-split phrases are not silently lost.
                    if (
                        len(source_counts) < len(normalized_to_display)
                        and _needs_rendered_fallback(
                            raw, content_type, path, str(row["original_url"])
                        )
                    ):
                        _title, visible, links = parse_page(raw, str(row["original_url"]))
                        view = normalize_search(visible + "\n" + "\n".join(links))
                        for pattern, count in _count_matches(automaton, view).items():
                            if pattern not in source_counts:
                                counts_by_pattern[pattern] += count
                                fields_by_pattern[pattern].add("rendered")
                else:
                    unavailable += 1
                    if data is not None:
                        non_text += 1

                after_counts = (local_checked,unavailable,discarded,missing,non_text,incomplete)
                mask = sum((1 << index) for index,(before,after) in enumerate(zip(before_counts,after_counts)) if after>before)
                coverage_rows.append((run_id,capture_id,int(row['body_revision']),mask,content_fingerprint))
                for pattern, count in counts_by_pattern.items():
                    display = normalized_to_display.get(pattern, pattern)
                    hit_rows.append(
                        (run_id, capture_id, display, ",".join(sorted(fields_by_pattern[pattern])), int(count))
                    )

            batch_match_count = len({row[1] for row in hit_rows})
            matched_total += batch_match_count - old_matched
            with database:
                if revisiting:
                    database.execute(f"DELETE FROM quick_search_hits WHERE run_id=? AND capture_id IN ({marks})",(run_id,*ids))
                database.executemany("INSERT OR REPLACE INTO quick_search_coverage(run_id,capture_id,body_revision,coverage_mask,content_fingerprint) VALUES(?,?,?,?,?)",coverage_rows)
                if hit_rows:
                    database.executemany(
                        """INSERT INTO quick_search_hits(run_id,capture_id,keyword,fields,count)
                           VALUES(?,?,?,?,?)
                           ON CONFLICT(run_id,capture_id,keyword) DO UPDATE SET
                           fields=excluded.fields,count=excluded.count""",
                        hit_rows,
                    )
                database.execute(
                    """UPDATE quick_search_runs SET last_capture_id=?,indexed_checked=indexed_checked+?,
                       local_checked=local_checked+?,unavailable_count=unavailable_count+?,
                       discarded_count=discarded_count+?,missing_count=missing_count+?,
                       non_text_count=non_text_count+?,incomplete_count=incomplete_count+?,
                       match_count=?,updated_at=? WHERE id=?""",
                    (last_id, 0 if revisiting else len(rows), local_checked, unavailable, discarded, missing,
                     non_text, incomplete, matched_total, utc_now(), run_id),
                )
            # These are per-loop counters in SQL; reset after checkpoint.
            local_checked = unavailable = discarded = missing = non_text = incomplete = 0
            if callback:
                callback(ProgressEvent(
                    "hitlist", f"Hitlist search {last_id:,}/{total:,}; matching captures {matched_total:,}",
                    last_id, total, {"run_id": run_id, "matches": matched_total},
                ))
    except Stopped:
        with database:
            database.execute(
                    "UPDATE quick_search_runs SET status='interrupted',updated_at=? WHERE id=?",
                (utc_now(), run_id),
            )
        raise

    row = database.execute("SELECT * FROM quick_search_runs WHERE id=?", (run_id,)).fetchone()
    report_dir = Path(root) / "reports" / f"hitlist_{run_id}"
    report_dir.mkdir(parents=True, exist_ok=True)
    csv_path = report_dir / "matches.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["keyword", "original_url", "timestamp", "fields", "count", "local_path"])
        for hit in database.execute(
            """SELECT h.keyword,c.original_url,c.timestamp,h.fields,h.count,COALESCE(c.local_path,d.path,'') AS local_path
               FROM quick_search_hits h JOIN captures c ON c.id=h.capture_id
               LEFT JOIN documents d ON d.id=c.document_id WHERE h.run_id=?
               ORDER BY c.id,h.keyword COLLATE NOCASE""",
            (run_id,),
        ):
            writer.writerow(list(hit))
    summary_path = report_dir / "summary.txt"
    summary_path.write_text(
        "Search with Hitlist\n\n"
        f"Run: {run_id}\n"
        f"Corpus capture ID boundary: {capture_limit} (newer captures require a new search)\n"
        f"Indexed URLs checked: {int(row['indexed_checked'] or 0):,}\n"
        f"Local capture contents checked: {int(row['local_checked'] or 0):,}\n"
        f"Captures without searchable local content: {int(row['unavailable_count'] or 0):,}\n"
        f"Intentionally discarded bodies: {int(row['discarded_count'] or 0):,}\n"
        f"Missing/unreadable bodies: {int(row['missing_count'] or 0):,}\n"
        f"Non-text exclusions: {int(row['non_text_count'] or 0):,}\n"
        f"Partial/incomplete bodies: {int(row['incomplete_count'] or 0):,}\n"
        f"Matching captures: {int(row['match_count'] or 0):,}\n",
        encoding="utf-8",
    )
    return {
        "run_id": run_id,
        "csv": csv_path,
        "summary": summary_path,
        "indexed_checked": int(row["indexed_checked"] or 0),
        "local_checked": int(row["local_checked"] or 0),
        "unavailable": int(row["unavailable_count"] or 0),
        "discarded": int(row["discarded_count"] or 0),
        "missing": int(row["missing_count"] or 0),
        "non_text": int(row["non_text_count"] or 0),
        "incomplete": int(row["incomplete_count"] or 0),
        "matches": int(row["match_count"] or 0),
    }
