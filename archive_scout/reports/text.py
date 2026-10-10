from __future__ import annotations

import json
import os
import shutil
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Iterator

from ..config import ProjectConfig
from ..downloads.downloader import replay_url
from ..utils import atomic_write_lines, atomic_write_text, json_value, utc_now

REPORT_NAMES = (
    "matches_ranked.txt",
    "matched_urls.txt",
    "wayback_urls.txt",
    "interesting_links.txt",
    "keyword_counts.txt",
    "all_indexed_urls.txt",
    "errors.txt",
    "site_issues.txt",
    "summary.txt",
)

RANKED_SELECT = """
    SELECT m.*,d.path,d.title,d.size_bytes,c.original_url,c.timestamp,c.mimetype,c.state,c.final_url,
           COALESCE(r.status,'unreviewed') AS review_status,
           COALESCE((SELECT text FROM notes n WHERE n.match_id=m.id ORDER BY n.id LIMIT 1),'') AS note,
           COALESCE((SELECT GROUP_CONCAT(t.name, ', ') FROM match_tags mt JOIN tags t ON t.id=mt.tag_id WHERE mt.match_id=m.id),'') AS tags
    FROM document_matches m
    JOIN documents d ON d.id=m.document_id
    JOIN captures c ON c.id=d.capture_id
    LEFT JOIN reviews r ON r.match_id=m.id
    WHERE m.scan_run_id=? AND m.score>=? AND m.excluded=0 AND m.required_missing=0
    ORDER BY m.score DESC,c.timestamp,c.original_url
"""

MATCH_URL_SELECT = """
    SELECT c.original_url,c.timestamp
    FROM document_matches m
    JOIN documents d ON d.id=m.document_id
    JOIN captures c ON c.id=d.capture_id
    WHERE m.scan_run_id=? AND m.score>=? AND m.excluded=0 AND m.required_missing=0
    ORDER BY m.score DESC,c.timestamp,c.original_url
"""


def safe_run_name(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in "-_" else "-" for character in value.strip())
    return cleaned.strip("-")[:60] or "scan"


def _copy_latest(run_path: Path, latest_path: Path) -> None:
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        latest_path.unlink(missing_ok=True)
        os.link(run_path, latest_path)
    except OSError:
        shutil.copyfile(run_path, latest_path)




def _report_settings(config: ProjectConfig):
    from ..config import ReportConfig
    value = getattr(config, "report", None)
    if isinstance(value, ReportConfig):
        return value.normalized()
    return ReportConfig(**value).normalized() if isinstance(value, dict) else ReportConfig()


def _write_selected(root: Path, run_dir: Path | None, name: str, lines: Iterator[str], paths: dict[str, Path]) -> None:
    destination = (run_dir or root) / (name + ".txt")
    atomic_write_lines(destination, lines)
    if run_dir is not None:
        current = root / (name + ".txt")
        _copy_latest(destination, current)
        paths[name] = current
    else:
        paths[name] = destination


def _indexed_inventory_lines(database: sqlite3.Connection, fields: list[str]) -> Iterator[str]:
    """Streaming classification/routing evidence; generated only when requested."""
    for row in database.execute(
        """SELECT c.timestamp,c.mimetype,c.state,c.original_url,
                  COALESCE(r.resource_class,'unknown') AS resource_class,
                  COALESCE(r.evidence,'not_classified') AS classification_reason,
                  COALESCE(r.routing,'pending') AS routing_decision
           FROM captures c LEFT JOIN capture_routing r ON r.capture_id=c.id
           ORDER BY c.original_url,c.timestamp"""
    ):
        kind=str(row["resource_class"])
        state=str(row["state"])
        route=str(row["routing_decision"])
        retained=state in {"downloaded","downloaded_unscanned"}
        availability="retained" if retained else "not_acquired"
        coverage=("non_text" if kind in {"image","video","audio","other_binary"}
                  else "body_available" if retained else "url_only")
        skip_reason=str(row["classification_reason"]) if state in {"skipped","error"} else ""
        values={"timestamp":str(row["timestamp"] or ""),"mime_type":str(row["mimetype"] or ""),
                "resource_class":kind,"classification_reason":str(row["classification_reason"]),
                "routing_decision":route,"body_coverage":coverage,"state":state,
                "payload_availability":availability,"skip_reason":skip_reason,
                "original_url":str(row["original_url"] or "")}
        yield "\t".join(values[name] for name in fields if name in values)


def generate_index_reports(config: ProjectConfig, database: sqlite3.Connection) -> dict[str, Path]:
    """Index-only reports; disabled outputs never execute their payload queries."""
    opts = _report_settings(config)
    root = config.output_dir / "reports"
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    if opts.output_enabled("all_indexed_urls"):
        _write_selected(root,None,"all_indexed_urls",
                        _indexed_inventory_lines(database,opts.fields_for("all_indexed_urls")),paths)
    if opts.output_enabled("summary"):
        state_counts = {str(row[0]): int(row[1]) for row in database.execute("SELECT state,COUNT(*) FROM captures GROUP BY state")}
        values = {
            "heading": "Scout",
            "generated": f"Generated: {utc_now()}",
            "operation": "Operation: Index URLs only",
            "targets": f"Targets: {', '.join(config.targets) or '(none)'}",
            "date_range": f"Date range: {config.from_date}-{config.to_date}",
            "indexed_captures": f"Indexed captures: {int(database.execute('SELECT COUNT(*) FROM captures').fetchone()[0]):,}",
            "unresolved_errors": f"Unresolved errors: {int(database.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0]):,}",
            "site_issues": f"Open site-specific issues: {int(database.execute('SELECT COUNT(*) FROM site_issues WHERE resolved=0').fetchone()[0]):,}",
            "states": "States: " + ", ".join(f"{key}={value:,}" for key, value in sorted(state_counts.items())),
        }
        _write_selected(root, None, "summary", (values[field] for field in opts.summary_fields if field in values), paths)
    if opts.output_enabled("errors"):
        def errors() -> Iterator[str]:
            for row in database.execute("""SELECT e.*,c.timestamp,c.original_url,d.path FROM errors e
              LEFT JOIN captures c ON c.id=e.capture_id LEFT JOIN documents d ON d.id=e.document_id
              WHERE e.resolved=0 ORDER BY e.operation,e.category,e.last_seen,e.id"""):
                values = _error_fields(row)
                yield "\t".join(values[field] for field in opts.fields_for("errors") if field in values)
        _write_selected(root, None, "errors", errors(), paths)
    if opts.output_enabled("site_issues"):
        _write_selected(root, None, "site_issues", _site_issue_rows(database, opts.fields_for("site_issues")), paths)
    return paths


def _error_fields(row: sqlite3.Row) -> dict[str, str]:
    return {
        "last_seen": str(row["last_seen"] or ""),
        "operation": f"operation={row['operation']}",
        "category": f"category={row['category']}",
        "attempts": f"attempts={row['attempt_count']}",
        "retryable": f"retryable={bool(row['retryable'])}",
        "http_status": f"status={row['http_status'] or ''}",
        "timestamp": str(row["timestamp"] or ""),
        "source": str(row["original_url"] or row["path"] or ""),
        "message": str(row["message"] or ""),
    }


def _site_issue_rows(database: sqlite3.Connection, fields: list[str]) -> Iterator[str]:
    for row in database.execute("""SELECT host,stage,category,http_status,occurrence_count,last_seen,message
       FROM site_issues WHERE resolved=0 ORDER BY last_seen DESC,id DESC"""):
        values = {
            "last_seen": str(row["last_seen"] or ""),
            "host": str(row["host"] or ""),
            "stage": f"stage={row['stage']}",
            "category": f"category={row['category']}",
            "http_status": f"status={int(row['http_status'] or 0) or ''}",
            "occurrences": f"occurrences={int(row['occurrence_count'] or 0)}",
            "message": str(row["message"] or ""),
        }
        yield "\t".join(values[field] for field in fields if field in values)


def generate_reports(config: ProjectConfig, database: sqlite3.Connection, scan_run_id: int) -> dict[str, Path]:
    """Stream exactly the enabled report outputs, without new acquisition work."""
    opts = _report_settings(config)
    run = database.execute("""SELECT sr.*,ks.name AS keyword_set_name,ks.keywords_json
       FROM scan_runs sr JOIN keyword_sets ks ON ks.id=sr.keyword_set_id WHERE sr.id=?""", (scan_run_id,)).fetchone()
    if not run:
        raise RuntimeError(f"scan run {scan_run_id} does not exist")
    root = config.output_dir / "reports"
    run_dir = root / f"scan-{scan_run_id:05d}-{safe_run_name(run['keyword_set_name'])}"
    run_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    sort_sql = {
        "score": "m.score DESC,c.timestamp,c.original_url",
        "oldest": "c.timestamp ASC,m.score DESC,c.original_url",
        "newest": "c.timestamp DESC,m.score DESC,c.original_url",
        "url": "c.original_url ASC,c.timestamp,m.score DESC",
    }[opts.sort_order]
    query = RANKED_SELECT.replace("ORDER BY m.score DESC,c.timestamp,c.original_url", "ORDER BY " + sort_sql)
    match_query = MATCH_URL_SELECT.replace("ORDER BY m.score DESC,c.timestamp,c.original_url", "ORDER BY " + sort_sql)

    need_ranked = opts.output_enabled("matches_ranked")
    need_counts = opts.output_enabled("keyword_counts")
    need_links = opts.output_enabled("interesting_links")
    counts: Counter[str] = Counter()
    if need_links:
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_report_links")
        database.execute("CREATE TEMP TABLE archive_scout_report_links(source TEXT NOT NULL,link TEXT NOT NULL,PRIMARY KEY(source,link)) WITHOUT ROWID")

    def remember(row: sqlite3.Row, hits: dict, links: list) -> None:
        if need_counts:
            counts.update(hits)
        if need_links and links:
            database.executemany("INSERT OR IGNORE INTO archive_scout_report_links(source,link) VALUES(?,?)",
                                 ((str(row["original_url"]), str(link)) for link in links))

    def ranked_lines() -> Iterator[str]:
        for rank, row in enumerate(database.execute(query, (scan_run_id, config.minimum_score)), 1):
            hits = json_value(row["hits_json"], {})
            fields = json_value(row["fields_json"], {})
            snippets = json_value(row["snippets_json"], [])
            links = json_value(row["interesting_links_json"], [])
            remember(row, hits, links)
            if opts.max_matches and rank > opts.max_matches:
                # Any count/link exports must still account for all matches.
                if not (need_counts or need_links):
                    break
                continue
            if opts.snippet_limit:
                snippets = snippets[:opts.snippet_limit]
            if opts.snippet_chars:
                snippets = [text[:opts.snippet_chars] for text in snippets]
            if opts.link_limit:
                links = links[:opts.link_limit]
            hit_lines = [f"{label}={count} [{','.join(fields.get(label, []))}]"
                         for label, count in sorted(hits.items(), key=lambda item: (-item[1], item[0].casefold()))]
            values = {
                "rank": f"RANK: {rank}", "score": f"SCORE: {row['score']}",
                "scan_run": f"SCAN RUN: {scan_run_id}", "timestamp": f"TIMESTAMP: {row['timestamp']}",
                "title": f"TITLE: {row['title'] or '(untitled)'}",
                "original_url": f"ORIGINAL URL: {row['original_url']}",
                "wayback_url": f"WAYBACK URL: {replay_url(row['timestamp'], row['original_url'])}",
                "redirect_destination": f"REDIRECT DESTINATION: {row['final_url'] or ''}",
                "local_file": f"LOCAL FILE: {row['path']}",
                "mime_type": f"MIME TYPE: {row['mimetype'] or '(unknown)'}",
                "review_status": f"REVIEW STATUS: {row['review_status']}",
                "tags": f"TAGS: {row['tags'] or '(none)'}", "note": f"NOTE: {row['note'] or '(none)'}",
                "keyword_hits": f"KEYWORD HITS: {'; '.join(hit_lines) if hit_lines else 'None'}",
                "snippets": "SNIPPETS:\n" + "\n".join(f"  {i}. {text}" for i, text in enumerate(snippets, 1)) if snippets else "SNIPPETS:\n  None",
                "interesting_links": "INTERESTING LINKS:\n" + ("\n".join("  " + str(link) for link in links) if links else "  None"),
            }
            yield "\n".join(["=" * 100] + [values[key] for key in opts.ranked_fields if key in values] + [""])

    if need_ranked:
        _write_selected(root, run_dir, "matches_ranked", ranked_lines(), paths)
    elif need_counts or need_links:
        # Only scan the minimal columns when ranked output is disabled.
        for row in database.execute("""SELECT c.original_url,m.hits_json,m.interesting_links_json
            FROM document_matches m JOIN documents d ON d.id=m.document_id
            JOIN captures c ON c.id=d.capture_id
            WHERE m.scan_run_id=? AND m.score>=? AND m.excluded=0 AND m.required_missing=0""",
            (scan_run_id, config.minimum_score)):
            remember(row, json_value(row["hits_json"], {}), json_value(row["interesting_links_json"], []))

    for name, formatter in (("matched_urls", lambda row: row["original_url"]),
                            ("wayback_urls", lambda row: replay_url(row["timestamp"], row["original_url"]))):
        if not opts.output_enabled(name):
            continue
        if not opts.fields_for(name):
            _write_selected(root, run_dir, name, iter(()), paths)
            continue
        def urls(func=formatter) -> Iterator[str]:
            seen: set[str] = set()
            for row in database.execute(match_query, (scan_run_id, config.minimum_score)):
                value = str(func(row))
                if value not in seen:
                    seen.add(value)
                    yield value
        _write_selected(root, run_dir, name, urls(), paths)
    if need_links:
        fields = opts.fields_for("interesting_links")
        def interesting() -> Iterator[str]:
            for row in database.execute("SELECT source,link FROM archive_scout_report_links ORDER BY source,link"):
                vals = {"source_url": str(row["source"]), "link": str(row["link"])}
                yield "\t".join(vals[field] for field in fields if field in vals)
        _write_selected(root, run_dir, "interesting_links", interesting(), paths)
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_report_links")
    if need_counts:
        fields = opts.fields_for("keyword_counts")
        def keywords() -> Iterator[str]:
            for label, count in counts.most_common():
                vals = {"count": str(count), "keyword": str(label)}
                yield "\t".join(vals[field] for field in fields if field in vals)
        _write_selected(root, run_dir, "keyword_counts", keywords(), paths)
    if opts.output_enabled("all_indexed_urls"):
        _write_selected(root,run_dir,"all_indexed_urls",
                        _indexed_inventory_lines(database,opts.fields_for("all_indexed_urls")),paths)
    if opts.output_enabled("errors"):
        def error_rows() -> Iterator[str]:
            for row in database.execute("""SELECT e.*,c.timestamp,c.original_url,d.path FROM errors e
                 LEFT JOIN captures c ON c.id=e.capture_id LEFT JOIN documents d ON d.id=e.document_id
                 WHERE e.resolved=0 ORDER BY e.operation,e.category,e.last_seen,e.id"""):
                values = _error_fields(row)
                yield "\t".join(values[field] for field in opts.fields_for("errors") if field in values)
        _write_selected(root, run_dir, "errors", error_rows(), paths)
    if opts.output_enabled("site_issues"):
        _write_selected(root, run_dir, "site_issues", _site_issue_rows(database, opts.fields_for("site_issues")), paths)
    if opts.output_enabled("summary"):
        state_counts = {str(row[0]): int(row[1]) for row in database.execute("SELECT state,COUNT(*) FROM captures GROUP BY state")}
        keywords = json.loads(run["keywords_json"])
        values = {
            "heading": "Scout", "generated": f"Generated: {utc_now()}",
            "output_directory": f"Output directory: {config.output_dir}",
            "scan_run": f"Scan run: {scan_run_id}", "keyword_set": f"Keyword set: {run['keyword_set_name']}",
            "keyword_rules": f"Keyword rules: {len(keywords):,}",
            "source_operation": f"Scan source operation: {run['source_operation']}",
            "scan_started": f"Scan started: {run['started_at']}",
            "scan_completed": f"Scan completed: {run['completed_at'] or '(not marked complete)'}",
            "targets": f"Targets: {', '.join(config.targets) or '(project database only)'}",
            "date_range": f"Date range: {config.from_date}-{config.to_date}",
            "indexed_captures": f"Indexed captures: {int(database.execute('SELECT COUNT(*) FROM captures').fetchone()[0]):,}",
            "ranked_matches": f"Ranked matches at score >= {config.minimum_score}: {int(database.execute('SELECT COUNT(*) FROM document_matches WHERE scan_run_id=? AND score>=? AND excluded=0 AND required_missing=0', (scan_run_id, config.minimum_score)).fetchone()[0]):,}",
            "unresolved_errors": f"Unresolved errors: {int(database.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0]):,}",
            "site_issues": f"Open site-specific issues: {int(database.execute('SELECT COUNT(*) FROM site_issues WHERE resolved=0').fetchone()[0]):,}",
            "states": "States: " + ", ".join(f"{key}={value:,}" for key, value in sorted(state_counts.items())),
        }
        _write_selected(root, run_dir, "summary", (values[name] for name in opts.summary_fields if name in values), paths)
    atomic_write_text(root / "latest_scan_run.txt", f"{scan_run_id}\n{run_dir}\n")
    paths["scan_folder"] = run_dir
    return paths
