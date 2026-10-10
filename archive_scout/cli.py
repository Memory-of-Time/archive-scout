from __future__ import annotations

import argparse
import json
import signal
import sqlite3
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .ai.relevance import run_ai_review
from .ai.settings import resolve_provider_settings
from .config import AIConfig, ProjectConfig, ResearchConfig, load_project_config
from .constants import VERSION
from .events import ProgressEvent, Stopped
from .operations import SUPPORTED_MODES, is_recoverable_pause, run_project
from .research.ai import run_grounded_answer
from .research.search import search_research

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_DEFERRED = 3
EXIT_INTERRUPTED = 130
FORMATS = ("text", "json", "jsonl")
COMMANDS = {"run", "status", "search", "results", "errors", "ai-review", "research", "research-index", "init"}


def _json_default(value: object):
    if isinstance(value, Path):
        return str(value)
    raise TypeError(type(value).__name__)


def _print_json(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default), flush=True)


def _emit_record(format_name: str, record: dict[str, Any]) -> None:
    if format_name in {"json", "jsonl"}:
        _print_json(record)
    else:
        kind = record.get("type")
        if kind == "progress":
            print(str((record.get("event") or {}).get("message") or ""), flush=True)
        elif kind == "result":
            print(str(record.get("message") or record.get("data") or ""), flush=True)
        else:
            print(str(record), flush=True)


def _readonly_database(config: ProjectConfig) -> sqlite3.Connection | None:
    path = config.output_dir / "archive_scout.sqlite3"
    if not path.is_file():
        return None
    database = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=10)
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA query_only=ON")
    return database


def _table_exists(database: sqlite3.Connection, table: str) -> bool:
    return bool(database.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def _count_states(database: sqlite3.Connection, table: str) -> dict[str, int]:
    if not _table_exists(database, table):
        return {}
    return {str(row[0]): int(row[1]) for row in database.execute(f"SELECT state,COUNT(*) FROM {table} GROUP BY state ORDER BY state")}


def _status(config: ProjectConfig) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "version": VERSION,
        "project": str(config.output_dir / "project.json"),
        "output_dir": str(config.output_dir),
        "targets": len(config.targets),
        "database": False,
    }
    database = _readonly_database(config)
    if database is None:
        return payload
    try:
        payload["database"] = True
        payload["captures"] = _count_states(database, "captures")
        payload["media_captures"] = _count_states(database, "media_captures")
        for table, key in (("documents", "documents"), ("document_matches", "matches"), ("errors", "errors"), ("research_vectors", "research_documents")):
            if _table_exists(database, table):
                payload[key] = int(database.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        if _table_exists(database, "operation_runs"):
            row = database.execute(
                "SELECT id,mode,status,message,started_at,updated_at,completed_at,progress_json FROM operation_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                latest = dict(row)
                try:
                    latest["progress"] = json.loads(str(latest.pop("progress_json") or "{}"))
                except Exception:
                    latest["progress"] = {}
                payload["latest_operation"] = latest
        if _table_exists(database, "index_state"):
            payload["indexing"] = {
                "complete": int(database.execute("SELECT COUNT(*) FROM index_state WHERE complete=1").fetchone()[0]),
                "incomplete": int(database.execute("SELECT COUNT(*) FROM index_state WHERE complete=0").fetchone()[0]),
                "seen": int(database.execute("SELECT COALESCE(SUM(seen),0) FROM index_state").fetchone()[0]),
            }
        return payload
    finally:
        database.close()


def _results(config: ProjectConfig, limit: int, scan_run_id: int | None = None) -> list[dict[str, Any]]:
    database = _readonly_database(config)
    if database is None:
        return []
    try:
        if scan_run_id is None:
            row = database.execute("SELECT id FROM scan_runs ORDER BY id DESC LIMIT 1").fetchone()
            if not row:
                return []
            scan_run_id = int(row[0])
        rows = database.execute(
            """
            SELECT m.id AS match_id,m.scan_run_id,m.document_id,m.score,m.hits_json,m.snippets_json,
                   d.title,d.path,c.original_url,c.timestamp,COALESCE(r.status,'unreviewed') AS review_status
            FROM document_matches m JOIN documents d ON d.id=m.document_id JOIN captures c ON c.id=d.capture_id
            LEFT JOIN reviews r ON r.match_id=m.id
            WHERE m.scan_run_id=? AND m.excluded=0 AND m.required_missing=0
            ORDER BY m.score DESC,m.id LIMIT ?
            """,
            (int(scan_run_id), max(1, min(100000, int(limit)))),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        database.close()


def _errors(config: ProjectConfig, limit: int) -> dict[str, Any]:
    database = _readonly_database(config)
    if database is None:
        return {"errors": [], "site_issues": []}
    try:
        errors = [dict(row) for row in database.execute(
            "SELECT * FROM errors ORDER BY resolved,ignored,id DESC LIMIT ?", (max(1, min(100000, int(limit))),)
        )] if _table_exists(database, "errors") else []
        site_issues = [dict(row) for row in database.execute(
            "SELECT * FROM site_issues ORDER BY resolved,last_seen DESC,id DESC LIMIT ?", (max(1, min(100000, int(limit))),)
        )] if _table_exists(database, "site_issues") else []
        return {"errors": errors, "site_issues": site_issues}
    finally:
        database.close()


def _output_collection(format_name: str, kind: str, items: Iterable[dict[str, Any]]) -> None:
    items = list(items)
    if format_name == "json":
        _print_json({kind: items, "count": len(items)})
    elif format_name == "jsonl":
        for item in items:
            _print_json({"type": kind.rstrip("s"), "data": item})
        _print_json({"type": "summary", "count": len(items)})
    else:
        for item in items:
            print(json.dumps(item, ensure_ascii=False, sort_keys=True, default=_json_default))
        print(f"{len(items):,} {kind}")


def _load(path: Path) -> ProjectConfig:
    if not path.is_file():
        raise ValueError(f"project file does not exist: {path}")
    return load_project_config(path)


def _add_project(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("project", type=Path, help="Path to project.json")
    parser.add_argument("--format", choices=FORMATS, default="text")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scout", description="Scout automation CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run or resume an Scout operation")
    _add_project(run)
    run.add_argument("--mode", choices=sorted(SUPPORTED_MODES), default="all")

    status = sub.add_parser("status", help="Read project/queue status without modifying it")
    _add_project(status)

    search = sub.add_parser("search", help="Read-only Research Intelligence search")
    _add_project(search)
    search.add_argument("--query", required=True)
    search.add_argument("--limit", type=int, default=100)

    results = sub.add_parser("results", help="Read deterministic scan results")
    _add_project(results)
    results.add_argument("--scan-run", type=int)
    results.add_argument("--limit", type=int, default=500)

    errors = sub.add_parser("errors", help="Read errors and site issues")
    _add_project(errors)
    errors.add_argument("--limit", type=int, default=1000)

    ai = sub.add_parser("ai-review", help="Run AI relevance review for deterministic report matches")
    _add_project(ai)
    ai.add_argument("--query", default="Identify and rank the strongest evidence relevant to this research project.", help="What you are researching")
    ai.add_argument("--scan-run", type=int)
    ai.add_argument("--limit", type=int, default=50)

    research = sub.add_parser("research", help="Search the evidence index, optionally with grounded AI synthesis")
    _add_project(research)
    research.add_argument("--query", required=True)
    research.add_argument("--limit", type=int, default=100)
    research.add_argument("--ai", action="store_true", help="Add a citation-grounded provider review")

    research_index = sub.add_parser("research-index", help="Build/refresh the local Research Intelligence index")
    _add_project(research_index)

    init = sub.add_parser("init", help="Create project.json without opening the GUI")
    init.add_argument("project", type=Path)
    init.add_argument("--output-dir", type=Path)
    init.add_argument("--target", action="append", default=[])
    init.add_argument("--keyword", action="append", default=[])
    init.add_argument("--from-year", type=int, default=2000)
    init.add_argument("--to-year", type=int, default=datetime.now().year)
    init.add_argument("--ai-provider", choices=("openai", "openrouter"), default="openai")
    init.add_argument("--ai-model", default="")
    init.add_argument("--research-backend", choices=("local-hash", "fastembed"), default="local-hash")
    init.add_argument("--no-auto-research", action="store_true", help="Do not build Research Intelligence automatically after normal scans")
    init.add_argument("--format", choices=FORMATS, default="text")
    return parser


def _run_command(args: argparse.Namespace) -> int:
    config = _load(args.project)
    stop_event = threading.Event()
    interrupted = False
    previous_handler = None

    def handle_interrupt(_signum, _frame) -> None:
        nonlocal interrupted
        interrupted = True
        stop_event.set()

    if threading.current_thread() is threading.main_thread():
        previous_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, handle_interrupt)

    def progress(event: ProgressEvent) -> None:
        record = {"type": "progress", "event": event.to_dict()}
        if args.format == "jsonl":
            _print_json(record)
        elif args.format == "text":
            print(event.message, flush=True)
        # json mode reserves stdout for one final JSON document. Progress is
        # intentionally silent there; bots wanting streaming progress use jsonl.

    try:
        paths = run_project(config, args.mode, stop_event, progress)
        result = {"status": "complete", "mode": args.mode, "paths": {key: str(value) for key, value in paths.items()}}
        if args.format == "jsonl":
            _print_json({"type": "complete", "data": result})
        elif args.format == "json":
            _print_json(result)
        else:
            for name, path in paths.items():
                print(f"{name}: {path}")
        return EXIT_OK
    except Stopped as exc:
        if args.format in {"json", "jsonl"}:
            _print_json({"type": "interrupted", "status": "interrupted", "message": str(exc) or "Stopped by user"})
        else:
            print("Stopped. Progress was saved and can be resumed.", file=sys.stderr)
        return EXIT_INTERRUPTED
    except Exception as exc:
        if is_recoverable_pause(exc):
            if args.format in {"json", "jsonl"}:
                _print_json({"type": "deferred", "status": "deferred", "message": str(exc)})
            else:
                print(f"PAUSED: {exc}", file=sys.stderr)
            return EXIT_DEFERRED
        raise
    finally:
        if previous_handler is not None:
            signal.signal(signal.SIGINT, previous_handler)
        if interrupted:
            stop_event.set()


def _ai_review(args: argparse.Namespace) -> int:
    config = _load(args.project)
    # Credentials remain external. Resolving settings validates the configured
    # provider and returns the environment-backed key without ever printing it.
    settings = resolve_provider_settings(config.ai.provider, config.ai.model, config.ai.request_timeout)
    database_path = config.output_dir / "archive_scout.sqlite3"
    if not database_path.is_file():
        raise ValueError("project database does not exist")
    database = sqlite3.connect(database_path)
    database.row_factory = sqlite3.Row
    try:
        scan_run = args.scan_run
        if scan_run is None:
            row = database.execute("SELECT id FROM scan_runs ORDER BY id DESC LIMIT 1").fetchone()
            if not row:
                raise ValueError("project contains no scan run")
            scan_run = int(row[0])
        # Limit is an automation convenience: use a temporary in-memory config
        # value rather than persisting it to project.json.
        config.ai.candidate_limit = max(1, min(5000, int(args.limit)))
        stop_event = threading.Event()
        run_id = run_ai_review(config, database, int(scan_run), args.query, settings.api_key, stop_event)
        payload = {"status": "complete", "ai_run_id": run_id, "scan_run_id": scan_run, "provider": settings.provider, "model": settings.model}
        if args.format in {"json", "jsonl"}:
            _print_json({"type": "complete", "data": payload} if args.format == "jsonl" else payload)
        else:
            print(f"AI review complete: run {run_id}")
        return EXIT_OK
    finally:
        database.close()


def cli_main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    # Backward compatibility with the original one-command CLI.
    if argv and argv[0] not in COMMANDS and not argv[0].startswith("-"):
        argv.insert(0, "run")
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        if args.command == "run":
            return _run_command(args)
        if args.command == "init":
            project_path = args.project.expanduser().absolute()
            output_dir = (args.output_dir or project_path.parent).expanduser().absolute()
            config = ProjectConfig(
                output_dir=output_dir,
                targets=list(args.target),
                keywords=list(args.keyword),
                from_year=int(args.from_year),
                to_year=int(args.to_year),
                ai=AIConfig(provider=args.ai_provider, model=args.ai_model),
                research=ResearchConfig(vector_backend=args.research_backend, auto_build=not args.no_auto_research),
            ).normalized()
            project_path.parent.mkdir(parents=True, exist_ok=True)
            project_path.write_text(json.dumps(config.to_payload(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            payload = {"status": "created", "project": str(project_path), "output_dir": str(output_dir)}
            if args.format in {"json", "jsonl"}:
                _print_json(payload if args.format == "json" else {"type": "complete", "data": payload})
            else:
                print(f"Created {project_path}")
            return EXIT_OK

        config = _load(args.project)
        if args.command == "status":
            data = _status(config)
            _print_json(data) if args.format in {"json", "jsonl"} else print(json.dumps(data, indent=2, ensure_ascii=False, default=_json_default))
            return EXIT_OK
        if args.command == "results":
            _output_collection(args.format, "results", _results(config, args.limit, args.scan_run))
            return EXIT_OK
        if args.command == "errors":
            data = _errors(config, args.limit)
            if args.format == "json":
                _print_json(data)
            elif args.format == "jsonl":
                for kind in ("errors", "site_issues"):
                    for item in data[kind]:
                        _print_json({"type": kind.rstrip("s"), "data": item})
                _print_json({"type": "summary", "errors": len(data["errors"]), "site_issues": len(data["site_issues"])})
            else:
                print(json.dumps(data, indent=2, ensure_ascii=False, default=_json_default))
            return EXIT_OK
        if args.command == "search":
            database = _readonly_database(config)
            if database is None:
                raise ValueError("project database does not exist")
            try:
                items = [item.to_dict() for item in search_research(config, database, args.query, args.limit, save_query=False)]
            finally:
                database.close()
            _output_collection(args.format, "results", items)
            return EXIT_OK
        if args.command == "research-index":
            args.mode = "research_index"
            return _run_command(args)
        if args.command == "research":
            if args.ai:
                database_path = config.output_dir / "archive_scout.sqlite3"
                if not database_path.is_file():
                    raise ValueError("project database does not exist")
                database = sqlite3.connect(database_path)
                database.row_factory = sqlite3.Row
                try:
                    answer = run_grounded_answer(config, database, args.query)
                    data = answer.to_dict()
                finally:
                    database.close()
                if args.format == "json":
                    _print_json(data)
                elif args.format == "jsonl":
                    _print_json({"type": "answer", "data": data})
                else:
                    print(data["answer"])
                    for claim in data["claims"]:
                        print(f"- {claim['text']} [documents {','.join(map(str, claim['support_ids']))}]")
                return EXIT_OK
            database = _readonly_database(config)
            if database is None:
                raise ValueError("project database does not exist")
            try:
                items = [item.to_dict() for item in search_research(config, database, args.query, args.limit, save_query=False)]
            finally:
                database.close()
            _output_collection(args.format, "results", items)
            return EXIT_OK
        if args.command == "ai-review":
            return _ai_review(args)
        parser.error("unknown command")
        return EXIT_USAGE
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED
    except (ValueError, FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"archive-scout: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except Exception as exc:
        if is_recoverable_pause(exc):
            print(f"archive-scout: deferred: {exc}", file=sys.stderr)
            return EXIT_DEFERRED
        print(f"archive-scout: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_FAILURE


def main() -> None:
    raise SystemExit(cli_main())


if __name__ == "__main__":
    main()
