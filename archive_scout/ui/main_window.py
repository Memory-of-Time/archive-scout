from __future__ import annotations

import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import traceback
import urllib.parse
import webbrowser
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk

from ..ai.relevance import AIReviewError, run_ai_review
from ..ai.reports import generate_ai_reports
from ..cdx.client import RateLimitDeferred
from ..cdx.parameters import build_cdx_params, cdx_year_window
from ..config import AIConfig, AnalysisConfig, KeywordSetConfig, MediaConfig, NetworkConfig, ProjectConfig, ResearchConfig, ReportConfig, REPORT_FIELD_NAMES, REPORT_OUTPUT_NAMES, load_project_config, save_project_config
from ..constants import APP_NAME, CDX_URL, DEFAULT_IMAGE_EXTENSIONS, DEFAULT_VIDEO_EXTENSIONS, OPERATION_MODES, REVIEW_STATUSES, SCOPE_LABELS, VERSION
from ..database.connection import open_database
from ..database.repositories import (
    ai_result_rows,
    delete_scan_run,
    ignore_errors,
    list_ai_runs,
    list_error_categories,
    list_errors,
    list_scan_runs,
    list_site_issues,
    rename_scan_run,
    result_count,
    result_rows,
    resolve_site_issue,
    save_note,
    set_match_tags,
    set_review,
)
from ..defaults import PRESETS
from ..downloads.downloader import replay_url
from ..events import ConnectivityPaused, ProgressEvent, Stopped
from ..operations import run_project
from ..reports.compare import generate_scan_comparison
from ..reports.export import export_review_package, export_scan
from ..reports.text import generate_reports
from ..research.ai import run_grounded_answer
from ..research.search import search_research
from ..runtime import FrozenBundleError, bundled_resource, ensure_frozen_bundle_available
from ..scanning.full_text import search_documents
from ..utils import normalize_cdx_date, normalize_target
from ..projects.backups import list_project_backups, restore_project_backup
from .dashboard import read_dashboard_counts, format_media_policy_summary
from .dashboard_refresh import DashboardRefreshController
from .eta import OperationEtaTracker
from .event_queue import CoalescingEventQueue
from .theme import REVIEW_COLORS, apply_text_theme, apply_theme, review_colors_for, enable_windows_dpi_awareness
from .widgets import ToolTip, ScrollablePage, WheelRouter, ScrollableTree

MODE_LABELS = OPERATION_MODES
MODE_HELP = {
    "all": "Queries CDX, downloads pending text captures, scans only the keyword set marked Use in next scan, and writes reports.",
    "external_media_after_scan": "Indexes the site, downloads and scans all selected text pages, then indexes only external media URLs found in those saved pages and downloads them after discovery finishes.",
    "index": "Queries CDX and stores capture metadata without downloading pages.",
    "download": "Downloads pending text captures and scans them with the keyword set marked Use in next scan.",
    "resume": "Continues interrupted pending work without automatically retrying earlier errors.",
    "rescan": "Reads saved files locally and creates new scan runs without making Wayback requests.",
    "retry_errors": "Retries unresolved text-page and media errors. Valid local text files are rescanned before redownloading.",
    "hitlist": "Searches all indexed URLs and saved text captures with a resumable literal hitlist; writes a CSV and coverage summary without downloading.",
    "report": "Recreates reports from the latest completed scan without downloading or rescanning.",
    "integrity": "Checks saved files and database links without deleting project data.",
    "repair": "Creates a safety backup, repairs stuck states and missing-file records, and rebuilds full-text indexes.",
    "backup": "Creates a consistent SQLite backup inside the project backup folder.",
    "diagnostics": "Exports a diagnostic ZIP with sanitized settings, integrity information, recent errors, and network events.",
    "import_folder": "Imports an existing folder of saved HTML and text pages into this project.",
    "media_all": "Indexes and downloads the selected image and video extensions.",
    "media_index": "Indexes selected image and video URLs without downloading the files.",
    "media_download": "Downloads pending media records already stored in this project.",
    "media_retry": "Retries only unresolved media download errors.",
    "analysis": "Reconstructs forum threads, extracts identifiers and legacy embeds, clusters duplicates, compares snapshots, and builds provenance reports.",
    "research_index": "Builds or refreshes the local Research Intelligence vector, entity, duplicate, and evidence-relationship index without making Wayback requests.",
    "forum_rebuild": "Rebuilds forum threads and posts from saved pages without rerunning the rest of the archive analysis.",
    "merge_project": "Merges captures, downloads, scans, reviews, notes, tags, media, and extraction results from another Scout project.",
}
REVIEW_LABELS = {
    "Unreviewed": "unreviewed",
    "Relevant": "relevant",
    "Possibly relevant": "possibly_relevant",
    "False positive": "false_positive",
    "Duplicate": "duplicate",
    "Dead end": "dead_end",
    "Needs follow-up": "needs_follow_up",
}


def app_support_dir() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / APP_NAME
    if os.name == "nt":
        return Path(os.environ.get("APPDATA", Path.home())) / APP_NAME
    return Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "archive-scout"


def open_path(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if sys.platform == "darwin":
        subprocess.Popen(["open", str(path)])
    elif os.name == "nt":
        os.startfile(str(path))
    else:
        for command in ("xdg-open", "gio"):
            if shutil.which(command):
                subprocess.Popen([command, str(path)] if command == "xdg-open" else [command, "open", str(path)])
                return
        raise RuntimeError("No desktop file opener was found: " + str(path))


def enforce_active_keyword_set_selection(
    keyword_sets: list[dict], active_index: int, enabled: bool
) -> None:
    """Make the open GUI keyword set the only set eligible for the next scan.

    Older/imported project files can contain several ``selected=True`` sets.
    The GUI exposes one checkbox labelled "Use in next scan", so treating old
    hidden selections as additional scan jobs is surprising and expensive.  A
    checked active set therefore becomes exclusive; an unchecked active set
    leaves no GUI-selected set for the next scan.  The sets themselves and all
    their rules remain saved.
    """
    if not (0 <= int(active_index) < len(keyword_sets)):
        return
    for index, item in enumerate(keyword_sets):
        item["selected"] = bool(enabled and index == int(active_index))


class ArchiveScoutApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(f"{APP_NAME} {VERSION}")
        self._app_icon: tk.PhotoImage | None = None
        self.apply_application_icon()
        self._set_initial_geometry()
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.events: CoalescingEventQueue[ProgressEvent] = CoalescingEventQueue(max_events=256)
        self.stop_event = threading.Event()
        self.worker_thread: threading.Thread | None = None
        self.last_paths: dict[str, Path] = {}
        self.keyword_sets: list[dict] = []
        self.current_keyword_set = -1
        self.scan_run_map: dict[str, int] = {}
        self.result_row_map: dict[str, dict] = {}
        self.error_row_map: dict[str, dict] = {}
        self.site_issue_row_map: dict[str, dict] = {}
        self.ai_run_map: dict[str, int] = {}
        self.ai_result_row_map: dict[str, dict] = {}
        self.research_result_row_map: dict[str, dict] = {}
        self.result_sort_column = "score"
        self.result_sort_reverse = True
        self.target_settings: dict[str, dict] = {}
        self.loaded_project_config: ProjectConfig | None = None
        self.nav_buttons: dict[str, ttk.Button] = {}
        self.page_names: list[str] = []
        self.result_page = 0
        self.result_page_size = 500
        self.dashboard_refresh_job: str | None = None
        self.log_line_count = 0
        self.dashboard_refresh = DashboardRefreshController(mode="manual")
        self._eta_tracker = OperationEtaTracker()
        self.active_operation_media_policy = None
        self.create_variables()
        self.resolved_theme, self.colors = apply_theme(self, self.theme_var.get(), float(self.font_scale_var.get()))
        self.create_ui()
        WheelRouter.ensure(self)
        first_run = not self.state_path().exists()
        self.load_app_state()
        self.invalidate_dashboard()  # Display selected project; leave all counts unqueried.
        self.apply_interface_theme()
        self.output_var.trace_add("write", lambda *_args: self.invalidate_dashboard())
        self.after(100, self.process_events)
        self._reschedule_dashboard_refresh(initial=True)
        if first_run:
            self.after(450, self.show_welcome)


    def apply_application_icon(self) -> None:
        try:
            png_path = bundled_resource("assets", "scout.png")
            if png_path.exists():
                self._app_icon = tk.PhotoImage(file=str(png_path))
                self.iconphoto(True, self._app_icon)
            if os.name == "nt":
                ico_path = bundled_resource("assets", "scout.ico")
                if ico_path.exists():
                    self.iconbitmap(default=str(ico_path))
        except (OSError, tk.TclError):
            self._app_icon = None

    def create_variables(self) -> None:
        default_output = Path.home() / "Downloads" / "Scout"
        self.output_var = tk.StringVar(value=str(default_output))
        self.preset_var = tk.StringVar(value="Ogrish 9/11 research")
        self.mode_var = tk.StringVar(value="Index, download, scan, and report")
        self.operation_help_var = tk.StringVar(value=MODE_HELP["all"])
        self.scope_var = tk.StringVar(value="All archived text pages (thorough)")
        self.from_date_var = tk.StringVar(value="2001")
        self.to_date_var = tk.StringVar(value="2010")
        self.cdx_match_type_var = tk.StringVar(value="Automatic")
        self.collapse_urlkey_var = tk.BooleanVar(value=True)
        self.collapse_digest_var = tk.BooleanVar(value=False)
        self.page_size_var = tk.StringVar(value="100000")
        self.workers_var = tk.StringVar(value="10")
        self.max_file_var = tk.StringVar(value="25")
        self.minimum_score_var = tk.StringVar(value="1")
        self.cdx_delay_var = tk.StringVar(value="2.5")
        self.download_delay_var = tk.StringVar(value="0.125")
        self.rate_limit_base_var = tk.StringVar(value="5")
        self.rate_limit_max_var = tk.StringVar(value="5")
        self.rate_limit_wait_var = tk.StringVar(value="15")
        self.status_var = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0)
        self.theme_var = tk.StringVar(value="System")
        self.interface_mode_var = tk.StringVar(value="Simple")
        self.font_scale_var = tk.StringVar(value="1.0")
        self.network_backend_var = tk.StringVar(value="auto")
        self.network_endpoint_var = tk.StringVar(value="auto")
        self.network_strategy_var = tk.StringVar(value="auto")
        self.network_page_blocks_var = tk.StringVar(value="0")
        self.network_cdx_workers_var = tk.StringVar(value="10")
        self.network_trust_env_var = tk.BooleanVar(value=True)
        self.network_persistent_var = tk.BooleanVar(value=True)
        self.network_retry_base_var = tk.StringVar(value="5")
        self.network_retry_max_var = tk.StringVar(value="120")
        self.network_failure_limit_var = tk.StringVar(value="8")
        self.auto_backup_var = tk.BooleanVar(value=True)
        self.backup_keep_var = tk.StringVar(value="5")
        self.import_source_var = tk.StringVar()
        self.dashboard_project_var = tk.StringVar(value="No project opened")
        self.dashboard_captures_var = tk.StringVar(value="—")
        self.dashboard_documents_var = tk.StringVar(value="—")
        self.dashboard_matches_var = tk.StringVar(value="—")
        self.dashboard_errors_var = tk.StringVar(value="—")
        self.result_page_var = tk.StringVar(value="Page 1")
        self.keyword_set_var = tk.StringVar()
        self.keyword_set_selected_var = tk.BooleanVar(value=True)
        self.report_sort_var = tk.StringVar(value="score")
        self.report_max_var = tk.StringVar(value="0")
        self.report_snippets_var = tk.StringVar(value="0")
        self.report_chars_var = tk.StringVar(value="0")
        self.report_links_var = tk.StringVar(value="0")
        self.report_retain_var = tk.BooleanVar(value=True)
        self.report_output_vars = {name: tk.BooleanVar(value=True) for name in REPORT_FIELD_NAMES}
        self.report_field_vars = {name: {field: tk.BooleanVar(value=True) for field in fields}
                                  for name, fields in REPORT_FIELD_NAMES.items()}
        self.report_field_output = tk.StringVar(value="matches_ranked")
        self.media_enabled_var = tk.BooleanVar(value=False)
        self.media_images_var = tk.BooleanVar(value=True)
        self.media_videos_var = tk.BooleanVar(value=True)
        self.media_embedded_var = tk.BooleanVar(value=True)
        self.media_external_var = tk.BooleanVar(value=False)
        self.media_strategy_var = tk.StringVar(value="earliest")
        self.media_max_var = tk.StringVar(value="500")
        self.media_preserve_var = tk.BooleanVar(value=False)
        self.result_scan_var = tk.StringVar()
        self.result_filter_var = tk.StringVar()
        self.result_review_filter_var = tk.StringVar(value="All")
        self.result_min_var = tk.StringVar(value="1")
        self.review_status_var = tk.StringVar(value="Unreviewed")
        self.review_tags_var = tk.StringVar()
        self.fts_query_var = tk.StringVar()
        self.fts_field_var = tk.StringVar(value="all")
        self.fts_domain_var = tk.StringVar()
        self.ai_scan_var = tk.StringVar()
        self.ai_run_var = tk.StringVar()
        self.ai_api_key_var = tk.StringVar()
        self.ai_provider_var = tk.StringVar(value="openai")
        self.ai_model_var = tk.StringVar(value="gpt-5-mini")
        self.ai_candidate_limit_var = tk.StringVar(value="200")
        self.ai_batch_size_var = tk.StringVar(value="8")
        self.ai_min_relevance_var = tk.StringVar(value="50")
        self.ai_excerpt_chars_var = tk.StringVar(value="5000")
        self.research_query_var = tk.StringVar()
        self.research_backend_var = tk.StringVar(value="local-hash")
        self.research_auto_var = tk.BooleanVar(value=False)
        self.research_limit_var = tk.StringVar(value="100")
        self.error_category_var = tk.StringVar(value="All")
        self.forum_profile_var = tk.StringVar(value="auto")
        self.analysis_threads_var = tk.BooleanVar(value=True)
        self.analysis_embeds_var = tk.BooleanVar(value=True)
        self.analysis_external_var = tk.BooleanVar(value=False)
        self.analysis_external_limit_var = tk.StringVar(value="5000")
        self.analysis_duplicate_var = tk.StringVar(value="0.90")
        self.analysis_compare_var = tk.BooleanVar(value=True)
        self.analysis_provenance_var = tk.BooleanVar(value=True)
        self.analysis_merge_source_var = tk.StringVar(value="")
        self.hitlist_file_var = tk.StringVar()
        self.target_override_status_var = tk.StringVar(value="No per-target override on the current line.")
        self.error_detail_var = tk.StringVar()
        self.scan_workers_var = tk.StringVar(value="0")
        self.scan_backend_var = tk.StringVar(value="auto")
        self.scan_overlap_var = tk.BooleanVar(value=True)
        self.scan_memory_mb_var = tk.StringVar(value="256")
        self.download_external_redirects_var = tk.BooleanVar(value=False)
        self.backup_max_var = tk.StringVar(value="1024")
        self.dashboard_recovery_var = tk.StringVar(value="0")
        self.dashboard_skipped_non_text_var = tk.StringVar(value="0")
        self.dashboard_skipped_url_var = tk.StringVar(value="0")
        self.dashboard_pending_var = tk.StringVar(value="0")
        self.dashboard_waiting_scan_var = tk.StringVar(value="0")
        self.dashboard_media_candidates_var = tk.StringVar(value="0")
        self.dashboard_media_selected_var = tk.StringVar(value="0")
        self.dashboard_media_pending_var = tk.StringVar(value="0")
        self.dashboard_media_downloaded_var = tk.StringVar(value="0")
        self.dashboard_media_excluded_var = tk.StringVar(value="0")
        self.dashboard_media_errors_var = tk.StringVar(value="0")
        self.dashboard_media_deferred_var = tk.StringVar(value="0")
        self.dashboard_media_policy_var = tk.StringVar(value="Supplemental media is disabled for text runs.")
        self.dashboard_refresh_mode_var = tk.StringVar(value="manual")
        self.dashboard_refresh_seconds_var = tk.StringVar(value="10")
        self.dashboard_eta_enabled_var = tk.BooleanVar(value=False)
        self.dashboard_eta_var = tk.StringVar(value="Estimated time remaining: off")
        self.dashboard_last_refresh_var = tk.StringVar(value="Not refreshed yet")
        self.dashboard_failure_summary_var = tk.StringVar(value="Failures: not refreshed")
        self.text_retention_var = tk.StringVar(value="Keep downloaded text files")
        self.error_status_filter_var = tk.StringVar(value="Open")
        self.retry_include_unavailable_var = tk.BooleanVar(value=False)
        self.error_page_var = tk.StringVar(value="Page 1")
        self.history_page_var = tk.StringVar(value="Page 1")
        self.history_page = self.error_page = 0
        self.history_page_size = self.error_page_size = 100
        self.history_row_map = {}

    def create_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        header = ttk.Frame(self, padding=(16, 12, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(1, weight=1)
        ttk.Label(header, text=APP_NAME, style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(header, text="A resilient Wayback research workspace", style="Muted.TLabel").grid(row=1, column=0, sticky="w")
        controls = ttk.Frame(header)
        controls.grid(row=0, column=1, rowspan=2, sticky="e")
        ttk.Label(controls, text="Mode:").grid(row=0, column=0, padx=(0, 4))
        mode_box = ttk.Combobox(controls, textvariable=self.interface_mode_var, values=("Simple", "Advanced"), state="readonly", width=10)
        mode_box.grid(row=0, column=1, padx=(0, 10))
        mode_box.bind("<<ComboboxSelected>>", lambda _e: self.refresh_navigation())
        ttk.Label(controls, text="Theme:").grid(row=0, column=2, padx=(0, 4))
        theme_box = ttk.Combobox(controls, textvariable=self.theme_var, values=("System", "Light", "Dark"), state="readonly", width=9)
        theme_box.grid(row=0, column=3, padx=(0, 10))
        theme_box.bind("<<ComboboxSelected>>", lambda _e: self.apply_interface_theme())
        ttk.Label(controls, text="Preset:").grid(row=0, column=4, padx=(0, 4))
        preset = ttk.Combobox(controls, textvariable=self.preset_var, values=list(PRESETS), state="readonly", width=24)
        preset.grid(row=0, column=5)
        preset.bind("<<ComboboxSelected>>", lambda _event: self.apply_preset())

        project = ttk.Frame(self, padding=(16, 8))
        project.grid(row=1, column=0, sticky="ew")
        project.columnconfigure(1, weight=1)
        ttk.Label(project, text="Project folder:").grid(row=0, column=0, sticky="w")
        ttk.Entry(project, textvariable=self.output_var).grid(row=0, column=1, sticky="ew", padx=8)
        ttk.Button(project, text="Browse…", command=self.choose_output).grid(row=0, column=2)
        ttk.Button(project, text="Open", command=self.open_output).grid(row=0, column=3, padx=(6, 0))
        ttk.Label(project, text="Operation:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        operation = ttk.Combobox(project, textvariable=self.mode_var, values=list(MODE_LABELS), state="readonly")
        operation.grid(row=1, column=1, columnspan=3, sticky="ew", padx=(8, 0), pady=(8, 0))
        operation.bind("<<ComboboxSelected>>", lambda _event: self.update_operation_help())
        ttk.Label(project, textvariable=self.operation_help_var, wraplength=1040, style="Muted.TLabel").grid(row=3, column=0, columnspan=4, sticky="w", pady=(6, 0))

        workspace = ttk.Frame(self)
        workspace.grid(row=2, column=0, sticky="nsew", padx=(14, 14), pady=6)
        workspace.columnconfigure(1, weight=1)
        workspace.rowconfigure(0, weight=1)
        self.sidebar_page = ScrollablePage(workspace, padding=(6, 10), frame_style="Sidebar.TFrame")
        self.sidebar_page.grid(row=0, column=0, sticky="nsw", padx=(0, 8))
        self.sidebar_page.configure(width=224)
        # Child label/button requested widths must not resize the entire sidebar.
        # Keep the navigation comfortably wide while giving smaller windows the
        # remaining width for the selected workspace.
        self.sidebar_page.grid_propagate(False)
        self.sidebar = self.sidebar_page.body
        self.sidebar.columnconfigure(0, weight=1)
        ttk.Label(self.sidebar, text="WORKSPACE", style="Sidebar.TLabel", font="TkHeadingFont").pack(fill="x", padx=10, pady=(0, 8))
        self.nav_container = ttk.Frame(self.sidebar, style="Sidebar.TFrame")
        self.nav_container.pack(fill="both", expand=True)
        ttk.Separator(self.sidebar).pack(fill="x", padx=8, pady=10)
        self.version_label = ttk.Label(self.sidebar, text=f"Version {VERSION}", style="Sidebar.TLabel")
        self.version_label.pack(fill="x", padx=10, pady=(0, 4))

        self.notebook = ttk.Notebook(workspace, style="Sidebar.TNotebook")
        self.notebook.grid(row=0, column=1, sticky="nsew")
        self.create_dashboard_tab()
        self.create_targets_tab()
        self.create_keywords_tab()
        self.create_cdx_tab()
        self.create_media_tab()
        self.create_reports_tab()
        self.create_analysis_tab()
        self.create_settings_tab()
        self.create_results_tab()
        self.create_ai_tab()
        self.create_research_tab()
        self.create_history_tab()
        self.create_errors_tab()
        self.create_activity_tab()
        self.notebook.bind("<<NotebookTabChanged>>", lambda _e: self.update_navigation_selection())
        self.refresh_navigation()

        footer = ttk.Frame(self, padding=(14, 8, 14, 12), style="Panel.TFrame")
        footer.grid(row=3, column=0, sticky="ew")
        footer.columnconfigure(2, weight=1)
        self.start_button = ttk.Button(footer, text="Start operation", command=self.start, style="Accent.TButton")
        self.start_button.grid(row=0, column=0)
        self.stop_button = ttk.Button(footer, text="Pause & save", command=self.stop, state="disabled")
        self.stop_button.grid(row=0, column=1, padx=(8, 0))
        ttk.Label(footer, textvariable=self.status_var, style="Status.TLabel", anchor="w").grid(row=0, column=2, sticky="ew", padx=10)
        ttk.Button(footer, text="Save", command=self.save_project).grid(row=0, column=3, padx=3)
        ttk.Button(footer, text="Load…", command=self.load_project).grid(row=0, column=4, padx=3)
        ttk.Button(footer, text="Reports", command=self.open_reports).grid(row=0, column=5, padx=3)
        self.progress = ttk.Progressbar(footer, variable=self.progress_var, maximum=100)
        self.progress.grid(row=1, column=0, columnspan=6, sticky="ew", pady=(8, 0))
        ToolTip(self.stop_button, "Stops after the current request and preserves the exact pending queue for Resume.")
        self.apply_preset()
        self.bind_shortcuts()

    def create_dashboard_tab(self) -> None:
        page = ScrollablePage(self.notebook, padding=16)
        tab = page.body
        tab.columnconfigure((0, 1, 2, 3), weight=1)
        tab.rowconfigure(5, weight=1)
        self.notebook.add(page, text="Dashboard")
        ttk.Label(tab, text="Project dashboard", style="Section.TLabel").grid(row=0, column=0, columnspan=4, sticky="w")
        ttk.Label(tab, textvariable=self.dashboard_project_var, style="Muted.TLabel").grid(row=1, column=0, columnspan=4, sticky="w", pady=(2, 12))
        cards = (
            ("Indexed captures", self.dashboard_captures_var),
            ("Saved documents", self.dashboard_documents_var),
            ("Ranked matches", self.dashboard_matches_var),
            ("Open errors", self.dashboard_errors_var),
        )
        for column, (title, variable) in enumerate(cards):
            card = ttk.Frame(tab, padding=14, style="Panel.TFrame")
            card.grid(row=2, column=column, sticky="ew", padx=(0 if column == 0 else 6, 6 if column < 3 else 0))
            ttk.Label(card, text=title, style="CardTitle.TLabel").pack(anchor="w")
            ttk.Label(card, textvariable=variable, style="CardValue.TLabel").pack(anchor="w", pady=(5, 0))
        status = ttk.Frame(tab, padding=(4, 10, 4, 0))
        status.grid(row=3, column=0, columnspan=4, sticky="ew")
        ttk.Label(status, text="Pending text:").pack(side="left")
        ttk.Label(status, textvariable=self.dashboard_pending_var).pack(side="left", padx=(4, 16))
        ttk.Label(status, text="Saved unscanned:").pack(side="left")
        ttk.Label(status, textvariable=self.dashboard_waiting_scan_var).pack(side="left", padx=(4, 16))
        ttk.Label(status, text="Skipped non-text:").pack(side="left")
        ttk.Label(status, textvariable=self.dashboard_skipped_non_text_var).pack(side="left", padx=(4, 16))
        ttk.Label(status, text="Skipped URL filter:").pack(side="left")
        ttk.Label(status, textvariable=self.dashboard_skipped_url_var).pack(side="left", padx=(4, 16))
        ttk.Label(status, text="Recovery events:").pack(side="left")
        ttk.Label(status, textvariable=self.dashboard_recovery_var).pack(side="left", padx=(4, 0))
        media_status = ttk.LabelFrame(tab, text="Supplemental media", padding=10)
        media_status.grid(row=4, column=0, columnspan=4, sticky="ew", pady=(14, 0))
        media_counts = ttk.Frame(media_status)
        media_counts.pack(fill="x")
        for title, variable in (
            ("Candidates", self.dashboard_media_candidates_var),
            ("Selected", self.dashboard_media_selected_var),
            ("Pending/in-flight", self.dashboard_media_pending_var),
            ("Downloaded", self.dashboard_media_downloaded_var),
            ("Excluded", self.dashboard_media_excluded_var),
            ("Errors", self.dashboard_media_errors_var),
            ("Deferred from text", self.dashboard_media_deferred_var),
        ):
            item = ttk.Frame(media_counts)
            item.pack(side="left", padx=(0, 18))
            ttk.Label(item, text=title, style="Muted.TLabel").pack(anchor="w")
            ttk.Label(item, textvariable=variable).pack(anchor="w")
        ttk.Label(
            media_status, textvariable=self.dashboard_media_policy_var, style="Muted.TLabel",
            justify="left", wraplength=1100,
        ).pack(fill="x", anchor="w", pady=(8, 0))

        outcomes = ttk.LabelFrame(tab, text="Other outcomes", padding=10)
        outcomes.grid(row=5, column=0, columnspan=4, sticky="ew", pady=(14, 0))
        ttk.Label(outcomes, textvariable=self.dashboard_failure_summary_var, justify="left", wraplength=1080).pack(fill="x")
        ttk.Label(outcomes, text="Skips and deferred media are intentional outcomes; open error records can include several records for one failed capture.", style="Muted.TLabel", wraplength=1080).pack(fill="x", pady=(4,0))

        actions = ttk.LabelFrame(tab, text="Project maintenance", padding=12)
        actions.grid(row=6, column=0, columnspan=2, sticky="nsew", pady=(14, 0), padx=(0, 6))
        refresh_controls = ttk.Frame(actions)
        refresh_controls.pack(fill="x", pady=(0, 6))
        ttk.Label(refresh_controls, text="Dashboard refresh:").grid(row=0, column=0, sticky="w")
        refresh_mode = ttk.Combobox(
            refresh_controls, textvariable=self.dashboard_refresh_mode_var,
            values=("auto", "manual"), state="readonly", width=8,
        )
        refresh_mode.grid(row=0, column=1, padx=(6, 4))
        refresh_interval = ttk.Combobox(
            refresh_controls, textvariable=self.dashboard_refresh_seconds_var,
            values=("5", "10", "30", "60"), width=6,
        )
        refresh_interval.grid(row=0, column=2, padx=4)
        ttk.Label(refresh_controls, text="seconds").grid(row=0, column=3, sticky="w")
        refresh_mode.bind("<<ComboboxSelected>>", lambda _e: self._dashboard_refresh_settings_changed())
        refresh_interval.bind("<<ComboboxSelected>>", lambda _e: self._dashboard_refresh_settings_changed())
        refresh_interval.bind("<FocusOut>", lambda _e: self._dashboard_refresh_settings_changed())
        ttk.Label(actions, textvariable=self.dashboard_last_refresh_var, style="Muted.TLabel").pack(anchor="w", pady=(0, 4))
        ttk.Button(actions, text="Refresh dashboard", command=lambda: self.refresh_dashboard(manual=True)).pack(fill="x", pady=3)
        ttk.Button(actions, text="Create backup", command=lambda: self.start(self.build_config(require_keywords=False), "backup")).pack(fill="x", pady=3)
        ttk.Button(actions, text="Restore a backup…", command=self.restore_backup_ui).pack(fill="x", pady=3)
        ttk.Button(actions, text="Check integrity", command=lambda: self.start(self.build_config(require_keywords=False), "integrity")).pack(fill="x", pady=3)
        ttk.Button(actions, text="Repair and rebuild indexes", command=lambda: self.start(self.build_config(require_keywords=False), "repair")).pack(fill="x", pady=3)
        ttk.Button(actions, text="Export diagnostics", command=lambda: self.start(self.build_config(require_keywords=False), "diagnostics")).pack(fill="x", pady=3)
        quick = ttk.LabelFrame(tab, text="Quick start", padding=12)
        quick.grid(row=6, column=2, columnspan=2, sticky="nsew", pady=(14, 0), padx=(6, 0))
        quick_text = (
            "1. Add one or more sites.\n"
            "2. Choose or import keyword sets.\n"
            "3. Select a date range.\n"
            "4. Start the operation.\n\n"
            "Network failures are retried through several independent connection methods. "
            "Progress is written to the project database before any long wait."
        )
        ttk.Label(quick, text=quick_text, justify="left", wraplength=430).pack(anchor="nw")
        ttk.Button(quick, text="Go to Sites and paths", command=lambda: self.show_page("Sites and paths")).pack(anchor="w", pady=(14, 4))
        ttk.Button(quick, text="Go to Network settings", command=lambda: self.show_page("Settings")).pack(anchor="w", pady=4)
        eta_frame = ttk.LabelFrame(tab, text="Estimated time remaining", padding=10)
        eta_frame.grid(row=7, column=0, columnspan=4, sticky="ew", pady=(14, 0))
        ttk.Checkbutton(eta_frame, text="Show estimated time remaining for operations",
                        variable=self.dashboard_eta_enabled_var,
                        command=self._eta_settings_changed).pack(anchor="w")
        ttk.Label(eta_frame, textvariable=self.dashboard_eta_var, wraplength=1080).pack(anchor="w", pady=(6, 0))
        ttk.Label(eta_frame, text="Uses recent completed work for the current phase. Unknown totals and later phases remain Estimating.",
                  style="Muted.TLabel", wraplength=1080).pack(anchor="w", pady=(4, 0))
        self.dashboard_tab = tab

    def bind_shortcuts(self) -> None:
        modifier = "Command" if sys.platform == "darwin" else "Control"
        self.bind_all(f"<{modifier}-s>", lambda _e: self.save_project())
        self.bind_all(f"<{modifier}-o>", lambda _e: self.load_project())
        self.bind_all(f"<{modifier}-Return>", lambda _e: self.start())
        self.bind_all("<Escape>", lambda _e: self.stop() if self.worker_thread and self.worker_thread.is_alive() else None)
        self.bind_all(f"<{modifier}-f>", lambda _e: self.show_page("Results and search"))

    def refresh_navigation(self) -> None:
        allowed_simple = {"Dashboard", "Sites and paths", "Keyword sets", "Media", "Reports", "Archive analysis", "Results and search", "AI relevance", "Research intelligence", "Errors", "Activity"}
        for button in self.nav_buttons.values():
            button.destroy()
        self.nav_buttons.clear()
        self.page_names = [self.notebook.tab(tab_id, "text") for tab_id in self.notebook.tabs()]
        nav_parent = self.__dict__.get("nav_container", self.sidebar)
        for name in self.page_names:
            if self.interface_mode_var.get() == "Simple" and name not in allowed_simple:
                continue
            button = ttk.Button(nav_parent, text=name, style="Sidebar.TButton", command=lambda value=name: self.show_page(value))
            button.pack(fill="x", pady=1)
            self.nav_buttons[name] = button
        if "version_label" in self.__dict__:
            self.version_label.configure(text=f"Version {VERSION}")
        selected = self.notebook.tab(self.notebook.select(), "text") if self.notebook.select() else "Dashboard"
        if self.interface_mode_var.get() == "Simple" and selected not in allowed_simple:
            self.show_page("Dashboard")
        if "sidebar_page" in self.__dict__:
            self.sidebar_page._queue_region()
        self.update_navigation_selection()

    def update_navigation_selection(self) -> None:
        if not getattr(self, "notebook", None) or not self.notebook.select():
            return
        selected = self.notebook.tab(self.notebook.select(), "text")
        for name, button in self.nav_buttons.items():
            button.configure(style="SidebarActive.TButton" if name == selected else "Sidebar.TButton")

    def show_page(self, name: str) -> None:
        for tab_id in self.notebook.tabs():
            if self.notebook.tab(tab_id, "text") == name:
                self.notebook.select(tab_id)
                self.update_navigation_selection()
                return

    def apply_interface_theme(self) -> None:
        try:
            self.resolved_theme, self.colors = apply_theme(self, self.theme_var.get(), float(self.font_scale_var.get() or 1.0))
            apply_text_theme(self, self.colors)
            if hasattr(self, "sidebar_page"):
                self.sidebar_page.canvas.configure(background=self.colors["sidebar"])
            self.update_navigation_selection()
            if hasattr(self, "results_tree"):
                for status, color in review_colors_for(self.resolved_theme).items():
                    self.results_tree.tag_configure(status, background=color)
        except Exception as exc:
            self.status_var.set(f"Could not apply theme: {exc}")

    def show_welcome(self) -> None:
        dialog = tk.Toplevel(self)
        dialog.title("Welcome to Scout")
        dialog.transient(self)
        dialog.grab_set()
        dialog.geometry("620x420")
        frame = ttk.Frame(dialog, padding=24)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text="Welcome to Scout", style="Title.TLabel").pack(anchor="w")
        ttk.Label(frame, text="Built for long-running public web-archive research", style="Muted.TLabel").pack(anchor="w", pady=(0, 18))
        ttk.Label(frame, text="The Simple workspace keeps the common workflow visible. Advanced mode exposes CDX, media, analysis, network, review, repair, and migration controls. Scout saves its indexing queue continuously and can switch between multiple HTTP connection methods when one stack cannot reach the Internet Archive.", wraplength=550, justify="left").pack(anchor="w")
        ttk.Button(frame, text="Open the dashboard", style="Accent.TButton", command=dialog.destroy).pack(anchor="e", pady=(24, 0))

    def invalidate_dashboard(self) -> None:
        root = Path(self.output_var.get()).expanduser()
        self.dashboard_project_var.set(str(root))
        self.dashboard_refresh.switch_project()
        for name in ("captures", "documents", "matches", "errors", "recovery", "skipped_non_text", "skipped_url", "pending", "waiting_scan", "media_candidates", "media_selected", "media_pending", "media_downloaded", "media_excluded", "media_errors", "media_deferred"):
            variable = self.__dict__.get("dashboard_" + name + "_var")
            if variable is not None:
                variable.set("—")
        self.dashboard_last_refresh_var.set("Choose Refresh dashboard to load persisted totals.")
        self.dashboard_failure_summary_var.set("No project totals loaded.")
        self._refresh_dashboard_media_policy_summary()

    def refresh_dashboard(self, manual: bool = True) -> None:
        if self.__dict__.get("project_restore_identity") == self.project_identity():
            return
        root = Path(self.output_var.get()).expanduser()
        self.dashboard_project_var.set(str(root))
        token = self.dashboard_refresh.begin(time.monotonic(), manual=manual)
        if token is None:
            self.dashboard_last_refresh_var.set("Dashboard refresh already running; please wait.")
            return
        self.dashboard_last_refresh_var.set("Refreshing persisted totals (database snapshot)…")
        database_path = root / "archive_scout.sqlite3"
        events = self.events

        def worker() -> None:
            try:
                counts = read_dashboard_counts(database_path, max_query_seconds=20.0 if manual else 2.0)
                events.put(("dashboard", (token, str(root), counts, "")))
            except Exception as exc:
                events.put(("dashboard", (token, str(root), None, str(exc))))

        threading.Thread(target=worker, name="archive-dashboard-read", daemon=True).start()

    def restore_backup_ui(self) -> None:
        root = Path(self.output_var.get()).expanduser()
        backups = list_project_backups(root)
        selected = filedialog.askopenfilename(
            title="Restore Scout database backup",
            initialdir=str(root / "backups"),
            filetypes=[("SQLite database", "*.sqlite3"), ("All files", "*")],
        )
        if not selected:
            return
        if not messagebox.askyesno(APP_NAME, "Restore this database backup? A safety copy of the current database will be created first."):
            return
        try:
            safety = restore_project_backup(root, Path(selected))
            messagebox.showinfo(APP_NAME, f"Backup restored. Safety copy:\n{safety}")
            self.invalidate_dashboard()
            self.refresh_history()
            self.refresh_results(reset_page=True)
            self.refresh_errors()
        except Exception as exc:
            messagebox.showerror(APP_NAME, f"Could not restore backup:\n{exc}")

    def create_targets_tab(self) -> None:
        page = ScrollablePage(self.notebook, padding=10)
        tab = page.body
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(1, weight=1)
        self.notebook.add(page, text="Sites and paths")
        ttk.Label(tab, text="One Wayback target per line. Examples: example.com/* or forum.example.com/path/*").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.targets_text = tk.Text(tab, wrap="none", undo=True, font="TkFixedFont")
        self.targets_text.grid(row=1, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(tab, orient="vertical", command=self.targets_text.yview)
        scroll.grid(row=1, column=1, sticky="ns")
        hscroll = ttk.Scrollbar(tab, orient="horizontal", command=self.targets_text.xview)
        hscroll.grid(row=2, column=0, sticky="ew")
        self.targets_text.configure(yscrollcommand=scroll.set, xscrollcommand=hscroll.set)
        controls = ttk.Frame(tab)
        controls.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        controls.columnconfigure(1, weight=1)
        ttk.Button(controls, text="Configure current target…", command=self.configure_target_settings).grid(row=0, column=0, sticky="w")
        ttk.Label(controls, text="Per-target settings are honored in both Simple and Advanced modes; slower pacing can never exceed the shared Wayback request rate.", style="Muted.TLabel", wraplength=820).grid(row=0, column=1, sticky="w", padx=(10, 0))
        ttk.Label(controls, textvariable=self.target_override_status_var, style="Muted.TLabel", wraplength=1000).grid(row=1, column=0, columnspan=2, sticky="w", pady=(5, 0))
        self.targets_text.bind("<KeyRelease>", lambda _e: self._refresh_target_override_status(), add=True)
        self.targets_text.bind("<ButtonRelease-1>", lambda _e: self._refresh_target_override_status(), add=True)

    def configure_target_settings(self) -> None:
        try:
            index = self.targets_text.index("insert")
            line = self.targets_text.get(f"{index.split('.')[0]}.0", f"{index.split('.')[0]}.end").strip()
        except Exception:
            line = ""
        if not line:
            messagebox.showinfo(APP_NAME, "Place the cursor on a target line first.")
            return
        existing = dict(self.target_settings.get(line) or {})
        dialog = tk.Toplevel(self)
        dialog.title(f"Target settings — {line}")
        dialog.transient(self)
        dialog.grab_set()
        frame = ttk.Frame(dialog, padding=16)
        frame.pack(fill="both", expand=True)
        fields = {
            "from_date": tk.StringVar(value=str(existing.get("from_date", ""))),
            "to_date": tk.StringVar(value=str(existing.get("to_date", ""))),
            "cdx_match_type": tk.StringVar(value=str(existing.get("cdx_match_type", ""))),
            "page_size": tk.StringVar(value=str(existing.get("page_size", ""))),
            "workers": tk.StringVar(value=str(existing.get("workers", ""))),
            "cdx_delay": tk.StringVar(value=str(existing.get("cdx_delay", ""))),
            "download_delay": tk.StringVar(value=str(existing.get("download_delay", ""))),
        }
        labels = {
            "from_date": "Start date", "to_date": "End date", "cdx_match_type": "matchType",
            "page_size": "CDX result limit", "workers": "Download workers",
            "cdx_delay": "CDX delay", "download_delay": "Download delay",
        }
        for row, key in enumerate(fields):
            ttk.Label(frame, text=labels[key] + ":").grid(row=row, column=0, sticky="w", pady=4)
            ttk.Entry(frame, textvariable=fields[key], width=28).grid(row=row, column=1, sticky="ew", padx=(10, 0), pady=4)
        def save() -> None:
            values = {key: variable.get().strip() for key, variable in fields.items() if variable.get().strip()}
            for key in ("page_size", "workers"):
                if key in values:
                    values[key] = int(values[key])
            for key in ("cdx_delay", "download_delay"):
                if key in values:
                    values[key] = float(values[key])
            self.target_settings[line] = values
            dialog.destroy()
        buttons = ttk.Frame(frame)
        buttons.grid(row=len(fields), column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(buttons, text="Clear override", command=lambda: (self.target_settings.pop(line, None), dialog.destroy())).pack(side="left", padx=4)
        ttk.Button(buttons, text="Save", command=save, style="Accent.TButton").pack(side="left", padx=4)

    def create_keywords_tab(self) -> None:
        page = ScrollablePage(self.notebook, padding=10)
        tab = page.body
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(2, weight=1)
        self.notebook.add(page, text="Keyword sets")
        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        controls.columnconfigure(1, weight=1)
        ttk.Label(controls, text="Set:").grid(row=0, column=0)
        self.keyword_set_box = ttk.Combobox(controls, textvariable=self.keyword_set_var, state="readonly")
        self.keyword_set_box.grid(row=0, column=1, sticky="ew", padx=6)
        self.keyword_set_box.bind("<<ComboboxSelected>>", self.switch_keyword_set)
        ttk.Checkbutton(controls, text="Use in next scan", variable=self.keyword_set_selected_var, command=self.save_current_keyword_set).grid(row=0, column=2, padx=6)
        ttk.Button(controls, text="New", command=self.new_keyword_set).grid(row=0, column=3, padx=2)
        ttk.Button(controls, text="Duplicate", command=self.duplicate_keyword_set).grid(row=0, column=4, padx=2)
        ttk.Button(controls, text="Delete", command=self.delete_keyword_set).grid(row=0, column=5, padx=2)
        ttk.Button(controls, text="Import…", command=self.import_keyword_set).grid(row=0, column=6, padx=2)
        ttk.Button(controls, text="Export…", command=self.export_keyword_set).grid(row=0, column=7, padx=2)
        ttk.Label(
            tab,
            text="One rule per line. Prefixes: required:, exclude:, exact:, regex:, high:. Options: | weight=3 | whole | case | label=Name. Only the open set can be marked Use in next scan; enabling it deselects older/imported sets.",
            wraplength=1000,
        ).grid(row=1, column=0, sticky="w", pady=(0, 6))
        self.keywords_text = tk.Text(tab, wrap="none", undo=True, font="TkFixedFont")
        self.keywords_text.grid(row=2, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(tab, orient="vertical", command=self.keywords_text.yview)
        scroll.grid(row=2, column=1, sticky="ns")
        hscroll = ttk.Scrollbar(tab, orient="horizontal", command=self.keywords_text.xview)
        hscroll.grid(row=3, column=0, sticky="ew")
        self.keywords_text.configure(yscrollcommand=scroll.set, xscrollcommand=hscroll.set)

    def create_cdx_tab(self) -> None:
        page = ScrollablePage(self.notebook, padding=12)
        tab = page.body
        tab.columnconfigure(1, weight=1)
        tab.rowconfigure(6, weight=1)
        self.notebook.add(page, text="CDX options")
        ttk.Label(tab, text="Start date:").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(tab, textvariable=self.from_date_var, width=22).grid(row=0, column=1, sticky="w", padx=(10, 0), pady=4)
        ttk.Label(tab, text="End date:").grid(row=1, column=0, sticky="w", pady=4)
        ttk.Entry(tab, textvariable=self.to_date_var, width=22).grid(row=1, column=1, sticky="w", padx=(10, 0), pady=4)
        ttk.Label(tab, text="Accepted: YYYY, YYYYMM, YYYYMMDD, YYYYMMDDhhmmss, MM/DD/YYYY, or YYYY-MM-DD.").grid(row=2, column=1, sticky="w", padx=(10, 0))
        ttk.Label(tab, text="matchType:").grid(row=3, column=0, sticky="w", pady=(10, 4))
        ttk.Combobox(tab, textvariable=self.cdx_match_type_var, values=("Automatic", "exact", "prefix", "host", "domain"), state="readonly", width=19).grid(row=3, column=1, sticky="w", padx=(10, 0), pady=(10, 4))
        collapse = ttk.Frame(tab)
        collapse.grid(row=4, column=1, sticky="w", padx=(10, 0), pady=4)
        ttk.Checkbutton(collapse, text="collapse=urlkey", variable=self.collapse_urlkey_var).grid(row=0, column=0)
        ttk.Checkbutton(collapse, text="collapse=digest", variable=self.collapse_digest_var).grid(row=0, column=1, padx=(18, 0))
        ttk.Label(tab, text="Resume batch size (CDX rows):").grid(row=5, column=0, sticky="w", pady=4)
        ttk.Entry(tab, textvariable=self.page_size_var, width=22).grid(row=5, column=1, sticky="w", padx=(10, 0), pady=4)
        options = ttk.Frame(tab)
        options.grid(row=6, column=0, columnspan=2, sticky="nsew", pady=(10, 0))
        options.columnconfigure(0, weight=1)
        options.columnconfigure(1, weight=1)
        options.rowconfigure(1, weight=1)
        ttk.Label(options, text="Filters, one per line").grid(row=0, column=0, sticky="w")
        ttk.Label(options, text="Additional key=value parameters").grid(row=0, column=1, sticky="w", padx=(12, 0))
        self.cdx_filters_text = tk.Text(options, height=8, wrap="none", font="TkFixedFont")
        self.cdx_filters_text.grid(row=1, column=0, sticky="nsew", pady=(4, 0))
        self.cdx_extra_text = tk.Text(options, height=8, wrap="none", font="TkFixedFont")
        self.cdx_extra_text.grid(row=1, column=1, sticky="nsew", padx=(12, 0), pady=(4, 0))
        ttk.Button(tab, text="Preview CDX request", command=self.preview_cdx).grid(row=8, column=0, columnspan=2, sticky="w", pady=(10, 0))

    def create_media_tab(self) -> None:
        page = ScrollablePage(self.notebook, padding=10, horizontal=True)
        tab = page.body
        tab.columnconfigure(0, weight=1)
        tab.columnconfigure(1, weight=1)
        tab.rowconfigure(2, weight=1)
        self.notebook.add(page, text="Media")
        options = ttk.Frame(tab)
        options.grid(row=0, column=0, columnspan=2, sticky="ew")
        ttk.Checkbutton(options, text="Also download media during a full text run", variable=self.media_enabled_var).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(options, text="Images", variable=self.media_images_var).grid(row=0, column=1, padx=(12, 0), sticky="w")
        ttk.Checkbutton(options, text="Videos", variable=self.media_videos_var).grid(row=0, column=2, padx=(8, 0), sticky="w")
        ttk.Checkbutton(options, text="Discover media linked inside saved pages", variable=self.media_embedded_var).grid(row=1, column=0, columnspan=2, sticky="w", pady=(7, 0))
        ttk.Checkbutton(options, text="Allow external hosts", variable=self.media_external_var).grid(row=1, column=2, sticky="w", pady=(7, 0))
        ttk.Label(
            tab,
            text="The ‘Index, download, scan, then download external embedded media’ operation waits until every saved text page has been scanned before it looks up and downloads external image/video links found in those pages.",
            wraplength=520,
            style="Muted.TLabel",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        editors = ttk.Frame(tab)
        editors.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(10, 0))
        editors.rowconfigure(1, weight=1)
        for column in range(3):
            editors.columnconfigure(column, weight=1, uniform="media_editors")
        descriptions = (
            "Media sites/paths (blank uses Sites and paths)",
            "Include extensions",
            "Exclude extensions",
        )
        for column, description in enumerate(descriptions):
            ttk.Label(editors, text=description, wraplength=160).grid(
                row=0, column=column, sticky="w", padx=(0 if column == 0 else 8, 0), pady=(0, 4)
            )
        self.media_targets_text = tk.Text(editors, wrap="none", font="TkFixedFont", width=12, height=10)
        self.media_include_text = tk.Text(editors, wrap="none", font="TkFixedFont", width=12, height=10)
        self.media_exclude_text = tk.Text(editors, wrap="none", font="TkFixedFont", width=12, height=10)
        for column, widget in enumerate((self.media_targets_text, self.media_include_text, self.media_exclude_text)):
            widget.grid(row=1, column=column, sticky="nsew", padx=(0 if column == 0 else 8, 0))
        settings = ttk.Frame(tab)
        settings.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        ttk.Label(settings, text="Snapshot:").grid(row=0, column=0)
        ttk.Combobox(settings, textvariable=self.media_strategy_var, values=("earliest", "latest", "all"), state="readonly", width=10).grid(row=0, column=1, padx=(5, 15))
        ttk.Label(settings, text="Maximum media size (MB):").grid(row=0, column=2)
        ttk.Entry(settings, textvariable=self.media_max_var, width=10).grid(row=0, column=3, padx=(5, 15))
        ttk.Label(settings, text="Media layout: media/images and media/videos (flat)").grid(
            row=1, column=0, columnspan=4, sticky="w", pady=(8, 0)
        )


    def create_reports_tab(self) -> None:
        page = ScrollablePage(self.notebook, padding=10)
        tab = page.body
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(3, weight=1)
        self.notebook.add(page, text="Reports")
        ttk.Label(tab, text="Report contents", style="Section.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            tab,
            text=(
                "Customize generated text-scan, index, media and archive-analysis reports. "
                "These settings never change which pages are acquired or matched, and never erase saved scan evidence. "
                "Use Regenerate reports in Scan history to apply them without downloading again. "
                "Index reports are written when indexing finishes or is paused. Match reports require a scan; "
                "use All indexed URLs for an index-only inventory. "
                "Hitlist, AI and manual export formats are separate operation outputs."
            ),
            wraplength=1080,
            style="Muted.TLabel",
        ).grid(row=1, column=0, sticky="ew", pady=(4, 10))

        controls = ttk.Frame(tab)
        controls.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        for column, (label, variable) in enumerate((
            ("Minimum score", self.minimum_score_var),
            ("Match limit", self.report_max_var),
            ("Snippets / match", self.report_snippets_var),
            ("Snippet characters", self.report_chars_var),
            ("Links / match", self.report_links_var),
        )):
            ttk.Label(controls, text=label).grid(row=0, column=column, sticky="w", padx=(0, 10))
            ttk.Entry(controls, textvariable=variable, width=12).grid(row=1, column=column, sticky="w", padx=(0, 10))
        ttk.Label(controls, text="Sort matches").grid(row=0, column=5, sticky="w")
        ttk.Combobox(controls, textvariable=self.report_sort_var, values=("score", "oldest", "newest", "url"), state="readonly", width=12).grid(row=1, column=5, sticky="w")
        ttk.Label(controls, text="Limits: 0 = all available. Match sorting/limits apply consistently to all match-derived reports.", style="Muted.TLabel").grid(row=2, column=0, columnspan=6, sticky="w", pady=4)
        ttk.Checkbutton(controls, text="Retain full scan details for later reports, review and AI (recommended)", variable=self.report_retain_var).grid(row=3, column=0, columnspan=6, sticky="w")
        ttk.Label(controls, text="Unchecked: future scans omit unused enrichment; enabling it later requires a local rescan.", wraplength=1000, style="Muted.TLabel").grid(row=4, column=0, columnspan=6, sticky="w")
        presets = ttk.Frame(controls)
        presets.grid(row=5, column=0, columnspan=6, sticky="w", pady=(6, 0))
        for label, preset in (("All files and fields", "all"), ("Matched URLs only", "urls"), ("Indexed URLs only", "index_urls"), ("No generated reports", "none")):
            ttk.Button(presets, text=label, command=lambda value=preset: self.set_report_preset(value)).pack(side="left", padx=(0, 8))

        book = ttk.Notebook(tab)
        book.grid(row=3, column=0, sticky="nsew")
        groups = (
            ("Text and index", ("matches_ranked", "matched_urls", "wayback_urls", "interesting_links", "keyword_counts", "all_indexed_urls", "summary")),
            ("Errors", ("errors", "site_issues")),
            ("Media", ("media_indexed", "media_downloaded", "media_wayback_urls", "media_errors", "media_summary")),
            ("Archive analysis", ("analysis_summary", "forum_threads", "extractions", "legacy_assets", "duplicate_groups", "provenance", "snapshot_diffs", "first_appearances")),
        )
        output_labels = {
            "matches_ranked": "Ranked matches (matches_ranked.txt)",
            "matched_urls": "Matched URLs (matched_urls.txt)",
            "wayback_urls": "Wayback URLs (wayback_urls.txt)",
            "interesting_links": "Interesting Links (interesting_links.txt)",
            "keyword_counts": "Keyword counts (keyword_counts.txt)",
            "all_indexed_urls": "All indexed URLs (all_indexed_urls.txt)",
            "errors": "Errors (errors.txt)",
            "site_issues": "Site issues (site_issues.txt)",
            "summary": "Text/index summary (summary.txt)",
            "media_indexed": "Indexed media (media_indexed.txt)",
            "media_downloaded": "Downloaded media (media_downloaded.txt)",
            "media_wayback_urls": "Media Wayback URLs (media_wayback_urls.txt)",
            "media_errors": "Media errors (media_errors.txt)",
            "media_summary": "Media summary (media_summary.txt)",
            "analysis_summary": "Analysis summary (analysis_summary.txt)",
            "forum_threads": "Forum threads (forum_threads.tsv)",
            "extractions": "Extractions (extractions.tsv)",
            "legacy_assets": "Legacy assets (legacy_assets.tsv)",
            "duplicate_groups": "Duplicate groups (duplicate_groups.tsv)",
            "provenance": "Provenance (provenance.tsv)",
            "snapshot_diffs": "Snapshot diffs (snapshot_diffs.tsv)",
            "first_appearances": "First appearances (first_appearances.tsv)",
        }

        for group_label, names in groups:
            container = ScrollablePage(book, padding=(8, 8, 8, 28), horizontal=True)
            page = container.body
            boxes = []
            page.columnconfigure(0, weight=1)
            page.columnconfigure(1, weight=1)
            book.add(container, text=group_label)
            for index, name in enumerate(names):
                row, column = divmod(index, 2)
                box = ttk.LabelFrame(page, text=output_labels[name], padding=7)
                box.grid(row=row, column=column, sticky="nsew", padx=(0, 6) if column == 0 else (6, 0), pady=5)
                box.columnconfigure((0, 1, 2, 3), weight=1)
                fields = []
                boxes.append((box, fields))
                ttk.Checkbutton(box, text="Generate this file", variable=self.report_output_vars[name]).grid(
                    row=0, column=0, columnspan=2, sticky="w"
                )
                ttk.Button(
                    box, text="All fields", width=10,
                    command=lambda value=name: self.set_report_fields(value, True),
                ).grid(row=0, column=2, sticky="e", padx=2)
                ttk.Button(
                    box, text="No fields", width=10,
                    command=lambda value=name: self.set_report_fields(value, False),
                ).grid(row=0, column=3, sticky="e", padx=2)
                for field_index, field_name in enumerate(REPORT_FIELD_NAMES[name]):
                    field = ttk.Checkbutton(box, text=field_name.replace("_", " ").title(), variable=self.report_field_vars[name][field_name])
                    field.grid(row=1 + field_index // 4, column=field_index % 4, sticky="w", padx=(0,8), pady=2)
                    fields.append(field)

            def reflow(event, items=boxes, surface=container):
                try:
                    scale = max(0.75, float(surface.canvas.winfo_fpixels("1p")) / (4 / 3))
                except (tk.TclError, AttributeError):
                    scale = 1.0
                width = max(1, int(event.width))
                groups = 2 if width >= int(1080 * scale) else 1
                surface.body.columnconfigure(1, weight=1 if groups == 2 else 0)
                field_columns = 4 if width / groups >= 540 * scale else 2 if width >= 330 * scale else 1
                for index, (box, fields) in enumerate(items):
                    row, column = divmod(index, groups)
                    box.grid_configure(row=row, column=column, padx=(0,6) if groups == 2 and column == 0 else (6,0) if groups == 2 else 0)
                    for field_index, field in enumerate(fields):
                        field.grid_configure(row=1 + field_index // field_columns, column=(field_index % field_columns) * (4 // field_columns), columnspan=4 // field_columns)
                surface._queue_region()
            container.canvas.bind("<Configure>", reflow, add=True)

    def refresh_report_fields(self) -> None:
        if not hasattr(self, "report_fields_frame"):
            return
        for child in self.report_fields_frame.winfo_children():
            child.destroy()
        output = self.report_field_output.get()
        supported = {
            "matches_ranked": set(REPORT_FIELD_NAMES["matches_ranked"]),
            "matched_urls": {"original_url"},
            "wayback_urls": {"wayback_url"},
            "interesting_links": {"source_url", "link"},
            "keyword_counts": {"count", "keyword"},
            "all_indexed_urls": {"timestamp", "mime_type", "state", "original_url"},
            "errors": set(REPORT_FIELD_NAMES["errors"]),
            "site_issues": set(REPORT_FIELD_NAMES["site_issues"]),
            "summary": set(REPORT_FIELD_NAMES["summary"]) - {"bodies_searched"},
        }.get(output, set())
        for index, (name, variable) in enumerate(self.report_field_vars.get(output, {}).items()):
            available = name in supported
            label = name.replace("_", " ") if available else name.replace("_", " ") + " (unavailable in schema 7)"
            ttk.Checkbutton(self.report_fields_frame, text=label, variable=variable,
                            state="normal" if available else "disabled").grid(
                row=index // 3, column=index % 3, sticky="w", padx=(0, 12), pady=2)

    def set_report_preset(self, preset: str) -> None:
        for name, variable in self.report_output_vars.items():
            variable.set(preset == "all" or (preset == "urls" and name == "matched_urls")
                         or (preset == "index_urls" and name == "all_indexed_urls"))
            if preset in {"all", "urls"}:
                self.set_report_fields(name, True)
            elif preset == "index_urls" and name == "all_indexed_urls":
                for field_name, field_variable in self.report_field_vars.get(name, {}).items():
                    field_variable.set(field_name == "original_url")

    def current_report_config(self) -> ReportConfig:
        return ReportConfig(
            outputs=[name for name, variable in self.report_output_vars.items() if variable.get()],
            fields={name: [field for field, variable in fields.items() if variable.get()]
                    for name, fields in self.report_field_vars.items()},
            retain_scan_details=self.report_retain_var.get(),
            sort_order=self.report_sort_var.get(),
            max_matches=int(self.report_max_var.get()),
            snippet_limit=int(self.report_snippets_var.get()),
            snippet_chars=int(self.report_chars_var.get()),
            link_limit=int(self.report_links_var.get()),
        ).normalized()

    def apply_report_config(self, report: ReportConfig) -> None:
        report = report.normalized()
        self.report_sort_var.set(report.sort_order)
        self.report_max_var.set(str(report.max_matches))
        self.report_snippets_var.set(str(report.snippet_limit))
        self.report_chars_var.set(str(report.snippet_chars))
        self.report_links_var.set(str(report.link_limit))
        self.report_retain_var.set(report.retain_scan_details)
        for name, variable in self.report_output_vars.items():
            variable.set(name in report.outputs)
        for name, fields in self.report_field_vars.items():
            for field, variable in fields.items():
                variable.set(field in report.fields_for(name))
        self.refresh_report_fields()

    def create_analysis_tab(self) -> None:
        page = ScrollablePage(self.notebook, padding=10)
        tab = page.body
        tab.columnconfigure(0, weight=1)
        tab.columnconfigure(1, weight=1)
        tab.rowconfigure(3, weight=1)
        self.notebook.add(page, text="Archive analysis")

        top = ttk.Frame(tab)
        top.grid(row=0, column=0, columnspan=2, sticky="ew")
        ttk.Checkbutton(top, text="Reconstruct forum threads and posts", variable=self.analysis_threads_var).grid(row=0, column=0, sticky="w")
        ttk.Label(top, text="Forum profile:").grid(row=0, column=1, padx=(18, 4))
        ttk.Combobox(
            top,
            textvariable=self.forum_profile_var,
            values=("auto", "generic", "vbulletin", "phpbb", "invision", "futaba", "2channel"),
            state="readonly",
            width=12,
        ).grid(row=0, column=2)
        ttk.Checkbutton(top, text="Recover legacy embeds and players", variable=self.analysis_embeds_var).grid(row=0, column=3, padx=(18, 0))
        ttk.Checkbutton(top, text="Compare snapshots", variable=self.analysis_compare_var).grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Checkbutton(top, text="Build source-to-mirror provenance", variable=self.analysis_provenance_var).grid(row=1, column=1, columnspan=2, sticky="w", padx=(18, 0), pady=(6, 0))
        ttk.Label(top, text="Near-duplicate threshold:").grid(row=1, column=3, sticky="e", padx=(18, 4), pady=(6, 0))
        ttk.Entry(top, textvariable=self.analysis_duplicate_var, width=8).grid(row=1, column=4, sticky="w", pady=(6, 0))

        external = ttk.Frame(tab)
        external.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 6))
        external.columnconfigure(4, weight=1)
        ttk.Checkbutton(external, text="Search Wayback for discovered external assets", variable=self.analysis_external_var).grid(row=0, column=0, sticky="w")
        ttk.Label(external, text="Maximum lookups:").grid(row=0, column=1, padx=(14, 4))
        ttk.Entry(external, textvariable=self.analysis_external_limit_var, width=9).grid(row=0, column=2)
        ttk.Label(external, text="Only explicitly allowed domains are searched.").grid(row=0, column=3, padx=(14, 0), sticky="w")

        labels = ttk.Frame(tab)
        labels.grid(row=2, column=0, columnspan=2, sticky="ew")
        labels.columnconfigure(0, weight=1)
        labels.columnconfigure(1, weight=1)
        ttk.Label(labels, text="Custom extractors: name :: regex or name :: field :: regex").grid(row=0, column=0, sticky="w")
        ttk.Label(labels, text="Allowed external domains, one per line").grid(row=0, column=1, sticky="w", padx=(12, 0))

        editors = ttk.Frame(tab)
        editors.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(4, 8))
        editors.columnconfigure(0, weight=1)
        editors.columnconfigure(1, weight=1)
        editors.rowconfigure(0, weight=1)
        self.analysis_extractors_text = tk.Text(editors, wrap="none", font="TkFixedFont")
        self.analysis_extractors_text.grid(row=0, column=0, sticky="nsew")
        self.analysis_domains_text = tk.Text(editors, wrap="none", font="TkFixedFont")
        self.analysis_domains_text.grid(row=0, column=1, sticky="nsew", padx=(12, 0))

        merge = ttk.LabelFrame(tab, text="Project and shared-review merge", padding=8)
        merge.grid(row=4, column=0, columnspan=2, sticky="ew")
        merge.columnconfigure(1, weight=1)
        ttk.Label(merge, text="Source project folder:").grid(row=0, column=0, sticky="w")
        ttk.Entry(merge, textvariable=self.analysis_merge_source_var).grid(row=0, column=1, sticky="ew", padx=8)
        ttk.Button(merge, text="Browse…", command=self.choose_merge_source).grid(row=0, column=2)
        ttk.Label(
            merge,
            text="Choose the ‘Merge another Scout project’ operation to copy captures, documents, media, scan history, reviews, notes, tags, and extraction results into this project.",
            wraplength=920,
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(6, 0))

    def choose_merge_source(self) -> None:
        selected = filedialog.askdirectory(title="Choose Scout project to merge")
        if selected:
            self.analysis_merge_source_var.set(selected)

    def create_settings_tab(self) -> None:
        page = ScrollablePage(self.notebook, padding=14)
        tab = page.body
        tab.columnconfigure(0, weight=1)
        tab.columnconfigure(1, weight=1)
        self.notebook.add(page, text="Settings")

        performance = ttk.LabelFrame(tab, text="Performance and acquisition", padding=10)
        performance.grid(row=0, column=0, sticky="nsew", padx=(0, 6), pady=(0, 8))
        performance.columnconfigure(1, weight=1)
        perf_rows = [
            ("Download workers (10 = fast default)", self.workers_var),
            
            
            ("Maximum text-page budget (MB)", self.max_file_var),
            ("Index/CDX spacing (2.5 sec = 24/min shared ceiling)", self.cdx_delay_var),
            ("Replay spacing (0.125 sec = 8/sec shared ceiling)", self.download_delay_var),
        ]
        for row, (label, variable) in enumerate(perf_rows):
            ttk.Label(performance, text=label + ":", wraplength=360).grid(row=row, column=0, sticky="w", pady=3)
            ttk.Entry(performance, textvariable=variable, width=18).grid(row=row, column=1, sticky="ew", padx=(8, 0), pady=3)
        row = len(perf_rows)
        ttk.Label(performance, text="Download scope:").grid(row=row, column=0, sticky="w", pady=3)
        ttk.Combobox(performance, textvariable=self.scope_var, values=list(SCOPE_LABELS), state="readonly").grid(row=row, column=1, sticky="ew", padx=(8, 0), pady=3)
        ttk.Label(
            performance,
            text="The existing download scope and size budget apply. Text/media classification follows this repository’s routing rules.",
            style="Muted.TLabel", wraplength=520, justify="left",
        ).grid(row=row + 1, column=0, columnspan=2, sticky="ew", pady=(6, 0))

        connection = ttk.LabelFrame(tab, text="Connection and indexing", padding=10)
        connection.grid(row=0, column=1, sticky="nsew", padx=(6, 0), pady=(0, 8))
        connection.columnconfigure(1, weight=1)
        network_rows = [
            ("Connection backend", self.network_backend_var, ("auto", "httpx", "urllib3", "curl")),
            ("CDX endpoint", self.network_endpoint_var, ("auto", "cdx", "timemap")),
            ("Index strategy (auto = resume first)", self.network_strategy_var, ("auto", "paged", "resume")),
        ]
        for row, (label, variable, values) in enumerate(network_rows):
            ttk.Label(connection, text=label + ":", wraplength=300).grid(row=row, column=0, sticky="w", pady=3)
            ttk.Combobox(connection, textvariable=variable, values=values, state="readonly").grid(row=row, column=1, sticky="ew", padx=(8, 0), pady=3)
        for row, (label, variable) in enumerate((("Parallel CDX requests", self.network_cdx_workers_var), ("Page blocks (custom paged mode only)", self.network_page_blocks_var)), start=3):
            ttk.Label(connection, text=label + ":", wraplength=300).grid(row=row, column=0, sticky="w", pady=3)
            ttk.Entry(connection, textvariable=variable, width=18).grid(row=row, column=1, sticky="ew", padx=(8, 0), pady=3)
        ttk.Checkbutton(connection, text="Honor system proxy and certificate environment", variable=self.network_trust_env_var).grid(row=5, column=0, columnspan=2, sticky="w", pady=3)
        ttk.Label(connection, text="Auto starts with resumable requests. Paged mode uses bounded native JSON requests and saves completed pages independently.", style="Muted.TLabel", wraplength=520, justify="left").grid(row=6, column=0, columnspan=2, sticky="ew", pady=(6, 0))

        recovery = ttk.LabelFrame(tab, text="Automatic archive recovery", padding=10)
        recovery.grid(row=1, column=0, sticky="nsew", padx=(0, 6), pady=8)
        recovery.columnconfigure(1, weight=1)
        recovery_rows = [
            ("Per-capture retry base (seconds)", self.network_retry_base_var),
            ("Per-capture retry ceiling (seconds)", self.network_retry_max_var),
            ("Failures before shared automatic recovery", self.network_failure_limit_var),
            ("429/503 retry wait (seconds)", self.rate_limit_base_var),
            ("429/503 maximum retry wait (seconds)", self.rate_limit_max_var),
        ]
        for row, (label, variable) in enumerate(recovery_rows):
            ttk.Label(recovery, text=label + ":", wraplength=340).grid(row=row, column=0, sticky="w", pady=3)
            ttk.Entry(recovery, textvariable=variable, width=18).grid(row=row, column=1, sticky="ew", padx=(8, 0), pady=3)
        ttk.Checkbutton(recovery, text="Automatically continue after recoverable Internet Archive outages", variable=self.network_persistent_var).grid(row=5, column=0, columnspan=2, sticky="w", pady=(5, 2))
        ttk.Label(recovery, text="Recommended and enabled by default. A recovery-cycle threshold pauses archive admissions and schedules another shared probe; it does not end the operation. Server Retry-After is never shortened.", style="Muted.TLabel", wraplength=520, justify="left").grid(row=6, column=0, columnspan=2, sticky="ew", pady=(4, 0))

        redirects = ttk.LabelFrame(tab, text="Redirect handling", padding=10)
        redirects.grid(row=1, column=1, sticky="nsew", padx=(6, 0), pady=8)
        ttk.Checkbutton(redirects, text="Download archived redirect destinations outside selected sites",
                        variable=self.download_external_redirects_var).grid(row=0, column=0, sticky="w")
        ttk.Label(redirects, text="By default, replay redirects stay within the source host or selected sites. Enable this to follow other archived hosts too. Live-web destinations are blocked; the destination is recorded.",
                  style="Muted.TLabel", wraplength=520, justify="left").grid(row=1, column=0, sticky="ew", pady=(6, 0))

        appearance = ttk.LabelFrame(tab, text="Appearance", padding=10)
        appearance.grid(row=2, column=0, sticky="nsew", padx=(0, 6), pady=8)
        appearance.columnconfigure(1, weight=1)
        ttk.Label(appearance, text="Font scale:").grid(row=0, column=0, sticky="w")
        ttk.Entry(appearance, textvariable=self.font_scale_var, width=18).grid(row=0, column=1, sticky="ew", padx=(8, 6))
        ttk.Button(appearance, text="Apply scale", command=self.apply_interface_theme).grid(row=0, column=2)

        storage = ttk.LabelFrame(tab, text="Storage and database backups", padding=10)
        storage.grid(row=2, column=1, sticky="nsew", padx=(6, 0), pady=8)
        storage.columnconfigure(1, weight=1)
        ttk.Checkbutton(storage, text="Create automatic safety backups", variable=self.auto_backup_var).grid(row=0, column=0, columnspan=2, sticky="w")
        ttk.Label(storage, text="Backups to keep:").grid(row=1, column=0, sticky="w", pady=3)
        ttk.Entry(storage, textvariable=self.backup_keep_var, width=18).grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=3)
        ttk.Label(storage, text="Database backups preserve project state, not capture/media payloads. Copy the whole project folder for a complete independent backup.", style="Muted.TLabel", wraplength=520, justify="left").grid(row=3, column=0, columnspan=2, sticky="ew", pady=(6, 0))

        importer = ttk.LabelFrame(tab, text="Import local archive", padding=10)
        importer.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        importer.columnconfigure(0, weight=1)
        ttk.Entry(importer, textvariable=self.import_source_var).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(importer, text="Browse…", command=self.choose_import_source).grid(row=0, column=1)
        ttk.Label(importer, text="Choose ‘Import an existing archive folder’ from Operation after selecting a source. Imported files are ingested into project storage so local search remains portable.", style="Muted.TLabel", wraplength=1050, justify="left").grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0))

    def choose_import_source(self) -> None:
        selected = filedialog.askdirectory(title="Choose an existing archive folder")
        if selected:
            self.import_source_var.set(selected)

    def create_results_tab(self) -> None:
        page = ttk.Frame(self.notebook, padding=8)
        tab = page
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(2, weight=1)
        self.notebook.add(page, text="Results and search")
        hitlist = ttk.LabelFrame(tab, text="Search with Hitlist", padding=8)
        hitlist.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        hitlist.columnconfigure(0, weight=1)
        self.hitlist_text = tk.Text(hitlist, height=4, wrap="word", font="TkFixedFont")
        self.hitlist_text.grid(row=0, column=0, columnspan=3, sticky="ew")
        ttk.Entry(hitlist, textvariable=self.hitlist_file_var).grid(row=1, column=0, sticky="ew", pady=(6, 0))
        ttk.Button(hitlist, text="Hitlist file…", command=self.choose_hitlist_file).grid(row=1, column=1, padx=6, pady=(6, 0))
        ttk.Button(hitlist, text="Search now", command=lambda: self.start(self.build_config(require_keywords=False), "hitlist")).grid(row=1, column=2, pady=(6, 0))
        filters = ttk.Frame(tab)
        filters.grid(row=0, column=0, sticky="ew")
        filters.columnconfigure(1, weight=1)
        ttk.Label(filters, text="Scan:").grid(row=0, column=0)
        self.result_scan_box = ttk.Combobox(filters, textvariable=self.result_scan_var, state="readonly", width=38)
        self.result_scan_box.grid(row=0, column=1, sticky="ew", padx=5)
        ttk.Label(filters, text="Min score:").grid(row=0, column=2)
        ttk.Entry(filters, textvariable=self.result_min_var, width=7).grid(row=0, column=3, padx=5)
        ttk.Label(filters, text="Review:").grid(row=0, column=4)
        ttk.Combobox(filters, textvariable=self.result_review_filter_var, values=("All", *REVIEW_LABELS), state="readonly", width=18).grid(row=0, column=5, padx=5)
        ttk.Label(filters, text="Filter:").grid(row=0, column=6)
        ttk.Entry(filters, textvariable=self.result_filter_var, width=20).grid(row=0, column=7, padx=5)
        ttk.Button(filters, text="Refresh", command=lambda: self.refresh_results(reset_page=True)).grid(row=0, column=8)
        ttk.Button(filters, text="‹", width=3, command=self.previous_result_page).grid(row=0, column=9, padx=(8, 2))
        ttk.Label(filters, textvariable=self.result_page_var, width=17, anchor="center").grid(row=0, column=10)
        ttk.Button(filters, text="›", width=3, command=self.next_result_page).grid(row=0, column=11, padx=(2, 0))
        search = ttk.Frame(tab)
        search.grid(row=1, column=0, sticky="ew", pady=(6, 4))
        search.columnconfigure(1, weight=1)
        ttk.Label(search, text="Instant full-text search:").grid(row=0, column=0)
        ttk.Entry(search, textvariable=self.fts_query_var).grid(row=0, column=1, sticky="ew", padx=5)
        ttk.Combobox(search, textvariable=self.fts_field_var, values=("all", "title", "body", "url"), state="readonly", width=8).grid(row=0, column=2, padx=5)
        ttk.Label(search, text="Domain:").grid(row=0, column=3)
        ttk.Entry(search, textvariable=self.fts_domain_var, width=18).grid(row=0, column=4, padx=5)
        ttk.Button(search, text="Search", command=self.run_fts_search).grid(row=0, column=5)
        pane = ttk.Panedwindow(tab, orient="vertical")
        pane.grid(row=2, column=0, sticky="nsew")
        top = ttk.Frame(pane)
        top.columnconfigure(0, weight=1)
        top.rowconfigure(0, weight=1)
        columns = ("score", "review", "timestamp", "title", "url", "hits")
        self.results_tree = ttk.Treeview(top, columns=columns, show="headings", selectmode="browse")
        widths = {"score": 70, "review": 120, "timestamp": 125, "title": 260, "url": 420, "hits": 100}
        for column in columns:
            self.results_tree.heading(column, text=column.title(), command=lambda name=column: self.sort_result_tree(name))
            self.results_tree.column(column, width=widths[column], anchor="w")
        self.results_tree.grid(row=0, column=0, sticky="nsew")
        self.results_tree.bind("<<TreeviewSelect>>", self.load_selected_result)
        scroll = ttk.Scrollbar(top, orient="vertical", command=self.results_tree.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        hscroll = ttk.Scrollbar(top, orient="horizontal", command=self.results_tree.xview)
        hscroll.grid(row=1, column=0, sticky="ew")
        self.results_tree.configure(yscrollcommand=scroll.set, xscrollcommand=hscroll.set)
        pane.add(top, weight=3)
        bottom = ttk.Frame(pane, padding=(0, 6, 0, 0))
        bottom.columnconfigure(1, weight=1)
        bottom.rowconfigure(1, weight=1)
        bottom.rowconfigure(2, weight=1)
        ttk.Label(bottom, text="Status:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(bottom, textvariable=self.review_status_var, values=list(REVIEW_LABELS), state="readonly", width=20).grid(row=0, column=1, sticky="w", padx=5)
        ttk.Label(bottom, text="Tags:").grid(row=0, column=2)
        ttk.Entry(bottom, textvariable=self.review_tags_var, width=30).grid(row=0, column=3, padx=5)
        ttk.Button(bottom, text="Save review", command=self.save_selected_review).grid(row=0, column=4, padx=4)
        ttk.Button(bottom, text="Next unreviewed", command=self.select_next_unreviewed).grid(row=0, column=5, padx=2)
        ttk.Button(bottom, text="Open local", command=self.open_selected_local).grid(row=0, column=6, padx=2)
        ttk.Button(bottom, text="Open Wayback", command=self.open_selected_wayback).grid(row=0, column=7, padx=2)
        ttk.Button(bottom, text="Copy URL", command=self.copy_selected_url).grid(row=0, column=8, padx=2)
        ttk.Button(bottom, text="Redirect destination", command=self.open_selected_destination).grid(row=0, column=9, padx=2)
        ttk.Label(bottom, text="Notes:").grid(row=1, column=0, sticky="nw", pady=(6, 0))
        self.result_detail_text = tk.Text(bottom, height=4, wrap="word")
        self.result_detail_text.grid(row=1, column=1, columnspan=8, sticky="nsew", padx=(5, 0), pady=(6, 0))
        result_notes_scroll = ttk.Scrollbar(bottom, orient="vertical", command=self.result_detail_text.yview)
        result_notes_scroll.grid(row=1, column=9, sticky="ns", pady=(6, 0))
        self.result_detail_text.configure(yscrollcommand=result_notes_scroll.set)
        ttk.Label(bottom, text="Matching snippets:").grid(row=2, column=0, sticky="nw", pady=(6, 0))
        self.result_snippets_text = tk.Text(bottom, height=5, wrap="word", state="disabled")
        self.result_snippets_text.grid(row=2, column=1, columnspan=8, sticky="nsew", padx=(5, 0), pady=(6, 0))
        result_snippets_scroll = ttk.Scrollbar(bottom, orient="vertical", command=self.result_snippets_text.yview)
        result_snippets_scroll.grid(row=2, column=9, sticky="ns", pady=(6, 0))
        self.result_snippets_text.configure(yscrollcommand=result_snippets_scroll.set)
        exports = ttk.Frame(bottom)
        exports.grid(row=3, column=1, columnspan=8, sticky="w", pady=(6, 0))
        ttk.Button(exports, text="Export CSV", command=lambda: self.export_results("csv")).grid(row=0, column=0, padx=2)
        ttk.Button(exports, text="Export JSON", command=lambda: self.export_results("json")).grid(row=0, column=1, padx=2)
        ttk.Button(exports, text="Export Markdown", command=lambda: self.export_results("markdown")).grid(row=0, column=2, padx=2)
        ttk.Button(exports, text="Review package", command=self.export_review_package_ui).grid(row=0, column=3, padx=2)
        ttk.Button(exports, text="Copy details", command=self.copy_selected_result_details).grid(row=0, column=4, padx=2)
        pane.add(bottom, weight=1)

    def create_ai_tab(self) -> None:
        page = ttk.Frame(self.notebook, padding=10)
        tab = page
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(4, weight=1)
        self.notebook.add(page, text="AI relevance")

        ttk.Label(tab, text="AI relevance review", style="Section.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            tab,
            text=(
                "After a normal scan, describe what you are actually searching for. Scout selects a bounded set of "
                "existing report matches, sends compact excerpts to the configured OpenAI or OpenRouter provider, and stores a separate explainable relevance ranking. "
                "Deterministic Scout scores and reviews are never replaced."
            ),
            style="Muted.TLabel", wraplength=1080,
        ).grid(row=1, column=0, sticky="ew", pady=(2, 8))

        settings = ttk.Frame(tab)
        settings.grid(row=2, column=0, sticky="ew")
        settings.columnconfigure(1, weight=1)
        ttk.Label(settings, text="Scan:").grid(row=0, column=0, sticky="w")
        self.ai_scan_box = ttk.Combobox(settings, textvariable=self.ai_scan_var, state="readonly", width=36)
        self.ai_scan_box.grid(row=0, column=1, sticky="ew", padx=(5, 12))
        self.ai_scan_box.bind("<<ComboboxSelected>>", lambda _e: self.refresh_ai_runs())
        ttk.Label(settings, text="Model:").grid(row=0, column=2, sticky="e")
        ttk.Entry(settings, textvariable=self.ai_model_var, width=18).grid(row=0, column=3, padx=(5, 12))
        ttk.Label(settings, text="Candidates:").grid(row=0, column=4)
        ttk.Entry(settings, textvariable=self.ai_candidate_limit_var, width=7).grid(row=0, column=5, padx=(5, 12))
        ttk.Label(settings, text="Minimum relevance:").grid(row=0, column=6)
        ttk.Entry(settings, textvariable=self.ai_min_relevance_var, width=6).grid(row=0, column=7, padx=(5, 0))

        ttk.Label(settings, text="Provider:").grid(row=1, column=0, sticky="w", pady=(7, 0))
        provider_box = ttk.Combobox(settings, textvariable=self.ai_provider_var, values=("openai", "openrouter"), state="readonly", width=14)
        provider_box.grid(row=1, column=1, sticky="w", padx=(5, 12), pady=(7, 0))
        provider_box.bind("<<ComboboxSelected>>", lambda _e: self.on_ai_provider_changed())
        ttk.Label(settings, text="API key:").grid(row=1, column=2, sticky="e", pady=(7, 0))
        key_entry = ttk.Entry(settings, textvariable=self.ai_api_key_var, show="•")
        key_entry.grid(row=1, column=3, columnspan=2, sticky="ew", padx=(5, 12), pady=(7, 0))
        ttk.Label(
            settings,
            text="Session only; environment variables are also supported. Keys are never written to the project.",
            style="Muted.TLabel",
        ).grid(row=1, column=5, columnspan=3, sticky="w", pady=(7, 0))

        prompt_frame = ttk.LabelFrame(tab, text="What are you searching for?", padding=8)
        prompt_frame.grid(row=3, column=0, sticky="ew", pady=(10, 8))
        prompt_frame.columnconfigure(0, weight=1)
        self.ai_prompt_text = tk.Text(prompt_frame, height=4, wrap="word")
        self.ai_prompt_text.grid(row=0, column=0, columnspan=6, sticky="ew")
        actions = ttk.Frame(prompt_frame)
        actions.grid(row=1, column=0, sticky="ew", pady=(7, 0))
        actions.columnconfigure(5, weight=1)
        self.ai_start_button = ttk.Button(actions, text="Analyze report", command=self.start_ai_review_ui, style="Accent.TButton")
        self.ai_start_button.grid(row=0, column=0, padx=(0, 5))
        ttk.Button(actions, text="Refresh previous reviews", command=self.refresh_ai_runs).grid(row=0, column=1, padx=5)
        ttk.Label(actions, text="Previous review:").grid(row=0, column=2, padx=(12, 4))
        self.ai_run_box = ttk.Combobox(actions, textvariable=self.ai_run_var, state="readonly", width=48)
        self.ai_run_box.grid(row=0, column=3, sticky="ew")
        self.ai_run_box.bind("<<ComboboxSelected>>", lambda _e: self.refresh_ai_results())
        ttk.Button(actions, text="Open reports", command=self.open_reports).grid(row=0, column=4, padx=(8, 0))

        pane = ttk.Panedwindow(tab, orient="vertical")
        pane.grid(row=4, column=0, sticky="nsew")
        top = ttk.Frame(pane)
        top.columnconfigure(0, weight=1)
        top.rowconfigure(0, weight=1)
        columns = ("relevance", "confidence", "archive_score", "timestamp", "title", "url", "category")
        self.ai_results_tree = ttk.Treeview(top, columns=columns, show="headings", selectmode="browse")
        widths = {
            "relevance": 80, "confidence": 80, "archive_score": 90, "timestamp": 120,
            "title": 230, "url": 380, "category": 150,
        }
        for column in columns:
            label = "Archive score" if column == "archive_score" else column.replace("_", " ").title()
            self.ai_results_tree.heading(column, text=label)
            self.ai_results_tree.column(column, width=widths[column], anchor="w")
        self.ai_results_tree.grid(row=0, column=0, sticky="nsew")
        self.ai_results_tree.bind("<<TreeviewSelect>>", self.load_selected_ai_result)
        scroll = ttk.Scrollbar(top, orient="vertical", command=self.ai_results_tree.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        hscroll = ttk.Scrollbar(top, orient="horizontal", command=self.ai_results_tree.xview)
        hscroll.grid(row=1, column=0, sticky="ew")
        self.ai_results_tree.configure(yscrollcommand=scroll.set, xscrollcommand=hscroll.set)
        pane.add(top, weight=3)

        bottom = ttk.Frame(pane, padding=(0, 6, 0, 0))
        bottom.columnconfigure(0, weight=1)
        bottom.rowconfigure(0, weight=1)
        self.ai_detail_text = tk.Text(bottom, height=7, wrap="word", state="disabled")
        self.ai_detail_text.grid(row=0, column=0, columnspan=4, sticky="nsew")
        ai_detail_scroll = ttk.Scrollbar(bottom, orient="vertical", command=self.ai_detail_text.yview)
        ai_detail_scroll.grid(row=0, column=4, sticky="ns")
        self.ai_detail_text.configure(yscrollcommand=ai_detail_scroll.set)
        ttk.Button(bottom, text="Open local", command=self.open_selected_ai_local).grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Button(bottom, text="Open Wayback", command=self.open_selected_ai_wayback).grid(row=1, column=1, sticky="w", padx=5, pady=(6, 0))
        ttk.Button(bottom, text="Copy URL", command=self.copy_selected_ai_url).grid(row=1, column=2, sticky="w", padx=5, pady=(6, 0))
        ttk.Button(bottom, text="Copy details", command=lambda: self.copy_text_widget(self.ai_detail_text)).grid(row=1, column=3, sticky="w", padx=5, pady=(6, 0))
        pane.add(bottom, weight=1)

    def create_research_tab(self) -> None:
        page = ttk.Frame(self.notebook, padding=10)
        tab = page
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(4, weight=1)
        self.notebook.add(page, text="Research intelligence")
        ttk.Label(tab, text="Research Intelligence", style="Section.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            tab,
            text=(
                "Search the entire saved project as one evidence corpus. Scout combines a local vector index, full-text search, "
                "entities, duplicate clusters, archive scores, and page-to-page relationships. Deep review is optional and sends only a "
                "bounded evidence set to your selected AI provider; archived text is always treated as untrusted source material."
            ),
            style="Muted.TLabel", wraplength=1080,
        ).grid(row=1, column=0, sticky="ew", pady=(2, 8))
        settings = ttk.Frame(tab)
        settings.grid(row=2, column=0, sticky="ew")
        settings.columnconfigure(1, weight=1)
        ttk.Label(settings, text="Local vector backend:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(settings, textvariable=self.research_backend_var, values=("local-hash", "fastembed"), state="readonly", width=16).grid(row=0, column=1, sticky="w", padx=(5, 12))
        ttk.Checkbutton(settings, text="Refresh research index after normal scans", variable=self.research_auto_var).grid(row=0, column=2, sticky="w", padx=(0, 12))
        ttk.Label(settings, text="Result limit:").grid(row=0, column=3, sticky="e")
        ttk.Entry(settings, textvariable=self.research_limit_var, width=7).grid(row=0, column=4, padx=(5, 0))

        query = ttk.LabelFrame(tab, text="Research question", padding=8)
        query.grid(row=3, column=0, sticky="ew", pady=(10, 8))
        query.columnconfigure(0, weight=1)
        ttk.Entry(query, textvariable=self.research_query_var).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        self.research_search_button = ttk.Button(query, text="Search evidence", command=self.start_research_search_ui, style="Accent.TButton")
        self.research_search_button.grid(row=0, column=1, padx=3)
        self.research_ai_button = ttk.Button(query, text="Deep AI review", command=self.start_research_ai_ui)
        self.research_ai_button.grid(row=0, column=2, padx=3)
        self.research_index_button = ttk.Button(query, text="Build / refresh index", command=self.start_research_index_ui)
        self.research_index_button.grid(row=0, column=3, padx=(8, 0))

        pane = ttk.Panedwindow(tab, orient="vertical")
        pane.grid(row=4, column=0, sticky="nsew")
        top = ttk.Frame(pane)
        top.columnconfigure(0, weight=1)
        top.rowconfigure(0, weight=1)
        columns = ("score", "vector", "archive", "timestamp", "title", "url")
        self.research_results_tree = ttk.Treeview(top, columns=columns, show="headings", selectmode="browse")
        widths = {"score": 72, "vector": 72, "archive": 72, "timestamp": 120, "title": 260, "url": 420}
        for column in columns:
            self.research_results_tree.heading(column, text=column.title())
            self.research_results_tree.column(column, width=widths[column], anchor="w")
        self.research_results_tree.grid(row=0, column=0, sticky="nsew")
        self.research_results_tree.bind("<<TreeviewSelect>>", self.load_selected_research_result)
        scroll = ttk.Scrollbar(top, orient="vertical", command=self.research_results_tree.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        hscroll = ttk.Scrollbar(top, orient="horizontal", command=self.research_results_tree.xview)
        hscroll.grid(row=1, column=0, sticky="ew")
        self.research_results_tree.configure(yscrollcommand=scroll.set, xscrollcommand=hscroll.set)
        pane.add(top, weight=3)

        bottom = ttk.Frame(pane, padding=(0, 6, 0, 0))
        bottom.columnconfigure(0, weight=1)
        bottom.rowconfigure(0, weight=1)
        self.research_detail_text = tk.Text(bottom, height=9, wrap="word", state="disabled")
        self.research_detail_text.grid(row=0, column=0, columnspan=3, sticky="nsew")
        research_detail_scroll = ttk.Scrollbar(bottom, orient="vertical", command=self.research_detail_text.yview)
        research_detail_scroll.grid(row=0, column=3, sticky="ns")
        self.research_detail_text.configure(yscrollcommand=research_detail_scroll.set)
        ttk.Button(bottom, text="Open Wayback", command=self.open_selected_research_wayback).grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Button(bottom, text="Copy URL", command=self.copy_selected_research_url).grid(row=1, column=1, sticky="w", padx=5, pady=(6, 0))
        ttk.Button(bottom, text="Copy details", command=lambda: self.copy_text_widget(self.research_detail_text)).grid(row=1, column=2, sticky="w", padx=5, pady=(6, 0))
        pane.add(bottom, weight=1)

    def create_history_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=3)
        tab.rowconfigure(2, weight=1)
        self.notebook.add(tab, text="Scan history")
        columns = ("id", "set", "status", "started", "documents", "matches", "seconds", "operation")
        self.history_tree = ttk.Treeview(tab, columns=columns, show="headings", selectmode="extended")
        for column in columns:
            self.history_tree.heading(column, text=column.title())
            self.history_tree.column(column, width=110 if column not in {"set", "operation"} else 180)
        self.history_tree.grid(row=0, column=0, sticky="nsew")
        self.history_tree.bind("<<TreeviewSelect>>", self.load_selected_history_detail)
        self.history_tree.bind("<Double-1>", self.load_selected_history_detail)
        history_v = ttk.Scrollbar(tab, orient="vertical", command=self.history_tree.yview)
        history_v.grid(row=0, column=1, sticky="ns")
        history_h = ttk.Scrollbar(tab, orient="horizontal", command=self.history_tree.xview)
        history_h.grid(row=1, column=0, sticky="ew")
        self.history_tree.configure(yscrollcommand=history_v.set, xscrollcommand=history_h.set)

        detail = ttk.Frame(tab)
        detail.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        detail.columnconfigure(0, weight=1)
        detail.rowconfigure(0, weight=1)
        self.history_detail_text = tk.Text(detail, height=6, wrap="word", state="disabled")
        self.history_detail_text.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail, orient="vertical", command=self.history_detail_text.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self.history_detail_text.configure(yscrollcommand=detail_scroll.set)

        buttons = ttk.Frame(tab)
        buttons.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        ttk.Button(buttons, text="Refresh", command=lambda: self.refresh_history(reset_page=True)).grid(row=0, column=0, padx=2)
        ttk.Button(buttons, text="‹", width=3, command=self.previous_history_page).grid(row=0, column=1, padx=2)
        ttk.Label(buttons, textvariable=self.history_page_var, width=18, anchor="center").grid(row=0, column=2, padx=2)
        ttk.Button(buttons, text="›", width=3, command=self.next_history_page).grid(row=0, column=3, padx=2)
        ttk.Button(buttons, text="Copy details", command=lambda: self.copy_text_widget(self.history_detail_text)).grid(row=0, column=4, padx=(10, 2))
        ttk.Button(buttons, text="Rename", command=self.rename_selected_scan).grid(row=0, column=5, padx=2)
        ttk.Button(buttons, text="Regenerate reports", command=self.regenerate_selected_scan).grid(row=0, column=6, padx=2)
        ttk.Button(buttons, text="Delete scan results", command=self.delete_selected_scan).grid(row=0, column=7, padx=2)
        ttk.Button(buttons, text="Compare two scans", command=self.compare_selected_scans).grid(row=0, column=8, padx=2)

    def create_errors_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(1, weight=2)
        tab.rowconfigure(3, weight=1)
        tab.rowconfigure(5, weight=1)
        self.notebook.add(tab, text="Errors")

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Label(controls, text="Category:").grid(row=0, column=0)
        self.error_category_box = ttk.Combobox(controls, textvariable=self.error_category_var, state="readonly", values=("All",), width=22)
        self.error_category_box.grid(row=0, column=1, padx=5)
        self.error_category_box.bind("<<ComboboxSelected>>", lambda _e: self.refresh_errors(reset_page=True))
        ttk.Label(controls, text="Status:").grid(row=0, column=2, padx=(8, 0))
        self.error_status_box = ttk.Combobox(
            controls, textvariable=self.error_status_filter_var, state="readonly",
            values=("Open", "Resolved", "Ignored", "All history"), width=14,
        )
        self.error_status_box.grid(row=0, column=3, padx=5)
        self.error_status_box.bind("<<ComboboxSelected>>", lambda _e: self.refresh_errors(reset_page=True))
        ttk.Button(controls, text="Refresh", command=lambda: self.refresh_errors(reset_page=True)).grid(row=0, column=4, padx=2)
        ttk.Button(controls, text="‹", width=3, command=self.previous_error_page).grid(row=0, column=5, padx=(8, 2))
        ttk.Label(controls, textvariable=self.error_page_var, width=18, anchor="center").grid(row=0, column=6)
        ttk.Button(controls, text="›", width=3, command=self.next_error_page).grid(row=0, column=7, padx=2)
        ttk.Button(controls, text="Retry selected errors", command=self.retry_selected_errors).grid(row=0, column=8, padx=(8, 2))
        ttk.Button(controls, text="Ignore selected", command=self.ignore_selected_errors).grid(row=0, column=9, padx=2)

        columns = ("operation", "category", "attempts", "retryable", "last_seen", "url", "message")
        self.errors_tree = ttk.Treeview(tab, columns=columns, show="headings", selectmode="extended")
        for column in columns:
            self.errors_tree.heading(column, text=column.title())
            self.errors_tree.column(column, width=110 if column not in {"url", "message"} else 300)
        self.errors_tree.grid(row=1, column=0, sticky="nsew")
        self.errors_tree.bind("<<TreeviewSelect>>", self.load_selected_error_detail)
        self.errors_tree.bind("<Double-1>", self.load_selected_error_detail)
        errors_v = ttk.Scrollbar(tab, orient="vertical", command=self.errors_tree.yview)
        errors_v.grid(row=1, column=1, sticky="ns")
        errors_h = ttk.Scrollbar(tab, orient="horizontal", command=self.errors_tree.xview)
        errors_h.grid(row=2, column=0, sticky="ew")
        self.errors_tree.configure(yscrollcommand=errors_v.set, xscrollcommand=errors_h.set)

        detail = ttk.Frame(tab)
        detail.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        detail.columnconfigure(0, weight=1)
        detail.rowconfigure(0, weight=1)
        self.error_detail_text = tk.Text(detail, height=7, wrap="word", state="disabled")
        self.error_detail_text.grid(row=0, column=0, sticky="nsew")
        detail_v = ttk.Scrollbar(detail, orient="vertical", command=self.error_detail_text.yview)
        detail_v.grid(row=0, column=1, sticky="ns")
        self.error_detail_text.configure(yscrollcommand=detail_v.set)
        detail_actions = ttk.Frame(detail)
        detail_actions.grid(row=1, column=0, sticky="w", pady=(4, 0))
        ttk.Button(detail_actions, text="Copy details", command=lambda: self.copy_text_widget(self.error_detail_text)).grid(row=0, column=0, padx=2)
        ttk.Button(detail_actions, text="Export selected", command=self.export_selected_error_details).grid(row=0, column=1, padx=2)

        site_header = ttk.Frame(tab)
        site_header.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(10, 5))
        ttk.Label(site_header, text="Site-specific Wayback issues", style="Section.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            site_header,
            text="Occurrence counts are accumulated host/stage history. The Category and Status filters above also scope this table; Ignored has no grouped-site equivalent.",
            style="Muted.TLabel", wraplength=780,
        ).grid(row=1, column=0, sticky="w")
        ttk.Button(site_header, text="Refresh", command=lambda: self.refresh_errors(reset_page=False)).grid(row=0, column=1, rowspan=2, padx=(12, 4))
        ttk.Button(site_header, text="Mark resolved", command=self.resolve_selected_site_issues).grid(row=0, column=2, rowspan=2, padx=4)
        site_columns = ("host", "stage", "category", "status", "count", "last_seen", "message")
        self.site_issues_tree = ttk.Treeview(tab, columns=site_columns, show="headings", selectmode="extended")
        widths = {"host": 180, "stage": 130, "category": 150, "status": 70, "count": 65, "last_seen": 150, "message": 430}
        for column in site_columns:
            self.site_issues_tree.heading(column, text=column.replace("_", " ").title())
            self.site_issues_tree.column(column, width=widths[column], anchor="w")
        self.site_issues_tree.grid(row=5, column=0, sticky="nsew")
        self.site_issues_tree.bind("<<TreeviewSelect>>", self.load_selected_site_issue_detail)
        self.site_issues_tree.bind("<Double-1>", self.load_selected_site_issue_detail)
        site_v = ttk.Scrollbar(tab, orient="vertical", command=self.site_issues_tree.yview)
        site_v.grid(row=5, column=1, sticky="ns")
        site_h = ttk.Scrollbar(tab, orient="horizontal", command=self.site_issues_tree.xview)
        site_h.grid(row=6, column=0, sticky="ew")
        self.site_issues_tree.configure(yscrollcommand=site_v.set, xscrollcommand=site_h.set)


    def create_activity_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=10)
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        self.notebook.add(tab, text="Activity")
        self.log_text = tk.Text(tab, wrap="word", state="disabled", font="TkFixedFont")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(tab, orient="vertical", command=self.log_text.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scroll.set)

    def lines_from(self, widget: tk.Text) -> list[str]:
        return [line.strip() for line in widget.get("1.0", "end").splitlines() if line.strip()]

    def replace_text(self, widget: tk.Text, values: list[str]) -> None:
        widget.delete("1.0", "end")
        widget.insert("1.0", "\n".join(values))

    def update_operation_help(self) -> None:
        self.operation_help_var.set(MODE_HELP.get(MODE_LABELS.get(self.mode_var.get(), "all"), ""))

    def save_current_keyword_set(self) -> None:
        if 0 <= self.current_keyword_set < len(self.keyword_sets):
            self.keyword_sets[self.current_keyword_set]["rules"] = self.lines_from(self.keywords_text)
            enforce_active_keyword_set_selection(self.keyword_sets, self.current_keyword_set, self.keyword_set_selected_var.get())

    def refresh_keyword_set_box(self, selected_index: int | None = None) -> None:
        names = [item["name"] for item in self.keyword_sets]
        self.keyword_set_box.configure(values=names)
        if not names:
            self.current_keyword_set = -1
            self.keyword_set_var.set("")
            self.replace_text(self.keywords_text, [])
            return
        index = selected_index if selected_index is not None else min(max(self.current_keyword_set, 0), len(names)-1)
        self.current_keyword_set = index
        self.keyword_set_var.set(names[index])
        self.keyword_set_selected_var.set(bool(self.keyword_sets[index].get("selected", True)))
        self.replace_text(self.keywords_text, list(self.keyword_sets[index].get("rules") or []))

    def switch_keyword_set(self, _event=None) -> None:
        self.save_current_keyword_set()
        name = self.keyword_set_var.get()
        for index, item in enumerate(self.keyword_sets):
            if item["name"] == name:
                self.refresh_keyword_set_box(index)
                break

    def new_keyword_set(self) -> None:
        self.save_current_keyword_set()
        name = simpledialog.askstring(APP_NAME, "Name for the new keyword set:", initialvalue="New keyword set")
        if not name:
            return
        existing = {item["name"].casefold() for item in self.keyword_sets}
        base = name.strip()
        candidate = base
        suffix = 2
        while candidate.casefold() in existing:
            candidate = f"{base} {suffix}"
            suffix += 1
        self.keyword_sets.append({"name": candidate, "rules": [], "selected": True})
        enforce_active_keyword_set_selection(self.keyword_sets, len(self.keyword_sets)-1, True)
        self.refresh_keyword_set_box(len(self.keyword_sets)-1)

    def duplicate_keyword_set(self) -> None:
        self.save_current_keyword_set()
        if self.current_keyword_set < 0:
            return
        source = self.keyword_sets[self.current_keyword_set]
        self.keyword_sets.append({"name": source["name"] + " copy", "rules": list(source["rules"]), "selected": True})
        enforce_active_keyword_set_selection(self.keyword_sets, len(self.keyword_sets)-1, True)
        self.refresh_keyword_set_box(len(self.keyword_sets)-1)

    def delete_keyword_set(self) -> None:
        if self.current_keyword_set < 0:
            return
        if not messagebox.askyesno(APP_NAME, "Delete this keyword set from the project configuration?"):
            return
        del self.keyword_sets[self.current_keyword_set]
        self.refresh_keyword_set_box(max(0, self.current_keyword_set-1))

    def import_keyword_set(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("Text or JSON", "*.txt *.json"), ("All files", "*")])
        if not path:
            return
        p = Path(path)
        if p.suffix.casefold() == ".json":
            payload = json.loads(p.read_text(encoding="utf-8"))
            name = str(payload.get("name") or p.stem)
            rules = list(payload.get("rules") or payload.get("keywords") or [])
        else:
            name = p.stem
            rules = [line.strip() for line in p.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
        self.keyword_sets.append({"name": name, "rules": rules, "selected": False})
        enforce_active_keyword_set_selection(self.keyword_sets, len(self.keyword_sets)-1, True)
        self.refresh_keyword_set_box(len(self.keyword_sets)-1)

    def export_keyword_set(self) -> None:
        self.save_current_keyword_set()
        if self.current_keyword_set < 0:
            return
        item = self.keyword_sets[self.current_keyword_set]
        path = filedialog.asksaveasfilename(defaultextension=".txt", initialfile=item["name"] + ".txt")
        if path:
            Path(path).write_text("\n".join(item["rules"]) + "\n", encoding="utf-8")

    def apply_preset(self) -> None:
        preset = PRESETS[self.preset_var.get()]
        self.replace_text(self.targets_text, list(preset["targets"]))
        self.keyword_sets = [{"name": "Current keywords", "rules": list(preset["keywords"]), "selected": True}]
        self.refresh_keyword_set_box(0)
        self.from_date_var.set(str(preset.get("from_date", preset["from_year"])))
        self.to_date_var.set(str(preset.get("to_date", preset["to_year"])))
        self.replace_text(self.cdx_filters_text, list(preset.get("cdx_filters", ["statuscode:200"])))
        self.replace_text(self.cdx_extra_text, list(preset.get("cdx_extra_params", [])))
        collapses = set(preset.get("cdx_collapses", ["urlkey"]))
        self.collapse_urlkey_var.set("urlkey" in collapses)
        self.collapse_digest_var.set("digest" in collapses)
        self.cdx_match_type_var.set(preset.get("cdx_match_type") or "Automatic")
        self.replace_text(self.media_include_text, list(DEFAULT_IMAGE_EXTENSIONS) + list(DEFAULT_VIDEO_EXTENSIONS))

    def build_config(self, require_keywords: bool = True) -> ProjectConfig:
        self.save_current_keyword_set()
        selected_mode = MODE_LABELS[self.mode_var.get()]
        external_after_scan = selected_mode == "external_media_after_scan"
        try:
            media = MediaConfig(
                enabled=self.media_enabled_var.get() or external_after_scan,
                targets=self.lines_from(self.media_targets_text),
                include_images=self.media_images_var.get(),
                include_videos=self.media_videos_var.get(),
                include_extensions=self.lines_from(self.media_include_text),
                exclude_extensions=self.lines_from(self.media_exclude_text),
                discover_embedded=self.media_embedded_var.get() or external_after_scan,
                allow_external_embeds=self.media_external_var.get() or external_after_scan,
                snapshot_strategy=self.media_strategy_var.get(),
                max_file_mb=float(self.media_max_var.get()),
                preserve_paths=(self.loaded_project_config.media.preserve_paths if self.loaded_project_config else False),
            )
            analysis = AnalysisConfig(
                forum_profile=self.forum_profile_var.get(),
                reconstruct_threads=self.analysis_threads_var.get(),
                extract_legacy_embeds=self.analysis_embeds_var.get(),
                extractor_rules=self.lines_from(self.analysis_extractors_text),
                search_external_assets=self.analysis_external_var.get(),
                external_domains=self.lines_from(self.analysis_domains_text),
                external_asset_limit=int(self.analysis_external_limit_var.get()),
                duplicate_threshold=float(self.analysis_duplicate_var.get()),
                compare_snapshots=self.analysis_compare_var.get(),
                build_provenance=self.analysis_provenance_var.get(),
                merge_source=self.analysis_merge_source_var.get(),
            )
            ai = AIConfig(
                provider=self.ai_provider_var.get(),
                model=self.ai_model_var.get(),
                candidate_limit=int(self.ai_candidate_limit_var.get()),
                batch_size=int(self.ai_batch_size_var.get()),
                minimum_relevance=int(self.ai_min_relevance_var.get()),
                excerpt_chars=int(self.ai_excerpt_chars_var.get()),
                max_output_tokens=(self.loaded_project_config.ai.max_output_tokens if self.loaded_project_config else AIConfig().max_output_tokens),
                request_timeout=(self.loaded_project_config.ai.request_timeout if self.loaded_project_config else AIConfig().request_timeout),
            )
            research = ResearchConfig(
                enabled=True,
                auto_build=self.research_auto_var.get(),
                vector_backend=self.research_backend_var.get(),
                result_limit=int(self.research_limit_var.get()),
                ai_evidence_limit=(self.loaded_project_config.research.ai_evidence_limit if self.loaded_project_config else ResearchConfig().ai_evidence_limit),
                candidate_limit=(self.loaded_project_config.research.candidate_limit if self.loaded_project_config else ResearchConfig().candidate_limit),
                duplicate_clustering=(self.loaded_project_config.research.duplicate_clustering if self.loaded_project_config else ResearchConfig().duplicate_clustering),
                entity_extraction=(self.loaded_project_config.research.entity_extraction if self.loaded_project_config else ResearchConfig().entity_extraction),
                excerpt_chars=(self.loaded_project_config.research.excerpt_chars if self.loaded_project_config else ResearchConfig().excerpt_chars),
                vector_dimensions=(self.loaded_project_config.research.vector_dimensions if self.loaded_project_config else ResearchConfig().vector_dimensions),
            )
            network = NetworkConfig(
                backend=self.network_backend_var.get(),
                trust_environment=self.network_trust_env_var.get(),
                endpoint_mode=self.network_endpoint_var.get(),
                index_strategy=self.network_strategy_var.get(),
                page_blocks=int(self.network_page_blocks_var.get()),
                cdx_workers=int(self.network_cdx_workers_var.get()),
                persistent_retries=self.network_persistent_var.get(),
                retry_base_seconds=float(self.network_retry_base_var.get()),
                retry_max_seconds=float(self.network_retry_max_var.get()),
                failure_pause_threshold=int(self.network_failure_limit_var.get()),
                connection_failure_pause_threshold=(self.loaded_project_config.network.connection_failure_pause_threshold if self.loaded_project_config else NetworkConfig().connection_failure_pause_threshold),
                connection_retry_seconds=(self.loaded_project_config.network.connection_retry_seconds if self.loaded_project_config else NetworkConfig().connection_retry_seconds),
                diagnostics=(self.loaded_project_config.network.diagnostics if self.loaded_project_config else NetworkConfig().diagnostics),
            )
            from_date = normalize_cdx_date(self.from_date_var.get(), end=False)
            to_date = normalize_cdx_date(self.to_date_var.get(), end=True)
            config = ProjectConfig(
                output_dir=Path(self.output_var.get()),
                targets=self.lines_from(self.targets_text),
                keywords=list(self.keyword_sets[0]["rules"]) if self.keyword_sets else [],
                keyword_set_name=self.keyword_sets[0]["name"] if self.keyword_sets else "Current keywords",
                keyword_sets=[KeywordSetConfig(item["name"], list(item["rules"]), bool(item.get("selected", True))) for item in self.keyword_sets],
                from_year=int(from_date[:4]),
                to_year=int(to_date[:4]),
                from_date=from_date,
                to_date=to_date,
                cdx_filters=self.lines_from(self.cdx_filters_text),
                cdx_collapses=[value for value, enabled in (("urlkey", self.collapse_urlkey_var.get()), ("digest", self.collapse_digest_var.get())) if enabled],
                cdx_match_type="" if self.cdx_match_type_var.get() == "Automatic" else self.cdx_match_type_var.get(),
                cdx_extra_params=self.lines_from(self.cdx_extra_text),
                page_size=int(self.page_size_var.get()),
                workers=int(self.workers_var.get()),
                download_scope=SCOPE_LABELS[self.scope_var.get()],
                minimum_score=int(self.minimum_score_var.get()),
                max_file_mb=float(self.max_file_var.get()),
                cdx_delay=float(self.cdx_delay_var.get()),
                download_delay=float(self.download_delay_var.get()),
                rate_limit_base_pause=float(self.rate_limit_base_var.get()),
                rate_limit_max_pause=float(self.rate_limit_max_var.get()),
                rate_limit_max_wait=float(self.rate_limit_wait_var.get()) * 60.0,
                retries=self.loaded_project_config.retries if self.loaded_project_config else 4,
                connect_timeout=self.loaded_project_config.connect_timeout if self.loaded_project_config else 30.0,
                read_timeout=self.loaded_project_config.read_timeout if self.loaded_project_config else 180.0,
                max_attempts=self.loaded_project_config.max_attempts if self.loaded_project_config else 4,
                rate_limit_attempts=self.loaded_project_config.rate_limit_attempts if self.loaded_project_config else 0,
                user_agent=self.loaded_project_config.user_agent if self.loaded_project_config else ProjectConfig.__dataclass_fields__["user_agent"].default,
                media=media,
                analysis=analysis,
                ai=ai,
                research=research,
                network=network,
                report=self.current_report_config(),
                target_settings=self.target_settings,
                auto_backup=self.auto_backup_var.get(),
                backup_keep=int(self.backup_keep_var.get()),
                import_source=self.import_source_var.get(),
                hitlist_keywords=self.lines_from(self.hitlist_text),
                hitlist_file=self.hitlist_file_var.get(),
                download_external_redirects=self.download_external_redirects_var.get(),
                text_collapse_scope=(self.loaded_project_config.text_collapse_scope if self.loaded_project_config else "range"),
                dashboard_refresh_mode=self.dashboard_refresh_mode_var.get(),
                dashboard_refresh_seconds=int(self.dashboard_refresh_seconds_var.get()),
                dashboard_eta_enabled=self.dashboard_eta_enabled_var.get(),
                retry_capture_ids=(self.loaded_project_config.retry_capture_ids if self.loaded_project_config else []),
                retry_error_categories=(self.loaded_project_config.retry_error_categories if self.loaded_project_config else []),
                retry_media_capture_ids=(self.loaded_project_config.retry_media_capture_ids if self.loaded_project_config else []),
            ).normalized()
        except (ValueError, KeyError) as exc:
            raise ValueError(f"Check the numeric settings, keyword rules, and target lines: {exc}") from exc
        mode = selected_mode
        if mode in {"all", "external_media_after_scan", "index"} and not config.targets:
            raise ValueError("Add at least one site or path.")
        if require_keywords and mode in {"all", "external_media_after_scan", "download", "resume", "rescan", "retry_errors"} and not config.selected_keyword_sets():
            raise ValueError("Select at least one non-empty keyword set.")
        if mode == "merge_project" and not config.analysis.merge_source:
            raise ValueError("Choose a source project folder in Archive analysis.")
        if mode == "import_folder" and not config.import_source:
            raise ValueError("Choose an existing archive folder in Settings.")
        return config

    def preview_cdx(self) -> None:
        try:
            config = self.build_config(require_keywords=False)
            if not config.targets:
                raise ValueError("Add a target first.")
            config = config.for_target(config.targets[0])
            window = (config.from_date, config.to_date) if config.text_collapse_scope == "range" else cdx_year_window(config, config.from_year)
            if not window:
                raise ValueError("The selected date range does not contain an indexable year.")
            url = CDX_URL + "?" + urllib.parse.urlencode(build_cdx_params(config, config.targets[0], window[0], window[1]), doseq=True)
        except Exception as exc:
            messagebox.showerror(APP_NAME, str(exc))
            return
        dialog = tk.Toplevel(self)
        dialog.title("CDX request preview")
        dialog.geometry("860x420")
        text = tk.Text(dialog, wrap="word", font="TkFixedFont")
        text.pack(fill="both", expand=True, padx=10, pady=10)
        text.insert("1.0", url)
        text.configure(state="disabled")

    def choose_output(self) -> None:
        selected = filedialog.askdirectory(initialdir=self.output_var.get() or str(Path.home()))
        if selected:
            self.output_var.set(selected)

    def open_output(self) -> None:
        open_path(Path(self.output_var.get()).expanduser())

    def open_reports(self) -> None:
        open_path(Path(self.output_var.get()).expanduser() / "reports")

    def start(self, override_config: ProjectConfig | None = None, override_mode: str | None = None) -> None:
        if self.worker_thread:
            if self.worker_thread.is_alive():
                self.status_var.set("The previous run is still shutting down…")
                self.log("Start ignored because the previous worker is still active.")
                messagebox.showinfo(APP_NAME, "The previous run is still active or shutting down. Wait for it to finish, then press Start again.")
                return
            self.worker_thread = None
        try:
            ensure_frozen_bundle_available()
            config = override_config or self.build_config(require_keywords=(MODE_LABELS[self.mode_var.get()] != "hitlist"))
        except (ValueError, FrozenBundleError) as exc:
            messagebox.showerror(APP_NAME, str(exc))
            return
        mode = override_mode or MODE_LABELS[self.mode_var.get()]
        self.stop_event.clear()
        self._eta_tracker.reset("Starting")
        self.progress_var.set(0)
        self.status_var.set("Starting…")
        self.start_button.configure(state="disabled")
        if hasattr(self, "ai_start_button"):
            self.ai_start_button.configure(state="disabled")
        for name in ("research_search_button", "research_ai_button", "research_index_button"):
            if hasattr(self, name):
                getattr(self, name).configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.log(f"Starting {mode} in {config.output_dir}")
        self.worker_thread = threading.Thread(target=self.run_worker, args=(config, mode), daemon=True)
        self.worker_thread.start()

    def run_worker(self, config: ProjectConfig, mode: str) -> None:
        try:
            self.events.put(("complete", run_project(config, mode, self.stop_event, self.on_engine_event)))
        except (RateLimitDeferred, ConnectivityPaused) as exc:
            self.events.put(("deferred", str(exc)))
        except Stopped:
            self.events.put(("stopped", None))
        except FrozenBundleError as exc:
            self.events.put(("error", str(exc)))
        except Exception:
            self.events.put(("error", traceback.format_exc()))

    def on_engine_event(self, event: ProgressEvent) -> None:
        self.events.put_progress(event)

    def stop(self) -> None:
        self.stop_event.set()
        self.status_var.set("Stopping after the current request…")
        self.stop_button.configure(state="disabled")

    def process_events(self) -> None:
        processed = 0
        try:
            while processed < 500:
                kind, payload = self.events.get_nowait()
                processed += 1
                if kind == "progress":
                    event = payload
                    self.update_dashboard_from_progress(event)
                    self.status_var.set(event.message)
                    self.log(event.message)
                    if event.current is not None and event.total:
                        self.progress.configure(mode="determinate")
                        self.progress_var.set(event.current / event.total * 100)
                    else:
                        self.progress.configure(mode="indeterminate")
                        self.progress.start(12)
                elif kind == "dashboard":
                    self._apply_dashboard_result(payload)
                elif kind == "complete":
                    self.last_paths = payload
                    self.progress.stop()
                    self.progress.configure(mode="determinate")
                    self.progress_var.set(100)
                    self.status_var.set("Complete")
                    self.log("Complete. Reports are ready.")
                    self.finish_run()
                    self.refresh_history()
                    self.refresh_results()
                    self.refresh_errors()
                    messagebox.showinfo(APP_NAME, "The run is complete.")
                elif kind == "ai_complete":
                    run_id = int(payload["run_id"])
                    self.last_paths.update(payload.get("paths") or {})
                    self.progress.stop()
                    self.progress.configure(mode="determinate")
                    self.progress_var.set(100)
                    self.status_var.set("AI relevance review complete")
                    self.log(f"AI relevance review {run_id} complete. Reports are ready.")
                    self.finish_run()
                    self.refresh_ai_runs(select_run_id=run_id)
                    messagebox.showinfo(APP_NAME, "AI relevance review is complete. The most relevant pages are displayed in AI relevance.")
                elif kind == "research_results":
                    self.progress.stop()
                    self.progress.configure(mode="determinate")
                    self.progress_var.set(100)
                    self.populate_research_results(payload)
                    self.status_var.set(f"Research search complete: {len(payload):,} results")
                    self.finish_run()
                elif kind == "research_ai_complete":
                    self.progress.stop()
                    self.progress.configure(mode="determinate")
                    self.progress_var.set(100)
                    self.populate_research_results(payload.get("evidence") or [])
                    self.show_research_answer(payload)
                    self.status_var.set("Grounded AI review complete")
                    self.finish_run()
                elif kind == "stopped":
                    self.progress.stop()
                    self.status_var.set("Stopped. Progress was saved.")
                    self.finish_run()
                elif kind == "deferred":
                    self.progress.stop()
                    self.status_var.set("Paused safely because Wayback is unreachable. Progress was saved.")
                    self.log(str(payload))
                    self.finish_run()
                    messagebox.showinfo(
                        APP_NAME,
                        "Scout could not obtain a stable Wayback connection after trying the available connection methods. "
                        "The exact queue was saved instead of marking the project failed. Use Resume after connectivity recovers.\n\n" + str(payload),
                    )
                elif kind == "error":
                    self.progress.stop()
                    self.status_var.set("Error")
                    self.log(str(payload))
                    self.finish_run()
                    messagebox.showerror(APP_NAME, str(payload))
        except queue.Empty:
            pass
        # A very large archive can emit progress faster than Tk can paint.
        # Drain bursts promptly while still yielding to the event loop so the
        # window remains responsive instead of accumulating an unbounded queue.
        self.after(10 if processed >= 500 else 100, self.process_events)

    def finish_run(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            self.after(50, self.finish_run)
            return
        self.worker_thread = None
        self.start_button.configure(state="normal")
        if hasattr(self, "ai_start_button"):
            self.ai_start_button.configure(state="normal")
        for name in ("research_search_button", "research_ai_button", "research_index_button"):
            if hasattr(self, name):
                getattr(self, name).configure(state="normal")
        self.stop_button.configure(state="disabled")
        self._eta_tracker.finish(self.status_var.get())
        self.save_app_state()

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{timestamp}] {message}\n")
        self.log_line_count += 1
        if self.log_line_count > 5000:
            self.log_text.delete("1.0", "501.0")
            self.log_line_count -= 500
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def project_database(self):
        return open_database(Path(self.output_var.get()).expanduser(), migrate=True)

    def refresh_scan_choices(self, database=None) -> None:
        own = database is None
        database = database or self.project_database()
        try:
            runs = list_scan_runs(database)
            self.scan_run_map = {f"{row['id']} — {row['keyword_set_name']} — {row['status']}": int(row["id"]) for row in runs}
            values = list(self.scan_run_map)
            self.result_scan_box.configure(values=values)
            if hasattr(self, "ai_scan_box"):
                self.ai_scan_box.configure(values=values)
            if values and self.result_scan_var.get() not in self.scan_run_map:
                self.result_scan_var.set(values[0])
            if values and self.ai_scan_var.get() not in self.scan_run_map:
                self.ai_scan_var.set(values[0])
        finally:
            if own:
                database.close()

    def refresh_results(self, reset_page: bool = False) -> None:
        if reset_page:
            self.result_page = 0
        root = Path(self.output_var.get()).expanduser()
        if not (root / "archive_scout.sqlite3").exists():
            return
        database = self.project_database()
        try:
            self.refresh_scan_choices(database)
            scan_id = self.scan_run_map.get(self.result_scan_var.get())
            if scan_id is None:
                return
            status = REVIEW_LABELS.get(self.result_review_filter_var.get(), "") if self.result_review_filter_var.get() != "All" else ""
            minimum = int(self.result_min_var.get() or 0)
            search_value = self.result_filter_var.get()
            total = result_count(database, scan_id, minimum, status, search_value)
            max_page = max(0, (total - 1) // self.result_page_size)
            self.result_page = min(self.result_page, max_page)
            rows = result_rows(
                database,
                scan_id,
                minimum,
                status,
                search_value,
                limit=self.result_page_size,
                offset=self.result_page * self.result_page_size,
            )
            self.result_page_var.set(f"Page {self.result_page + 1}/{max_page + 1} · {total:,}")
            self.results_tree.delete(*self.results_tree.get_children())
            self.result_row_map.clear()
            for row in rows:
                status_value = row["review_status"]
                item = self.results_tree.insert("", "end", values=(row["score"], status_value, row["timestamp"], row["title"] or "(untitled)", row["original_url"], len(json.loads(row["hits_json"] or "{}"))), tags=(status_value,))
                self.result_row_map[item] = dict(row)
        except Exception as exc:
            self.log(f"Could not load results: {exc}")
        finally:
            database.close()

    def previous_result_page(self) -> None:
        if self.result_page > 0:
            self.result_page -= 1
            self.refresh_results()

    def next_result_page(self) -> None:
        self.result_page += 1
        self.refresh_results()

    def run_fts_search(self) -> None:
        database = self.project_database()
        try:
            rows = search_documents(
                database,
                self.fts_query_var.get(),
                field=self.fts_field_var.get(),
                domain=self.fts_domain_var.get(),
                scan_run_id=self.current_scan_id(),
            )
            self.results_tree.delete(*self.results_tree.get_children())
            self.result_row_map.clear()
            for row in rows:
                data = dict(row)
                data.update({"id": 0, "score": round(-float(row["rank"]), 3), "review_status": "search", "hits_json": "{}", "snippets_json": json.dumps([row["snippet"] or ""]), "note": "", "tags": ""})
                item = self.results_tree.insert("", "end", values=(data["score"], "search", row["timestamp"], row["title"] or "(untitled)", row["original_url"], "FTS"))
                self.result_row_map[item] = data
        except Exception as exc:
            messagebox.showerror(APP_NAME, str(exc))
        finally:
            database.close()

    def sort_result_tree(self, column: str) -> None:
        if self.result_sort_column == column:
            self.result_sort_reverse = not self.result_sort_reverse
        else:
            self.result_sort_column = column
            self.result_sort_reverse = column in {"score", "timestamp", "hits"}
        items = list(self.results_tree.get_children())
        def key(item: str):
            value = self.results_tree.set(item, column)
            if column in {"score", "hits"}:
                try:
                    return float(value)
                except ValueError:
                    return float("-inf")
            return value.casefold()
        items.sort(key=key, reverse=self.result_sort_reverse)
        for index, item in enumerate(items):
            self.results_tree.move(item, "", index)

    def select_next_unreviewed(self) -> None:
        items = list(self.results_tree.get_children())
        if not items:
            return
        selected = self.results_tree.selection()
        start = items.index(selected[0]) + 1 if selected and selected[0] in items else 0
        for offset in range(len(items)):
            item = items[(start + offset) % len(items)]
            row = self.result_row_map.get(item, {})
            if row.get("review_status", "unreviewed") == "unreviewed":
                self.results_tree.selection_set(item)
                self.results_tree.focus(item)
                self.results_tree.see(item)
                self.load_selected_result()
                return
        messagebox.showinfo(APP_NAME, "There are no unreviewed results in the current view.")

    def selected_result(self) -> dict | None:
        selected = self.results_tree.selection()
        return self.result_row_map.get(selected[0]) if selected else None

    def load_selected_result(self, _event=None) -> None:
        row = self.selected_result()
        if not row:
            return
        reverse = {value: label for label, value in REVIEW_LABELS.items()}
        self.review_status_var.set(reverse.get(row.get("review_status"), "Unreviewed"))
        self.review_tags_var.set(row.get("tags") or "")
        snippets = json.loads(row.get("snippets_json") or "[]")
        # Notes are human-authored state.  Deterministic evidence is displayed
        # separately and is never written back through Save review.
        self.result_detail_text.delete("1.0", "end")
        self.result_detail_text.insert("1.0", row.get("note") or "")
        self._set_readonly_text(self.result_snippets_text, "\n\n".join(str(value) for value in snippets))

    def save_selected_review(self) -> None:
        row = self.selected_result()
        if not row or not row.get("id"):
            return
        database = self.project_database()
        try:
            with database:
                set_review(database, int(row["id"]), REVIEW_LABELS[self.review_status_var.get()])
                save_note(database, int(row["id"]), self.result_detail_text.get("1.0", "end").strip())
                set_match_tags(database, int(row["id"]), [value.strip() for value in self.review_tags_var.get().split(",")])
            self.refresh_results()
        finally:
            database.close()

    def open_selected_local(self) -> None:
        row = self.selected_result()
        if row and row.get("path"):
            path = Path(row["path"])
            if path.exists():
                if sys.platform == "darwin": subprocess.Popen(["open", str(path)])
                elif os.name == "nt": os.startfile(str(path))
                else: subprocess.Popen(["xdg-open", str(path)])

    def open_selected_wayback(self) -> None:
        row = self.selected_result()
        if row:
            webbrowser.open(replay_url(row["timestamp"], row["original_url"]))

    def copy_selected_url(self) -> None:
        row = self.selected_result()
        if row:
            self.clipboard_clear()
            self.clipboard_append(row["original_url"])

    def open_selected_destination(self) -> None:
        row = self.selected_result()
        if row and row.get("final_url"):
            destination = str(row["final_url"])
            if urllib.parse.urlsplit(destination).scheme in {"http", "https"}:
                webbrowser.open(destination)
        else:
            messagebox.showinfo(APP_NAME, "No redirect destination was recorded for this capture.")

    def current_scan_id(self) -> int | None:
        return self.scan_run_map.get(self.result_scan_var.get())

    def export_results(self, format_name: str) -> None:
        scan_id = self.current_scan_id()
        if not scan_id:
            return
        extension = ".md" if format_name == "markdown" else "." + format_name
        path = filedialog.asksaveasfilename(defaultextension=extension, initialfile=f"scan-{scan_id}{extension}")
        if not path:
            return
        database = self.project_database()
        try:
            status = REVIEW_LABELS.get(self.result_review_filter_var.get(), "") if self.result_review_filter_var.get() != "All" else ""
            export_scan(
                database, scan_id, Path(path), format_name, status,
                int(self.result_min_var.get() or 0), self.result_filter_var.get(),
            )
        finally:
            database.close()

    def export_review_package_ui(self) -> None:
        scan_id = self.current_scan_id()
        if not scan_id:
            return
        path = filedialog.asksaveasfilename(defaultextension=".zip", initialfile=f"scan-{scan_id}-review.zip")
        if not path:
            return
        database = self.project_database()
        try:
            status = REVIEW_LABELS.get(self.result_review_filter_var.get(), "") if self.result_review_filter_var.get() != "All" else ""
            export_review_package(
                database, scan_id, Path(path), status,
                int(self.result_min_var.get() or 0), self.result_filter_var.get(),
            )
        finally:
            database.close()

    def on_ai_provider_changed(self) -> None:
        provider = self.ai_provider_var.get().strip().casefold()
        current = self.ai_model_var.get().strip()
        if provider == "openrouter" and (not current or current == "gpt-5-mini"):
            self.ai_model_var.set("anthropic/claude-sonnet-4.5")
        elif provider == "openai" and (not current or current.startswith("anthropic/")):
            self.ai_model_var.set("gpt-5-mini")

    def start_research_index_ui(self) -> None:
        try:
            config = self.build_config(require_keywords=False)
        except Exception as exc:
            messagebox.showerror(APP_NAME, str(exc))
            return
        self.start(config, "research_index")

    def _begin_research_worker(self, target, args: tuple, status: str) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo(APP_NAME, "Another operation is already running. Let it finish or stop it first.")
            return
        self.stop_event.clear()
        self.progress_var.set(0)
        self.status_var.set(status)
        self.start_button.configure(state="disabled")
        if hasattr(self, "ai_start_button"):
            self.ai_start_button.configure(state="disabled")
        for name in ("research_search_button", "research_ai_button", "research_index_button"):
            if hasattr(self, name):
                getattr(self, name).configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.worker_thread = threading.Thread(target=target, args=args, daemon=True)
        self.worker_thread.start()

    def start_research_search_ui(self) -> None:
        try:
            config = self.build_config(require_keywords=False)
            query = self.research_query_var.get().strip()
            if not query:
                raise ValueError("Describe what you are searching for.")
            limit = int(self.research_limit_var.get())
        except Exception as exc:
            messagebox.showerror(APP_NAME, str(exc))
            return
        self._begin_research_worker(self.run_research_search_worker, (config, query, limit), "Searching Research Intelligence…")

    def run_research_search_worker(self, config: ProjectConfig, query: str, limit: int) -> None:
        database = None
        try:
            database = open_database(config.output_dir, migrate=True)
            results = [item.to_dict() for item in search_research(config, database, query, limit)]
            self.events.put(("research_results", results))
        except Stopped:
            self.events.put(("stopped", None))
        except Exception:
            self.events.put(("error", traceback.format_exc()))
        finally:
            if database is not None:
                database.close()

    def start_research_ai_ui(self) -> None:
        try:
            config = self.build_config(require_keywords=False)
            query = self.research_query_var.get().strip()
            if not query:
                raise ValueError("Describe the research question before starting deep review.")
            api_key = self.ai_api_key_var.get().strip()
        except Exception as exc:
            messagebox.showerror(APP_NAME, str(exc))
            return
        self._begin_research_worker(self.run_research_ai_worker, (config, query, api_key), "Starting grounded AI review…")

    def run_research_ai_worker(self, config: ProjectConfig, query: str, api_key: str) -> None:
        database = None
        try:
            database = open_database(config.output_dir, migrate=True)
            answer = run_grounded_answer(
                config, database, query, api_key=api_key, stop_event=self.stop_event, callback=self.on_engine_event
            )
            self.events.put(("research_ai_complete", answer.to_dict()))
        except Stopped:
            self.events.put(("stopped", None))
        except Exception:
            self.events.put(("error", traceback.format_exc()))
        finally:
            if database is not None:
                database.close()

    def populate_research_results(self, rows: list[dict]) -> None:
        if not hasattr(self, "research_results_tree"):
            return
        self.research_results_tree.delete(*self.research_results_tree.get_children())
        self.research_result_row_map.clear()
        for row in rows:
            item = self.research_results_tree.insert(
                "", "end",
                values=(
                    f"{float(row.get('score') or 0) * 100:.1f}",
                    f"{float(row.get('vector_score') or 0) * 100:.1f}",
                    f"{float(row.get('archive_score') or 0) * 100:.1f}",
                    row.get("timestamp") or "",
                    row.get("title") or "(untitled)",
                    row.get("original_url") or "",
                ),
            )
            self.research_result_row_map[item] = dict(row)
        children = self.research_results_tree.get_children()
        if children:
            self.research_results_tree.selection_set(children[0])
            self.load_selected_research_result()

    def selected_research_result(self) -> dict | None:
        selected = self.research_results_tree.selection() if hasattr(self, "research_results_tree") else ()
        return self.research_result_row_map.get(selected[0]) if selected else None

    def load_selected_research_result(self, _event=None) -> None:
        row = self.selected_research_result()
        if not row:
            return
        entities = ", ".join(f"{item.get('kind')}:{item.get('value')}" for item in (row.get("entities") or [])[:12])
        related = ", ".join(str(value) for value in (row.get("related_document_ids") or [])[:12])
        relationship_lines = []
        for item in (row.get("relationships") or [])[:12]:
            relationship_lines.append(
                f"  {item.get('timestamp') or 'unknown time'} | {item.get('edge_type') or 'related'} | "
                f"doc {item.get('related_document_id')} | {item.get('title') or item.get('original_url') or ''}"
            )
        timeline = "\n".join(relationship_lines) or "  —"
        detail = (
            f"Research score: {float(row.get('score') or 0) * 100:.1f}/100\n"
            f"Vector similarity: {float(row.get('vector_score') or 0) * 100:.1f}/100\n"
            f"Full-text component: {float(row.get('text_score') or 0) * 100:.1f}/100\n"
            f"Entity component: {float(row.get('entity_score') or 0) * 100:.1f}/100\n"
            f"Scout evidence component: {float(row.get('archive_score') or 0) * 100:.1f}/100\n"
            f"Document ID: {row.get('document_id')}\n"
            f"Duplicate group: {row.get('duplicate_group_id') or '—'}\n"
            f"Related documents: {related or '—'}\n"
            f"Entities: {entities or '—'}\n\n"
            f"Evidence graph / timeline\n{timeline}\n\n"
            f"Evidence excerpt\n{row.get('snippet') or ''}"
        )
        self.research_detail_text.configure(state="normal")
        self.research_detail_text.delete("1.0", "end")
        self.research_detail_text.insert("1.0", detail)
        self.research_detail_text.configure(state="disabled")

    def show_research_answer(self, payload: dict) -> None:
        claims = payload.get("claims") or []
        lines = ["AI interpretation (citation-grounded)", "", str(payload.get("answer") or ""), "", "Claims"]
        for claim in claims:
            ids = ", ".join(str(value) for value in claim.get("support_ids") or [])
            lines.append(f"• {claim.get('text') or ''} [documents {ids}] confidence={float(claim.get('confidence') or 0):.2f}")
            if claim.get("uncertainty"):
                lines.append(f"  Uncertainty: {claim['uncertainty']}")
        self.research_detail_text.configure(state="normal")
        self.research_detail_text.delete("1.0", "end")
        self.research_detail_text.insert("1.0", "\n".join(lines))
        self.research_detail_text.configure(state="disabled")

    def open_selected_research_wayback(self) -> None:
        row = self.selected_research_result()
        if row:
            webbrowser.open(replay_url(str(row.get("timestamp") or ""), str(row.get("original_url") or "")))

    def copy_selected_research_url(self) -> None:
        row = self.selected_research_result()
        if row:
            self.clipboard_clear()
            self.clipboard_append(str(row.get("original_url") or ""))

    def current_ai_scan_id(self) -> int | None:
        return self.scan_run_map.get(self.ai_scan_var.get())

    def current_ai_run_id(self) -> int | None:
        return self.ai_run_map.get(self.ai_run_var.get())

    def start_ai_review_ui(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo(APP_NAME, "Another operation is already running. Let it finish or stop it first.")
            return
        try:
            ensure_frozen_bundle_available()
            config = self.build_config(require_keywords=False)
            scan_id = self.current_ai_scan_id()
            if not scan_id:
                raise ValueError("Choose a completed scan to review.")
            prompt = self.ai_prompt_text.get("1.0", "end").strip()
            if not prompt:
                raise ValueError("Describe what you are searching for before starting AI relevance review.")
            api_key = self.ai_api_key_var.get().strip()
            # resolve_api_key is intentionally called inside the worker so an
            # OPENAI_API_KEY environment variable can be used without exposing it
            # to the Tk state or project configuration.
        except (ValueError, FrozenBundleError) as exc:
            messagebox.showerror(APP_NAME, str(exc))
            return
        self.stop_event.clear()
        self.progress_var.set(0)
        self.status_var.set("Starting AI relevance review…")
        self.start_button.configure(state="disabled")
        self.ai_start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.log(f"Starting AI relevance review for scan {scan_id}")
        self.worker_thread = threading.Thread(
            target=self.run_ai_worker,
            args=(config, int(scan_id), prompt, api_key),
            daemon=True,
        )
        self.worker_thread.start()

    def run_ai_worker(self, config: ProjectConfig, scan_id: int, prompt: str, api_key: str) -> None:
        database = None
        try:
            database = open_database(config.output_dir, migrate=True)
            ai_run_id = run_ai_review(
                config, database, scan_id, prompt, api_key, self.stop_event, self.on_engine_event
            )
            paths = generate_ai_reports(
                config.output_dir, database, ai_run_id, config.ai.normalized().minimum_relevance
            )
            self.events.put(("ai_complete", {"run_id": ai_run_id, "paths": paths}))
        except Stopped:
            self.events.put(("stopped", None))
        except (AIReviewError, FrozenBundleError) as exc:
            self.events.put(("error", str(exc)))
        except Exception:
            self.events.put(("error", traceback.format_exc()))
        finally:
            if database is not None:
                database.close()

    def refresh_ai_runs(self, database=None, select_run_id: int | None = None) -> None:
        root = Path(self.output_var.get()).expanduser()
        if not (root / "archive_scout.sqlite3").exists():
            return
        own = database is None
        database = database or self.project_database()
        try:
            scan_id = self.current_ai_scan_id()
            rows = list_ai_runs(database, scan_id) if scan_id else list_ai_runs(database)
            self.ai_run_map = {}
            selected_label = ""
            values: list[str] = []
            for row in rows:
                prompt = str(row["prompt"] or "").replace("\n", " ").strip()
                if len(prompt) > 54:
                    prompt = prompt[:51] + "…"
                label = f"{row['id']} — {row['status']} — {row['model']} — {prompt}"
                values.append(label)
                self.ai_run_map[label] = int(row["id"])
                if select_run_id is not None and int(row["id"]) == int(select_run_id):
                    selected_label = label
            self.ai_run_box.configure(values=values)
            if selected_label:
                self.ai_run_var.set(selected_label)
            elif values and self.ai_run_var.get() not in self.ai_run_map:
                self.ai_run_var.set(values[0])
            elif not values:
                self.ai_run_var.set("")
            self.refresh_ai_results(database)
        finally:
            if own:
                database.close()

    def refresh_ai_results(self, database=None) -> None:
        if not hasattr(self, "ai_results_tree"):
            return
        run_id = self.current_ai_run_id()
        self.ai_results_tree.delete(*self.ai_results_tree.get_children())
        self.ai_result_row_map.clear()
        if not run_id:
            return
        own = database is None
        database = database or self.project_database()
        try:
            minimum = int(self.ai_min_relevance_var.get() or 0)
            for row in ai_result_rows(database, run_id, minimum, limit=2000):
                values = (
                    int(row["relevance_score"]),
                    f"{float(row['confidence']):.2f}",
                    int(row["archive_score"]),
                    row["timestamp"],
                    row["title"] or "(untitled)",
                    row["original_url"],
                    row["category"] or "",
                )
                item = self.ai_results_tree.insert("", "end", values=values)
                self.ai_result_row_map[item] = dict(row)
        except Exception as exc:
            self.log(f"Could not load AI relevance results: {exc}")
        finally:
            if own:
                database.close()

    def selected_ai_result(self) -> dict | None:
        selected = self.ai_results_tree.selection() if hasattr(self, "ai_results_tree") else ()
        return self.ai_result_row_map.get(selected[0]) if selected else None

    def load_selected_ai_result(self, _event=None) -> None:
        row = self.selected_ai_result()
        if not row:
            return
        detail = (
            f"Relevance: {int(row.get('relevance_score') or 0)}/100\n"
            f"Confidence: {float(row.get('confidence') or 0):.2f}\n"
            f"Scout score: {int(row.get('archive_score') or 0)}\n"
            f"Category: {row.get('category') or '—'}\n\n"
            f"Reason\n{row.get('reason') or ''}\n\n"
            f"Evidence summary\n{row.get('evidence') or ''}"
        )
        self.ai_detail_text.configure(state="normal")
        self.ai_detail_text.delete("1.0", "end")
        self.ai_detail_text.insert("1.0", detail)
        self.ai_detail_text.configure(state="disabled")

    def open_selected_ai_local(self) -> None:
        row = self.selected_ai_result()
        if row and row.get("path"):
            path = Path(row["path"])
            if path.exists():
                if sys.platform == "darwin":
                    subprocess.Popen(["open", str(path)])
                elif os.name == "nt":
                    os.startfile(str(path))
                else:
                    subprocess.Popen(["xdg-open", str(path)])

    def open_selected_ai_wayback(self) -> None:
        row = self.selected_ai_result()
        if row:
            webbrowser.open(replay_url(str(row["timestamp"]), str(row["original_url"])))

    def copy_selected_ai_url(self) -> None:
        row = self.selected_ai_result()
        if row:
            self.clipboard_clear()
            self.clipboard_append(str(row["original_url"]))

    def refresh_history(self, reset_page: bool = False) -> None:
        if reset_page:
            self.history_page = 0
        root = Path(self.output_var.get()).expanduser()
        if not (root / "archive_scout.sqlite3").exists():
            return
        database = self.project_database()
        try:
            total = int(database.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0])
            max_page = max(0, (total - 1) // self.history_page_size)
            self.history_page = min(self.history_page, max_page)
            rows = database.execute(
                "SELECT sr.*,ks.name AS keyword_set_name FROM scan_runs sr JOIN keyword_sets ks ON ks.id=sr.keyword_set_id ORDER BY sr.id DESC LIMIT ? OFFSET ?",
                (self.history_page_size, self.history_page * self.history_page_size),
            ).fetchall()
            self.history_tree.delete(*self.history_tree.get_children())
            self.history_row_map.clear()
            for row in rows:
                item = self.history_tree.insert("", "end", iid=str(row["id"]), values=(row["id"], row["keyword_set_name"], row["status"], row["started_at"], row["document_count"], row["match_count"], f"{row['duration_seconds']:.1f}", row["source_operation"]))
                self.history_row_map[item] = dict(row)
            self.history_page_var.set(f"Page {self.history_page + 1}/{max_page + 1} · {total:,}")
            self._set_readonly_text(self.history_detail_text, "")
            self.refresh_scan_choices(database)
        finally:
            database.close()

    def selected_history_id(self) -> int | None:
        selected = self.history_tree.selection()
        return int(selected[0]) if selected else None

    def compare_selected_scans(self) -> None:
        selected = [int(value) for value in self.history_tree.selection()]
        if len(selected) != 2:
            messagebox.showinfo(APP_NAME, "Select exactly two scan runs to compare.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=f"scan-{selected[0]}-vs-{selected[1]}.txt",
        )
        if not path:
            return
        database = self.project_database()
        try:
            generate_scan_comparison(database, selected[0], selected[1], Path(path))
        finally:
            database.close()

    def rename_selected_scan(self) -> None:
        scan_id = self.selected_history_id()
        if not scan_id:
            return
        name = simpledialog.askstring(APP_NAME, "New scan name:")
        if name:
            database = self.project_database()
            with database:
                rename_scan_run(database, scan_id, name)
            database.close()
            self.refresh_history()

    def regenerate_selected_scan(self) -> None:
        scan_id = self.selected_history_id()
        if not scan_id:
            return
        database = self.project_database()
        try:
            generate_reports(self.build_config(require_keywords=False), database, scan_id)
        finally:
            database.close()

    def delete_selected_scan(self) -> None:
        scan_id = self.selected_history_id()
        if not scan_id or not messagebox.askyesno(APP_NAME, "Delete this scan's matches, reviews, notes, and tags? Downloaded files will remain."):
            return
        database = self.project_database()
        with database:
            delete_scan_run(database, scan_id)
        database.close()
        self.refresh_history()

    def refresh_errors(self, reset_page: bool = False) -> None:
        if reset_page:
            self.error_page = 0
        root = Path(self.output_var.get()).expanduser()
        if not (root / "archive_scout.sqlite3").exists():
            return
        database = self.project_database()
        try:
            categories = list_error_categories(database, unresolved_only=False)
            self.error_category_box.configure(values=("All", *categories))
            category = "" if self.error_category_var.get() == "All" else self.error_category_var.get()
            status = self.error_status_filter_var.get().casefold()
            clauses = {"open": "resolved=0 AND ignored=0", "resolved": "resolved=1 AND ignored=0", "ignored": "ignored=1"}.get(status, "1=1")
            params = []
            if category:
                clauses += " AND category=?"
                params.append(category)
            total = int(database.execute("SELECT COUNT(*) FROM errors WHERE " + clauses, params).fetchone()[0])
            max_page = max(0, (total - 1) // self.error_page_size)
            self.error_page = min(self.error_page, max_page)
            rows = list_errors(database, category=category, unresolved_only=False, status=status,
                               limit=self.error_page_size, offset=self.error_page*self.error_page_size)
            self.errors_tree.delete(*self.errors_tree.get_children())
            self.error_row_map.clear()
            for row in rows:
                url = row["original_url"] or row["media_url"] or row["path"] or row["media_path"] or ""
                item = self.errors_tree.insert("", "end", values=(row["operation"], row["category"], row["attempt_count"], bool(row["retryable"]), row["last_seen"], url, row["message"]))
                self.error_row_map[item] = dict(row)
            self.error_page_var.set(f"Page {self.error_page + 1}/{max_page + 1} · {total:,}")
            self._set_readonly_text(self.error_detail_text, "")
            self.refresh_site_issues(database)
        finally:
            database.close()

    def refresh_site_issues(self, database=None) -> None:
        if not hasattr(self, "site_issues_tree"):
            return
        root = Path(self.output_var.get()).expanduser()
        if not (root / "archive_scout.sqlite3").exists():
            return
        own = database is None
        database = database or self.project_database()
        try:
            self.site_issues_tree.delete(*self.site_issues_tree.get_children())
            self.site_issue_row_map.clear()
            for row in list_site_issues(database, unresolved_only=False, limit=1000):
                status_filter = self.error_status_filter_var.get()
                category_filter = self.error_category_var.get()
                if status_filter == "Ignored" or (status_filter == "Open" and row["resolved"]) or (status_filter == "Resolved" and not row["resolved"]):
                    continue
                if category_filter != "All" and row["category"] != category_filter:
                    continue
                status = int(row["http_status"] or 0)
                item = self.site_issues_tree.insert(
                    "", "end",
                    values=(
                        row["host"], row["stage"], row["category"], status or "",
                        int(row["occurrence_count"] or 0), row["last_seen"], row["message"],
                    ),
                )
                self.site_issue_row_map[item] = dict(row)
        finally:
            if own:
                database.close()

    def resolve_selected_site_issues(self) -> None:
        ids = [
            int(self.site_issue_row_map[item]["id"])
            for item in self.site_issues_tree.selection()
            if item in self.site_issue_row_map
        ]
        if not ids:
            return
        database = self.project_database()
        try:
            with database:
                for issue_id in ids:
                    resolve_site_issue(database, issue_id, True)
        finally:
            database.close()
        self.refresh_site_issues()

    def retry_selected_errors(self) -> None:
        selected = [self.error_row_map[item] for item in self.errors_tree.selection() if item in self.error_row_map]
        capture_ids = sorted({int(row["capture_id"]) for row in selected if row.get("capture_id")})
        media_ids = sorted({int(row["media_capture_id"]) for row in selected if row.get("media_capture_id")})
        if not capture_ids and not media_ids:
            messagebox.showinfo(APP_NAME, "Select one or more retryable text-page or media errors.")
            return
        config = self.build_config(require_keywords=bool(capture_ids))
        config.retry_capture_ids = capture_ids
        config.retry_media_capture_ids = media_ids
        if capture_ids:
            self.start(config.normalized(), "retry_errors")
        else:
            self.start(config.normalized(), "media_retry")

    def ignore_selected_errors(self) -> None:
        ids = [int(self.error_row_map[item]["id"]) for item in self.errors_tree.selection() if item in self.error_row_map]
        if not ids:
            return
        database = self.project_database()
        with database:
            ignore_errors(database, ids, True)
        database.close()
        self.refresh_errors()

    def save_project(self) -> None:
        try:
            path = save_project_config(self.build_config(require_keywords=False))
            messagebox.showinfo(APP_NAME, f"Project saved to:\n{path}")
        except Exception as exc:
            messagebox.showerror(APP_NAME, str(exc))

    def load_project(self) -> None:
        selected = filedialog.askopenfilename(title="Load Scout project", filetypes=[("Scout project", "project.json"), ("JSON files", "*.json"), ("All files", "*")])
        if not selected:
            return
        try:
            self.apply_config(load_project_config(Path(selected)))
            self.refresh_history()
            self.refresh_results()
            self.refresh_ai_runs()
            self.refresh_errors()
            self.refresh_site_issues()
        except Exception as exc:
            messagebox.showerror(APP_NAME, f"Could not load project:\n{exc}")

    def apply_config(self, config: ProjectConfig) -> None:
        config = config.normalized()
        self.loaded_project_config = config
        self.download_external_redirects_var.set(config.download_external_redirects)
        self.output_var.set(str(config.output_dir))
        self.replace_text(self.targets_text, config.targets)
        self.keyword_sets = [item.to_payload() for item in config.normalized_keyword_sets()]
        active_indices = [i for i, item in enumerate(self.keyword_sets) if item.get("selected")]
        if len(active_indices) > 1:
            enforce_active_keyword_set_selection(self.keyword_sets, active_indices[0], True)
        self.refresh_keyword_set_box(active_indices[0] if active_indices else 0)
        self.from_date_var.set(config.from_date)
        self.to_date_var.set(config.to_date)
        self.replace_text(self.cdx_filters_text, config.cdx_filters)
        self.replace_text(self.cdx_extra_text, config.cdx_extra_params)
        self.collapse_urlkey_var.set("urlkey" in config.cdx_collapses)
        self.collapse_digest_var.set("digest" in config.cdx_collapses)
        self.cdx_match_type_var.set(config.cdx_match_type or "Automatic")
        self.page_size_var.set(str(config.page_size))
        self.workers_var.set(str(config.workers))
        self.max_file_var.set(str(config.max_file_mb))
        self.minimum_score_var.set(str(config.minimum_score))
        self.cdx_delay_var.set(str(config.cdx_delay))
        self.download_delay_var.set(str(config.download_delay))
        self.rate_limit_base_var.set(str(config.rate_limit_base_pause))
        self.rate_limit_max_var.set(str(config.rate_limit_max_pause))
        self.rate_limit_wait_var.set(str(config.rate_limit_max_wait / 60.0))
        network = config.network.normalized()
        self.network_backend_var.set(network.backend)
        self.network_endpoint_var.set(network.endpoint_mode)
        self.network_strategy_var.set(network.index_strategy)
        self.network_page_blocks_var.set(str(network.page_blocks))
        self.network_cdx_workers_var.set(str(network.cdx_workers))
        self.network_trust_env_var.set(network.trust_environment)
        self.network_persistent_var.set(network.persistent_retries)
        self.network_retry_base_var.set(str(network.retry_base_seconds))
        self.network_retry_max_var.set(str(network.retry_max_seconds))
        self.network_failure_limit_var.set(str(network.failure_pause_threshold))
        self.apply_report_config(config.report if isinstance(config.report, ReportConfig) else ReportConfig(**config.report))
        self.target_settings = dict(config.target_settings)
        self.auto_backup_var.set(config.auto_backup)
        self.backup_keep_var.set(str(config.backup_keep))
        self.dashboard_refresh_mode_var.set(config.dashboard_refresh_mode)
        self.dashboard_refresh_seconds_var.set(str(config.dashboard_refresh_seconds))
        self.dashboard_eta_enabled_var.set(config.dashboard_eta_enabled)
        self._eta_tracker.set_enabled(config.dashboard_eta_enabled)
        self.import_source_var.set(config.import_source)
        self.replace_text(self.hitlist_text, config.hitlist_keywords)
        self.hitlist_file_var.set(config.hitlist_file)
        for label, value in SCOPE_LABELS.items():
            if value == config.download_scope:
                self.scope_var.set(label)
                break
        media = config.media
        self.media_enabled_var.set(media.enabled)
        self.replace_text(self.media_targets_text, media.targets)
        self.media_images_var.set(media.include_images)
        self.media_videos_var.set(media.include_videos)
        self.replace_text(self.media_include_text, media.include_extensions)
        self.replace_text(self.media_exclude_text, media.exclude_extensions)
        self.media_embedded_var.set(media.discover_embedded)
        self.media_external_var.set(media.allow_external_embeds)
        self.media_strategy_var.set(media.snapshot_strategy)
        self.media_max_var.set(str(media.max_file_mb))
        self.media_preserve_var.set(False)
        ai = config.ai.normalized()
        self.ai_provider_var.set(ai.provider)
        self.ai_model_var.set(ai.model)
        self.ai_candidate_limit_var.set(str(ai.candidate_limit))
        self.ai_batch_size_var.set(str(ai.batch_size))
        self.ai_min_relevance_var.set(str(ai.minimum_relevance))
        self.ai_excerpt_chars_var.set(str(ai.excerpt_chars))
        research = config.research.normalized()
        self.research_backend_var.set(research.vector_backend)
        self.research_auto_var.set(research.auto_build)
        self.research_limit_var.set(str(research.result_limit))
        analysis = config.analysis
        self.forum_profile_var.set(analysis.forum_profile)
        self.analysis_threads_var.set(analysis.reconstruct_threads)
        self.analysis_embeds_var.set(analysis.extract_legacy_embeds)
        self.replace_text(self.analysis_extractors_text, analysis.extractor_rules)
        self.analysis_external_var.set(analysis.search_external_assets)
        self.replace_text(self.analysis_domains_text, analysis.external_domains)
        self.analysis_external_limit_var.set(str(analysis.external_asset_limit))
        self.analysis_duplicate_var.set(str(analysis.duplicate_threshold))
        self.analysis_compare_var.set(analysis.compare_snapshots)
        self.analysis_provenance_var.set(analysis.build_provenance)
        self.analysis_merge_source_var.set(analysis.merge_source)

    def state_path(self) -> Path:
        return app_support_dir() / "settings.json"

    def save_app_state(self) -> None:
        try:
            config = self.build_config(require_keywords=False)
            path = self.state_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = config.to_payload()
            payload["mode"] = self.mode_var.get()
            payload["appearance"] = {
                "theme": self.theme_var.get(),
                "interface_mode": self.interface_mode_var.get(),
                "font_scale": self.font_scale_var.get(),
                "geometry": self.geometry(),
                "dashboard_refresh_mode": self.dashboard_refresh_mode_var.get(),
                "dashboard_refresh_seconds": self.dashboard_refresh_seconds_var.get(),
                "dashboard_eta_enabled": self.dashboard_eta_enabled_var.get(),
            }
            path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        except Exception:
            pass

    def load_app_state(self) -> None:
        path = self.state_path()
        if not path.exists():
            return
        try:
            config = load_project_config(path)
            self.apply_config(config)
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("mode") in MODE_LABELS:
                self.mode_var.set(payload["mode"])
            appearance = payload.get("appearance") or {}
            if appearance.get("theme") in {"System", "Light", "Dark"}:
                self.theme_var.set(appearance["theme"])
            if appearance.get("interface_mode") in {"Simple", "Advanced"}:
                self.interface_mode_var.set(appearance["interface_mode"])
            if appearance.get("font_scale"):
                self.font_scale_var.set(str(appearance["font_scale"]))
            self.dashboard_refresh_mode_var.set(appearance.get("dashboard_refresh_mode", "manual"))
            self.dashboard_refresh_seconds_var.set(str(appearance.get("dashboard_refresh_seconds", 10)))
            self.dashboard_eta_enabled_var.set(bool(appearance.get("dashboard_eta_enabled", False)))
            self._eta_tracker.set_enabled(self.dashboard_eta_enabled_var.get())
            if appearance.get("geometry"):
                try:
                    self._apply_saved_geometry(str(appearance["geometry"]))
                except tk.TclError:
                    pass
            self.update_operation_help()
            self.apply_interface_theme()
            self.refresh_navigation()
        except Exception:
            pass

    def on_close(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            if not messagebox.askyesno(APP_NAME, "A run is active. Stop it and close the application?"):
                return
            self.stop_event.set()
        if self.dashboard_refresh_job is not None:
            try:
                self.after_cancel(self.dashboard_refresh_job)
            except tk.TclError:
                pass
            self.dashboard_refresh_job = None
        self.save_app_state()
        self.destroy()

    def _set_initial_geometry(self) -> None:
        self.update_idletasks()
        screen_w, screen_h = self.winfo_screenwidth(), self.winfo_screenheight()
        width = min(1180, max(760, screen_w - 80))
        height = min(820, max(560, screen_h - 120))
        self.geometry(f"{width}x{height}")
        self.minsize(min(940, width), min(680, height))

    def _apply_saved_geometry(self, value: str) -> None:
        import re
        match = re.match(r"^(\d+)x(\d+)(?:\+(-?\d+)\+(-?\d+))?$", value.strip())
        if not match:
            return
        screen_w, screen_h = self.winfo_screenwidth(), self.winfo_screenheight()
        width = min(max(640, int(match.group(1))), max(640, screen_w - 40))
        height = min(max(500, int(match.group(2))), max(500, screen_h - 80))
        x = int(match.group(3) or 20); y = int(match.group(4) or 20)
        x = max(0, min(x, max(0, screen_w - width)))
        y = max(0, min(y, max(0, screen_h - height)))
        try:
            self.geometry(f"{width}x{height}+{x}+{y}")
        except tk.TclError:
            pass

    def set_report_fields(self, output_name: str, enabled: bool) -> None:
        for variable in self.report_field_vars.get(output_name, {}).values():
            variable.set(bool(enabled))

    def choose_hitlist_file(self) -> None:
        selected = filedialog.askopenfilename(title="Choose hitlist", filetypes=[("Text files", "*.txt"), ("All files", "*")])
        if selected:
            self.hitlist_file_var.set(selected)

    def _set_readonly_text(self, widget: tk.Text, text: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", text)
        widget.configure(state="disabled")

    def copy_text_widget(self, widget: tk.Text) -> None:
        try:
            text = widget.get("1.0", "end-1c")
        except tk.TclError:
            return
        self.clipboard_clear()
        self.clipboard_append(text)

    def _current_target_line(self) -> str:
        try:
            index = self.targets_text.index("insert")
            return self.targets_text.get(f"{index.split('.')[0]}.0", f"{index.split('.')[0]}.end").strip()
        except Exception:
            return ""

    @staticmethod
    def _target_override_key(line: str) -> str:
        try:
            return normalize_target(line)
        except (TypeError, ValueError):
            return str(line or "").strip()

    def _refresh_target_override_status(self) -> None:
        line = self._current_target_line()
        if not line:
            self.target_override_status_var.set("No target selected. Place the cursor on a target line to inspect its override.")
            return
        key = self._target_override_key(line)
        settings = dict(self.target_settings.get(key) or self.target_settings.get(line) or {})
        if not settings:
            self.target_override_status_var.set(f"No per-target override for {key or line}.")
            return
        labels = {
            "from_date": "from", "to_date": "to", "cdx_match_type": "matchType",
            "page_size": "CDX limit", "workers": "workers", "scan_workers": "scan workers",
            "cdx_delay": "CDX delay", "download_delay": "replay delay",
        }
        summary = ", ".join(f"{labels.get(name, name)}={value}" for name, value in settings.items())
        self.target_override_status_var.set(f"Active override for {key}: {summary}")

    def copy_selected_result_details(self) -> None:
        row = self.selected_result()
        if not row:
            return
        snippets = json.loads(row.get("snippets_json") or "[]")
        text = (
            f"URL: {row.get('original_url') or ''}\n"
            f"Timestamp: {row.get('timestamp') or ''}\n"
            f"Title: {row.get('title') or ''}\n"
            f"Score: {row.get('score') or 0}\n"
            f"Review: {row.get('review_status') or ''}\n"
            f"Tags: {row.get('tags') or ''}\n\n"
            f"Notes:\n{row.get('note') or ''}\n\n"
            f"Matching snippets:\n" + "\n\n".join(str(value) for value in snippets)
        )
        self.clipboard_clear(); self.clipboard_append(text)

    def _dashboard_refresh_settings_changed(self) -> None:
        try:
            interval = int(self.dashboard_refresh_seconds_var.get())
        except ValueError:
            interval = 10
        interval = min(3600, max(5, interval))
        self.dashboard_refresh_seconds_var.set(str(interval))
        self.dashboard_refresh.configure(self.dashboard_refresh_mode_var.get(), interval)
        self._reschedule_dashboard_refresh()
        self.save_app_state()

    def _reschedule_dashboard_refresh(self, initial: bool = False) -> None:
        if self.dashboard_refresh_job is not None:
            try:
                self.after_cancel(self.dashboard_refresh_job)
            except tk.TclError:
                pass
            self.dashboard_refresh_job = None
        try:
            self.dashboard_refresh.configure(
                self.dashboard_refresh_mode_var.get(), int(self.dashboard_refresh_seconds_var.get())
            )
        except (ValueError, tk.TclError):
            self.dashboard_refresh.configure("auto", 10)
        delay = 250 if initial else 1000
        self.dashboard_refresh_job = self.after(delay, self.dashboard_refresh_loop)

    def _refresh_dashboard_media_policy_summary(self, media=None) -> None:
        # Several unit tests intentionally construct ArchiveScoutApp without a
        # Tk interpreter to verify worker startup behavior. Avoid Tk's __getattr__
        # recursion when those lightweight objects do not own dashboard variables.
        dashboard_var = self.__dict__.get("dashboard_media_policy_var")
        if dashboard_var is None:
            return
        try:
            worker = self.__dict__.get("worker_thread")
            active_policy = self.__dict__.get("active_operation_media_policy")
            if media is None and worker and worker.is_alive() and active_policy is not None:
                media = active_policy
            if media is None:
                if "media_include_text" not in self.__dict__:
                    return
                media = MediaConfig(
                    enabled=self.media_enabled_var.get(),
                    include_images=self.media_images_var.get(),
                    include_videos=self.media_videos_var.get(),
                    include_extensions=self.lines_from(self.media_include_text),
                    exclude_extensions=self.lines_from(self.media_exclude_text),
                    snapshot_strategy=self.media_strategy_var.get(),
                )
            dashboard_var.set(format_media_policy_summary(media))
        except Exception as exc:
            dashboard_var.set(f"Media policy unavailable: {exc}")

    def dashboard_refresh_loop(self) -> None:
        try:
            selected = self.notebook.tab(self.notebook.select(), "text") if self.notebook.select() else ""
            active = bool(self.worker_thread and self.worker_thread.is_alive())
            controller = self.__dict__.get("dashboard_refresh")
            if controller is None:
                if not active and selected == "Dashboard":
                    self.refresh_dashboard()
                return
            now = time.monotonic()
            try:
                interval = int(self.dashboard_refresh_seconds_var.get())
            except (ValueError, tk.TclError):
                # The Entry may be temporarily empty while the user types. Keep
                # the heartbeat alive and preserve the last valid interval.
                interval = int(getattr(controller, "interval_seconds", 10) or 10)
            try:
                controller.configure(self.dashboard_refresh_mode_var.get(), interval)
            except ValueError:
                controller.configure("auto", interval)
            if controller.automatic_due(now, visible=selected == "Dashboard", operation_active=active):
                self.refresh_dashboard(manual=False)
        except tk.TclError:
            pass
        finally:
            try:
                self.dashboard_refresh_job = self.after(1000, self.dashboard_refresh_loop)
            except tk.TclError:
                self.dashboard_refresh_job = None

    def update_dashboard_from_progress(self, event: ProgressEvent) -> None:
        """Keep live operation telemetry separate from project-wide Dashboard cards.

        Exact cards are reconciled by the bounded snapshot scheduler.  Progress
        payloads are phase/current-run counters and must not replace a card whose
        meaning is project-wide backlog or open-error state.
        """
        tracker = self.__dict__.get("_eta_tracker")
        if tracker is not None:
            tracker.observe(event)
            ArchiveScoutApp._refresh_eta_display(self)

    def _eta_settings_changed(self) -> None:
        self._eta_tracker.set_enabled(self.dashboard_eta_enabled_var.get())
        self._refresh_eta_display(force=True)
        self.save_app_state()

    def _refresh_eta_display(self, force: bool = False) -> None:
        tracker = self.__dict__.get("_eta_tracker")
        variable = self.__dict__.get("dashboard_eta_var")
        if tracker is None or variable is None:
            return
        now = time.monotonic()
        if force or now - self.__dict__.get("_eta_last_display", 0.0) >= 0.5:
            variable.set(tracker.label(now))
            self._eta_last_display = now

    def _apply_dashboard_result(self, payload) -> None:
        token, root_text, counts, error = payload
        if not self.dashboard_refresh.finish(int(token)):
            return
        if root_text != str(Path(self.output_var.get()).expanduser()):
            return
        if error:
            self.dashboard_last_refresh_var.set(f"Refresh unavailable: {error}")
            return
        if not counts.get("_exact", True):
            self.dashboard_last_refresh_var.set(
                "Refresh exceeded the query deadline; existing totals retained. Try again after heavy disk activity."
            )
            return
        def display_count(key: str) -> str:
            value = counts.get(key)
            return "—" if value is None else f"{int(value):,}"
        self.dashboard_captures_var.set(display_count("captures"))
        self.dashboard_documents_var.set(display_count("documents"))
        self.dashboard_matches_var.set(display_count("matches"))
        self.dashboard_errors_var.set(display_count("errors"))
        self.dashboard_recovery_var.set(display_count("recovery_events"))
        self.dashboard_skipped_non_text_var.set(display_count("skipped_non_text"))
        self.dashboard_skipped_url_var.set(display_count("skipped_url_filter"))
        self.dashboard_pending_var.set(display_count("pending"))
        self.dashboard_waiting_scan_var.set(display_count("downloaded_unscanned"))
        self.dashboard_media_candidates_var.set(display_count("media_candidates"))
        selected = display_count("media_selected")
        images = display_count("media_selected_images")
        videos = display_count("media_selected_videos")
        self.dashboard_media_selected_var.set(
            selected if "…" in selected else f"{selected} (images {images}, videos {videos})"
        )
        media_pending = counts.get("media_pending")
        media_downloading = counts.get("media_downloading")
        if media_pending is None or media_downloading is None:
            self.dashboard_media_pending_var.set("…")
        else:
            self.dashboard_media_pending_var.set(f"{int(media_pending) + int(media_downloading):,}")
        self.dashboard_media_downloaded_var.set(display_count("media_downloaded"))
        self.dashboard_media_excluded_var.set(display_count("media_excluded"))
        self.dashboard_media_errors_var.set(display_count("media_errors"))
        self.dashboard_media_deferred_var.set(display_count("media_deferred_from_text"))
        self.dashboard_failure_summary_var.set(
            "Other outcomes — "
            f"non-text/media excluded {display_count('skipped_non_text')} • "
            f"URL-filter skips {display_count('skipped_url_filter')} • "
            f"media handoffs {display_count('deferred_to_media')} • "
            f"other skips {display_count('skipped_other')} • "
            f"failed captures {display_count('failed_captures')} • "
            f"recovered incidents {display_count('recovery_events')}"
        )
        if counts.get("_exact", True):
            label = "Exact counts refreshed "
        else:
            label = "Refresh incomplete (query deadline; unavailable values shown as —) "
        self.dashboard_last_refresh_var.set(label + datetime.now().strftime("%H:%M:%S"))

    def previous_history_page(self) -> None:
        if self.history_page > 0:
            self.history_page -= 1
            self.refresh_history()

    def next_history_page(self) -> None:
        self.history_page += 1
        self.refresh_history()

    def selected_history_row(self) -> dict | None:
        selected = self.history_tree.selection()
        return self.history_row_map.get(selected[0]) if selected else None

    def load_selected_history_detail(self, _event=None) -> None:
        row = self.selected_history_row()
        if not row:
            self._set_readonly_text(self.history_detail_text, "")
            return
        detail = "\n".join(
            f"{label}: {row.get(key) if row.get(key) is not None else ''}"
            for label, key in (
                ("Scan run", "id"), ("Keyword set", "keyword_set_name"), ("Name", "name"),
                ("Status", "status"), ("Source operation", "source_operation"),
                ("Started", "started_at"), ("Completed", "completed_at"),
                ("Documents", "document_count"), ("Matches", "match_count"),
                ("Duration seconds", "duration_seconds"), ("Minimum score", "minimum_score"),
                ("Metadata", "metadata_json"),
            )
        )
        self._set_readonly_text(self.history_detail_text, detail)

    def previous_error_page(self) -> None:
        if self.error_page > 0:
            self.error_page -= 1
            self.refresh_errors()

    def next_error_page(self) -> None:
        self.error_page += 1
        self.refresh_errors()

    def _selected_error_row(self) -> dict | None:
        selected = self.errors_tree.selection()
        return self.error_row_map.get(selected[0]) if selected else None

    def _selected_site_issue_row(self) -> dict | None:
        selected = self.site_issues_tree.selection()
        return self.site_issue_row_map.get(selected[0]) if selected else None

    def _format_error_detail(self, row: dict) -> str:
        url = row.get("original_url") or row.get("media_url") or row.get("path") or row.get("media_path") or ""
        return (
            f"Error ID: {row.get('id') or ''}\n"
            f"Operation: {row.get('operation') or ''}\n"
            f"Category: {row.get('category') or ''}\n"
            f"HTTP status: {row.get('http_status') or ''}\n"
            f"Retryable: {bool(row.get('retryable'))}\n"
            f"Resolved: {bool(row.get('resolved'))}\n"
            f"Ignored: {bool(row.get('ignored'))}\n"
            f"Attempts: {row.get('attempt_count') or 0}\n"
            f"First seen: {row.get('first_seen') or ''}\n"
            f"Last seen: {row.get('last_seen') or ''}\n"
            f"URL/path: {url}\n\n"
            f"Cause:\n{row.get('message') or ''}"
        )

    def load_selected_error_detail(self, _event=None) -> None:
        row = self._selected_error_row()
        if row:
            self._set_readonly_text(self.error_detail_text, self._format_error_detail(row))

    def load_selected_site_issue_detail(self, _event=None) -> None:
        row = self._selected_site_issue_row()
        if not row:
            return
        detail = (
            f"Site issue ID: {row.get('id') or ''}\nHost: {row.get('host') or ''}\n"
            f"Target: {row.get('target') or ''}\nStage: {row.get('stage') or ''}\n"
            f"Category: {row.get('category') or ''}\nHTTP status: {row.get('http_status') or ''}\n"
            f"Accumulated occurrences: {row.get('occurrence_count') or 0}\n"
            f"First seen: {row.get('first_seen') or ''}\nLast seen: {row.get('last_seen') or ''}\n\n"
            f"Message:\n{row.get('message') or ''}"
        )
        self._set_readonly_text(self.error_detail_text, detail)

    def export_selected_error_details(self) -> None:
        row = self._selected_error_row()
        if not row:
            row = self._selected_site_issue_row()
            if not row:
                return
            text = self.error_detail_text.get("1.0", "end-1c")
        else:
            text = self._format_error_detail(row)
        path = filedialog.asksaveasfilename(defaultextension=".txt", initialfile="archive-scout-error-details.txt")
        if path:
            Path(path).write_text(text + "\n", encoding="utf-8")


def main() -> None:
    enable_windows_dpi_awareness()
    app = ArchiveScoutApp()
    if os.environ.get("ARCHIVE_SCOUT_STARTUP_PROBE") == "1":
        app.after(250, app.destroy)
    app.mainloop()


if __name__ == "__main__":
    main()
