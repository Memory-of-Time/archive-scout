from __future__ import annotations

import sqlite3

from ..constants import SCHEMA_VERSION

BASE_SCHEMA_SQL = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_info(version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS project_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS targets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    settings_json TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS captures(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_url TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    target_id INTEGER,
    query_signature TEXT NOT NULL,
    urlkey TEXT NOT NULL DEFAULT '',
    mimetype TEXT,
    statuscode TEXT,
    digest TEXT,
    length INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',
    skip_reason TEXT,
    classifier_revision INTEGER NOT NULL DEFAULT 0,
    local_path TEXT,
    content_hash TEXT,
    detected_encoding TEXT,
    body_revision INTEGER NOT NULL DEFAULT 0,
    download_attempts INTEGER NOT NULL DEFAULT 0,
    document_id INTEGER,
    http_status INTEGER,
    final_url TEXT,
    bytes_saved INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resource_class TEXT NOT NULL DEFAULT 'unknown',
    classification_reason TEXT,
    resource_classifier_revision INTEGER NOT NULL DEFAULT 0,
    payload_availability TEXT NOT NULL DEFAULT 'not_acquired',
    payload_origin TEXT NOT NULL DEFAULT '',
    payload_retention TEXT NOT NULL DEFAULT 'keep',
    cleanup_pending INTEGER NOT NULL DEFAULT 0,
    discarded_at TEXT,
    UNIQUE(original_url,timestamp,query_signature),
    FOREIGN KEY(target_id) REFERENCES targets(id)
);
CREATE INDEX IF NOT EXISTS captures_state_idx ON captures(state,download_attempts,timestamp);
CREATE INDEX IF NOT EXISTS captures_original_idx ON captures(original_url,timestamp);
CREATE INDEX IF NOT EXISTS captures_signature_idx ON captures(query_signature,timestamp);
CREATE INDEX IF NOT EXISTS captures_download_idx ON captures(query_signature,state,download_attempts,id);
CREATE INDEX IF NOT EXISTS captures_acquisition_order_idx ON captures(query_signature,state,length,id,download_attempts);
CREATE TABLE IF NOT EXISTS documents(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_id INTEGER NOT NULL UNIQUE,
    path TEXT NOT NULL,
    title TEXT,
    body_text TEXT,
    body_zlib BLOB,
    body_chars INTEGER NOT NULL DEFAULT 0,
    original_url TEXT,
    links_json TEXT,
    content_hash TEXT,
    normalized_hash TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(capture_id) REFERENCES captures(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS documents_hash_idx ON documents(content_hash);
CREATE INDEX IF NOT EXISTS documents_normalized_hash_idx ON documents(normalized_hash);
CREATE TABLE IF NOT EXISTS document_fts_versions(
    fts_rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL UNIQUE,
    signature BLOB NOT NULL DEFAULT X'',
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS keyword_sets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE,
    keywords_json TEXT NOT NULL,
    rules_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scan_runs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword_set_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    minimum_score INTEGER NOT NULL DEFAULT 1,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    source_operation TEXT NOT NULL,
    document_count INTEGER NOT NULL DEFAULT 0,
    match_count INTEGER NOT NULL DEFAULT 0,
    duration_seconds REAL NOT NULL DEFAULT 0,
    metadata_json TEXT,
    FOREIGN KEY(keyword_set_id) REFERENCES keyword_sets(id)
);
CREATE INDEX IF NOT EXISTS scan_runs_status_idx ON scan_runs(status,started_at);
CREATE TABLE IF NOT EXISTS document_matches(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_run_id INTEGER NOT NULL,
    document_id INTEGER NOT NULL,
    score INTEGER NOT NULL DEFAULT 0,
    hits_json TEXT,
    fields_json TEXT,
    snippets_json TEXT,
    interesting_links_json TEXT,
    excluded INTEGER NOT NULL DEFAULT 0,
    required_missing INTEGER NOT NULL DEFAULT 0,
    proximity_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(scan_run_id,document_id),
    FOREIGN KEY(scan_run_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS document_matches_score_idx ON document_matches(scan_run_id,score DESC);
CREATE INDEX IF NOT EXISTS document_matches_document_idx ON document_matches(document_id,excluded,required_missing,score DESC);
CREATE TABLE IF NOT EXISTS keyword_hits(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    label TEXT NOT NULL,
    count INTEGER NOT NULL,
    fields_json TEXT,
    UNIQUE(match_id,label),
    FOREIGN KEY(match_id) REFERENCES document_matches(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS errors(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_id INTEGER,
    document_id INTEGER,
    media_capture_id INTEGER,
    operation TEXT NOT NULL,
    category TEXT NOT NULL,
    message TEXT NOT NULL,
    http_status INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    retryable INTEGER NOT NULL DEFAULT 1,
    resolved INTEGER NOT NULL DEFAULT 0,
    ignored INTEGER NOT NULL DEFAULT 0,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    FOREIGN KEY(capture_id) REFERENCES captures(id) ON DELETE CASCADE,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS errors_unresolved_idx ON errors(resolved,ignored,retryable,operation,category);
CREATE INDEX IF NOT EXISTS errors_capture_lookup_idx ON errors(capture_id,operation,category,resolved,ignored,id DESC);
CREATE INDEX IF NOT EXISTS errors_media_lookup_idx ON errors(media_capture_id,operation,category,resolved,ignored,id DESC);
CREATE TABLE IF NOT EXISTS index_state(
    target_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    query_signature TEXT NOT NULL,
    resume_key TEXT,
    complete INTEGER NOT NULL DEFAULT 0,
    seen INTEGER NOT NULL DEFAULT 0,
    error_id INTEGER,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(target_id,year,query_signature),
    FOREIGN KEY(target_id) REFERENCES targets(id) ON DELETE CASCADE,
    FOREIGN KEY(error_id) REFERENCES errors(id)
);
CREATE TABLE IF NOT EXISTS index_pages(
    query_signature TEXT NOT NULL,
    target_id INTEGER NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    page INTEGER NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'complete',
    layout_signature TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY(query_signature,target_id,window_start,window_end,page),
    FOREIGN KEY(target_id) REFERENCES targets(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS index_pages_status_idx ON index_pages(query_signature,target_id,window_start,window_end,status,page);
CREATE TABLE IF NOT EXISTS media_index_pages(
    query_signature TEXT NOT NULL,
    target_id INTEGER NOT NULL,
    extension TEXT NOT NULL DEFAULT '',
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    page INTEGER NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'complete',
    layout_signature TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY(query_signature,target_id,extension,window_start,window_end,page)
);
CREATE INDEX IF NOT EXISTS media_index_pages_status_idx ON media_index_pages(query_signature,target_id,extension,window_start,window_end,status,page);
CREATE TABLE IF NOT EXISTS index_coverage(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id INTEGER NOT NULL,
    query_signature TEXT NOT NULL,
    range_start TEXT NOT NULL,
    range_end TEXT NOT NULL,
    plan_json TEXT,
    strategy TEXT NOT NULL DEFAULT 'resume',
    layout_signature TEXT NOT NULL DEFAULT '',
    complete INTEGER NOT NULL DEFAULT 0,
    seen INTEGER NOT NULL DEFAULT 0,
    error_id INTEGER,
    updated_at TEXT NOT NULL,
    UNIQUE(target_id,query_signature,range_start,range_end),
    FOREIGN KEY(target_id) REFERENCES targets(id) ON DELETE CASCADE,
    FOREIGN KEY(error_id) REFERENCES errors(id)
);
CREATE INDEX IF NOT EXISTS index_coverage_lookup_idx ON index_coverage(target_id,query_signature,complete,range_start,range_end);
CREATE TABLE IF NOT EXISTS media_index_coverage(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id INTEGER NOT NULL,
    query_signature TEXT NOT NULL,
    range_start TEXT NOT NULL,
    range_end TEXT NOT NULL,
    plan_json TEXT,
    strategy TEXT NOT NULL DEFAULT 'resume',
    layout_signature TEXT NOT NULL DEFAULT '',
    complete INTEGER NOT NULL DEFAULT 0,
    seen INTEGER NOT NULL DEFAULT 0,
    error_id INTEGER,
    updated_at TEXT NOT NULL,
    UNIQUE(target_id,query_signature,range_start,range_end),
    FOREIGN KEY(target_id) REFERENCES media_targets(id) ON DELETE CASCADE,
    FOREIGN KEY(error_id) REFERENCES errors(id)
);
CREATE INDEX IF NOT EXISTS media_index_coverage_lookup_idx ON media_index_coverage(target_id,query_signature,complete,range_start,range_end);
CREATE TABLE IF NOT EXISTS quick_search_runs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    keywords_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    last_capture_id INTEGER NOT NULL DEFAULT 0,
    indexed_checked INTEGER NOT NULL DEFAULT 0,
    local_checked INTEGER NOT NULL DEFAULT 0,
    unavailable_count INTEGER NOT NULL DEFAULT 0,
    discarded_count INTEGER NOT NULL DEFAULT 0,
    missing_count INTEGER NOT NULL DEFAULT 0,
    non_text_count INTEGER NOT NULL DEFAULT 0,
    incomplete_count INTEGER NOT NULL DEFAULT 0,
    match_count INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    coverage_version INTEGER NOT NULL DEFAULT 0,
    capture_limit INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS quick_search_runs_fingerprint_idx ON quick_search_runs(fingerprint,id DESC);
CREATE TABLE IF NOT EXISTS quick_search_hits(
    run_id INTEGER NOT NULL,
    capture_id INTEGER NOT NULL,
    keyword TEXT NOT NULL,
    fields TEXT NOT NULL DEFAULT '',
    count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(run_id,capture_id,keyword),
    FOREIGN KEY(run_id) REFERENCES quick_search_runs(id) ON DELETE CASCADE,
    FOREIGN KEY(capture_id) REFERENCES captures(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS quick_search_coverage(
    run_id INTEGER NOT NULL,
    capture_id INTEGER NOT NULL,
    body_revision INTEGER NOT NULL,
    coverage_mask INTEGER NOT NULL,
    content_fingerprint TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(run_id,capture_id),
    FOREIGN KEY(run_id) REFERENCES quick_search_runs(id) ON DELETE CASCADE,
    FOREIGN KEY(capture_id) REFERENCES captures(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS quick_search_hits_capture_idx ON quick_search_hits(run_id,capture_id);
CREATE TABLE IF NOT EXISTS recovery_events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    category TEXT NOT NULL,
    message TEXT NOT NULL,
    capture_id INTEGER,
    media_capture_id INTEGER,
    details_json TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS recovery_events_created_idx ON recovery_events(created_at,id);
CREATE TABLE IF NOT EXISTS storage_objects(
    content_hash TEXT PRIMARY KEY,
    canonical_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    reference_count INTEGER NOT NULL DEFAULT 1,
    storage_method TEXT NOT NULL DEFAULT 'file',
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reviews(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'unreviewed',
    reviewer TEXT,
    reviewed_at TEXT,
    UNIQUE(match_id),
    FOREIGN KEY(match_id) REFERENCES document_matches(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS notes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER,
    capture_id INTEGER,
    text TEXT NOT NULL,
    author TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(match_id) REFERENCES document_matches(id) ON DELETE CASCADE,
    FOREIGN KEY(capture_id) REFERENCES captures(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS notes_match_idx ON notes(match_id,id);
CREATE TABLE IF NOT EXISTS tags(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS match_tags(
    match_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY(match_id,tag_id),
    FOREIGN KEY(match_id) REFERENCES document_matches(id) ON DELETE CASCADE,
    FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS duplicate_groups(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    method TEXT NOT NULL,
    representative_document_id INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(representative_document_id) REFERENCES documents(id)
);
CREATE TABLE IF NOT EXISTS duplicate_members(
    group_id INTEGER NOT NULL,
    document_id INTEGER NOT NULL,
    similarity REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY(group_id,document_id),
    FOREIGN KEY(group_id) REFERENCES duplicate_groups(id) ON DELETE CASCADE,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS duplicate_members_document_idx ON duplicate_members(document_id,group_id);
CREATE TABLE IF NOT EXISTS forum_threads(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_key TEXT NOT NULL UNIQUE,
    canonical_url TEXT,
    title TEXT,
    profile TEXT,
    first_timestamp TEXT,
    last_timestamp TEXT,
    post_count INTEGER NOT NULL DEFAULT 0,
    document_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS forum_posts(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id INTEGER NOT NULL,
    document_id INTEGER NOT NULL,
    capture_id INTEGER,
    post_key TEXT,
    username TEXT,
    posted_at TEXT,
    position INTEGER,
    body_text TEXT,
    body_hash TEXT,
    source_url TEXT,
    UNIQUE(thread_id,document_id,post_key),
    FOREIGN KEY(thread_id) REFERENCES forum_threads(id) ON DELETE CASCADE,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY(capture_id) REFERENCES captures(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS forum_posts_document_idx ON forum_posts(document_id,thread_id);
CREATE TABLE IF NOT EXISTS extractions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    extractor_name TEXT NOT NULL,
    extractor_type TEXT NOT NULL DEFAULT 'regex',
    field TEXT NOT NULL DEFAULT 'body',
    value TEXT NOT NULL,
    context TEXT,
    start_offset INTEGER,
    end_offset INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS extractions_value_idx ON extractions(extractor_name,value);
CREATE TABLE IF NOT EXISTS snapshot_diffs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    earlier_capture_id INTEGER NOT NULL,
    later_capture_id INTEGER NOT NULL,
    summary_json TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(earlier_capture_id,later_capture_id),
    FOREIGN KEY(earlier_capture_id) REFERENCES captures(id) ON DELETE CASCADE,
    FOREIGN KEY(later_capture_id) REFERENCES captures(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS media_targets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS media_captures(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_url TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    target_id INTEGER,
    source_document_id INTEGER,
    source_type TEXT NOT NULL DEFAULT 'cdx',
    query_signature TEXT NOT NULL,
    urlkey TEXT NOT NULL DEFAULT '',
    media_kind TEXT NOT NULL,
    extension TEXT,
    mimetype TEXT,
    statuscode TEXT,
    digest TEXT,
    length INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',
    skip_reason TEXT,
    classifier_revision INTEGER NOT NULL DEFAULT 0,
    download_attempts INTEGER NOT NULL DEFAULT 0,
    path TEXT,
    http_status INTEGER,
    final_url TEXT,
    bytes_saved INTEGER NOT NULL DEFAULT 0,
    content_hash TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(original_url,timestamp,query_signature),
    FOREIGN KEY(target_id) REFERENCES media_targets(id),
    FOREIGN KEY(source_document_id) REFERENCES documents(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS media_captures_state_idx ON media_captures(state,download_attempts,timestamp);
CREATE INDEX IF NOT EXISTS media_captures_url_idx ON media_captures(original_url,timestamp);
CREATE INDEX IF NOT EXISTS media_captures_signature_idx ON media_captures(query_signature,state,download_attempts,id);
CREATE INDEX IF NOT EXISTS media_captures_download_length_idx ON media_captures(query_signature,state,length,id,download_attempts);
CREATE TABLE IF NOT EXISTS media_index_state(
    target_id INTEGER NOT NULL,
    extension TEXT NOT NULL,
    year INTEGER NOT NULL,
    query_signature TEXT NOT NULL,
    resume_key TEXT,
    complete INTEGER NOT NULL DEFAULT 0,
    seen INTEGER NOT NULL DEFAULT 0,
    error_id INTEGER,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(target_id,extension,year,query_signature),
    FOREIGN KEY(target_id) REFERENCES media_targets(id) ON DELETE CASCADE,
    FOREIGN KEY(error_id) REFERENCES errors(id)
);

CREATE TABLE IF NOT EXISTS analysis_runs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    metadata_json TEXT,
    summary_json TEXT
);
CREATE TABLE IF NOT EXISTS legacy_assets(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    original_url TEXT NOT NULL,
    resolved_url TEXT,
    asset_type TEXT NOT NULL,
    player TEXT,
    external INTEGER NOT NULL DEFAULT 0,
    archive_status TEXT NOT NULL DEFAULT 'discovered',
    media_capture_id INTEGER,
    context TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(document_id,original_url,asset_type),
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY(media_capture_id) REFERENCES media_captures(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS legacy_assets_url_idx ON legacy_assets(original_url,archive_status);
CREATE INDEX IF NOT EXISTS legacy_assets_lookup_idx ON legacy_assets(external,archive_status,id);
CREATE TABLE IF NOT EXISTS provenance_edges(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_document_id INTEGER NOT NULL,
    mirror_document_id INTEGER NOT NULL,
    method TEXT NOT NULL,
    similarity REAL NOT NULL DEFAULT 1.0,
    source_timestamp TEXT,
    mirror_timestamp TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(source_document_id,mirror_document_id,method),
    FOREIGN KEY(source_document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY(mirror_document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS provenance_source_idx ON provenance_edges(source_document_id,mirror_document_id);
CREATE TABLE IF NOT EXISTS first_appearances(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    original_url TEXT NOT NULL,
    first_capture_id INTEGER NOT NULL,
    first_timestamp TEXT NOT NULL,
    last_capture_id INTEGER,
    last_timestamp TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(query,original_url),
    FOREIGN KEY(first_capture_id) REFERENCES captures(id) ON DELETE CASCADE,
    FOREIGN KEY(last_capture_id) REFERENCES captures(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS operation_runs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    message TEXT,
    progress_json TEXT,
    process_id INTEGER,
    app_version TEXT,
    retention_policy TEXT NOT NULL DEFAULT 'keep',
    config_json TEXT
);
CREATE INDEX IF NOT EXISTS operation_runs_status_idx ON operation_runs(status,updated_at);
CREATE TABLE IF NOT EXISTS network_events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    backend TEXT,
    endpoint TEXT,
    status INTEGER,
    elapsed REAL,
    message TEXT NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS network_events_created_idx ON network_events(created_at,id);
CREATE TABLE IF NOT EXISTS project_backups(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL UNIQUE,
    reason TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS repair_actions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    details TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ai_runs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_run_id INTEGER NOT NULL,
    prompt TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT 'openai',
    model TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    candidate_count INTEGER NOT NULL DEFAULT 0,
    result_count INTEGER NOT NULL DEFAULT 0,
    minimum_relevance INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error_message TEXT,
    metadata_json TEXT,
    FOREIGN KEY(scan_run_id) REFERENCES scan_runs(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ai_runs_scan_idx ON ai_runs(scan_run_id,id DESC);
CREATE TABLE IF NOT EXISTS ai_results(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ai_run_id INTEGER NOT NULL,
    match_id INTEGER NOT NULL,
    relevance_score INTEGER NOT NULL DEFAULT 0,
    confidence REAL NOT NULL DEFAULT 0,
    category TEXT,
    reason TEXT,
    evidence TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(ai_run_id,match_id),
    FOREIGN KEY(ai_run_id) REFERENCES ai_runs(id) ON DELETE CASCADE,
    FOREIGN KEY(match_id) REFERENCES document_matches(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ai_results_rank_idx ON ai_results(ai_run_id,relevance_score DESC,confidence DESC,id);
CREATE TABLE IF NOT EXISTS media_discovery_queue(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_signature TEXT NOT NULL,
    original_url TEXT NOT NULL,
    source_document_id INTEGER,
    source_type TEXT NOT NULL DEFAULT 'external_embedded',
    kind_hint TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    lookup_attempts INTEGER NOT NULL DEFAULT 0,
    result_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(query_signature,original_url),
    FOREIGN KEY(source_document_id) REFERENCES documents(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS media_discovery_state_idx ON media_discovery_queue(query_signature,state,lookup_attempts,id);
CREATE TABLE IF NOT EXISTS media_discovery_documents(
    query_signature TEXT NOT NULL,
    document_id INTEGER NOT NULL,
    content_hash TEXT,
    candidate_count INTEGER NOT NULL DEFAULT 0,
    scanned_at TEXT NOT NULL,
    PRIMARY KEY(query_signature,document_id),
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS media_discovery_captures(
    query_signature TEXT NOT NULL,
    capture_id INTEGER NOT NULL,
    extraction_version INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    mtime_ns INTEGER NOT NULL DEFAULT 0,
    candidate_count INTEGER NOT NULL DEFAULT 0,
    scanned_at TEXT NOT NULL,
    PRIMARY KEY(query_signature,capture_id,extraction_version),
    FOREIGN KEY(capture_id) REFERENCES captures(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS media_discovery_captures_capture_idx
ON media_discovery_captures(capture_id,extraction_version,query_signature);
CREATE TABLE IF NOT EXISTS site_issues(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target TEXT,
    host TEXT NOT NULL,
    stage TEXT NOT NULL,
    category TEXT NOT NULL,
    http_status INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    resolved INTEGER NOT NULL DEFAULT 0,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    UNIQUE(host,stage,category,http_status)
);
CREATE INDEX IF NOT EXISTS site_issues_open_idx ON site_issues(resolved,last_seen,host);

CREATE TABLE IF NOT EXISTS research_vectors(
    document_id INTEGER PRIMARY KEY,
    content_hash TEXT NOT NULL,
    backend TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    vector_blob BLOB NOT NULL,
    norm REAL NOT NULL DEFAULT 1.0,
    token_count INTEGER NOT NULL DEFAULT 0,
    indexed_at TEXT NOT NULL,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS research_vectors_backend_idx ON research_vectors(backend,indexed_at,document_id);
CREATE TABLE IF NOT EXISTS research_vector_bands(
    document_id INTEGER NOT NULL,
    band INTEGER NOT NULL,
    bucket TEXT NOT NULL,
    PRIMARY KEY(document_id,band),
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS research_vector_band_idx ON research_vector_bands(band,bucket,document_id);
CREATE TABLE IF NOT EXISTS research_entities(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    normalized TEXT NOT NULL,
    UNIQUE(kind,normalized)
);
CREATE INDEX IF NOT EXISTS research_entities_value_idx ON research_entities(normalized,kind);
CREATE TABLE IF NOT EXISTS research_document_entities(
    document_id INTEGER NOT NULL,
    entity_id INTEGER NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(document_id,entity_id),
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY(entity_id) REFERENCES research_entities(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS research_document_entities_entity_idx ON research_document_entities(entity_id,document_id);
CREATE TABLE IF NOT EXISTS research_edges(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_document_id INTEGER NOT NULL,
    target_document_id INTEGER NOT NULL,
    edge_type TEXT NOT NULL,
    weight REAL NOT NULL DEFAULT 1.0,
    evidence TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(source_document_id,target_document_id,edge_type),
    FOREIGN KEY(source_document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY(target_document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS research_edges_source_idx ON research_edges(source_document_id,edge_type,id);
CREATE INDEX IF NOT EXISTS research_edges_target_idx ON research_edges(target_document_id,edge_type,id);
CREATE TABLE IF NOT EXISTS research_queries(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    backend TEXT NOT NULL,
    result_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS research_query_results(
    query_id INTEGER NOT NULL,
    document_id INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    score REAL NOT NULL,
    vector_score REAL NOT NULL DEFAULT 0,
    text_score REAL NOT NULL DEFAULT 0,
    entity_score REAL NOT NULL DEFAULT 0,
    archive_score REAL NOT NULL DEFAULT 0,
    explanation_json TEXT,
    PRIMARY KEY(query_id,document_id),
    FOREIGN KEY(query_id) REFERENCES research_queries(id) ON DELETE CASCADE,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS research_query_rank_idx ON research_query_results(query_id,rank,document_id);
CREATE TABLE IF NOT EXISTS research_ai_runs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    input_document_ids_json TEXT,
    input_hashes_json TEXT,
    output_json TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    estimated_cost REAL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS research_ai_runs_created_idx ON research_ai_runs(created_at,id DESC);
CREATE TABLE IF NOT EXISTS research_ai_claims(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ai_run_id INTEGER NOT NULL,
    claim_text TEXT NOT NULL,
    support_document_ids_json TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    uncertainty TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(ai_run_id) REFERENCES research_ai_runs(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS research_ai_claims_run_idx ON research_ai_claims(ai_run_id,id);

CREATE TABLE IF NOT EXISTS project_merges(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_path TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL UNIQUE,
    merged_at TEXT NOT NULL,
    summary_json TEXT
);
"""


def column_names(database: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in database.execute(f"PRAGMA table_info({table})")}


def add_column_if_missing(database: sqlite3.Connection, table: str, definition: str) -> None:
    name = definition.split()[0]
    if name not in column_names(database, table):
        database.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")


def migrate_v2_to_v3(database: sqlite3.Connection) -> None:
    database.executescript(BASE_SCHEMA_SQL)
    add_column_if_missing(database, "targets", "settings_json TEXT")
    add_column_if_missing(database, "keyword_sets", "rules_json TEXT")
    add_column_if_missing(database, "scan_runs", "document_count INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "scan_runs", "match_count INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "scan_runs", "duration_seconds REAL NOT NULL DEFAULT 0")
    add_column_if_missing(database, "scan_runs", "metadata_json TEXT")
    add_column_if_missing(database, "document_matches", "excluded INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "document_matches", "required_missing INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "document_matches", "proximity_json TEXT")
    add_column_if_missing(database, "errors", "media_capture_id INTEGER")
    add_column_if_missing(database, "errors", "ignored INTEGER NOT NULL DEFAULT 0")
    database.execute("UPDATE schema_info SET version=3")


def migrate_v3_to_v4(database: sqlite3.Connection) -> None:
    database.executescript(BASE_SCHEMA_SQL)
    add_column_if_missing(database, "forum_threads", "canonical_url TEXT")
    add_column_if_missing(database, "forum_threads", "first_timestamp TEXT")
    add_column_if_missing(database, "forum_threads", "last_timestamp TEXT")
    add_column_if_missing(database, "forum_threads", "post_count INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "forum_threads", "document_count INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "forum_posts", "capture_id INTEGER")
    add_column_if_missing(database, "forum_posts", "body_hash TEXT")
    add_column_if_missing(database, "forum_posts", "source_url TEXT")
    add_column_if_missing(database, "extractions", "extractor_type TEXT NOT NULL DEFAULT 'regex'")
    add_column_if_missing(database, "extractions", "field TEXT NOT NULL DEFAULT 'body'")
    add_column_if_missing(database, "extractions", "start_offset INTEGER")
    add_column_if_missing(database, "extractions", "end_offset INTEGER")
    database.execute("CREATE INDEX IF NOT EXISTS extractions_value_idx ON extractions(extractor_name,value)")
    database.execute("CREATE INDEX IF NOT EXISTS forum_posts_thread_idx ON forum_posts(thread_id,position)")
    database.execute("UPDATE schema_info SET version=4")


def migrate_v4_to_v5(database: sqlite3.Connection) -> None:
    database.executescript(BASE_SCHEMA_SQL)
    database.execute("UPDATE schema_info SET version=5")


def migrate_v5_to_v6(database: sqlite3.Connection) -> None:
    database.executescript(BASE_SCHEMA_SQL)
    database.execute("UPDATE schema_info SET version=6")


def migrate_v6_to_v7(database: sqlite3.Connection) -> None:
    database.executescript(BASE_SCHEMA_SQL)
    database.execute("UPDATE schema_info SET version=7")



def migrate_v7_to_v8(database: sqlite3.Connection) -> None:
    database.executescript(BASE_SCHEMA_SQL)
    add_column_if_missing(database, "captures", "skip_reason TEXT")
    add_column_if_missing(database, "captures", "classifier_revision INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "captures", "local_path TEXT")
    add_column_if_missing(database, "captures", "content_hash TEXT")
    add_column_if_missing(database, "captures", "detected_encoding TEXT")
    add_column_if_missing(database, "documents", "body_zlib BLOB")
    add_column_if_missing(database, "documents", "body_chars INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "documents", "original_url TEXT")
    add_column_if_missing(database, "media_captures", "skip_reason TEXT")
    add_column_if_missing(database, "media_captures", "classifier_revision INTEGER NOT NULL DEFAULT 0")
    database.execute(
        "UPDATE documents SET original_url=(SELECT original_url FROM captures WHERE captures.id=documents.capture_id) "
        "WHERE COALESCE(original_url,'')=''"
    )
    database.execute("UPDATE schema_info SET version=8")

def migrate_v8_to_v9(database: sqlite3.Connection) -> None:
    database.executescript(BASE_SCHEMA_SQL)
    database.execute("UPDATE schema_info SET version=9")



def migrate_v9_to_v10(database: sqlite3.Connection) -> None:
    # Existing v9 tables must receive new columns before BASE_SCHEMA_SQL creates
    # indexes that reference them. This ordering is essential on every platform.
    add_column_if_missing(database, "captures", "resource_class TEXT NOT NULL DEFAULT 'unknown'")
    add_column_if_missing(database, "captures", "classification_reason TEXT")
    add_column_if_missing(database, "captures", "resource_classifier_revision INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "captures", "payload_availability TEXT NOT NULL DEFAULT 'not_acquired'")
    add_column_if_missing(database, "captures", "cleanup_pending INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "captures", "discarded_at TEXT")
    add_column_if_missing(database, "operation_runs", "retention_policy TEXT NOT NULL DEFAULT 'keep'")
    add_column_if_missing(database, "operation_runs", "config_json TEXT")
    add_column_if_missing(database, "index_pages", "layout_signature TEXT NOT NULL DEFAULT ''")
    add_column_if_missing(database, "media_index_pages", "layout_signature TEXT NOT NULL DEFAULT ''")
    add_column_if_missing(database, "quick_search_runs", "discarded_count INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "quick_search_runs", "missing_count INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "quick_search_runs", "non_text_count INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "quick_search_runs", "incomplete_count INTEGER NOT NULL DEFAULT 0")
    database.executescript(BASE_SCHEMA_SQL)
    database.execute(
        """UPDATE captures SET payload_availability=CASE
               WHEN local_path IS NOT NULL AND state='downloaded' THEN 'retained'
               WHEN local_path IS NOT NULL AND state IN ('downloaded_unscanned','scanning') THEN 'retained_unscanned'
               WHEN state='downloading' THEN 'partial' ELSE 'not_acquired' END
           WHERE payload_availability='not_acquired'"""
    )
    database.execute("UPDATE schema_info SET version=10")


def migrate_v10_to_v11(database: sqlite3.Connection) -> None:
    add_column_if_missing(database, "captures", "urlkey TEXT NOT NULL DEFAULT ''")
    add_column_if_missing(database, "captures", "payload_origin TEXT NOT NULL DEFAULT ''")
    add_column_if_missing(database, "captures", "payload_retention TEXT NOT NULL DEFAULT 'keep'")
    add_column_if_missing(database, "media_captures", "urlkey TEXT NOT NULL DEFAULT ''")
    database.executescript(BASE_SCHEMA_SQL)
    database.execute(
        """UPDATE captures SET payload_origin=CASE
               WHEN payload_availability IN ('spooled_unscanned','cleanup_pending') THEN 'acquired'
               WHEN local_path IS NOT NULL THEN 'legacy' ELSE payload_origin END,
               payload_retention=CASE
               WHEN payload_availability IN ('spooled_unscanned','cleanup_pending') THEN 'discard_after_scan'
               WHEN local_path IS NOT NULL THEN 'keep' ELSE payload_retention END
           WHERE COALESCE(payload_origin,'')=''"""
    )
    database.execute("UPDATE schema_info SET version=11")


def migrate_v11_to_v12(database: sqlite3.Connection) -> None:
    add_column_if_missing(database, "captures", "body_revision INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "quick_search_runs", "coverage_version INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(database, "quick_search_runs", "capture_limit INTEGER NOT NULL DEFAULT 0")
    database.executescript(BASE_SCHEMA_SQL)
    database.execute("UPDATE schema_info SET version=12")


def migrate_v12_to_v13(database: sqlite3.Connection) -> None:
    add_column_if_missing(database, "quick_search_coverage", "content_fingerprint TEXT NOT NULL DEFAULT ''")
    database.executescript(BASE_SCHEMA_SQL)
    database.execute("UPDATE schema_info SET version=13")


def _ensure_index(database: sqlite3.Connection, name: str, table: str, columns: tuple[str, ...]) -> None:
    existing = tuple(row[2] for row in database.execute(f"PRAGMA index_info({name})"))
    if existing and existing != columns:
        database.execute(f"DROP INDEX {name}")
    database.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table}({','.join(columns)})")


def initialize_schema(database: sqlite3.Connection) -> None:
    database.execute("PRAGMA foreign_keys=ON")
    has_schema = database.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_info'"
    ).fetchone()
    if not has_schema:
        database.executescript(BASE_SCHEMA_SQL)
        database.execute("DELETE FROM schema_info")
        database.execute("INSERT INTO schema_info(version) VALUES(?)", (SCHEMA_VERSION,))
    else:
        row = database.execute("SELECT version FROM schema_info LIMIT 1").fetchone()
        version = int(row[0]) if row else 0
        if version == 2:
            migrate_v2_to_v3(database)
            migrate_v3_to_v4(database)
            migrate_v4_to_v5(database)
            migrate_v5_to_v6(database)
            migrate_v6_to_v7(database)
            migrate_v7_to_v8(database)
            migrate_v8_to_v9(database)
            migrate_v9_to_v10(database)
            migrate_v10_to_v11(database)
        elif version == 3:
            migrate_v3_to_v4(database)
            migrate_v4_to_v5(database)
            migrate_v5_to_v6(database)
            migrate_v6_to_v7(database)
            migrate_v7_to_v8(database)
            migrate_v8_to_v9(database)
            migrate_v9_to_v10(database)
            migrate_v10_to_v11(database)
        elif version == 4:
            migrate_v4_to_v5(database)
            migrate_v5_to_v6(database)
            migrate_v6_to_v7(database)
            migrate_v7_to_v8(database)
            migrate_v8_to_v9(database)
            migrate_v9_to_v10(database)
            migrate_v10_to_v11(database)
        elif version == 5:
            migrate_v5_to_v6(database)
            migrate_v6_to_v7(database)
            migrate_v7_to_v8(database)
            migrate_v8_to_v9(database)
            migrate_v9_to_v10(database)
            migrate_v10_to_v11(database)
        elif version == 6:
            migrate_v6_to_v7(database)
            migrate_v7_to_v8(database)
            migrate_v8_to_v9(database)
            migrate_v9_to_v10(database)
            migrate_v10_to_v11(database)
        elif version == 7:
            migrate_v7_to_v8(database)
            migrate_v8_to_v9(database)
            migrate_v9_to_v10(database)
            migrate_v10_to_v11(database)
        elif version == 8:
            migrate_v8_to_v9(database)
            migrate_v9_to_v10(database)
            migrate_v10_to_v11(database)
        elif version == 9:
            migrate_v9_to_v10(database)
            migrate_v10_to_v11(database)
        elif version == 10:
            migrate_v10_to_v11(database)
        elif version == 11:
            migrate_v11_to_v12(database)
        elif version == 12:
            migrate_v12_to_v13(database)
        elif version != SCHEMA_VERSION:
            raise RuntimeError(f"unsupported Archive Scout schema version: {version}")
        else:
            database.executescript(BASE_SCHEMA_SQL)
    if int(database.execute("SELECT version FROM schema_info LIMIT 1").fetchone()[0]) == 11:
        migrate_v11_to_v12(database)
    if int(database.execute("SELECT version FROM schema_info LIMIT 1").fetchone()[0]) == 12:
        migrate_v12_to_v13(database)
    database.executescript("""
CREATE TRIGGER IF NOT EXISTS captures_body_revision_update
AFTER UPDATE OF local_path,content_hash,bytes_saved,payload_availability,detected_encoding,mimetype,resource_class ON captures
WHEN OLD.local_path IS NOT NEW.local_path OR OLD.content_hash IS NOT NEW.content_hash
 OR OLD.bytes_saved IS NOT NEW.bytes_saved OR OLD.payload_availability IS NOT NEW.payload_availability
 OR OLD.detected_encoding IS NOT NEW.detected_encoding OR OLD.mimetype IS NOT NEW.mimetype
 OR OLD.resource_class IS NOT NEW.resource_class
BEGIN
    UPDATE captures SET body_revision=body_revision+1 WHERE id=NEW.id;
END;
""")
    # URL-derived path collision checks are on the replay hot path.
    # Create this after older-schema migrations have added captures.local_path.
    database.execute("CREATE INDEX IF NOT EXISTS captures_local_path_idx ON captures(local_path)")
    # audit2: replay selection orders ordinary pending work by (length,id). The
    # older index placed the range-constrained attempt counter before that order,
    # forcing SQLite to build a temporary B-tree for every keyset page. Replace
    # that overlapping index on existing schema-9 projects as well as new ones.
    database.execute("DROP INDEX IF EXISTS captures_download_length_idx")
    database.execute(
        "CREATE INDEX IF NOT EXISTS captures_acquisition_order_idx "
        "ON captures(query_signature,state,length,id,download_attempts)"
    )
    _ensure_index(database, "captures_classification_idx", "captures",
                  ("query_signature", "id", "resource_classifier_revision"))
    _ensure_index(database, "media_captures_download_length_idx", "media_captures",
                  ("query_signature", "state", "length", "id", "download_attempts"))
    database.execute(
        "CREATE INDEX IF NOT EXISTS captures_payload_idx "
        "ON captures(payload_availability,state,id)"
    )
    try:
        # Pre-release schemas used a contentless FTS5 index. The canonical replay payload
        # remains on disk, so FTS stores only its inverted token index rather
        # than another full copy of each document body.
        fts_sql = database.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='documents_fts'"
        ).fetchone()
        recreate_fts = bool(fts_sql and "content=''" not in str(fts_sql[0] or ""))
        if recreate_fts:
            database.execute("DROP TABLE documents_fts")
            database.execute("DELETE FROM document_fts_versions")
            database.execute("DELETE FROM project_meta WHERE key='fts_current_mapping'")
        database.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(title,body_text,original_url,content='')"
        )
        database.execute("INSERT OR REPLACE INTO project_meta(key,value) VALUES('fts5','1')")
        database.execute("INSERT OR REPLACE INTO project_meta(key,value) VALUES('fts_contentless','1')")
        database.execute("DELETE FROM project_meta WHERE key='fts_external_content'")
        # During migration from an older development schema the legacy body_text is still
        # available. Seed the new compact token index before compaction removes
        # that redundant full-text copy. New documents are indexed on upsert.
        if recreate_fts:
            rows = database.execute(
                """SELECT d.id,d.title,d.body_text,d.original_url,c.original_url AS capture_original_url
                   FROM documents d JOIN captures c ON c.id=d.capture_id
                   WHERE COALESCE(d.body_text,'')<>''"""
            )
            database.executemany(
                "INSERT INTO documents_fts(rowid,title,body_text,original_url) VALUES(?,?,?,?)",
                ((int(row[0]), str(row[1] or ''), str(row[2] or ''), str(row[3] or row[4] or '')) for row in rows),
            )
        if not database.execute("SELECT 1 FROM project_meta WHERE key='fts_current_mapping'").fetchone():
            # Preserve the previous index and all evidence during migration.
            # Later replacements allocate fresh token IDs; repair/compact can
            # explicitly rebuild pre-existing stale postings from local bodies.
            database.execute("INSERT OR IGNORE INTO document_fts_versions(fts_rowid,document_id) SELECT id,id FROM documents")
            database.execute("INSERT INTO project_meta(key,value) VALUES('fts_current_mapping','1')")
    except Exception:
        database.execute("INSERT OR REPLACE INTO project_meta(key,value) VALUES('fts5','0')")
