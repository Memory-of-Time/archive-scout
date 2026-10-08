from __future__ import annotations

import hashlib
import math
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass

from ..document_store import document_body
from ..utils import normalize_search, utc_now
from ..events import Stopped

TOKEN_PATTERN = re.compile(r"[\w'-]+", re.UNICODE)


@dataclass(slots=True)
class DuplicateSummary:
    exact_groups: int = 0
    near_groups: int = 0
    grouped_documents: int = 0


def _tokens(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(normalize_search(text))


def simhash64(text: str) -> int:
    tokens = _tokens(text)
    if not tokens:
        return 0
    if len(tokens) < 4:
        features = tokens
    else:
        features = (" ".join(tokens[index:index + 4]) for index in range(len(tokens) - 3))
    vector = [0] * 64
    counts: dict[str, int] = defaultdict(int)
    for feature in features:
        counts[feature] += 1
    for feature, weight in counts.items():
        digest = hashlib.blake2b(feature.encode("utf-8", "replace"), digest_size=8).digest()
        value = int.from_bytes(digest, "big")
        scaled = 1 + int(math.log2(weight))
        for bit in range(64):
            vector[bit] += scaled if value & (1 << bit) else -scaled
    result = 0
    for bit, score in enumerate(vector):
        if score >= 0:
            result |= 1 << bit
    return result


def hamming_similarity(left: int, right: int) -> float:
    return 1.0 - ((left ^ right).bit_count() / 64.0)


class _UnionFind:
    def __init__(self, values: list[int]) -> None:
        self.parent = {value: value for value in values}

    def find(self, value: int) -> int:
        root = value
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[value] != value:
            next_value = self.parent[value]
            self.parent[value] = root
            value = next_value
        return root

    def union(self, left: int, right: int) -> None:
        a = self.find(left)
        b = self.find(right)
        if a != b:
            self.parent[max(a, b)] = min(a, b)


class _DiskMetricTree:
    """Exact Hamming-radius search. No band approximation or bucket cutoff."""
    def __init__(self, database):
        self.db = database
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_duplicate_tree")
        database.execute("CREATE TEMP TABLE archive_scout_duplicate_tree(id INTEGER PRIMARY KEY,value TEXT UNIQUE,parent INTEGER,distance INTEGER)")
        database.execute("CREATE INDEX temp.archive_scout_duplicate_child ON archive_scout_duplicate_tree(parent,distance)")
        self.root = None

    def matches(self, value, radius, stop_event=None):
        if self.root is None:
            return
        pending = [self.root]
        while pending:
            if stop_event is not None and stop_event.is_set():
                raise Stopped
            node = pending.pop()
            stored = self.db.execute("SELECT value FROM archive_scout_duplicate_tree WHERE id=?", (node,)).fetchone()[0]
            distance = (int(stored, 16) ^ value).bit_count()
            if distance <= radius:
                yield node
            pending.extend(int(row[0]) for row in self.db.execute(
                "SELECT id FROM archive_scout_duplicate_tree WHERE parent=? AND distance BETWEEN ? AND ?",
                (node, distance - radius, distance + radius)))

    def insert(self, document_id, value):
        node = self.root
        parent = None
        distance = 0
        while node is not None:
            stored = self.db.execute("SELECT value FROM archive_scout_duplicate_tree WHERE id=?", (node,)).fetchone()[0]
            distance = (int(stored, 16) ^ value).bit_count()
            if distance == 0:
                return node
            parent = node
            child = self.db.execute("SELECT id FROM archive_scout_duplicate_tree WHERE parent=? AND distance=?", (node, distance)).fetchone()
            node = int(child[0]) if child else None
        self.db.execute("INSERT INTO archive_scout_duplicate_tree VALUES(?,?,?,?)", (document_id, f"{value:016x}", parent, distance))
        if self.root is None:
            self.root = document_id
        return document_id


def cluster_duplicates(database: sqlite3.Connection, threshold: float = 0.90, *, stop_event=None, callback=None) -> DuplicateSummary:
    threshold = min(1.0, max(0.5, float(threshold)))
    radius = max(distance for distance in range(65) if 1 - distance / 64 >= threshold)
    def stopped():
        if stop_event is not None and stop_event.is_set():
            raise Stopped
    database.execute("DROP TABLE IF EXISTS temp.archive_scout_duplicate_work")
    database.execute("CREATE TEMP TABLE archive_scout_duplicate_work(id INTEGER PRIMARY KEY,parent INTEGER,key TEXT,value TEXT,active INTEGER NOT NULL DEFAULT 0)")
    database.execute("INSERT INTO archive_scout_duplicate_work(id,parent,key) SELECT id,id,COALESCE(NULLIF(normalized_hash,''),content_hash,'') FROM documents")
    database.execute("CREATE INDEX temp.archive_scout_duplicate_exact ON archive_scout_duplicate_work(key,id)")
    def find(value):
        root = value
        while True:
            parent = int(database.execute("SELECT parent FROM archive_scout_duplicate_work WHERE id=?", (root,)).fetchone()[0])
            if root == parent:
                break
            grandparent = int(database.execute("SELECT parent FROM archive_scout_duplicate_work WHERE id=?", (parent,)).fetchone()[0])
            database.execute("UPDATE archive_scout_duplicate_work SET parent=? WHERE id=?", (grandparent, root))
            root = parent
        return root
    active_components = 0
    def union(left, right):
        nonlocal active_components
        a, b = find(left), find(right)
        if a != b:
            first, second = min(a,b), max(a,b)
            if active_components:
                first_active = database.execute("SELECT active FROM archive_scout_duplicate_work WHERE id=?", (first,)).fetchone()[0]
                second_active = database.execute("SELECT active FROM archive_scout_duplicate_work WHERE id=?", (second,)).fetchone()[0]
                if first_active and second_active:
                    active_components -= 1
                if second_active and not first_active:
                    database.execute("UPDATE archive_scout_duplicate_work SET active=1 WHERE id=?", (first,))
            database.execute("UPDATE archive_scout_duplicate_work SET parent=? WHERE id=?", (first, second))
    tree = _DiskMetricTree(database)
    try:
        previous_key = None
        representative = None
        for row in database.execute("SELECT id,key FROM archive_scout_duplicate_work WHERE key<>'' ORDER BY key,id"):
            stopped()
            if row['key'] == previous_key:
                union(representative, int(row['id']))
            else:
                previous_key, representative = row['key'], int(row['id'])
        total = int(database.execute("SELECT COUNT(*) FROM documents").fetchone()[0])
        for index, row in enumerate(database.execute("SELECT d.*,c.mimetype,c.detected_encoding FROM documents d JOIN captures c ON c.id=d.capture_id ORDER BY d.id"), 1):
            stopped()
            document_id = int(row['id'])
            value = simhash64(document_body(row))
            database.execute("UPDATE archive_scout_duplicate_work SET value=? WHERE id=?", (f"{value:016x}", document_id))
            identical = database.execute("SELECT id FROM archive_scout_duplicate_tree WHERE value=?", (f"{value:016x}",)).fetchone()
            if identical:
                union(int(identical[0]), document_id)
            else:
                for other in tree.matches(value, radius, stop_event):
                    union(document_id, other)
                    # The published contract is complete connected groups. Once
                    # every inserted fingerprint belongs to one component, any
                    # further matching edges are redundant. Keep every distinct
                    # fingerprint in the tree so later bridges remain discoverable.
                    if active_components == 1:
                        break
                tree.insert(document_id, value)
            root = find(document_id)
            if not database.execute("SELECT active FROM archive_scout_duplicate_work WHERE id=?", (root,)).fetchone()[0]:
                database.execute("UPDATE archive_scout_duplicate_work SET active=1 WHERE id=?", (root,))
                active_components += 1
            if callback and (index % 100 == 0 or index == total):
                from ..events import ProgressEvent
                callback(ProgressEvent('duplicates', f'Compared {index:,}/{total:,} document fingerprints', index, total))
        for row in database.execute("SELECT id FROM archive_scout_duplicate_work ORDER BY id"):
            stopped()
            find(int(row[0]))
        database.execute("CREATE INDEX temp.archive_scout_duplicate_groups ON archive_scout_duplicate_work(parent,id)")
        summary = DuplicateSummary()
        if callback:
            from ..events import ProgressEvent
            callback(ProgressEvent('duplicates_publish', 'Publishing complete duplicate groups'))
        stopped()
        with database:
            database.execute("DELETE FROM duplicate_members")
            database.execute("DELETE FROM duplicate_groups")
            for group in database.execute("SELECT parent,COUNT(*) FROM archive_scout_duplicate_work GROUP BY parent HAVING COUNT(*)>1 ORDER BY parent"):
                stopped()
                representative, size = int(group[0]), int(group[1])
                first = database.execute("SELECT key,value FROM archive_scout_duplicate_work WHERE id=?", (representative,)).fetchone()
                all_exact = bool(first['key']) and not database.execute(
                    "SELECT 1 FROM archive_scout_duplicate_work WHERE parent=? AND key<>? LIMIT 1", (representative, first['key'])).fetchone()
                method = 'exact' if all_exact else 'near'
                cursor = database.execute("INSERT INTO duplicate_groups(method,representative_document_id,created_at) VALUES(?,?,?)", (method, representative, utc_now()))
                group_id = int(cursor.lastrowid)
                for member in database.execute("SELECT id,value FROM archive_scout_duplicate_work WHERE parent=? ORDER BY id", (representative,)):
                    stopped()
                    similarity = 1.0 if all_exact else hamming_similarity(int(first['value'],16), int(member['value'],16))
                    database.execute("INSERT INTO duplicate_members(group_id,document_id,similarity) VALUES(?,?,?)", (group_id, int(member['id']), similarity))
                summary.exact_groups += int(all_exact)
                summary.near_groups += int(not all_exact)
                summary.grouped_documents += size
        return summary
    finally:
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_duplicate_work")
        database.execute("DROP TABLE IF EXISTS temp.archive_scout_duplicate_tree")
