from __future__ import annotations
from ..events import Stopped

import hashlib
import math
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass

from ..utils import normalize_search, utc_now

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
    features: list[str] = []
    if len(tokens) < 4:
        features = tokens
    else:
        features = [" ".join(tokens[index:index + 4]) for index in range(len(tokens) - 3)]
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


def cluster_duplicates(database: sqlite3.Connection, threshold: float = 0.90, *, stop_event=None) -> DuplicateSummary:
    from ..database.staging import derived_generation
    if database.execute("SELECT 1 FROM temp.sqlite_master WHERE type='table' AND name='duplicate_groups'").fetchone():
        return _cluster(database,threshold,stop_event)
    with derived_generation(database,['duplicate_groups','duplicate_members'],stop_event):
        return _cluster(database,threshold,stop_event)


def _cluster(database,threshold,stop_event):
    threshold = min(1.0,max(0.5,float(threshold)))
    def check():
        if stop_event is not None and stop_event.is_set(): raise Stopped
    # Full bodies are read only for changed/missing fingerprints.
    for row in database.execute("""SELECT d.id,COALESCE(r.revision,0) AS revision,
      d.normalized_hash,d.content_hash FROM documents d LEFT JOIN document_revisions r ON r.document_id=d.id
      LEFT JOIN duplicate_fingerprints f ON f.document_id=d.id
      WHERE COALESCE(d.body_text,'')<>'' AND (f.document_id IS NULL OR f.revision<>COALESCE(r.revision,0)) ORDER BY d.id"""):
        check()
        body=database.execute('SELECT body_text FROM documents WHERE id=?',(row['id'],)).fetchone()[0]
        value=simhash64(body or '')
        database.execute('INSERT OR REPLACE INTO duplicate_fingerprints VALUES(?,?,?,?,?,?,?,?)',
            (row['id'],row['revision'],str(row['normalized_hash'] or row['content_hash'] or ''),f'{value:016x}',
             *((value>>(band*16))&0xffff for band in range(4))))
    database.execute("DELETE FROM duplicate_fingerprints WHERE document_id NOT IN (SELECT id FROM documents WHERE COALESCE(body_text,'')<>'')")
    database.execute('DROP TABLE IF EXISTS temp.scout_duplicate_parents')
    database.execute('CREATE TEMP TABLE scout_duplicate_parents(id INTEGER PRIMARY KEY,parent INTEGER NOT NULL)')
    database.execute('INSERT INTO scout_duplicate_parents SELECT document_id,document_id FROM duplicate_fingerprints')
    def find(value):
        while True:
            parent=database.execute('SELECT parent FROM scout_duplicate_parents WHERE id=?',(value,)).fetchone()[0]
            if parent==value: return value
            value=parent
    def union(left,right):
        a,b=find(left),find(right)
        if a!=b: database.execute('UPDATE scout_duplicate_parents SET parent=? WHERE id=?',(min(a,b),max(a,b)))
    # Exact groups are streamed, including groups beyond the near-search cap.
    first=None;previous=None
    for row in database.execute("SELECT document_id,exact_key FROM duplicate_fingerprints WHERE exact_key<>'' ORDER BY exact_key,document_id"):
        check()
        if row[1]!=previous: first=row[0];previous=row[1]
        else: union(first,row[0])
    # Approximate four-band policy is preserved; only one bounded bucket is in RAM.
    for band in range(4):
        check()
        for bucket in database.execute(f'SELECT b{band} FROM duplicate_fingerprints GROUP BY b{band} HAVING COUNT(*)>1'):
            check()
            exact_count = database.execute(f"SELECT COUNT(DISTINCT exact_key),MIN(exact_key) FROM duplicate_fingerprints WHERE b{band}=?",(bucket[0],)).fetchone()
            if exact_count[0] == 1 and exact_count[1]:
                continue  # already unified exactly, no quadratic near comparisons
            rows=database.execute(f'SELECT document_id,value,exact_key FROM duplicate_fingerprints WHERE b{band}=? ORDER BY document_id LIMIT 2000',(bucket[0],)).fetchall()
            values=[(int(row[0]),int(row[1],16),str(row[2])) for row in rows]
            for index,(left,lv,lk) in enumerate(values):
                check()
                for right,rv,rk in values[index+1:]:
                    if lk and lk==rk: continue
                    if any(((lv>>(prior*16))&0xffff)==((rv>>(prior*16))&0xffff) for prior in range(band)): continue
                    if hamming_similarity(lv,rv)>=threshold: union(left,right)
    for row in database.execute('SELECT id FROM scout_duplicate_parents ORDER BY id'):
        check()
        database.execute('UPDATE scout_duplicate_parents SET parent=? WHERE id=?',(find(row[0]),row[0]))
    database.execute('CREATE INDEX IF NOT EXISTS scout_duplicate_parent_index ON scout_duplicate_parents(parent,id)')
    database.execute('DELETE FROM duplicate_members');database.execute('DELETE FROM duplicate_groups')
    summary=DuplicateSummary()
    for group in database.execute('SELECT parent,COUNT(*) FROM scout_duplicate_parents GROUP BY parent HAVING COUNT(*)>1 ORDER BY parent'):
        check()
        representative=int(group[0])
        fp=database.execute('SELECT exact_key,value FROM duplicate_fingerprints WHERE document_id=?',(representative,)).fetchone()
        distinct=database.execute('SELECT COUNT(DISTINCT f.exact_key) FROM duplicate_fingerprints f JOIN scout_duplicate_parents p ON p.id=f.document_id WHERE p.parent=?',(representative,)).fetchone()[0]
        method='exact' if distinct==1 and fp[0] else 'near'
        cursor=database.execute('INSERT INTO duplicate_groups(method,representative_document_id,created_at) VALUES(?,?,?)',(method,representative,utc_now()))
        group_id=cursor.lastrowid
        for member in database.execute('SELECT p.id,f.value,f.exact_key FROM scout_duplicate_parents p JOIN duplicate_fingerprints f ON f.document_id=p.id WHERE p.parent=? ORDER BY p.id',(representative,)):
            check()
            similarity=1.0 if fp[0] and member[2]==fp[0] else hamming_similarity(int(fp[1],16),int(member[1],16))
            database.execute('INSERT INTO duplicate_members VALUES(?,?,?)',(group_id,member[0],similarity))
        if method=='exact': summary.exact_groups+=1
        else: summary.near_groups+=1
        summary.grouped_documents+=group[1]
    return summary
