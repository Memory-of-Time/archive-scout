"""Offline hostname extraction with independent, transactional evidence coverage."""
from __future__ import annotations

import hashlib
import html
import ipaddress
import json
import re
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote, urlsplit

import idna

from .events import ProgressEvent, Stopped
from .utils import atomic_write_lines, atomic_write_text

DATA = Path(__file__).parent / 'assets' / 'domains'
_CANDIDATE = re.compile(r'(?<![\w.-])[\w-]+(?:\.[\w-]+)+\.?(?![\w.-])', re.UNICODE)
_WRAPPER = re.compile(r'https?://web\.archive\.org/(?:web/)?\d{1,14}[a-z_]{0,8}/(?=https?://)', re.I)
_ESCAPE = re.compile(r'\\(?:u00|x)(2[eEfF]|3[aA])')
_AUTHORITY = re.compile(r'(https?://)([^\s/<>"\']+)', re.I)
_EMAIL_LOCAL = re.compile(r'(?<![\w.+%~-])[\w.+%~-]+@')
_INLINE = re.compile(r'(?<=[\w.-])</?(?:a|b|i|u|s|span|em|strong|font|small|big|code|label)(?:\s[^<>]*)?\s*/?>(?=[\w.-])', re.I)


def canonical_host(value: str) -> str:
    """Canonical full host, with no public-suffix/root-domain reduction."""
    value = str(value).strip().rstrip('.').translate(str.maketrans('。．｡', '...'))
    if '://' in value or value.startswith('//'):
        try:
            value = urlsplit(value if '://' in value else 'https:' + value).hostname or ''
        except ValueError:
            return ''
    if len(value) > 253:
        return ''
    try:
        ipaddress.ip_address(value)
        return ''
    except ValueError:
        pass
    try:
        host = idna.encode(value, uts46=True, transitional=False).decode('ascii').lower()
    except (idna.IDNAError, UnicodeError):
        return ''
    if len(host) > 253 or '.' not in host:
        return ''
    return host


@lru_cache(maxsize=1)
def rules():
    tlds_data = (DATA / 'tlds.txt').read_bytes()
    suffix_data = (DATA / 'public_suffix_list.dat').read_bytes()
    tlds = {line.lower() for line in tlds_data.decode().splitlines() if line and not line.startswith('#')}
    exact, wildcard, exception = set(), set(), set()
    for line in suffix_data.decode().splitlines():
        line = line.strip()
        if not line or line.startswith('//'):
            continue
        target = exception if line.startswith('!') else wildcard if line.startswith('*.') else exact
        value = line.removeprefix('!').removeprefix('*.')
        try:
            target.add(idna.encode(value,uts46=True).decode('ascii').lower())
        except idna.IDNAError:
            raise RuntimeError('Invalid bundled public suffix rule: ' + line)
    recipe = 'domains-v1-full-host:' + hashlib.sha256(tlds_data+suffix_data).hexdigest()
    return tlds, exact, wildcard, exception, recipe


def _admitted(host, data):
    tlds, exact, wildcard, exception, _ = data
    parts = host.split('.')
    if parts[-1] not in tlds:
        return False
    suffix_length = 1
    for index in range(len(parts)):
        suffix = '.'.join(parts[index:])
        if suffix in exception:
            suffix_length = len(parts)-index-1
            break
        if suffix in exact:
            suffix_length = max(suffix_length,len(parts)-index)
        if index and suffix in wildcard:
            suffix_length = max(suffix_length,len(parts)-index+1)
    return len(parts) > suffix_length


def iter_domains(text: str):
    """One candidate pass, no DNS, no per-TLD full-body search, no global set."""
    data = rules()
    text = html.unescape(unquote(text)).translate(str.maketrans('。．｡', '...'))
    text = _ESCAPE.sub(lambda m: chr(int(m[1],16)),text).replace('\\/','/')
    text = _WRAPPER.sub('',text)
    # Credentials and email local-parts must not become apparent dotted hosts.
    if '@' in text:
        text = _AUTHORITY.sub(lambda m: m[1] + m[2].rsplit('@',1)[-1],text)
        text = _EMAIL_LOCAL.sub('',text)
    for match in _CANDIDATE.finditer(text):
        host = canonical_host(match[0])
        if host and _admitted(host,data):
            yield host


def domain_text(raw, visible='', links=()):
    """Reuse parsed views only when markup/escaping made them distinct.

    Visible labels can span tags. Raw markup alone would miss such a host.
    Common plaintext/HTML bodies already contain their visible text verbatim.
    """
    extra = []
    if '<' in raw:
        inline = _INLINE.sub('',raw)
        if inline != raw:
            extra.append(inline)
    if visible and visible not in raw:
        extra.append(visible)
    extra.extend(str(link) for link in links if link)
    return raw if not extra else '\n'.join((raw,*extra))


def _recipe_id(database, create=True):
    fingerprint = rules()[-1]
    row = database.execute('SELECT id FROM domain_recipes WHERE fingerprint=?',(fingerprint,)).fetchone()
    if row:
        return int(row[0])
    if not create:
        return -1
    return int(database.execute('INSERT INTO domain_recipes(fingerprint) VALUES(?)',(fingerprint,)).lastrowid)


def commit_domains(database, capture_id, content_hash, source):
    """Caller commits source coverage and unique hosts in the same transaction."""
    recipe = _recipe_id(database)
    old = database.execute('SELECT content_hash,status FROM domain_sources WHERE recipe=? AND capture_id=?',(recipe,capture_id)).fetchone()
    if old and old[0] == content_hash and old[1] == 'complete':
        return
    database.execute('DELETE FROM domain_hosts WHERE recipe=? AND capture_id=?',(recipe,capture_id))
    batch = set()
    for host in iter_domains(source):
        batch.add(host)
        if len(batch) >= 256:
            database.executemany('INSERT OR IGNORE INTO domain_hosts VALUES(?,?,?)',((recipe,capture_id,h) for h in batch))
            batch.clear()
    database.executemany('INSERT OR IGNORE INTO domain_hosts VALUES(?,?,?)',((recipe,capture_id,h) for h in batch))
    database.execute("INSERT OR REPLACE INTO domain_sources VALUES(?,?,?,'complete','')",(recipe,capture_id,content_hash))


def _scope(database,config):
    from .database.scope import prepare_saved_scope,saved_scope_predicate
    if not config.targets:
        return '1'
    prepare_saved_scope(database,config)
    return saved_scope_predicate()


def backfill_domains(database,config,stop_event,callback=None):
    """Only unproven saved inputs are read; source errors stay unresolved."""
    from .content import decode_bytes, parse_page
    with database:
        recipe = _recipe_id(database)
    scope = _scope(database,config)
    last_id = 0
    count = 0
    while True:
        rows = database.execute(f"""SELECT c.id,d.path,d.content_hash,
          COALESCE(NULLIF(v.response_content_type,''),c.mimetype,'') AS mimetype FROM captures c
          JOIN documents d ON d.capture_id=c.id
          LEFT JOIN capture_payload_evidence v ON v.capture_id=c.id LEFT JOIN domain_sources ds
          ON ds.capture_id=c.id AND ds.recipe=? WHERE c.id>? AND ({scope})
          AND (ds.capture_id IS NULL OR ds.status<>'complete' OR ds.content_hash<>d.content_hash)
          ORDER BY c.id LIMIT 32""",(recipe,last_id)).fetchall()
        if not rows:
            break
        for row in rows:
            if stop_event.is_set():
                raise Stopped
            last_id = int(row['id'])
            try:
                raw = Path(row['path']).read_bytes()
                actual = hashlib.sha256(raw).hexdigest()
                if row['content_hash'] and actual != row['content_hash']:
                    raise RuntimeError('Saved body differs from committed domain input')
                decoded = decode_bytes(raw,row['mimetype'] or '')
                original = database.execute('SELECT original_url FROM captures WHERE id=?',(last_id,)).fetchone()[0]
                _,visible,links = parse_page(decoded,original)
                with database:
                    commit_domains(database,last_id,actual,domain_text(decoded,visible,links))
            except OSError as exc:
                with database:
                    database.execute("INSERT OR REPLACE INTO domain_sources VALUES(?,?,?,'error',?)",(recipe,last_id,row['content_hash'] or '',repr(exc)))
            except RuntimeError as exc:
                with database:
                    database.execute("INSERT OR REPLACE INTO domain_sources VALUES(?,?,?,'error',?)",(recipe,last_id,row['content_hash'] or '',str(exc)))
            count += 1
            if callback and count % 100 == 0:
                callback(ProgressEvent('domains',f'Extracted domains from {count:,} saved inputs'))


def publish_domains(database,config):
    recipe = _recipe_id(database,create=False)
    scope = _scope(database,config)
    gaps = int(database.execute(f"""SELECT COUNT(*) FROM captures c JOIN documents d ON d.capture_id=c.id
      LEFT JOIN domain_sources ds ON ds.capture_id=c.id AND ds.recipe=? WHERE ({scope})
      AND (ds.capture_id IS NULL OR ds.status<>'complete' OR ds.content_hash<>d.content_hash)""",(recipe,)).fetchone()[0])
    def lines():
        for row in database.execute(f"""SELECT DISTINCT h.host FROM domain_hosts h
          JOIN domain_sources ds ON ds.recipe=h.recipe AND ds.capture_id=h.capture_id
          JOIN captures c ON c.id=h.capture_id JOIN documents d ON d.capture_id=c.id
          WHERE h.recipe=? AND ds.status='complete' AND ds.content_hash=d.content_hash
          AND ({scope}) ORDER BY h.host""",(recipe,)):
            yield '*.' + row[0] + '/*'
    path = config.output_dir / 'reports' / 'domains.txt'
    atomic_write_lines(path,lines())
    examples = [dict(row) for row in database.execute(f"""SELECT c.id AS capture_id,
      COALESCE(NULLIF(ds.error,''),'Not yet processed under this extraction recipe') AS reason
      FROM captures c JOIN documents d ON d.capture_id=c.id
      LEFT JOIN domain_sources ds ON ds.capture_id=c.id AND ds.recipe=? WHERE ({scope})
      AND (ds.capture_id IS NULL OR ds.status<>'complete' OR ds.content_hash<>d.content_hash)
      ORDER BY c.id LIMIT 16""",(recipe,))]
    atomic_write_text(path.with_name('domains_status.json'),json.dumps({'recipe':rules()[-1],'unresolved_saved_inputs':gaps,
        'unresolved_examples':examples,
        'status':'incomplete' if gaps else 'complete_for_saved_inputs','scope':config.targets,
        'from':config.from_date,'to':config.to_date,'notice':'Saved-input extraction coverage; acquisition/index completeness is reported separately.'},indent=2)+'\n')
    return path,gaps
