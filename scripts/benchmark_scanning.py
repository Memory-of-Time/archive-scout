"""Offline synthetic scanner benchmark; pass the repository root as the first argument."""
import argparse, cProfile, hashlib, json, pstats, random, statistics, sys, time
from pathlib import Path
p = argparse.ArgumentParser()
p.add_argument('repository')
p.add_argument('--repeat', type=int, default=3)
p.add_argument('--profile')
p.add_argument('--output')
args = p.parse_args()
sys.path.insert(0, str(Path(args.repository).resolve()))
from archive_scout.content import parse_page
from archive_scout.scanning.keywords import compile_keywords, compile_prefilter
from archive_scout.scanning.scoring import analyze_content
from archive_scout.scanning.automaton import ahocorasick_rs
from archive_scout.constants import VERSION
rules = ['needle', 'mirror', 'upload', 'episode', 'rare footage', 'required: archive', 'exclude: irrelevant ad']
para = '<p>This archive contains an episode and a mirror of the original upload. More notes on rare footage. needle.</p>\n'
neutral = '<p>The document describes ordinary historical events with additional context and background information.</p>\n'
long_plain = 'ordinary background context ' * 24000 + 'needle rare footage archive'
large_rules = [f'candidate {i:04d}' for i in range(1500)] + rules
cases = [('no_match_html', neutral * 3500, rules), ('dense_html', para * 3500, rules), ('large_plain', long_plain, rules), ('large_hitlist', neutral * 2200 + para, large_rules), ('many_rules_hit', ''.join(('<p>archive ' + ' '.join((f'candidate {i:04d}' for i in range(50))) + '</p>\n' for _ in range(600))), large_rules)]
cases.append(('unique_paragraphs', ''.join((f'<p>Record {i}: archive mirror episode. Details {i * i} concern rare footage with needle and upload.</p>' for i in range(3500))), rules))
records = []
for name, body, terms in cases:
    raw = '<html><head><title>Source</title></head><body>' + body + '</body></html>' if name != 'large_plain' else body
    patterns = compile_keywords(terms)
    prefilter = compile_prefilter(patterns)

    def scan():
        title, visible, links = parse_page(raw, 'http://example.com/page')
        return analyze_content('http://example.com/page', title, visible, raw, links, patterns, prefilter)
    value = scan()
    durations = []
    for _ in range(args.repeat):
        start = time.perf_counter()
        value = scan()
        durations.append(time.perf_counter() - start)
    rec = {'case': name, 'chars': len(raw), 'rules': len(terms), 'seconds_median': statistics.median(durations), 'score': value['score'], 'result_sha256': hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()}
    records.append(rec)
    print(json.dumps(rec), flush=True)
    if args.profile and name == args.profile:
        prof = cProfile.Profile()
        prof.runcall(scan)
        pstats.Stats(prof).strip_dirs().sort_stats('cumtime').print_stats(24)
if args.output:
    Path(args.output).write_text(json.dumps({'version': VERSION, 'native': ahocorasick_rs is not None, 'cases': records}, indent=2) + '\n')
