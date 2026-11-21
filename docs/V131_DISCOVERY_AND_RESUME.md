# Fast URL discovery and large project resume

## Fast URL discovery

In the desktop app, choose **Fast URL discovery from a domain list** in **Operation** and click **Start operation**. Pick a UTF-8 text file with one domain per line. Paste your original-URL regex, then accept the default capture dates of 2001–2003 or enter different dates.

Both `example.org` and `*.example.org/*` select the domain and its subdomains. Resuming an older list that contained rejected lines checks the input once and preserves the results and request counts for domains already processed.

The CLI runs the same operation. This PowerShell example looks for either filename at the end of a path or parameter value, allowing query strings afterward.

```powershell
archive-scout discover-urls "$HOME\Downloads\domains.txt" --output-dir "$HOME\Downloads\scout-url-results" --from 2001 --to 2003 --url-filter '(?i).*(?:/|=)(?:wtcbody2|jumper12)[.]jpg(?:[?&].*|%26.*)?$'
```

You can paste the raw regex or a literal `filter=original:…` argument. Paste the regex before URL encoding. The CLI defaults to a ceiling of one CDX request per second. `--requests-per-second` sets another ceiling up to eight. Wayback response times and cooldowns determine the actual rate. The default alone means six million distinct domains need at least 69 days for one request each. For faster first results, put the most relevant domains at the top of the list before starting.

Scout deduplicates domain spellings and searches each domain with subdomains included. It sends the capture dates and regex to CDX in the same query, and requests one filtered page of up to 100 matching URL keys. It retries a failed domain once after the first pass. The operation makes no replay requests. Capture dates record when Wayback indexed the capture in the selected interval. It includes indexed redirects and errors as potential leads and reports their status codes.

In the desktop app, the results folder is under `reports/fast-url-discovery` in the selected output folder. Its files are:

| File | What it contains |
|---|---|
| `matching_urls.txt` | Distinct original URLs, written as results arrive |
| `matches.tsv` | Earliest returned capture time for each exact original URL, its indexed HTTP status, and matching input domain |
| `deferred_domains.tsv` | Failed or limited lookups with their reason and any CDX continuation key |
| `summary.json` | Counts of domains by state, unique URLs, and rejected input lines |
| `discovery.sqlite3` | Resumable domain work and committed matches |

Use the same input file, output folder, dates, and regex to resume. The input must stay unchanged, including its modification time. Changing the query or input creates a separate run. After an abrupt exit, Scout restores the URL list from committed matches. Any in-flight domains whose results were not committed can be attempted again.

A page cap, malformed response, blocked domain, or timeout cannot establish that the domain contains no matching URLs. The result list also cannot establish that the underlying media is playable or that it was first published in the selected date range.

## Main text acquisition

The supplied v1.3.0 run recorded 479.656 seconds of queue preparation against an inventory of 2,870,647 captures. The last diagnostic recorded 11,377 new saves, 10,909 connection failures, 15 connect timeouts, 631 incomplete representations, 73 digest mismatches, 18 replay deadlines, 13 missing captures, and one HTTP server error. It recorded no HTTP 429s. The per-backend connection phases show refusals and setup failures across HTTPX, urllib3, and Curl. They do not establish whether the refusals originated at the local connection, a proxy, the network, or Wayback. Owner and file-publication timings also leave room for local costs, although the logs do not establish SQLite contention as the dominant cause of this run.

v1.3.1 stages at most 2,000 capture IDs at a time. It applies the existing target, date, keyword, and text/media routing checks before admission. Within a window, it offers pending work before prior errors and prioritizes smaller known payloads within each group. Persisted retry deadlines still apply. The owner reads due retries from the durable schedule and interleaves them with pending work during acquisition. Captures in future windows remain in the main inventory and are reconsidered on resume. Exact cumulative counts still require a scoped query, and recovery still checks interrupted local saves before replay.

A completed HTTP body previously updated backend reachability a second time, after the backend had already reported receiving headers. A late result could clear a cooldown triggered by newer connection failures. The transport now records that reachability evidence once per backend turn. The fix preserves validation for incomplete responses and digest mismatches. It does not establish the cause of the refusals in the supplied run.

## Validation

The canonical Linux/Python 3.12 test run executed 859 tests with zero failures and six display-dependent skips. The release verifier, offline benchmark, and source wheel build passed. A separate local HTTP fixture exercised the CLI against a real loopback server. It confirmed query encoding, domain deduplication, HTTP 429 recovery respecting a one-second Retry-After, and two returned matches.

On a synthetic inventory of 2,870,647 captures, staging the first useful window and counting scoped progress took 2.53 seconds. The first window contained 1,434 download candidates. The next 1.04 million eligible rows took 48.44 seconds to stage, and the final 1.04 million took 39.80 seconds. Peak process RSS including SQLite mappings was 357 MiB at the first window and 360 MiB at the middle and end. This fixture measured local queue staging and counts with no replay or body scanning. It used one target, mixed capture states, and no live service traffic. It is not a controlled speed comparison against the user’s logged run.

The full six-million-domain run, live overnight saves per second, and the packaged Windows/macOS desktop builds remain unverified.
