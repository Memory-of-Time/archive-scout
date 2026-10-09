from __future__ import annotations

import threading
import unittest
import urllib.parse

from archive_scout.cdx.client import (
    HttpClient,
    MalformedCDXResponse,
    TransientRequestError,
    cdx_text_fallback_params,
    parse_cdx_text_response,
    parse_json_response,
    request_cdx_json_rows,
)
from archive_scout.cdx.parameters import parse_cdx
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.network.transports import TransportResponse


class SequenceTransport:
    def __init__(self, bodies: list[bytes]) -> None:
        self.bodies = list(bodies)
        self.urls: list[str] = []

    def request(self, url, headers, max_bytes, stop_event):
        self.urls.append(url)
        body = self.bodies.pop(0)
        return TransportResponse(200, {"Content-Type": "application/json"}, url, body, "test", 0.01)

    def close(self) -> None:
        return


class CDXResponseRecoveryTests(unittest.TestCase):
    def test_malformed_valid_looking_json_is_transient(self) -> None:
        body = b'[["timestamp","original"],["20010101000000","http://example.com/"], BROKEN]'
        with self.assertRaises(MalformedCDXResponse) as raised:
            parse_json_response(body, "https://web.archive.org/cdx/search/cdx")
        self.assertTrue(raised.exception.splittable)
        self.assertIn("line 1", str(raised.exception))

    def test_plain_text_fallback_parses_rows_and_resume_key(self) -> None:
        params = cdx_text_fallback_params([
            ("url", "example.com/*"),
            ("output", "json"),
            ("fl", "timestamp,original,mimetype,statuscode,digest,length"),
            ("showResumeKey", "true"),
        ])
        body = (
            "20010101000000 image/jpeg 200 ABC 123 http://example.com/photo.jpg&ref=thumb\n"
            "20010102000000 video/x-ms-wmv 200 DEF 456 http://example.com/a clip.wmv\n"
            "\n"
            "com%2Cexample%29%2F+20010102000000%21\n"
        ).encode()
        payload = parse_cdx_text_response(body, "endpoint", params)
        rows, resume = parse_cdx(payload)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["original"], "http://example.com/photo.jpg&ref=thumb")
        self.assertEqual(rows[1]["original"], "http://example.com/a clip.wmv")
        self.assertEqual(resume, "com%2Cexample%29%2F+20010102000000%21")

    def test_http_client_automatically_reissues_malformed_json_as_text(self) -> None:
        malformed = b'[["timestamp","original","mimetype","statuscode","digest","length"],["20010101000000"'
        text = b"20010101000000 image/jpeg 200 ABC 12 http://example.com/a.jpg\n"
        transport = SequenceTransport([malformed, text])
        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=1,
            user_agent="test",
            stop_event=threading.Event(),
            transport=transport,
        )
        payload = client.get_json_any(
            ("https://web.archive.org/cdx/search/cdx",),
            [
                ("url", "example.com/*"),
                ("output", "json"),
                ("fl", "timestamp,original,mimetype,statuscode,digest,length"),
                ("showResumeKey", "true"),
            ],
        )
        rows, resume = parse_cdx(payload)
        self.assertEqual(len(rows), 1)
        self.assertIsNone(resume)
        self.assertEqual(len(transport.urls), 2)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(transport.urls[1]).query)
        self.assertEqual(query["output"], ["txt"])
        self.assertEqual(query["gzip"], ["false"])
        self.assertEqual(query["fl"], ["urlkey,timestamp,mimetype,statuscode,digest,length,original"])

    def test_text_preference_does_not_make_timemap_json_fetch_twice(self) -> None:
        body = (
            b'[["urlkey","timestamp","original","mimetype","statuscode","digest","length"],'
            b'["com,example)/","20010101000000","http://example.com/","text/html","200","ABC","12"]]'
        )
        transport = SequenceTransport([body])
        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=1,
            user_agent="test",
            stop_event=threading.Event(),
            transport=transport,
        )
        result = client.get_cdx_rows_any(
            ("https://web.archive.org/web/timemap/json",),
            [
                ("url", "example.com/*"),
                ("output", "json"),
                ("fl", "urlkey,timestamp,original,mimetype,statuscode,digest,length"),
                ("page", "0"),
                ("pageSize", "9"),
            ],
            prefer_text=True,
        )
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0][1], "http://example.com/")
        self.assertEqual(len(transport.urls), 1)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(transport.urls[0]).query)
        self.assertEqual(query["output"], ["json"])

    def test_valid_json_returned_for_text_request_is_parsed_without_refetch(self) -> None:
        body = (
            b'[["urlkey","timestamp","original","mimetype","statuscode","digest","length"],'
            b'["com,example)/a","20010101000000","http://example.com/a","text/html","200","ABC","12"]]'
        )
        transport = SequenceTransport([body])
        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=1,
            user_agent="test",
            stop_event=threading.Event(),
            transport=transport,
        )
        result = client.get_cdx_rows_any(
            ("https://web.archive.org/cdx/search/cdx",),
            [
                ("url", "example.com/*"),
                ("output", "json"),
                ("fl", "urlkey,timestamp,original,mimetype,statuscode,digest,length"),
            ],
            prefer_text=True,
        )
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(len(transport.urls), 1)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(transport.urls[0]).query)
        self.assertEqual(query["output"], ["txt"])

    def test_valid_text_returned_for_json_request_is_parsed_without_refetch(self) -> None:
        body = b"com,example)/ 20010101000000 text/html 200 ABC 12 http://example.com/\n"
        transport = SequenceTransport([body])
        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=1,
            user_agent="test",
            stop_event=threading.Event(),
            transport=transport,
        )
        result = client.get_cdx_rows_any(
            ("https://web.archive.org/cdx/search/cdx",),
            [
                ("url", "example.com/*"),
                ("output", "json"),
                ("fl", "urlkey,timestamp,original,mimetype,statuscode,digest,length"),
            ],
            prefer_text=False,
        )
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0][1], "http://example.com/")
        self.assertEqual(len(transport.urls), 1)

    def test_native_timemap_page_never_reissues_malformed_json_as_text(self) -> None:
        transport = SequenceTransport([b'[["timestamp","original"],[' ])
        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=1,
            user_agent="test",
            stop_event=threading.Event(),
            transport=transport,
        )
        with self.assertRaises(TransientRequestError):
            request_cdx_json_rows(
                client,
                ("https://web.archive.org/web/timemap/json",),
                [
                    ("url", "example.com/*"),
                    ("output", "json"),
                    ("fl", "timestamp,original,mimetype,statuscode,digest,length"),
                    ("page", "0"),
                    ("pageSize", "9"),
                ],
            )
        self.assertEqual(len(transport.urls), 1)
        self.assertNotIn("output=txt", transport.urls[0])

    def test_page_count_text_fallback_is_numeric(self) -> None:
        params = cdx_text_fallback_params([
            ("url", "example.com/*"),
            ("output", "json"),
            ("showNumPages", "true"),
        ])
        self.assertEqual(parse_cdx_text_response(b"17\n", "endpoint", params), 17)


if __name__ == "__main__":
    unittest.main()
