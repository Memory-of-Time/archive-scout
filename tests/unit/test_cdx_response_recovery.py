from __future__ import annotations

import threading
import unittest
import urllib.parse

from archive_scout.cdx.client import (
    HttpClient,
    MalformedCDXResponse,
    cdx_text_fallback_params,
    parse_cdx_text_response,
    parse_json_response,
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

    def test_page_count_text_fallback_is_numeric(self) -> None:
        params = cdx_text_fallback_params([
            ("url", "example.com/*"),
            ("output", "json"),
            ("showNumPages", "true"),
        ])
        self.assertEqual(parse_cdx_text_response(b"17\n", "endpoint", params), 17)


if __name__ == "__main__":
    unittest.main()
