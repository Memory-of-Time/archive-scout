from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from archive_scout.config import MediaConfig, ProjectConfig
from archive_scout.constants import VERSION
from archive_scout.downloads.downloader import _download_capture
from archive_scout.media.downloader import fetch_media
from archive_scout.network.transports import PreviewRejected


class PrefixClient:
    def __init__(self, body: bytes, content_type: str, prefix_bytes: int = 8192):
        self.body = body
        self.content_type = content_type
        self.prefix_bytes = prefix_bytes
        self.calls = 0
        self.bytes_consumed = 0

    def close(self):
        return None

    def download_to_path(self, url, destination, max_bytes, **kwargs):
        del max_bytes
        self.calls += 1
        prefix = self.body[: self.prefix_bytes]
        self.bytes_consumed += len(prefix)
        validator = kwargs.get("preview_validator")
        if validator:
            classification = validator({"content-type": self.content_type}, prefix)
            if classification:
                raise PreviewRejected(classification)
        self.bytes_consumed = len(self.body)
        Path(destination).write_bytes(self.body)
        return {
            "path": Path(destination),
            "bytes": len(self.body),
            "content_hash": hashlib.sha256(self.body).hexdigest(),
            "preview": self.body[:20000],
            "status": 200,
            "headers": {"content-type": self.content_type},
            "final_url": url,
        }


def run(payload_bytes: int) -> dict:
    png = b"\x89PNG\r\n\x1a\n" + b"x" * max(0, payload_bytes - 8)
    output: dict[str, object] = {"version": VERSION, "live_network": False}

    with tempfile.TemporaryDirectory(prefix="archive-scout-audit5-defer-") as temp:
        root = Path(temp)
        config = ProjectConfig(
            root,
            ["example.com/*"],
            [],
            max_file_mb=max(1, payload_bytes / 1024 / 1024 + 1),
            media=MediaConfig(
                enabled=True,
                include_images=True,
                include_videos=False,
                include_extensions=["png"],
            ),
        ).normalized()
        row = {
            "id": 1,
            "timestamp": "20010101000000",
            "original_url": "http://example.com/get?id=1",
            "mimetype": "application/octet-stream",
            "length": len(png),
            "content_hash": "",
        }
        client = PrefixClient(png, "application/octet-stream")
        result = _download_capture(row, root / "captures" / "probe.txt", config, client, compute_hash=False)
        output["ambiguous_text_phase"] = {
            "result_kind": result.get("kind"),
            "http_calls": client.calls,
            "bytes_consumed": client.bytes_consumed,
            "final_text_file": (root / "captures" / "probe.txt").exists(),
        }

    with tempfile.TemporaryDirectory(prefix="archive-scout-audit5-format-") as temp:
        root = Path(temp)
        config = ProjectConfig(
            root,
            ["example.com/*"],
            [],
            max_file_mb=max(1, payload_bytes / 1024 / 1024 + 1),
            media=MediaConfig(
                enabled=True,
                include_images=True,
                include_videos=False,
                include_extensions=["jpg"],
            ),
        ).normalized()
        row = {
            "id": 1,
            "timestamp": "20010101000000",
            "original_url": "http://example.com/get?id=1",
            "mimetype": "image/jpeg",
            "media_kind": "image",
            "extension": ".jpg",
        }
        client = PrefixClient(png, "image/png")
        result = fetch_media(row, config, client)
        output["jpg_only_png_payload"] = {
            "result_kind": result.get("kind"),
            "reason": result.get("reason", ""),
            "http_calls": client.calls,
            "bytes_consumed": client.bytes_consumed,
            "final_media_files": sum(1 for path in (root / "media").rglob("*") if path.is_file()),
        }

    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit5 supplemental-media phase benchmark")
    parser.add_argument("--payload-bytes", type=int, default=512 * 1024)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(max(8192, args.payload_bytes))
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
