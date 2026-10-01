from __future__ import annotations

import codecs
import re

_CHARSET_RE = re.compile(r"charset\s*=\s*['\"]?([A-Za-z0-9._-]+)", re.IGNORECASE)
_SVG_ROOT_RE = re.compile(
    r"^\s*(?:<\?xml[^>]*>\s*)?(?:<!--.*?-->\s*)*(?:<!DOCTYPE[^>]*>\s*)*<svg(?:\s|>)",
    re.IGNORECASE | re.DOTALL,
)

_IMAGE_FTYP_BRANDS = {
    b"avif", b"avis", b"heic", b"heix", b"hevc", b"hevx", b"heim", b"heis", b"mif1", b"msf1",
}
_AUDIO_FTYP_BRANDS = {b"M4A ", b"M4B ", b"M4P ", b"M4R ", b"F4A ", b"F4B "}


def _charset(content_type: str) -> str:
    match = _CHARSET_RE.search(content_type or "")
    return match.group(1).casefold() if match else ""


def _text_encoding(data: bytes, content_type: str) -> str:
    if data.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    if data.startswith(codecs.BOM_UTF32_LE):
        return "utf-32-le"
    if data.startswith(codecs.BOM_UTF32_BE):
        return "utf-32-be"
    if data.startswith(codecs.BOM_UTF16_LE):
        return "utf-16-le"
    if data.startswith(codecs.BOM_UTF16_BE):
        return "utf-16-be"
    charset = _charset(content_type)
    aliases = {
        "utf16": "utf-16", "utf-16le": "utf-16-le", "utf-16be": "utf-16-be",
        "utf32": "utf-32", "utf-32le": "utf-32-le", "utf-32be": "utf-32-be",
    }
    normalized = aliases.get(charset, charset)
    if normalized.startswith(("utf-16", "utf-32")):
        return normalized
    return ""


def _decoded_text_shape(data: bytes, content_type: str) -> tuple[str | None, str | None]:
    encoding = _text_encoding(data, content_type)
    if not encoding:
        return None, None
    try:
        decoder = codecs.getincrementaldecoder(encoding)("strict")
        text = decoder.decode(data, final=False)
    except (LookupError, UnicodeDecodeError):
        return None, None
    if not text:
        return None, None
    # A decoded BOM is a format marker, not user text. Exclude it from the
    # printable-ratio test so short valid UTF-16/32 prefixes cannot fall
    # through into MPEG sync detection merely because U+FEFF is non-printing.
    visible_text = text.lstrip("\ufeff")
    if not visible_text:
        return None, None
    printable = sum(ch.isprintable() or ch.isspace() for ch in visible_text) / max(1, len(visible_text))
    if printable < 0.85:
        return None, None
    if _SVG_ROOT_RE.search(text[:8192]):
        return "image", "svg_root"
    return "text", f"decoded_{encoding}"


def _ascii_svg_root(data: bytes) -> bool:
    # SVG syntax is ASCII-compatible in UTF-8/legacy encodings. UTF-16/32 is
    # handled by _decoded_text_shape before this helper is reached.
    try:
        text = data[:16384].decode("utf-8", "strict")
    except UnicodeDecodeError:
        try:
            text = data[:16384].decode("windows-1252", "strict")
        except UnicodeDecodeError:
            return False
    return bool(_SVG_ROOT_RE.search(text))


def _valid_bmp(data: bytes) -> bool:
    if len(data) < 14 or data[:2] != b"BM":
        return False
    file_size = int.from_bytes(data[2:6], "little")
    reserved = data[6:10]
    pixel_offset = int.from_bytes(data[10:14], "little")
    if reserved != b"\x00\x00\x00\x00" or pixel_offset < 14:
        return False
    return file_size == 0 or file_size >= pixel_offset


def _valid_id3(data: bytes) -> bool:
    if len(data) < 10 or data[:3] != b"ID3":
        return False
    major, revision, flags = data[3], data[4], data[5]
    if major not in {2, 3, 4} or revision == 0xFF:
        return False
    if flags & 0x0F:
        return False
    return all(byte < 0x80 for byte in data[6:10])


def _valid_swf(data: bytes) -> bool:
    if len(data) < 8 or data[:3] not in {b"FWS", b"CWS", b"ZWS"}:
        return False
    version = data[3]
    declared = int.from_bytes(data[4:8], "little")
    return 1 <= version <= 50 and declared >= 8


def _mpeg_frame_class(data: bytes) -> str | None:
    if len(data) < 4 or data[0] != 0xFF or (data[1] & 0xE0) != 0xE0:
        return None
    version_bits = (data[1] >> 3) & 0x03
    layer_bits = (data[1] >> 1) & 0x03
    bitrate_index = (data[2] >> 4) & 0x0F
    sample_index = (data[2] >> 2) & 0x03
    if version_bits == 1 or layer_bits == 0 or bitrate_index in {0, 15} or sample_index == 3:
        return None
    return "audio"


def structural_payload_class(data: bytes, content_type: str = "") -> tuple[str | None, str | None]:
    """Classify only when a bounded prefix contains trustworthy structural evidence.

    Returns ``(None, None)`` when the prefix is insufficient or ambiguous. This
    deliberately avoids two/three-byte magic checks that can reject ordinary
    prose such as "BMW..." or "ID3 formats...".
    """
    if not data:
        return None, None

    # Strong signatures whose full identifying bytes are already present.
    if data.startswith((b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF87a", b"GIF89a", b"II*\x00", b"MM\x00*")):
        return "image", "image_magic"
    if data.startswith((b"\x00\x00\x01\x00", b"\x00\x00\x02\x00")):
        return "image", "icon_magic"
    if data.startswith(b"FLV\x01"):
        return "video", "flv_magic"
    if data.startswith((b"OggS", b"fLaC")):
        return "audio", "audio_magic"
    if data.startswith((b"%PDF-", b"PK\x03\x04", b"PK\x05\x06", b"\x1f\x8b", b"Rar!", b"7z\xbc\xaf\x27\x1c", b"\xd0\xcf\x11\xe0", b"\x7fELF", b"SQLite format 3\x00")):
        return "other_binary", "binary_magic"
    if data.startswith((b"wOFF", b"wOF2", b"OTTO", b"\x00\x01\x00\x00")):
        return "other_binary", "font_magic"
    if data.startswith((b"\x1a\x45\xdf\xa3", b"\x30\x26\xb2\x75\x8e\x66\xcf\x11")):
        return "video", "container_magic"

    # Text encodings must be considered before short MPEG-style sync patterns.
    text_kind, text_reason = _decoded_text_shape(data, content_type)
    if text_kind:
        return text_kind, text_reason

    if _ascii_svg_root(data):
        return "image", "svg_root"
    if _valid_bmp(data):
        return "image", "bmp_header"
    if _valid_id3(data):
        return "audio", "id3_header"
    if _valid_swf(data):
        return "video", "swf_header"

    if len(data) >= 12 and data[:4] == b"RIFF":
        subtype = data[8:12]
        if subtype == b"WEBP":
            return "image", "riff_webp"
        if subtype == b"WAVE":
            return "audio", "riff_wave"
        if subtype == b"AVI ":
            return "video", "riff_avi"

    if len(data) >= 12 and data[4:8] == b"ftyp":
        brand = data[8:12]
        if brand in _IMAGE_FTYP_BRANDS:
            return "image", "isobmff_image"
        if brand in _AUDIO_FTYP_BRANDS:
            return "audio", "isobmff_audio"
        return "video", "isobmff_video"

    if len(data) >= 4 and data[:3] == b"\x00\x00\x01" and 0xB0 <= data[3] < 0xC0:
        return "video", "mpeg_video_start"
    mpeg = _mpeg_frame_class(data)
    if mpeg:
        return mpeg, "mpeg_audio_frame"
    return None, None


def payload_media_format(data: bytes, content_type: str = "") -> tuple[str | None, str, str | None]:
    """Return structural media kind + effective extension when the prefix proves it.

    The extension describes the payload format, not the URL suffix. Empty extension
    means the broad media kind is known but the container subtype is unresolved.
    """
    kind, reason = structural_payload_class(data, content_type)
    if kind not in {"image", "video", "audio"}:
        return kind, "", reason
    lower = data[:64].lower()
    ext = ""
    if reason == "image_magic":
        if data.startswith(b"\x89PNG\r\n\x1a\n"): ext = ".png"
        elif data.startswith(b"\xff\xd8\xff"): ext = ".jpg"
        elif data.startswith((b"GIF87a", b"GIF89a")): ext = ".gif"
        elif data.startswith((b"II*\x00", b"MM\x00*")): ext = ".tif"
    elif reason == "icon_magic": ext = ".ico"
    elif reason == "svg_root": ext = ".svg"
    elif reason == "bmp_header": ext = ".bmp"
    elif reason == "flv_magic": ext = ".flv"
    elif reason == "swf_header": ext = ".swf"
    elif reason == "riff_webp": ext = ".webp"
    elif reason == "riff_avi": ext = ".avi"
    elif reason == "riff_wave": ext = ".wav"
    elif reason == "id3_header" or reason == "mpeg_audio_frame": ext = ".mp3"
    elif reason == "audio_magic":
        if data.startswith(b"fLaC"): ext = ".flac"
        elif data.startswith(b"OggS"): ext = ".ogg"
    elif reason == "isobmff_image":
        brand = data[8:12] if len(data) >= 12 else b""
        ext = ".avif" if brand in {b"avif", b"avis"} else ".heic"
    elif reason == "isobmff_audio": ext = ".m4a"
    elif reason == "isobmff_video":
        brand = data[8:12] if len(data) >= 12 else b""
        ext = ".mov" if brand == b"qt  " else (".3gp" if brand[:3] == b"3gp" else ".mp4")
    elif reason == "mpeg_video_start": ext = ".mpeg"
    elif reason == "container_magic":
        mime = (content_type or "").split(";", 1)[0].strip().casefold()
        if "webm" in mime: ext = ".webm"
        elif "matroska" in mime: ext = ".mkv"
        elif "asf" in mime or "wmv" in mime: ext = ".wmv"
    if not ext:
        mime = (content_type or "").split(";", 1)[0].strip().casefold()
        mime_map = {
            "image/png": ".png", "image/jpeg": ".jpg", "image/jpg": ".jpg",
            "image/gif": ".gif", "image/webp": ".webp", "image/bmp": ".bmp",
            "image/svg+xml": ".svg", "image/avif": ".avif", "image/heic": ".heic",
            "video/mp4": ".mp4", "video/quicktime": ".mov", "video/x-flv": ".flv",
            "video/webm": ".webm", "video/x-msvideo": ".avi", "video/x-ms-wmv": ".wmv",
            "audio/mpeg": ".mp3", "audio/flac": ".flac", "audio/wav": ".wav",
            "audio/x-wav": ".wav", "audio/ogg": ".ogg", "audio/mp4": ".m4a",
        }
        ext = mime_map.get(mime, "")
    return kind, ext, reason
