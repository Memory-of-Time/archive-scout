from __future__ import annotations

import hashlib
import re
from urllib.parse import quote


def url_filename(original_url: str, timestamp: str = "", extension: str = ".txt",
                 capture_id: int | None = None) -> str:
    extension = extension if re.fullmatch(r"\.[A-Za-z0-9]{1,16}", extension or "") else ".bin"
    identity = f"{original_url}\0{timestamp}\0{capture_id}"
    token = hashlib.sha256(identity.encode("utf-8", "surrogatepass")).hexdigest()[:16]
    date = re.sub(r"[^0-9]", "", str(timestamp))[:14]
    capture = str(int(capture_id)) if capture_id is not None else ""
    suffix = "__" + "_".join(part for part in (date, capture, token) if part) + extension
    label = quote(str(original_url), safe="!$&'()+,-.;=@_~[]#", errors="surrogatepass")
    first = label.split(".", 1)[0].upper()
    if first in {"CON", "PRN", "AUX", "NUL", *(f"COM{n}" for n in range(1,10)),
                 *(f"LPT{n}" for n in range(1,10))}:
        label = "url_" + label
    label = label[:max(1, 140 - len(suffix))]
    label = re.sub(r"%[0-9A-Fa-f]?$", "", label)
    return (label or "url") + suffix
