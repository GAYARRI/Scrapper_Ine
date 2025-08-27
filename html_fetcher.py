# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Optional, Set
from urllib.parse import urlparse, urlunparse, urljoin
import requests

UA = "Mozilla/5.0 (compatible; AgenticCrawler/1.0; +https://example.local)"

REDIRECT_CODES = {301, 302, 303, 307, 308}

def _canonicalize(u: str) -> str:
    """Normaliza para comparar URLs y detectar bucles: sin fragmento,
    netloc lowercase, sin puerto por defecto, y sin slash final salvo raíz."""
    p = urlparse(u)
    scheme = p.scheme.lower()
    netloc = p.netloc.lower()
    if (scheme == "http" and netloc.endswith(":80")) or (scheme == "https" and netloc.endswith(":443")):
        netloc = netloc.rsplit(":", 1)[0]
    path = p.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((scheme, netloc, path, "", p.query or "", ""))

def _toggle_www(u: str) -> Optional[str]:
    p = urlparse(u)
    host = p.netloc
    if host.startswith("www."):
        host2 = host[4:]
    else:
        host2 = "www." + host
    return urlunparse((p.scheme, host2, p.path or "/", p.params, p.query, ""))

def _decode_body(resp: requests.Response) -> str:
    # Respeta encoding del servidor; si no, usa aparente; fallback utf-8
    enc = resp.encoding or getattr(resp, "apparent_encoding", None) or "utf-8"
    try:
        return resp.content.decode(enc, errors="ignore")
    except Exception:
        return resp.text  # último recurso

def fetch_html(url: str, timeout: int = 30, max_redirects: int = 15) -> str:
    """Descarga HTML con manejo de redirecciones y fallback www para SSL mismatch."""
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es,en;q=0.8",
    })

    def _try(u: str) -> str:
        seen: Set[str] = set()
        current = u
        for _ in range(max_redirects + 1):
            can = _canonicalize(current)
            if can in seen:
                raise requests.TooManyRedirects(f"Redirect loop detected at {current}")
            seen.add(can)

            r = sess.get(current, timeout=timeout, allow_redirects=False, verify=True)
            if r.status_code in REDIRECT_CODES:
                loc = r.headers.get("Location")
                if not loc:
                    r.raise_for_status()
                current = urljoin(current, loc)
                continue
            r.raise_for_status()
            return _decode_body(r)
        raise requests.TooManyRedirects(f"Exceeded {max_redirects} redirects for {url}")

    try:
        return _try(url)
    except requests.exceptions.SSLError:
        # Reintenta con variante www / no-www (hostname mismatch típico)
        alt = _toggle_www(url)
        if alt:
            return _try(alt)
        raise
