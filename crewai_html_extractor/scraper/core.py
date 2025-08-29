# crewai_html_extractor/scraper/core.py
from __future__ import annotations

import re, time, random, logging
from typing import Tuple, Dict, Any, Optional
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

# NUEVO: cache y robots
try:
    import requests_cache
except Exception:
    requests_cache = None

import urllib.robotparser as robotparser

LOG = logging.getLogger("crewai.core")

def parse_landing(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.string.strip() if soup.title and soup.title.string else None
    md = soup.find("meta", attrs={"name": "description"})
    meta_description = md.get("content").strip() if md and md.has_attr("content") else None
    h1 = [h.get_text(strip=True) for h in soup.find_all("h1")]
    h2 = [h.get_text(strip=True) for h in soup.find_all("h2")]
    h3 = [h.get_text(strip=True) for h in soup.find_all("h3")]
    return {"url": url, "title": title, "meta_description": meta_description, "h1": h1, "h2": h2, "h3": h3, "data_items": []}

def normalize(record: dict) -> dict:
    rec = dict(record)
    if rec.get("title"): rec["title"] = re.sub(r"\s+", " ", rec["title"]).strip()
    if rec.get("meta_description"): rec["meta_description"] = re.sub(r"\s+", " ", rec["meta_description"]).strip()
    return rec

def _decode_html(resp: requests.Response) -> str:
    raw = resp.content
    if resp.encoding:
        txt = resp.text
    else:
        try:
            from charset_normalizer import from_bytes
            best = from_bytes(raw).best()
            txt = raw.decode(best.encoding, errors="replace") if best and best.encoding else raw.decode("utf-8", errors="replace")
        except Exception:
            txt = raw.decode("utf-8", errors="replace")
    if "Ã" in txt or "√" in txt or "�" in txt:
        try:
            from ftfy import fix_text
            txt = fix_text(txt)
        except Exception:
            pass
    return txt

class Core:
    """
    fetch(url) -> (final_url, html)
    - Rate limit con jitter
    - Reintentos 429/503 (Retry-After)
    - Respeto básico de robots.txt (crawl-delay)
    - Cache (requests-cache) si está instalado
    """

    def __init__(
        self,
        timeout: int = 25,
        verify_ssl: bool = True,
        headers: Optional[Dict[str, str]] = None,
        min_delay_s: float = 3.0,
        max_delay_s: float = 8.0,
        max_retries: int = 4,
        cache_name: Optional[str] = ".http_cache",
        cache_expire_s: int = 3600,
    ) -> None:
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.headers = headers or {
            # UA realista
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "es-ES,es;q=0.9",
        }
        self.min_delay_s = min_delay_s
        self.max_delay_s = max_delay_s
        self.max_retries = max_retries

        # Sesión (con cache si disponible)
        if requests_cache and cache_name:
            self.session = requests_cache.CachedSession(
                cache_name=cache_name,
                expire_after=cache_expire_s,
                allowable_methods=("GET",),
                stale_if_error=True,
            )
        else:
            self.session = requests.Session()

        self.session.headers.update(self.headers)
        self._robots_cache: Dict[str, robotparser.RobotFileParser] = {}
        self._last_fetch_per_host: Dict[str, float] = {}

    def _respect_robots(self, url: str) -> float:
        """Devuelve crawl-delay (segundos) si existe; 0 si no."""
        parsed = urlparse(url)
        host = f"{parsed.scheme}://{parsed.netloc}"
        if host not in self._robots_cache:
            rp = robotparser.RobotFileParser()
            rp.set_url(f"{host}/robots.txt")
            try:
                rp.read()
            except Exception:
                rp = None
            self._robots_cache[host] = rp  # puede ser None
        rp = self._robots_cache[host]
        if rp and hasattr(rp, "crawl_delay"):
            try:
                cd = rp.crawl_delay(self.headers.get("User-Agent", "*"))
                return float(cd) if cd else 0.0
            except Exception:
                return 0.0
        return 0.0

    def _throttle(self, url: str) -> None:
        parsed = urlparse(url)
        host = parsed.netloc
        # Respeta crawl-delay si lo hay
        crawl_delay = self._respect_robots(url)
        base_delay = max(self.min_delay_s, crawl_delay or 0.0)
        # Evita ráfagas por host
        now = time.time()
        last = self._last_fetch_per_host.get(host, 0.0)
        min_gap = base_delay + random.uniform(0, self.max_delay_s - self.min_delay_s)
        to_wait = max(0.0, last + min_gap - now)
        if to_wait > 0:
            time.sleep(to_wait)
        self._last_fetch_per_host[host] = time.time()

    def fetch(self, url: str) -> Tuple[str, str]:
        self._throttle(url)

        attempt = 0
        last_err = None
        while attempt <= self.max_retries:
            attempt += 1
            try:
                # Cabeceras oportunas por sitio: añade Referer a la primera petición
                headers = {}
                if attempt == 1:
                    parsed = urlparse(url)
                    headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"

                r = self.session.get(url, timeout=self.timeout, verify=self.verify_ssl, headers=headers)
                # Respuestas cacheadas no cuentan contra rate (pero mantenemos throttle entre dominios)
                if r.status_code in (200, 304):
                    return r.url, _decode_html(r)

                # Si 429/503, respeta Retry-After
                if r.status_code in (429, 503):
                    ra = r.headers.get("Retry-After")
                    if ra:
                        try:
                            wait_s = int(ra)
                        except ValueError:
                            wait_s = self.min_delay_s * (2 ** attempt)
                    else:
                        wait_s = self.min_delay_s * (2 ** attempt)
                    LOG.warning(f"[core] {r.status_code} recibido. Esperando {wait_s:.1f}s (attempt {attempt}).")
                    time.sleep(wait_s + random.uniform(0, 1.0))
                    continue

                # 403: prueba un pequeño backoff y sigue
                if r.status_code == 403:
                    wait_s = self.min_delay_s * (1.5 ** attempt) + random.uniform(0, 1.0)
                    LOG.warning(f"[core] 403 recibido. Backoff {wait_s:.1f}s (attempt {attempt}).")
                    time.sleep(wait_s)
                    continue

                # Otros códigos: lanza
                r.raise_for_status()

                return r.url, _decode_html(r)

            except Exception as e:
                last_err = e
                wait_s = self.min_delay_s * (2 ** attempt) + random.uniform(0, 1.0)
                LOG.warning(f"[core] Error {type(e).__name__}: {e}. Reintentando en {wait_s:.1f}s (attempt {attempt}).")
                time.sleep(wait_s)

        raise RuntimeError(f"Failed to fetch {url}: {last_err}")
