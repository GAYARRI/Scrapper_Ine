# crewai_html_extractor/crawl_cli.py
from __future__ import annotations

import argparse
import logging
import re
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

import pandas as pd
from bs4 import BeautifulSoup

from crewai_html_extractor.scraper.core import Core
from crewai_html_extractor.scraper.extractors import tourism, html_tables
from crewai_html_extractor.scraper.extractors import ine as ine_extractor


LOG = logging.getLogger("crewai.crawl")

# Palabras clave para priorizar URLs de interés turístico
DEFAULT_KEYWORDS = (
    r"hotel|alojamiento|apartament|hostal|camping|rural|restauran|gastronom|bar|caf[eé]|"
    r"atracci|museo|playa|monumento|ruta|sender|que-?ver|agenda|evento|fest|activ|ocio|turism"
)
KEYWORD_RX = re.compile(DEFAULT_KEYWORDS, re.I)


def normalize_url(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
        return None
    full = urljoin(base, href)
    # quita fragmentos
    full, _ = urldefrag(full)
    # solo http/https
    parsed = urlparse(full)
    if parsed.scheme not in ("http", "https"):
        return None
    return full


def same_host(u: str, v: str) -> bool:
    pu, pv = urlparse(u), urlparse(v)
    return pu.netloc == pv.netloc


def extract_links(html: str, base_url: str) -> List[Tuple[str, int]]:
    """Devuelve [(url, score)] donde score alto si la URL/anchor coincide con KEYWORD_RX."""
    soup = BeautifulSoup(html, "lxml")
    links: List[Tuple[str, int]] = []
    for a in soup.find_all("a", href=True):
        url = normalize_url(base_url, a.get("href"))
        if not url:
            continue
        text = (a.get_text(" ", strip=True) or "") + " " + (a.get("title") or "")
        score = 1
        if KEYWORD_RX.search(url) or KEYWORD_RX.search(text):
            score += 2
        links.append((url, score))
    # ordena por score desc
    links.sort(key=lambda x: x[1], reverse=True)
    return links


def run_extractors(html: str, url: str) -> List[Dict[str, Any]]:
    """Reutiliza tus extractores sin volver a hacer fetch."""
    items: List[Dict[str, Any]] = []
    # Turismo JSON-LD / microdatos
    try:
        items.extend(tourism.extract_tourism_entities(html, url))
    except Exception as e:
        LOG.debug(f"[extract] tourism jsonld failed: {e}")
    # Listados genéricos (tarjetas con datos)
    try:
        items.extend(tourism.extract_portal_listings_generic(html, url))
    except Exception as e:
        LOG.debug(f"[extract] tourism listings failed: {e}")
    # INE (no suele aplicar a portales turismo, pero no molesta)
    try:
        items.extend(ine_extractor.extract_ine_tables(html, url))
    except Exception as e:
        LOG.debug(f"[extract] ine failed: {e}")
    # Tablas HTML genéricas
    try:
        items.extend(html_tables.extract_html_tables(html, url))
    except Exception as e:
        LOG.debug(f"[extract] html tables failed: {e}")
    return items


def dedupe_entities(entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """De-dupe por (name, entity_type, telephone|email|url)."""
    seen: Set[Tuple] = set()
    out: List[Dict[str, Any]] = []
    for e in entities:
        key = (
            (e.get("name") or "").strip().lower(),
            e.get("entity_type"),
            (e.get("telephone") or "").strip(),
            (e.get("email") or "").strip(),
            (e.get("url") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Crawler de portales turísticos (descubre entidades)")
    ap.add_argument("--seed", action="append", required=True,
                    help="URL semilla (puedes repetir la opción varias veces)")
    ap.add_argument("--outdir", default="outputs_crawl", help="Directorio de salida")
    ap.add_argument("--max-pages", type=int, default=200, help="Límite de páginas a visitar")
    ap.add_argument("--same-domain", action="store_true", default=True,
                    help="Restringir al mismo dominio que las semillas (default: True)")
    ap.add_argument("--allow", default="", help="Regex de allow para URLs (opcional)")
    ap.add_argument("--deny", default=r"\.(pdf|jpg|jpeg|png|gif|svg|webp|ico|zip|rar|7z|mp4|mp3|wav)$",
                    help="Regex de deny para URLs")
    ap.add_argument("--log-level", default="INFO", choices=["CRITICAL","ERROR","WARNING","INFO","DEBUG"])
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    allow_rx = re.compile(args.allow, re.I) if args.allow else None
    deny_rx = re.compile(args.deny, re.I) if args.deny else None

    core = Core()  # tu Core respeta robots, cache, backoff, etc.

    # Conjunto de hosts permitidos (si same-domain=True, los de las seeds)
    allowed_hosts: Set[str] = set(urlparse(s).netloc for s in args.seed)

    q: deque[str] = deque()
    seen_urls: Set[str] = set()

    # Inicializa cola con semillas
    for s in args.seed:
        q.append(s)

    pages_crawled = 0
    all_entities: List[Dict[str, Any]] = []
    pages_log: List[Dict[str, Any]] = []

    while q and pages_crawled < args.max_pages:
        url = q.popleft()
        if url in seen_urls:
            continue
        seen_urls.add(url)

        # Restricción de dominio
        if args.same_domain:
            host = urlparse(url).netloc
            if host not in allowed_hosts:
                LOG.debug(f"[skip] fuera de dominio: {url}")
                continue

        # Filtros allow/deny
        if deny_rx and deny_rx.search(url):
            LOG.debug(f"[deny] {url}")
            continue
        if allow_rx and not allow_rx.search(url):
            LOG.debug(f"[not-allowed] {url}")
            continue

        LOG.info(f"[GET] {url}")
        try:
            final_url, html = core.fetch(url)
        except Exception as e:
            LOG.warning(f"[fail] {url}: {e}")
            continue

        # Extrae
        items = run_extractors(html, final_url)
        # Guarda log de página
        pages_log.append({
            "url": final_url,
            "items": len(items),
            "ts": pd.Timestamp.utcnow().isoformat()
        })

        # Acumula entidades
        for it in items:
            if it.get("type") == "entity":
                all_entities.append(it)

        # Descubre nuevos enlaces
        for next_url, score in extract_links(html, final_url):
            if next_url in seen_urls:
                continue
            # misma restricción que arriba, pero barata antes de encolar
            if args.same_domain and urlparse(next_url).netloc not in allowed_hosts:
                continue
            if deny_rx and deny_rx.search(next_url):
                continue
            # Prioriza: palabras clave van al frente
            if KEYWORD_RX.search(next_url):
                q.appendleft(next_url)
            else:
                q.append(next_url)

        pages_crawled += 1
        LOG.info(f"[progress] {pages_crawled}/{args.max_pages} páginas, entidades={len(all_entities)}")

        # Pausa ligera entre iteraciones para no encadenar rápido (Core ya hace throttle per host)
        time.sleep(0.3)

    # De-dupe y exporta
    all_entities = dedupe_entities(all_entities)
    ent_path = outdir / "entities.csv"
    if all_entities:
        # normaliza same_as a string
        for e in all_entities:
            if isinstance(e.get("same_as"), list):
                e["same_as"] = ";".join(map(str, e["same_as"]))
        pd.DataFrame(all_entities).to_csv(ent_path, index=False)

    # Log de páginas
    pd.DataFrame(pages_log).to_csv(outdir / "pages.csv", index=False)

    print(f"[OK] Páginas rastreadas: {pages_crawled}")
    print(f"[OK] Entidades encontradas (únicas): {len(all_entities)}")
    if all_entities:
        print(f"[OK] Guardado: {ent_path}")
    print(f"[OK] Log de páginas: {outdir / 'pages.csv'}")
