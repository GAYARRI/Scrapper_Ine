# -*- coding: utf-8 -*-
"""
Crawl del sitio con filtros (.es / es_es), JSON por página, MD por página (obligatorio) y MD combinado final.

- Descarga/lee HTML (usa html_fetcher.fetch_html si existe; si no, fallback).
- Parsea con extractor.parse_landing_html.
- Adjunta agrupación de enlaces (por host y URL normalizada) dentro del JSON.
- Guarda JSON y (opcional) HTML crudo.
- Genera SIEMPRE un Markdown por página con presenter.render_report.
- Al final, genera un MD combinado (índice + páginas + agregados site-wide).

Uso:
    python main_site_filtered.py --site https://tu-dominio.com --max-pages 150 --combined-md reports/tu-dominio.com.md

Filtros:
    --only-host-suffixes ".es"            # dominios permitidos (CSV)
    --only-url-contains "es_es,es-es,/es/" # subcadenas permitidas en la ruta (CSV)
    --filter-mode any|all                 # OR/AND entre grupos (por defecto any)

Requisitos:
    pip install beautifulsoup4 lxml
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import gzip
import io
import json
import re
import sys
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
from urllib.request import Request, urlopen

# ---------------- Dependencias del proyecto ----------------
try:
    from extractor import parse_landing_html  # type: ignore
except Exception as e:
    print("[ERROR] No se pudo importar 'parse_landing_html' desde extractor.py:", e, file=sys.stderr)
    sys.exit(1)

# Intento importar fetcher del proyecto; si no existe, usaremos fallback
try:
    from html_fetcher import fetch_html  # type: ignore
except Exception:
    fetch_html = None  # type: ignore

# Presenter (obligatorio)
try:
    from presenter import render_report, build_combined_report  # type: ignore
except Exception as e:
    print("[ERROR] No se pudo importar presenter.render_report/build_combined_report:", e, file=sys.stderr)
    sys.exit(1)

# BeautifulSoup para extraer enlaces
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception as e:
    print("[ERROR] Se requiere beautifulsoup4:", e, file=sys.stderr)
    sys.exit(1)

# ---------------- Utilidades ----------------

def is_url(s: str) -> bool:
    return isinstance(s, str) and s.lower().startswith(("http://", "https://"))

def slugify(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-\.]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")[:180]

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def http_get(url: str, timeout: int = 30) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; PipelineBot/1.0)"}
    req = Request(url, headers=headers)
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()

def fetch_html_fallback(url: str, timeout: int = 30) -> str:
    raw = http_get(url, timeout=timeout)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1", errors="ignore")

# ---------------- Normalización/agrupación de enlaces ----------------

TRACKING_PARAMS_PREFIXES = ("utm_",)
TRACKING_PARAMS = {"gclid", "fbclid", "mc_cid", "mc_eid", "msclkid", "ref", "igshid"}

def strip_tracking(url: str, keep_query: bool = True) -> str:
    try:
        p = urlparse(url)
        fragless = p._replace(fragment="")
        if keep_query and fragless.query:
            q = [(k, v) for (k, v) in parse_qsl(fragless.query, keep_blank_values=True)
                 if (k not in TRACKING_PARAMS and not any(k.startswith(pref) for pref in TRACKING_PARAMS_PREFIXES))]
            fragless = fragless._replace(query=urlencode(q))
        else:
            fragless = fragless._replace(query="")
        return urlunparse(fragless)
    except Exception:
        return url

def _pick_text(l: dict) -> str:
    return (l.get("text") or l.get("aria_label") or l.get("title_attr") or "").strip()

def _group_links(links: List[dict]) -> Tuple[List[dict], List[str], int]:
    """
    Agrupa por host y URL normalizada (sin #fragment ni trackers).
    Devuelve: (by_host_list, unique_urls_list, total_count)
    """
    from collections import defaultdict
    by_host: Dict[str, Dict[str, dict]] = defaultdict(dict)
    unique_urls: Set[str] = set()
    total = 0

    for l in links or []:
        url = l.get("absolute_href") or l.get("href")
        if not url:
            continue
        url = strip_tracking(url, keep_query=True)
        host = urlparse(url).netloc.lower()
        text = _pick_text(l)
        slot = by_host[host].setdefault(url, {"count": 0, "texts": set()})
        slot["count"] += 1
        total += 1
        if text:
            slot["texts"].add(text)
        unique_urls.add(url)

    by_host_list: List[dict] = []
    for host, urls_map in sorted(by_host.items()):
        urls_list = []
        host_total = 0
        for u, info in sorted(urls_map.items()):
            host_total += info["count"]
            urls_list.append({"url": u, "count": info["count"], "texts": sorted(info["texts"])})
        by_host_list.append({
            "host": host,
            "total": host_total,
            "unique_urls": len(urls_map),
            "urls": urls_list,
        })
    return by_host_list, sorted(unique_urls), total

def attach_link_groups(data: dict) -> dict:
    gi, ui, ti = _group_links(data.get("links_internal") or [])
    ge, ue, te = _group_links(data.get("links_external") or [])
    data["links_grouped"] = {
        "internal": {"total": ti, "unique_urls": len(ui), "by_host": gi, "urls": ui},
        "external": {"total": te, "unique_urls": len(ue), "by_host": ge, "urls": ue},
    }
    return data

# ---------------- Filtros/normalización URLs para crawl ----------------

def parse_csv_list(val: Optional[str]) -> List[str]:
    if not val:
        return []
    return [x.strip().lower() for x in val.split(",") if x.strip()]

def url_passes_filters(url: str, host_suffixes: List[str], contains: List[str], mode: str = "any") -> bool:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()

        checks: List[bool] = []
        if host_suffixes:
            checks.append(any(host.endswith(suf) for suf in host_suffixes))
        if contains:
            checks.append(any(sub in path for sub in contains))

        if not checks:
            return True
        return any(checks) if mode == "any" else all(checks)
    except Exception:
        return True

def normalize_url(base: str, href: str, allow_query: bool) -> Optional[str]:
    if not href:
        return None
    if href.startswith(("mailto:", "tel:", "javascript:")):
        return None
    absu = urljoin(base, href)
    p = urlparse(absu)
    if p.scheme not in ("http", "https"):
        return None
    path = p.path or "/"
    params = ""
    query = p.query if allow_query else ""
    fragment = ""
    return urlunparse((p.scheme, p.netloc, path, params, query, fragment))

def same_host(u: str, base: str) -> bool:
    return urlparse(u).netloc == urlparse(base).netloc

# ---------------- Procesado individual ----------------

def target_name(source: str, base_url: Optional[str]) -> str:
    if is_url(source):
        parsed = urlparse(source)
        name = f"{parsed.netloc}{parsed.path}" or parsed.netloc
        if name == "/":
            name = parsed.netloc
        return slugify(name)
    else:
        p = Path(source)
        base = base_url or "local"
        return slugify(f"{base}_{p.stem}")

def process_one(
    source: str,
    base_url_cli: Optional[str],
    outdir: Path,
    save_raw: bool,
    only_blocks: bool,
    reports_out: Path,
    limit_paragraphs: int,
    max_col_width: int,
) -> Tuple[str, bool, Optional[str], Optional[Path]]:
    """Procesa una fuente (URL o HTML local). Devuelve (name, ok, error_msg, json_path)."""
    try:
        # 1) Obtener HTML
        if is_url(source):
            base_url = source
            if fetch_html is not None:
                html = fetch_html(source)
            else:
                html = fetch_html_fallback(source)
        else:
            html_path = Path(source)
            if not html_path.exists():
                return source, False, f"Archivo no encontrado: {html_path}", None
            html = html_path.read_text(encoding="utf-8", errors="ignore")
            base_url = base_url_cli

        # 2) Parsear
        data = parse_landing_html(html, base_url)
        if only_blocks:
            data = {
                "summary": data.get("summary"),
                "metadata": data.get("metadata"),
                "content_blocks": data.get("content_blocks"),
                "links_internal": data.get("links_internal"),
                "links_external": data.get("links_external"),
                "link_texts": data.get("link_texts"),
                "images": data.get("images"),
                "forms": data.get("forms"),
                "tables": data.get("tables"),
                "headings": data.get("headings"),
                "main_content": data.get("main_content"),
                "structured_data": data.get("structured_data"),
            }

        # 2b) Agrupar enlaces
        data = attach_link_groups(data)

        # 3) Guardar JSON
        name = target_name(source, base_url)
        ensure_dir(outdir)
        json_path = outdir / f"{name}.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        # 4) Guardar HTML crudo (opcional)
        if save_raw:
            raw_dir = Path("data/raw_html")
            ensure_dir(raw_dir)
            (raw_dir / f"{name}.html").write_text(html, encoding="utf-8", errors="ignore")

        # 5) Markdown por página (obligatorio)
        ensure_dir(reports_out)
        report_md = render_report(data, title=name, limit_paragraphs=limit_paragraphs, max_col_width=max_col_width)
        (reports_out / f"{name}.md").write_text(report_md, encoding="utf-8")

        return name, True, None, json_path
    except Exception as e:
        return source, False, f"{type(e).__name__}: {e}", None

# ---------------- Sitemap + BFS ----------------

def discover_sitemaps(base_url: str) -> List[str]:
    sitemaps: List[str] = []
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    # robots.txt
    try:
        robots = fetch_html_fallback(urljoin(origin, "/robots.txt"))
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                sm = line.split(":", 1)[1].strip()
                sitemaps.append(sm if is_url(sm) else urljoin(origin, sm))
    except Exception:
        pass
    # /sitemap.xml
    try:
        sitemaps.append(urljoin(origin, "/sitemap.xml"))
    except Exception:
        pass
    # de-dup conservando orden
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in sitemaps:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def parse_sitemap(url: str, limit: int = 5000) -> List[str]:
    urls: List[str] = []
    try:
        raw = http_get(url)
        if url.lower().endswith(".gz"):
            raw = gzip.decompress(raw)
        tree = ET.parse(io.BytesIO(raw))
        root = tree.getroot()
        tag = root.tag.lower()
        if tag.endswith("sitemapindex"):
            for sm in root.findall(".//{*}sitemap"):
                loc_el = sm.find("{*}loc")
                if loc_el is not None and loc_el.text:
                    urls.extend(parse_sitemap(loc_el.text.strip(), limit=limit))
                    if len(urls) >= limit:
                        break
        else:
            for u in root.findall(".//{*}url"):
                loc_el = u.find("{*}loc")
                if loc_el is not None and loc_el.text:
                    urls.append(loc_el.text.strip())
                    if len(urls) >= limit:
                        break
    except Exception:
        return []
    # dedup
    seen: Set[str] = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def extract_links_from_html(base_url: str, html: str, allow_query: bool) -> List[str]:
    links: List[str] = []
    try:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            norm = normalize_url(base_url, a.get("href"), allow_query=allow_query)
            if norm and same_host(norm, base_url):
                links.append(norm)
    except Exception:
        pass
    # dedup preservando orden
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in links:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def crawl_site(
    start_url: str,
    outdir: Path,
    reports_out: Path,
    save_raw: bool,
    only_blocks: bool,
    limit_paragraphs: int,
    max_col_width: int,
    max_pages: int = 100,
    allow_query: bool = False,
    sitemap_first: bool = True,
    host_suffixes: Optional[List[str]] = None,
    contains: Optional[List[str]] = None,
    filter_mode: str = "any",
) -> Tuple[List[str], List[Path]]:
    """Crawlea un sitio con filtros. Devuelve (urls_procesadas, json_paths)."""
    host_suffixes = host_suffixes or []
    contains = contains or []

    visited: Set[str] = set()
    json_paths: List[Path] = []

    # Semillas
    queue: deque[str] = deque()
    seed_urls: List[str] = []
    if sitemap_first:
        for sm in discover_sitemaps(start_url):
            seed_urls.extend(parse_sitemap(sm, limit=max_pages * 2))
    seed_urls = [start_url] + seed_urls

    # normaliza y filtra
    seen_seed: Set[str] = set()
    for u in seed_urls:
        norm = normalize_url(start_url, u, allow_query=allow_query) if not is_url(u) else u
        if not norm:
            continue
        if same_host(norm, start_url) and norm not in seen_seed and url_passes_filters(norm, host_suffixes, contains, filter_mode):
            seen_seed.add(norm)
            queue.append(norm)

    while queue and len(visited) < max_pages:
        url = queue.pop()  # DFS (puedes cambiar a popleft() para BFS)
        if url in visited:
            continue
        visited.add(url)

        # fetch
        try:
            html = fetch_html(url) if fetch_html is not None else fetch_html_fallback(url)
        except Exception as e:
            print(f"[WARN] No se pudo descargar {url}: {e}", file=sys.stderr)
            continue

        # procesar (JSON + MD)
        name = target_name(url, None)
        try:
            data = parse_landing_html(html, url)
            if only_blocks:
                data = {
                    "summary": data.get("summary"),
                    "metadata": data.get("metadata"),
                    "content_blocks": data.get("content_blocks"),
                    "links_internal": data.get("links_internal"),
                    "links_external": data.get("links_external"),
                    "link_texts": data.get("link_texts"),
                    "images": data.get("images"),
                    "forms": data.get("forms"),
                    "tables": data.get("tables"),
                    "headings": data.get("headings"),
                    "main_content": data.get("main_content"),
                    "structured_data": data.get("structured_data"),
                }
            data = attach_link_groups(data)

            ensure_dir(outdir)
            json_path = outdir / f"{name}.json"
            json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            json_paths.append(json_path)

            if save_raw:
                raw_dir = Path("data/raw_html")
                ensure_dir(raw_dir)
                (raw_dir / f"{name}.html").write_text(html, encoding="utf-8", errors="ignore")

            ensure_dir(reports_out)
            report_md = render_report(data, title=name, limit_paragraphs=limit_paragraphs, max_col_width=max_col_width)
            (reports_out / f"{name}.md").write_text(report_md, encoding="utf-8")
        except Exception as e:
            print(f"[FAIL] {url} :: {type(e).__name__}: {e}", file=sys.stderr)

        # extraer y encolar enlaces internos
        try:
            for link in extract_links_from_html(url, html, allow_query=allow_query):
                if link not in visited and link not in queue and same_host(link, start_url):
                    if url_passes_filters(link, host_suffixes, contains, filter_mode):
                        queue.append(link)
        except Exception:
            pass

    return list(visited), json_paths

# ---------------- Argumentos y main ----------------

def parse_args(argv: List[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Crawl sitio (.es / es_es), JSON por página, MD por página y combinado")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--site", type=str, help="URL inicial del sitio a crawlear")
    g.add_argument("--urls", nargs="+", help="Una o más URLs")
    g.add_argument("--file", type=str, help="Archivo con lista de entradas (URL o ruta_local[,base_url])")
    g.add_argument("--html", type=str, help="Ruta a un archivo HTML local")

    ap.add_argument("--base-url", dest="base_url", type=str, default=None, help="Base URL para HTML local")
    ap.add_argument("--outdir", type=str, default="data/parsed", help="Directorio de salida JSON")
    ap.add_argument("--save-raw", action="store_true", help="Guarda HTML crudo en data/raw_html")
    ap.add_argument("--only-blocks", action="store_true", help="Guardar sólo content_blocks + summary + metadata")
    ap.add_argument("--workers", type=int, default=4, help="Número de hilos (sólo modo URLs/archivo)")

    # Reporter obligatorio
    ap.add_argument("--reports-out", type=str, default="reports", help="Directorio de informes .md")
    ap.add_argument("--limit-paragraphs", type=int, default=50, help="Máximo de párrafos en el informe")
    ap.add_argument("--max-col-width", type=int, default=100, help="Ancho máx. de columna en tablas del informe")

    # Modo sitio
    ap.add_argument("--max-pages", type=int, default=100, help="Máximo de páginas a procesar en modo --site")
    ap.add_argument("--allow-query", action="store_true", help="No eliminar querystrings al normalizar URLs internas")
    ap.add_argument("--no-sitemap-first", action="store_true", help="No usar sitemap como semilla inicial")
    ap.add_argument("--combined-md", type=str, default=None, help="Ruta del informe combinado final (por defecto reports/<host>.md)")

    # Filtros (por defecto: .es O que la ruta contenga es_es)
    ap.add_argument("--only-host-suffixes", type=str, default=".es", help="Sufijos de host permitidos (CSV)")
    ap.add_argument("--only-url-contains", type=str, default="es_es", help="Subcadenas permitidas en la ruta (CSV)")
    ap.add_argument("--filter-mode", type=str, choices=["any", "all"], default="any", help="Combinar filtros con OR ('any') o AND ('all')")

    return ap.parse_args(argv)

def main(argv: List[str]) -> int:
    args = parse_args(argv)
    outdir = Path(args.outdir)
    reports_out = Path(args.reports_out)

    host_suffixes = parse_csv_list(args.only_host_suffixes)
    contains = parse_csv_list(args.only_url_contains)

    # --------- MODO SITIO ---------
    if args.site:
        start = args.site.rstrip("/")
        print(f"[INFO] Crawling del sitio: {start} (max_pages={args.max_pages})")
        visited, json_paths = crawl_site(
            start_url=start,
            outdir=outdir,
            reports_out=reports_out,
            save_raw=args.save_raw,
            only_blocks=args.only_blocks,
            limit_paragraphs=args.limit_paragraphs,
            max_col_width=args.max_col_width,
            max_pages=args.max_pages,
            allow_query=args.allow_query,
            sitemap_first=not args.no_sitemap_first,
            host_suffixes=host_suffixes,
            contains=contains,
            filter_mode=args.filter_mode,
        )
        print(f"[DONE] Páginas procesadas (tras filtros): {len(visited)}")

        # Informe combinado
        combined_path = Path(args.combined_md) if args.combined_md else (reports_out / f"{urlparse(start).netloc}.md")
        try:
            build_combined_report(
                inputs=sorted(json_paths),
                out_path=combined_path,
                limit_paragraphs=args.limit_paragraphs,
                max_col_width=args.max_col_width,
            )
            print(f"[OK] Informe combinado → {combined_path}")
        except Exception as e:
            print(f"[FAIL] Combinado: {type(e).__name__}: {e}", file=sys.stderr)
        return 0

    # --------- MODO NO-SITIO (URLs/FILE/HTML) ---------
    items: List[Tuple[str, Optional[str]]] = []
    if args.urls:
        items = [(u, None) for u in args.urls if (not is_url(u)) or url_passes_filters(u, host_suffixes, contains, args.filter_mode)]
    elif args.file:
        # lee archivo y filtra URLs absolutas por los mismos criterios
        def read_inputs(file_path: Path) -> List[Tuple[str, Optional[str]]]:
            pairs: List[Tuple[str, Optional[str]]] = []
            for raw in file_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "," in line:
                    left, right = line.split(",", 1)
                    pairs.append((left.strip(), (right.strip() or None)))
                else:
                    pairs.append((line, None))
            return pairs

        raw_items = read_inputs(Path(args.file))
        items = []
        for (src, base) in raw_items:
            if is_url(src):
                if url_passes_filters(src, host_suffixes, contains, args.filter_mode):
                    items.append((src, None))
            else:
                items.append((src, base))
    else:
        items = [(args.html, args.base_url)]

    print(f"[INFO] Entradas: {len(items)} | outdir={outdir} | workers={args.workers}")

    ok_count = 0
    fail_count = 0
    json_paths: List[Path] = []

    def submit_process(executor, src: str, base: Optional[str]):
        base_for_local = (base if not is_url(src) else None) or args.base_url
        return executor.submit(
            process_one,
            src,
            base_for_local,
            outdir,
            args.save_raw,
            args.only_blocks,
            reports_out,
            args.limit_paragraphs,
            args.max_col_width,
        )

    with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = [submit_process(ex, src, base) for (src, base) in items]
        for fut in cf.as_completed(futures):
            name, ok, err, jp = fut.result()
            if ok:
                ok_count += 1
                if jp:
                    json_paths.append(jp)
                print(f"[OK] {name}")
            else:
                fail_count += 1
                print(f"[FAIL] {name} :: {err}", file=sys.stderr)

    print(f"[DONE] OK={ok_count} FAIL={fail_count}")
    return 0 if fail_count == 0 else 2

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
