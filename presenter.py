# -*- coding: utf-8 -*-
"""
Agente de PRESENTACIÓN: toma la salida del extractor (JSON) y genera informes Markdown.

Características clave:
- MD por página con estructura: Resumen, Encabezados, Contenido, Párrafos, Listas, Enlaces,
  Textos de enlaces, Imágenes, Formularios, Tablas, Structured Data.
- Modo combinado: genera un único MD con índice y cada página en su propia sección.
- Agregados site-wide (en modo combinado): tabla con TODOS los enlaces internos/externos,
  deduplicados y normalizados, e índice de imágenes (galería).
- Opciones de deduplicación de párrafos en combinado (para evitar repetir footers/menus).

Uso:
    # MD por página
    python presenter.py --glob "data/parsed/*.json" --out reports

    # MD combinado del sitio (sin MD por página)
    python presenter.py --glob "data/parsed/*.json" --combine reports/site.md --no-per-page

Parámetros:
    --limit-paragraphs 50
    --max-col-width 100
    --combine reports/site.md
    --no-per-page
    --dedupe-min-len 40
    --max-agg-links 2000
    --max-gallery 200
"""
from __future__ import annotations
import argparse
import glob
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# ---------------------------- Utilidades de formato ----------------------------

def shorten(s: Optional[str], max_len: int = 100) -> str:
    if s is None:
        return ""
    s = str(s)
    if len(s) <= max_len:
        return s
    keep = max(10, max_len // 2 - 3)
    return f"{s[:keep]}…{s[-keep:]}"

def md_escape(s: str) -> str:
    # Escapa backslashes y pipes para tablas Markdown
    bs = chr(92)  # \
    return s.replace(bs, bs + bs).replace("|", bs + "|")

def md_table(headers: List[str], rows: List[List[Any]], max_col_width: int = 100) -> str:
    h = "| " + " | ".join(md_escape(str(x)) for x in headers) + " |"
    sep = "| " + " | ".join(["---"] * len(headers)) + " |"
    body_lines: List[str] = []
    for r in rows:
        vals: List[str] = []
        for v in r:
            txt = "" if v is None else str(v)
            vals.append(md_escape(shorten(txt, max_col_width)))
        body_lines.append("| " + " | ".join(vals) + " |")
    return "\n".join([h, sep] + body_lines) if rows else "(sin datos)"

# ---------------------------- Normalización URLs ----------------------------

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

# ---------------------------- Render por página ----------------------------

def render_headings_tree(headings: List[Dict[str, Any]], max_items: int = 300) -> str:
    lines: List[str] = []
    for h in headings[:max_items]:
        level = int(h.get("tag", "h6")[1]) if h.get("tag") else 6
        text = str(h.get("text", "")).strip()
        indent = "  " * (level - 1)
        lines.append(f"{indent}- {text}")
    return "\n".join(lines) if lines else "(sin encabezados)"

def render_paragraphs(blocks: List[Dict[str, Any]], limit: int) -> str:
    out: List[str] = []
    count = 0
    for b in blocks:
        if b.get("type") == "paragraph":
            count += 1
            hp = " › ".join(b.get("heading_path", []))
            prefix = f"P{count:02d}"
            if hp:
                out.append(f"**{prefix} — {hp}:** {b.get('text','')}")
            else:
                out.append(f"**{prefix}:** {b.get('text','')}")
            if count >= limit:
                break
    return "\n\n".join(out) if out else "(sin párrafos relevantes)"

def group_lists_by_section(blocks: List[Dict[str, Any]]) -> List[Tuple[str, List[List[str]]]]:
    acc: Dict[str, List[List[str]]] = {}
    for b in blocks:
        if b.get("type") == "list":
            sec = " › ".join(b.get("heading_path", [])) or "(sin sección)"
            rows = acc.setdefault(sec, [])
            for i, item in enumerate(b.get("items", []), 1):
                rows.append([str(i), item])
    return list(acc.items())

# ---------------------------- Enlaces e Imágenes ----------------------------

def render_links_bullets(links: List[Dict[str, Any]]) -> str:
    if not links:
        return "(sin enlaces)"
    lines: List[str] = []
    for l in links:
        text = l.get("text") or l.get("aria_label") or l.get("title_attr") or l.get("absolute_href") or l.get("href") or "(enlace)"
        href = l.get("absolute_href") or l.get("href") or "#"
        lines.append(f"- [{text}]({href})")
    return "\n".join(lines)

def render_link_texts_as_paragraphs(data: Dict[str, Any]) -> str:
    texts = data.get("link_texts") or []
    if not texts:
        # Fallback si no existe el campo (compatibilidad)
        links = data.get("links") or []
        texts = []
        seen = set()
        for l in links:
            for cand in [l.get("text"), l.get("title_attr"), l.get("aria_label")]:
                if cand and cand not in seen:
                    seen.add(cand)
                    texts.append(cand)
    if not texts:
        return "(sin textos de enlaces)"
    return "\n\n".join(f"**L{idx+1:02d}:** {t}" for idx, t in enumerate(texts))

def render_images_gallery(images: List[Dict[str, Any]], max_images: int = 200) -> str:
    if not images:
        return "(sin imágenes)"
    lines: List[str] = []
    for im in images[:max_images]:
        src = im.get("absolute_src") or im.get("src") or ""
        alt = im.get("alt") or ""
        if not src:
            continue
        lines.append(f"![]({src})")
        if alt:
            lines.append(f"*{alt}*")
    return "\n\n".join(lines)

# ---------------------------- Structured Data helpers ----------------------------

def jget(obj: Any, path: List[Any], default: Any = "") -> Any:
    cur = obj
    try:
        for p in path:
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur.get(p)
        return cur if cur is not None else default
    except Exception:
        return default

# ---------------------------- Report Builder ----------------------------

def render_report(
    data: Dict[str, Any],
    title: Optional[str] = None,
    limit_paragraphs: int = 50,
    max_col_width: int = 100
) -> str:
    md: List[str] = []

    s = data.get("summary", {}) or {}
    md.append(f"# Informe de página — {title or (s.get('title') or 'sin título')}")
    md.append("")

    md.append("## Resumen")
    for key, label in [
        ("title", "Title"),
        ("description", "Description"),
        ("lang", "Lang"),
        ("canonical", "Canonical"),
        ("robots", "Robots"),
        ("og:title", "OG:Title"),
        ("og:description", "OG:Description"),
    ]:
        val = s.get(key)
        if val:
            md.append(f"- **{label}:** {val}")

    md.append("\n## Encabezados (árbol)")
    md.append(render_headings_tree(data.get("headings", []) or []))

    main = data.get("main_content", {}) or {}
    if main.get("text"):
        md.append("\n## Contenido principal (extracto)")
        md.append(shorten(main.get("text"), 800))

    blocks = data.get("content_blocks", []) or []
    md.append("\n## Párrafos (ordenados)")
    md.append(render_paragraphs(blocks, limit=limit_paragraphs))

    md.append("\n## Listas por sección")
    from collections import defaultdict
    acc = defaultdict(list)
    for sec, rows in group_lists_by_section(blocks):
        acc[sec] += rows
    if acc:
        for sec, rows in acc.items():
            md.append(f"**Sección:** {sec}")
            md.append(md_table(["#", "Item"], rows, max_col_width))
            md.append("")
    else:
        md.append("(sin listas)")

    md.append("\n## Enlaces internos")
    md.append(render_links_bullets(data.get("links_internal", []) or []))
    md.append("\n## Enlaces externos")
    md.append(render_links_bullets(data.get("links_external", []) or []))

    md.append("\n## Textos de enlaces (como textos)")
    md.append(render_link_texts_as_paragraphs(data))

    md.append("\n## Imágenes (galería)")
    md.append(render_images_gallery(data.get("images", []) or []))

    forms = data.get("forms", []) or []
    if forms:
        md.append("\n## Formularios")
        for i, f in enumerate(forms, 1):
            md.append(f"**Formulario {i}** — método: `{f.get('method','')}`, action: `{f.get('action','')}`")
            headers = ["tag", "type", "name", "id", "placeholder", "required"]
            rows = []
            for fld in f.get("fields", []):
                rows.append([
                    fld.get("tag", ""),
                    fld.get("type", ""),
                    fld.get("name", ""),
                    fld.get("id", ""),
                    fld.get("placeholder", ""),
                    "sí" if fld.get("required") else "no",
                ])
            md.append(md_table(headers, rows, max_col_width))
            md.append("")
    else:
        md.append("\n## Formularios\n(sin formularios)")

    tables = data.get("tables", []) or []
    if tables:
        md.append("\n## Tablas (vista previa)")
        for i, t in enumerate(tables, 1):
            headers = t.get("headers") or []
            rows = t.get("rows") or []
            md.append(f"**Tabla {i} — vista previa (hasta 10 filas)**")
            md.append(md_table(headers or ["col1","col2"], rows[:10], max_col_width))
            md.append("")

    sd = data.get("structured_data", {}) or {}
    md.append("\n## Structured Data — Resumen")
    md.append(f"JSON-LD bloques detectados: **{len(sd.get('jsonld', []))}**")

    return "\n\n".join(md).strip() + "\n"

# ---------------------------- Combine helpers ----------------------------

def _render_links_grouped_table(grouped: Dict[str, Any], max_rows: int, max_col_width: int) -> str:
    rows: List[List[Any]] = []
    for h in grouped.get("by_host", []):
        rows.append([h.get("host",""), h.get("unique_urls",0), h.get("total",0)])
    return md_table(["Host", "URLs únicas", "Total apariciones"], rows[:max_rows], max_col_width)

def strip_and_dedup_links(links: List[Dict[str, Any]]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for l in links or []:
        u = l.get("absolute_href") or l.get("href")
        if not u:
            continue
        u = strip_tracking(u, keep_query=True)
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def render_report_body_only(data: Dict[str, Any], title: Optional[str], limit_paragraphs: int, max_col_width: int) -> str:
    full = render_report(data, title=title, limit_paragraphs=limit_paragraphs, max_col_width=max_col_width)
    lines = full.splitlines()
    # quitar el H1 y la línea vacía siguiente
    if len(lines) >= 2 and lines[0].startswith("# "):
        return "\n".join(lines[2:]).strip() + "\n"
    return full

def build_combined_report(
    inputs: List[Path],
    out_path: Path,
    limit_paragraphs: int,
    max_col_width: int,
    dedupe_min_len: int = 40,
    max_agg_links: int = 2000,
    max_gallery: int = 200
) -> Path:
    # Cargar
    pages: List[Tuple[str, str, Dict[str, Any]]] = []  # (title, canonical, data)
    for p in inputs:
        data = json.loads(p.read_text(encoding="utf-8", errors="ignore"))
        s = data.get("summary", {}) or {}
        title = s.get("title") or p.stem
        canonical = s.get("canonical") or ""
        pages.append((title, canonical, data))

    # Orden por canonical, luego title
    pages.sort(key=lambda x: (x[1] or "", x[0] or ""))

    # De-dup de párrafos global (simple por hash de texto normalizado)
    def norm_text(t: str) -> str:
        return " ".join((t or "").split()).lower()

    seen_paras: Set[str] = set()

    # Agregados site-wide
    all_internal: List[Dict[str, Any]] = []
    all_external: List[Dict[str, Any]] = []
    all_images: List[Dict[str, Any]] = []

    md: List[str] = []
    md.append(f"# Informe combinado del sitio — {len(pages)} páginas")
    md.append("")
    md.append("## Índice")
    for (title, canonical, _d) in pages:
        anchor = title.strip().lower().replace(" ", "-")[:80] or "pagina"
        if canonical:
            md.append(f"- [{title}](#{anchor}) — {canonical}")
        else:
            md.append(f"- [{title}](#{anchor})")
    md.append("")

    # Páginas
    for (title, canonical, d) in pages:
        # recolectar agregados
        all_internal += (d.get("links_internal") or [])
        all_external += (d.get("links_external") or [])
        all_images += (d.get("images") or [])

        md.append(f"\n## {title}")
        if canonical:
            md.append(f"URL: {canonical}")
        md.append("")

        # filtro de párrafos duplicados
        if dedupe_min_len > 0:
            blocks = d.get("content_blocks") or []
            new_blocks = []
            for b in blocks:
                if b.get("type") == "paragraph":
                    txt = b.get("text") or ""
                    if len(txt) >= dedupe_min_len:
                        key = norm_text(txt)
                        if key in seen_paras:
                            continue
                        seen_paras.add(key)
                new_blocks.append(b)
            d = dict(d)
            d["content_blocks"] = new_blocks

        body = render_report_body_only(d, title=title, limit_paragraphs=limit_paragraphs, max_col_width=max_col_width)
        md.append(body)

    # Sección de agregados site-wide
    md.append("\n## Agregados del sitio")

    # Enlaces internos/externos
    uniq_internal = strip_and_dedup_links(all_internal)[:max_agg_links]
    uniq_external = strip_and_dedup_links(all_external)[:max_agg_links]

    md.append("\n### Enlaces internos (únicos)")
    md.append("\n".join(f"- {u}" for u in uniq_internal) or "(sin enlaces)")
    md.append("\n### Enlaces externos (únicos)")
    md.append("\n".join(f"- {u}" for u in uniq_external) or "(sin enlaces)")

    # Galería de imágenes
    if all_images:
        md.append("\n### Imágenes (galería)")
        imgs_lines: List[str] = []
        seen_img: Set[str] = set()
        for im in all_images:
            src = im.get("absolute_src") or im.get("src")
            if not src:
                continue
            src = strip_tracking(src, keep_query=True)
            if src in seen_img:
                continue
            seen_img.add(src)
            alt = im.get("alt") or ""
            imgs_lines.append(f"![]({src})")
            if alt:
                imgs_lines.append(f"*{alt}*")
            if len(seen_img) >= max_gallery:
                break
        md.append("\n\n".join(imgs_lines) if imgs_lines else "(sin imágenes)")
    else:
        md.append("\n### Imágenes (galería)\n(sin imágenes)")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(md).strip() + "\n", encoding="utf-8")
    return out_path

# ---------------------------- CLI ----------------------------

def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8", errors="ignore"))

def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def process_file(in_path: Path, out_dir: Path, limit_paragraphs: int, max_col_width: int) -> Path:
    data = load_json(in_path)
    title = in_path.stem
    md = render_report(data, title=title, limit_paragraphs=limit_paragraphs, max_col_width=max_col_width)
    out_path = out_dir / f"{in_path.stem}.md"
    write_text(out_path, md)
    return out_path

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Renderiza informes Markdown desde JSON del extractor (y combinado opcional)")
    #ap.add_argument("--input", type=str, help="Ruta a un JSON concreto")
    #ap.add_argument("--glob", type=str, help='Patrón glob para múltiples JSON (p.ej., "data/parsed/*.json")')
    #ap.add_argument("--out", type=str, default="reports", help="Directorio de salida de informes .md por página")
    #ap.add_argument("--limit-paragraphs", type=int, default=50)
    #ap.add_argument("--max-col-width", type=int, default=100)
    #ap.add_argument("--combine", type=str, help="Ruta del MD combinado (p.ej., reports/site.md)")
    #ap.add_argument("--no-per-page", action="store_true", help="No generar MD por página; sólo combinado")
    #ap.add_argument("--dedupe-min-len", type=int, default=40, help="Longitud mínima de párrafos a deduplicar en combinado")
    #ap.add_argument("--max-agg-links", type=int, default=2000, help="Máximo de enlaces únicos listados en agregados")
    #ap.add_argument("--max-gallery", type=int, default=200, help="Máximo de imágenes en galería agregada")
    args = ap.parse_args(argv)

    # entradas
    inputs: List[Path] = []
    if args.input:
        inputs = [Path(args.input)]
    elif args.glob:
        inputs = [Path(p) for p in glob.glob(args.glob)]
    else:
        ap.error("Debe proporcionar --input o --glob")

    if not inputs:
        print("[WARN] No se encontraron entradas")
        return 0

    # per-page
    if not args.no_per_page:
        out_dir = Path(args.out)
        print(f"[INFO] Generando informes por página en {out_dir} …")
        for p in inputs:
            try:
                out_p = process_file(p, out_dir, args.limit_paragraphs, args.max_col_width)
                print(f"[OK] {p.name} → {out_p}")
            except Exception as e:
                print(f"[FAIL] {p}: {type(e).__name__}: {e}")

    # combinado
    if args.combine:
        try:
            print(f"[INFO] Generando combinado → {args.combine}")
            build_combined_report(
                inputs=inputs,
                out_path=Path(args.combine),
                limit_paragraphs=args.limit_paragraphs,
                max_col_width=args.max_col_width,
                dedupe_min_len=args.dedupe_min_len,
                max_agg_links=args.max_agg_links,
                max_gallery=args.max_gallery,
            )
            print("[OK] Combinado listo")
        except Exception as e:
            print(f"[FAIL] Combinado: {type(e).__name__}: {e}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
