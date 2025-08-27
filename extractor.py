# -*- coding: utf-8 -*-
"""
Extractor de HTML para landing pages.

Devuelve un diccionario con:
- summary: title, description, lang, canonical, robots, og:title, og:description
- metadata: mapa con meta name/property => content
- headings: lista de {tag: hN, text}
- main_content: {text}  (heurística sobre <main>/<article> y bloques largos)
- content_blocks:
    - paragraph: {text, heading_path}
    - list: {items, heading_path}
- links_internal / links_external: [{href, absolute_href, text, title_attr, aria_label}]
- link_texts: lista con textos de enlaces (anchor/title/aria y alt de imágenes dentro de <a>)
- images: [{src, absolute_src, alt, width, height}]
- forms: [{method, action, fields:[{tag,type,name,id,placeholder,required}]}]
- tables: [{headers:[...], rows:[[...], ...]}]
- structured_data: {jsonld:[...]} (bloques LD+JSON parseados)

Requisitos:
    pip install beautifulsoup4 lxml
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import re

def _soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def _text(el) -> str:
    return " ".join((el.get_text(separator=" ", strip=True) if el else "").split())

def _abs(base: Optional[str], url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if not base:
        return url
    try:
        return urljoin(base, url)
    except Exception:
        return url

def _domain(url: Optional[str]) -> Optional[str]:
    try:
        return urlparse(url or "").netloc
    except Exception:
        return None

def _is_internal(target: str, base_url: Optional[str]) -> bool:
    try:
        if not base_url:
            return True
        return urlparse(target).netloc == urlparse(base_url).netloc
    except Exception:
        return False

def _meta_map(soup: BeautifulSoup) -> Dict[str, str]:
    meta: Dict[str, str] = {}
    for m in soup.find_all("meta"):
        name = (m.get("name") or m.get("property") or "").strip()
        content = (m.get("content") or "").strip()
        if name and content:
            meta[name.lower()] = content
    return meta

def _get_lang(soup: BeautifulSoup) -> Optional[str]:
    html = soup.find("html")
    if html and html.get("lang"):
        return html.get("lang").strip()
    return None

def _get_canonical(soup: BeautifulSoup, base_url: Optional[str]) -> Optional[str]:
    link = soup.find("link", rel=lambda v: v and "canonical" in v)
    if link and link.get("href"):
        return _abs(base_url, link["href"])
    return None

def _collect_headings(soup: BeautifulSoup) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for i in range(1, 7):
        for h in soup.find_all(f"h{i}"):
            out.append({"tag": f"h{i}", "text": _text(h)})
    return out

def _walk_blocks_with_heading_path(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Recorre el body y recopila bloques con el 'heading_path' (rastro de h1..h6).
    """
    body = soup.body or soup
    blocks: List[Dict[str, Any]] = []

    # stack por nivel h (1..6)
    current: List[Optional[str]] = [None] * 6

    def heading_level(tagname: str) -> Optional[int]:
        if tagname and len(tagname) == 2 and tagname[0] == "h" and tagname[1].isdigit():
            lvl = int(tagname[1])
            if 1 <= lvl <= 6:
                return lvl
        return None

    # Itera por elementos relevantes manteniendo estado de headings
    for el in body.descendants:
        if not getattr(el, "name", None):
            continue
        name = el.name.lower()

        lvl = heading_level(name)
        if lvl:
            # actualizar la ruta de headings
            current[lvl - 1] = _text(el)
            for i in range(lvl, 6):
                current[i] = None
            continue

        # párrafos
        if name == "p":
            txt = _text(el)
            if txt:
                blocks.append({"type": "paragraph", "text": txt,
                               "heading_path": [h for h in current if h]})
            continue

        # listas
        if name in ("ul", "ol"):
            items: List[str] = []
            for li in el.find_all("li", recursive=False):
                t = _text(li)
                if t:
                    items.append(t)
            if items:
                blocks.append({"type": "list", "items": items,
                               "heading_path": [h for h in current if h]})
            continue

    return blocks

def _main_content(soup: BeautifulSoup) -> Dict[str, str]:
    # preferimos <main> o <article>; si no, el bloque con más texto
    candidates = []
    for sel in ["main", "article"]:
        for el in soup.find_all(sel):
            candidates.append((len(_text(el)), el))
    if not candidates:
        # por secciones grandes
        for el in soup.find_all(["section", "div"]):
            t = _text(el)
            if len(t) > 400:
                candidates.append((len(t), el))
    if not candidates:
        t = _text(soup.body or soup)
        return {"text": t}
    best = max(candidates, key=lambda x: x[0])[1]
    return {"text": _text(best)}

def _collect_links(soup: BeautifulSoup, base_url: Optional[str]) -> Tuple[List[dict], List[dict], List[str]]:
    internal: List[dict] = []
    external: List[dict] = []
    texts: List[str] = []

    for a in soup.find_all("a", href=True):
        href = a.get("href")
        absu = _abs(base_url, href)
        text = _text(a)
        title_attr = a.get("title") or ""
        aria_label = a.get("aria-label") or ""

        # textos de imágenes dentro del <a>
        alt_img = ""
        img = a.find("img", alt=True)
        if img:
            alt_img = (img.get("alt") or "").strip()

        link_obj = {
            "href": href,
            "absolute_href": absu,
            "text": text,
            "title_attr": title_attr,
            "aria_label": aria_label,
        }

        if absu and _is_internal(absu, base_url):
            internal.append(link_obj)
        else:
            external.append(link_obj)

        for cand in [text, title_attr, aria_label, alt_img]:
            if cand:
                texts.append(cand)

    return internal, external, texts

def _collect_images(soup: BeautifulSoup, base_url: Optional[str]) -> List[dict]:
    imgs: List[dict] = []
    for im in soup.find_all("img"):
        src = im.get("src")
        absu = _abs(base_url, src)
        imgs.append({
            "src": src,
            "absolute_src": absu,
            "alt": (im.get("alt") or "").strip(),
            "width": im.get("width"),
            "height": im.get("height"),
        })
    return imgs

def _collect_forms(soup: BeautifulSoup, base_url: Optional[str]) -> List[dict]:
    forms: List[dict] = []
    for f in soup.find_all("form"):
        method = (f.get("method") or "GET").upper()
        action = _abs(base_url, f.get("action"))
        fields: List[dict] = []
        # inputs
        for inp in f.find_all("input"):
            fields.append({
                "tag": "input",
                "type": (inp.get("type") or "").lower(),
                "name": inp.get("name"),
                "id": inp.get("id"),
                "placeholder": inp.get("placeholder"),
                "required": bool(inp.get("required")),
            })
        # selects
        for sel in f.find_all("select"):
            fields.append({
                "tag": "select",
                "type": "",
                "name": sel.get("name"),
                "id": sel.get("id"),
                "placeholder": None,
                "required": bool(sel.get("required")),
            })
        # textareas
        for ta in f.find_all("textarea"):
            fields.append({
                "tag": "textarea",
                "type": "",
                "name": ta.get("name"),
                "id": ta.get("id"),
                "placeholder": ta.get("placeholder"),
                "required": bool(ta.get("required")),
            })
        forms.append({"method": method, "action": action, "fields": fields})
    return forms

def _collect_tables(soup: BeautifulSoup) -> List[dict]:
    tables: List[dict] = []
    for t in soup.find_all("table"):
        headers: List[str] = []
        thead = t.find("thead")
        if thead:
            headers = [_text(th) for th in thead.find_all("th")]
        if not headers:
            # intenta con la primera fila
            first_tr = t.find("tr")
            if first_tr:
                headers = [_text(th) for th in first_tr.find_all(["th", "td"])]

        rows: List[List[str]] = []
        for tr in t.find_all("tr"):
            cells = [_text(td) for td in tr.find_all(["td", "th"])]
            if cells:
                rows.append(cells)
        # elimina la fila de headers si se duplicó
        if headers and rows and rows[0] == headers:
            rows = rows[1:]
        tables.append({"headers": headers, "rows": rows})
    return tables

def _collect_jsonld(soup: BeautifulSoup) -> List[Any]:
    out: List[Any] = []
    for sc in soup.find_all("script", type=lambda v: v and "ld+json" in v):
        try:
            txt = sc.string or sc.get_text() or ""
            if not txt.strip():
                continue
            # algunos sitios concatenan varios JSON-LD; intenta parsear bloque a bloque
            try:
                out.append(json.loads(txt))
            except json.JSONDecodeError:
                # heurística: intenta encontrar objetos {} tope de línea
                for m in re.finditer(r"\{.*?\}", txt, flags=re.DOTALL):
                    try:
                        out.append(json.loads(m.group(0)))
                    except Exception:
                        pass
        except Exception:
            pass
    return out

# ---------------------------- API principal ----------------------------

def parse_landing_html(html: str, base_url: Optional[str] = None) -> Dict[str, Any]:
    soup = _soup(html)

    meta = _meta_map(soup)
    summary = {
        "title": (soup.title.string.strip() if soup.title and soup.title.string else None),
        "description": meta.get("description"),
        "lang": _get_lang(soup),
        "canonical": _get_canonical(soup, base_url),
        "robots": meta.get("robots"),
        "og:title": meta.get("og:title"),
        "og:description": meta.get("og:description"),
    }

    headings = _collect_headings(soup)
    blocks = _walk_blocks_with_heading_path(soup)
    main_content = _main_content(soup)
    links_internal, links_external, link_texts = _collect_links(soup, base_url)
    images = _collect_images(soup, base_url)
    forms = _collect_forms(soup, base_url)
    tables = _collect_tables(soup)
    jsonld = _collect_jsonld(soup)

    data: Dict[str, Any] = {
        "summary": summary,
        "metadata": meta,
        "headings": headings,
        "main_content": main_content,
        "content_blocks": blocks,
        "links_internal": links_internal,
        "links_external": links_external,
        "link_texts": link_texts,
        "images": images,
        "forms": forms,
        "tables": tables,
        "structured_data": {"jsonld": jsonld},
    }
    return data
