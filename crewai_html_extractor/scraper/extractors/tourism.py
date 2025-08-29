from __future__ import annotations
import json, re
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup




# ---- Heurísticas España: NIF/CIF, licencias turísticas, teléfono, email ----

RE_EMAIL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
# Teléfonos ES (muy permisivo; prioriza tel: y og:phone)
RE_TEL = re.compile(r"(?:(?:\+34|0034)\s*)?(?:\d[\s\-\.]?){9,11}")

# NIF/CIF
RE_DNI = re.compile(r"\b\d{8}[A-Z]\b")
RE_NIE = re.compile(r"\b[XYZ]\d{7}[A-Z]\b", re.I)
RE_CIF = re.compile(r"\b[ABCDEFGHJKLMNPQRSUVW]\d{7}[0-9A-J]\b", re.I)

# Licencias turísticas típicas (no exhaustivo; heurístico)
RE_LICENCIAS = re.compile(
    r"\b(?:VFT|VTAR|CTC|H|A|CR|CA|AL|AT|VFTAR)/[A-Z]{1,2}/\d{1,6}\b|"
    r"\bHU(?:TB|TG)-\d{4,6}\b|"                 # Cataluña HUTB/HUTG
    r"\bVT-\d{1,5}-[A-Z]\b|"                   # C. Valenciana VT-xxxxx-X
    r"\bETV(?:PL)?/\d{1,6}\b|"                 # Illes Balears ETV/ ETVPL/
    r"\bVV-\d{1,6}\b",                         # Canarias VV-
    re.I
)

INTERESTING_TYPES = {
    "Hotel": "hotel",
    "LodgingBusiness": "hotel",
    "TouristAttraction": "attraction",
    "Restaurant": "restaurant",
    "LocalBusiness": "local_business",
    "Organization": "organization",
    "Event": "event",
    "TravelAgency": "travel_agency",
    "TouristDestination": "destination",
}

def _json_loads_loose(s: str) -> Optional[Any]:
    try:
        return json.loads(s)
    except Exception:
        try:
            import json5  # opcional
            return json5.loads(s)
        except Exception:
            return None

def _first(obj, *keys, default=None):
    for k in keys:
        if isinstance(obj, dict) and k in obj and obj[k]:
            return obj[k]
    return default

def _as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def _postal_address(addr: dict) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    if not isinstance(addr, dict):
        return None, None, None, None, None
    street = _first(addr, "streetAddress")
    locality = _first(addr, "addressLocality", "locality")
    region = _first(addr, "addressRegion", "region")
    pc = _first(addr, "postalCode", "zip")
    country = _first(addr, "addressCountry", "country")
    if isinstance(country, dict):
        country = _first(country, "name")
    return street, locality, region, pc, country

def _geo(geo: dict) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(geo, dict):
        return None, None
    lat = _first(geo, "latitude", "lat")
    lon = _first(geo, "longitude", "lng", "lon")
    try:
        return float(lat), float(lon)
    except Exception:
        return None, None

def _rating(node: dict) -> Tuple[Optional[float], Optional[int]]:
    agr = _first(node, "aggregateRating")
    if isinstance(agr, dict):
        val = _first(agr, "ratingValue")
        cnt = _first(agr, "ratingCount", "reviewCount")
        try:
            val = float(str(val).replace(",", "."))
        except Exception:
            val = None
        try:
            cnt = int(cnt)
        except Exception:
            cnt = None
        return val, cnt
    return None, None

def _collect_contacts(soup: BeautifulSoup, text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # tel prioritario desde <a href="tel:">
    tel = None
    a_tel = soup.select('a[href^="tel:"]')
    if a_tel:
        tel = a_tel[0].get("href", "").split(":", 1)[-1].strip()
    if not tel:
        m = RE_TEL.search(text)
        tel = m.group(0).strip() if m else None

    email = None
    a_mail = soup.select('a[href^="mailto:"]')
    if a_mail:
        email = a_mail[0].get("href", "").split(":", 1)[-1].strip()
    if not email:
        m = RE_EMAIL.search(text)
        email = m.group(0).strip() if m else None

    # og:phone (raro pero existe)
    og_phone = soup.find("meta", property="og:phone_number")
    if og_phone and not tel:
        tel = og_phone.get("content", "").strip() or tel

    return tel, email, None  # website se expondrá como url principal

def _detect_ids(text: str) -> Tuple[Optional[str], Optional[str]]:
    lic = None
    m = RE_LICENCIAS.search(text)
    if m:
        lic = m.group(0)
    cif = None
    for rx in (RE_CIF, RE_NIE, RE_DNI):
        mm = rx.search(text)
        if mm:
            cif = mm.group(0)
            break
    return lic, cif

def _normalize_entity(node: dict, base_url: str, soup: BeautifulSoup, full_text: str) -> Dict[str, Any]:
    typ = _first(node, "@type", "type")
    if isinstance(typ, list):
        typ = next((t for t in typ if t in INTERESTING_TYPES), typ[0] if typ else None)
    entity_type = INTERESTING_TYPES.get(str(typ), "other")

    name = _first(node, "name")
    legal_name = _first(node, "legalName")
    url = _first(node, "url")
    same_as = _as_list(_first(node, "sameAs"))
    desc = _first(node, "description")

    addr = _first(node, "address")
    street, locality, region, pc, country = _postal_address(addr if isinstance(addr, dict) else (addr[0] if _as_list(addr) else {}))

    geo = _first(node, "geo")
    lat, lon = _geo(geo if isinstance(geo, dict) else (geo[0] if _as_list(geo) else {}))

    tel = _first(node, "telephone", "phone")
    email = _first(node, "email")
    if not tel or not email:
        tel2, email2, _ = _collect_contacts(soup, full_text)
        tel = tel or tel2
        email = email or email2

    price_range = _first(node, "priceRange")
    rating, rating_count = _rating(node)

    checkin = _first(node, "checkinTime", "checkin")
    checkout = _first(node, "checkoutTime", "checkout")

    event_start = _first(node, "startDate") if entity_type == "event" else None
    event_end = _first(node, "endDate") if entity_type == "event" else None

    lic, cif = _detect_ids(full_text)

    return {
        "type": "entity",
        "entity_type": entity_type,
        "name": name,
        "legal_name": legal_name,
        "description": desc,
        "tourism_license": lic,
        "cif_nif": cif,
        "url": url or base_url,
        "same_as": same_as,
        "telephone": tel,
        "email": email,
        "price_range": price_range,
        "rating": rating,
        "rating_count": rating_count,
        "address_street": street,
        "address_locality": locality,
        "address_region": region,
        "address_postal_code": pc,
        "address_country": country,
        "lat": lat,
        "lon": lon,
        "checkin": checkin,
        "checkout": checkout,
        "event_start": event_start,
        "event_end": event_end,
        "source": {"method": "jsonld/heuristics", "url": base_url, "schema_type": typ},
        "confidence": 0.95 if name else 0.8,
    }

def extract_tourism_entities(html: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Extrae entidades turísticas desde JSON-LD + heurísticas HTML.
    Devuelve items con type="entity".
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    items: List[Dict[str, Any]] = []

    # 1) JSON-LD
    for s in soup.find_all("script", {"type": "application/ld+json"}):
        data = _json_loads_loose(s.string or "")
        if not data:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            # @graph suele agrupar múltiples entidades
            if "@graph" in node and isinstance(node["@graph"], list):
                for g in node["@graph"]:
                    if isinstance(g, dict):
                        typ = g.get("@type")
                        if typ in INTERESTING_TYPES or (isinstance(typ, list) and any(t in INTERESTING_TYPES for t in typ)):
                            items.append(_normalize_entity(g, base_url, soup, text))
                continue
            typ = node.get("@type")
            if typ in INTERESTING_TYPES or (isinstance(typ, list) and any(t in INTERESTING_TYPES for t in typ)):
                items.append(_normalize_entity(node, base_url, soup, text))

    # 2) (Opcional) microdatos/opengraph con extruct si está instalado
    try:
        import extruct
        from w3lib.html import get_base_url
        ex = extruct.extract(html, base_url=get_base_url(html, base_url), syntaxes=["microdata", "opengraph"])
        # microdata
        for md in ex.get("microdata", []):
            node = md.get("properties") or {}
            if not isinstance(node, dict):
                continue
            typ = md.get("type") or node.get("@type")
            if isinstance(typ, list):
                tt = next((t for t in typ if t in INTERESTING_TYPES), None)
            else:
                tt = typ
            if tt in INTERESTING_TYPES:
                node["@type"] = tt
                items.append(_normalize_entity(node, base_url, soup, text))
        # og: basic fallback (solo nombre/desc/url)
        for og in ex.get("opengraph", []):
            ogp = og.get("properties") or {}
            if "og:title" in ogp or "title" in ogp:
                node = {
                    "@type": "Organization",
                    "name": ogp.get("og:title") or ogp.get("title"),
                    "description": ogp.get("og:description") or ogp.get("description"),
                    "url": ogp.get("og:url") or base_url,
                }
                items.append(_normalize_entity(node, base_url, soup, text))
    except Exception:
        pass

    # 3) De-duplicación básica por (name, entity_type)
    seen = set()
    deduped = []
    for it in items:
        key = (it.get("name"), it.get("entity_type"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)

    return deduped

# --- EXTRA: extractor genérico para listados tipo "alojamientos" ---


RE_EMAIL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
RE_TEL   = re.compile(r"(?:(?:\+34|0034)\s*)?(?:\d[\s\-\.]?){9,11}")

def _clean_line(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _looks_name(line: str) -> bool:
    if not line: return False
    if RE_EMAIL.search(line): return False
    if re.search(r"https?://|www\.", line, re.I): return False
    if RE_TEL.search(line): return False
    if len(line) > 120: return False
    return True

def extract_portal_listings_generic(html: str, base_url: str):
    """
    Heurístico para páginas de listados (p.ej., Hoteles en turisme.vinaros.es).
    Detecta 'tarjetas' a partir de enlaces externos (web del hotel) y
    recoge líneas de texto cercanas como nombre, dirección, teléfono y email.
    Devuelve items con type='entity'.
    """
    soup = BeautifulSoup(html, "lxml")
    netloc = urlparse(base_url).netloc

    # Candidato a contenedor principal (fallback al body)
    main = soup.find("main") or soup

    items = []
    seen_urls = set()

    # Anclas hacia dominios externos (suelen ser la web del hotel)
    for a in main.select('a[href^="http"]'):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        host = urlparse(href).netloc
        if not host or host.endswith(netloc):
            continue  # interno; no es web del negocio

        if href in seen_urls:
            continue
        seen_urls.add(href)

        # Toma el bloque de texto del contenedor cercano (p, li, div, article, section)
        cont = a.find_parent(["p", "li", "div", "article", "section"]) or a.parent
        lines = [_clean_line(s) for s in cont.get_text("\n", strip=True).split("\n")]
        lines = [ln for ln in lines if ln]

        # Campos
        name = None
        address = []
        tel = None
        email = None

        for ln in lines:
            if not name and _looks_name(ln):
                name = ln
                continue
            if not tel:
                m = RE_TEL.search(ln)
                if m: tel = m.group(0)
            if not email:
                m = RE_EMAIL.search(ln)
                if m: email = m.group(0)
            # Dirección: heurística simple (calles, “C.”, “Av.”, “Ctra.”, número, CP)
            if re.search(r"\b(C\.|Calle|Carrer|Av\.|Avenida|Ctra\.|Passeig|Plaça|Plaza|Avinguda|Camino|Km|N-?\d+| \d{5} )", ln, re.I):
                address.append(ln)

        item = {
            "type": "entity",
            "entity_type": "hotel",  # si lo usas en otras secciones, podrás parametrizar
            "name": name,
            "description": None,
            "tourism_license": None,
            "cif_nif": None,
            "url": href,
            "same_as": [],
            "telephone": tel,
            "email": email,
            "address_street": " ; ".join(address) if address else None,
            "address_locality": None,
            "address_region": None,
            "address_postal_code": None,
            "address_country": "ES",
            "lat": None,
            "lon": None,
            "price_range": None,
            "rating": None,
            "rating_count": None,
            "checkin": None,
            "checkout": None,
            "event_start": None,
            "event_end": None,
            "source": {"method": "portal-listing", "url": base_url},
            "confidence": 0.85 if name or tel or email else 0.7,
        }
        items.append(item)

    return items
