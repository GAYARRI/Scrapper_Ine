from __future__ import annotations
import json, re
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse

# --- Clasificador de entidades turismo: segment & subtype ---

SCHEMA_TO_SEG_SUB = {
    # Alojamiento
    "Hotel": ("accommodation", "hotel"),
    "LodgingBusiness": ("accommodation", None),
    "Hostel": ("accommodation", "hostal"),
    "BedAndBreakfast": ("accommodation", "casa_rural"),
    "Campground": ("accommodation", "camping"),
    # Experiencia / Evento
    "Event": ("experience", "evento"),
    "TouristAttraction": ("experience", "cultural"),
    # Empresa/recursos
    "Restaurant": ("business", "restaurante"),
    "CafeOrCoffeeShop": ("business", "bar_cafe"),
    "BarOrPub": ("business", "bar_cafe"),
    "LocalBusiness": ("business", None),
    "TravelAgency": ("business", "agencia_viajes"),
    "TouristInformationCenter": ("business", "info_turistica"),
    "SportsActivityLocation": ("business", "turismo_activo"),
}

# Palabras clave (es/ca/en) por grupo y subtipo
KW = {
    "accommodation": {
        "hotel": r"\bhotel(?:es)?|h\s*\d?★|\b4\s*estrellas\b|\b5\s*estrellas\b",
        "hostal": r"\bhostal(?:es)?|pensi[oó]n",
        "apartamento": r"apart(amento|ament|ament[oó]s)|vivienda(?:s)? tur[ií]stica(?:s)?|aparthotel",
        "casa_rural": r"casa(?:s)? rural(?:es)?|agroturism|mas[ií]a",
        "camping": r"\bcamping|campament",
    },
    "experience": {
        "tour_guiado": r"\btour|visita guiada|guided tour|ruta guiada",
        "ruta_senderismo": r"\bruta(s)?|sender|itinerari|trail|track",
        "actividad_nautica": r"kayak|barco|paseo en barco|snorkel|buceo|vela|paddle\s*surf|sup|submarin",
        "gastronomia": r"cata|degustaci[oó]n|taller|cocina|showcooking|enotur|oleotur|gastronom",
        "cultural": r"museo|monumento|patrimonio|castillo|ermita|teatro",
        "evento": r"\bevento|agenda|festival|concierto|feria|fiesta",
    },
    "business": {
        "restaurante": r"restauran|trattoria|osteria|asador|arrocer|marisquer|pizzer",
        "bar_cafe": r"\bbar\b|caf[eé]|taper|bodega",
        "agencia_viajes": r"agencia de viajes|receptivo|incoming|tour operator",
        "turismo_activo": r"multiaventura|aventura|turismo activo|excursiones|guias? de montaña",
        "alquiler": r"alquiler|rent a (bike|car|scooter)|rent(?:al)?",
        "info_turistica": r"oficina de turismo|informaci[oó]n tur[ií]stica",
    },
}



def _tok(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

def _match_any(rx: str, texto: str) -> bool:
    return bool(re.search(rx, texto, re.I)) if rx else False

def _classify_by_schema(schema_type) -> tuple[str|None, str|None, str]:
    types = schema_type if isinstance(schema_type, list) else [schema_type]
    for t in types:
        if not t: 
            continue
        t = str(t)
        if t in SCHEMA_TO_SEG_SUB:
            seg, sub = SCHEMA_TO_SEG_SUB[t]
            return seg, sub, "schema"
    return None, None, "none"

def classify_entity(raw_type, url: str, name: str, description: str, full_text: str) -> tuple[str|None, str|None, str, int]:
    """
    Devuelve (segment, subtype, source, score)
    source: 'schema' | 'keywords' | 'url' | 'fallback'
    score:  1..100
    """
    seg, sub, src = _classify_by_schema(raw_type)
    if seg:
        base_score = 80 if sub else 70  # schema acertado con/ sin subtipo
        return seg, sub, "schema", base_score

    u = _tok(url)
    n = _tok(name)
    d = _tok(description)
    t = _tok(full_text)
    hay = lambda rx: (_match_any(rx, n) or _match_any(rx, d) or _match_any(rx, u) or _match_any(rx, t))

    # Keywords por grupo y subtipo
    for seg_key, submap in KW.items():
        for sub_key, rx in submap.items():
            if hay(rx):
                # preferencia por URL/texto nombre (menos ruido)
                score = 65 if _match_any(rx, u) or _match_any(rx, n) else 60
                return seg_key, sub_key, "keywords", score

    # Pistas de URL (segment genérico)
    if re.search(r"/(hotel|aloj|apart|hostal|camping)/", u): 
        return "accommodation", None, "url", 55
    if re.search(r"/(experien|actividad|agenda|evento|ruta|tour)/", u): 
        return "experience", None, "url", 50
    if re.search(r"/(restaur|bar|agencia|servici|empresa|info)/", u): 
        return "business", None, "url", 50

    return None, None, "fallback", 40


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
    
    def _normalize_entity(node: dict, base_url: str, soup: BeautifulSoup, full_text: str) -> Dict[str, Any]:
        typ = _first(node, "@type", "type")
        if isinstance(typ, list):
            typ = next((t for t in typ if t in INTERESTING_TYPES), typ[0] if typ else None)

        name = _first(node, "name")
        legal_name = _first(node, "legalName")
        url = _first(node, "url") or base_url
        same_as = _as_list(_first(node, "sameAs"))
        desc = _first(node, "description")

    


        lic, cif = _detect_ids(full_text)

        # 👇 NUEVO: clasificación segment/subtype
        segment, subtype, seg_source, seg_score = classify_entity(typ, url, name or "", desc or "", full_text)

        # si no hubo segment por schema, usar mapa INTERESTING_TYPES como fallback para "entity_type" legacy
        legacy_entity_type = INTERESTING_TYPES.get(str(typ), "other")

        base_conf = 0.95 if name else 0.8
        # refuerza confianza si hay schema + tel/email o address
        if segment and seg_source == "schema":
            base_conf += 0.02
        if (tel or email) and (street or locality):
            base_conf += 0.01
        base_conf = min(base_conf, 0.99)

        return {
            "type": "entity",
            "segment": segment,             # accommodation | experience | business
            "subtype": subtype,             # hotel | tour_guiado | restaurante | ...
            "segment_source": seg_source,   # schema | keywords | url | fallback
            "segment_score": seg_score,     # 40..80
            "entity_type": legacy_entity_type,  # retrocompatibilidad
            "name": name,
            "legal_name": legal_name,
            "description": desc,
            "tourism_license": lic,
            "cif_nif": cif,
            "url": url,
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
            "confidence": base_conf,
        }


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

    def _guess_expected_segment_from_url(url: str) -> tuple[str|None, str|None]:
        u = (url or "").lower()
        if any(k in u for k in ("restauracion", "restauración", "restaurantes", "bares", "bar", "donde-comer", "dondecomer", "gastronomia")):
            return "business", "restaurante"
        if any(k in u for k in ("agenda", "event", "evento", "experienc", "activid", "que-ver", "quever", "rutas", "ruta", "tours", "tour")):
            return "experience", None
        if any(k in u for k in ("aloj", "hotel", "hostal", "apart", "camping")):
            return "accommodation", None
        return None, None

def extract_portal_listings_generic(html: str, base_url: str, expected_segment: str|None = None, subtype_hint: str|None = None):
    """
    Heurístico para páginas de listados (hoteles/restaurantes/experiencias).
    - Usa enlaces externos SI existen.
    - Si no, procesa tarjetas internas (.views-row, article, li, .card).
    - Etiqueta segment/subtype por: schema -> keywords -> URL -> expected_segment.
    """
    soup = BeautifulSoup(html, "lxml")
    netloc = urlparse(base_url).netloc

    # 0) Pistas por URL
    url_seg, url_sub = _guess_expected_segment_from_url(base_url)
    if not expected_segment:
        expected_segment = url_seg
    if not subtype_hint:
        subtype_hint = url_sub

    items = []
    seen_urls = set()

    def build_item(name: str|None, href: str|None, lines: list[str], container_text: str) -> dict:
        # contactos
        tel, email, _ = _collect_contacts(soup, container_text)
        # clasificación
        seg, sub, seg_src, seg_score = classify_entity(
            raw_type=None,
            url=href or base_url,
            name=name or "",
            description="",
            full_text=container_text
        )
        if not seg and expected_segment:
            seg, sub, seg_src, seg_score = expected_segment, subtype_hint, "fallback", 45

        address = []
        for ln in lines:
            if re.search(r"\b(C\.|Calle|Carrer|Av\.|Avenida|Ctra\.|Passeig|Plaça|Plaza|Avinguda|Camino|Km|N-?\d+|\b\d{5}\b)", ln, re.I):
                address.append(ln)

        return {
            "type": "entity",
            "segment": seg or "business",
            "subtype": sub,
            "segment_source": seg_src,
            "segment_score": seg_score,
            "name": name,
            "description": None,
            "tourism_license": None,
            "cif_nif": None,
            "url": (href or base_url),
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
            "confidence": 0.87 if (name or tel or email) else 0.72,
        }

    def extract_lines(node) -> list[str]:
        return [_clean_line(s) for s in node.get_text("\n", strip=True).split("\n") if _clean_line(s)]

    # 1) Caso 1: tarjetas con ENLACE EXTERNO (p. ej. webs propias)
    main = soup.find("main") or soup
    for a in main.select('a[href^="http"]'):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        host = urlparse(href).netloc
        if not host or host.endswith(netloc):
            continue  # interno -> lo procesamos en caso 2
        if href in seen_urls:
            continue
        seen_urls.add(href)

        cont = a.find_parent(["article","li","div","section"]) or a.parent
        lines = extract_lines(cont)
        name = next((ln for ln in lines if _looks_name(ln)), None)
        item = build_item(name, href, lines, cont.get_text(" ", strip=True))
        items.append(item)

    # 2) Caso 2: tarjetas INTERNAS (sin enlace externo): Drupal/WordPress típicos
    cards = []
    cards += main.select(".view-content .views-row")
    cards += main.select("article")
    cards += main.select("li")
    cards += main.select(".card, .c-card, .listing, .teaser")

    for c in cards:
        # evita duplicar si ya vino por el caso 1
        ext_link = c.select_one('a[href^="http"]')
        if ext_link:
            href = ext_link.get("href", "").strip()
            if href and href in seen_urls:
                continue

        # nombre: usa heading o primer <strong>
        h = c.find(re.compile(r"^h[1-4]$"))
        name = _clean_line(h.get_text(strip=True)) if h else None
        if not name:
            st = c.find("strong")
            name = _clean_line(st.get_text(strip=True)) if st else None

        # url: usa el 1er enlace (interno o externo)
        link = c.find("a", href=True)
        href = link.get("href").strip() if link else None
        href = normalize_url(base_url, href) if href else None

        lines = extract_lines(c)
        item = build_item(name, href, lines, c.get_text(" ", strip=True))
        # Evita duplicados por nombre+url
        key = (item.get("name"), item.get("url"))
        if key not in seen_urls:
            items.append(item)

    # De-dup por (name,url,segment)
    seen = set()
    out = []
    for it in items:
        key = ((it.get("name") or "").lower(), it.get("url"), it.get("segment"))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

    
