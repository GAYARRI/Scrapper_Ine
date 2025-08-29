# crewai_html_extractor/scraper/orchestrator.py

from __future__ import annotations

import logging
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .core import Core
from .extractors import tourism
from .extractors import html_tables
from .extractors import ine as ine_extractor

# Extractor de red (opcional)
try:
    from .extractors import network as network_extractor  # type: ignore
    HAS_NETWORK = True
except Exception:
    network_extractor = None  # type: ignore
    HAS_NETWORK = False


def _guess_seg_from_url(u: str) -> tuple[Optional[str], Optional[str]]:
    s = (u or "").lower()
    if any(k in s for k in ("restauracion", "restauración", "restaurantes", "bares", "gastronomia", "donde-comer", "dondecomer")):
        return "business", "restaurante"
    if any(k in s for k in ("agenda", "event", "evento", "experienc", "activid", "que-ver", "quever", "ruta", "tour")):
        return "experience", None
    if any(k in s for k in ("aloj", "hotel", "hostal", "apart", "camping")):
        return "accommodation", None
    return None, None


class Orchestrator:
    def __init__(self, core: Optional[Core] = None, logger: Optional[logging.Logger] = None, **kwargs) -> None:
        self.core = core or Core()
        self.log = logger or logging.getLogger("crewai.orchestrator")

    def _pause(self, lo: float = 0.4, hi: float = 0.8) -> None:
        time.sleep(lo + random.uniform(0, hi - lo))

    def run_once(self, url: str, enable_network: bool = False) -> Dict[str, Any]:
        method_chain: List[str] = ["fetch"]
        items: List[Dict[str, Any]] = []

        # --- FETCH robusto: inicializa final_url y captura errores ---
        final_url = url
        try:
            final_url, html = self.core.fetch(url)
        except Exception as e:
            # Devuelve un record “vacío” con el error de fetch
            return {
                "url": final_url,
                "extracted_at": datetime.utcnow().isoformat(),
                "data_items": [],
                "meta": {
                    "method_chain": method_chain,
                    "count": 0,
                    "source_url": final_url,
                    "error": f"fetch_failed: {e}",
                },
            }

        # Pista de segmento por URL (ya tenemos final_url seguro)
        exp_seg, exp_sub = _guess_seg_from_url(final_url)

        # Turismo (JSON-LD / microdatos)
        try:
            t_jsonld = tourism.extract_tourism_entities(html, final_url)
            if t_jsonld:
                items.extend(t_jsonld)
            method_chain.append("tourism-jsonld")
        except Exception as e:
            self.log.debug(f"[orchestrator] Tourism JSON-LD failed: {e}")
        self._pause()

        # Turismo (listados genéricos con pista por URL)
        try:
            t_list = tourism.extract_portal_listings_generic(
                html, final_url, expected_segment=exp_seg, subtype_hint=exp_sub
            )
            if t_list:
                items.extend(t_list)
            method_chain.append("tourism-listings")
        except Exception as e:
            self.log.debug(f"[orchestrator] Tourism listings failed: {e}")
        self._pause()

        # INE
        try:
            ine_items = ine_extractor.extract_ine_tables(html, final_url)
            if ine_items:
                items.extend(ine_items)
            method_chain.append("ine-html")
        except Exception as e:
            self.log.debug(f"[orchestrator] INE extractor failed: {e}")
        self._pause()

        # Tablas HTML genéricas
        try:
            generic_items = html_tables.extract_html_tables(html, final_url)
            if generic_items:
                items.extend(generic_items)
            method_chain.append("html-tables")
        except Exception as e:
            self.log.debug(f"[orchestrator] HTML tables extractor failed: {e}")
        self._pause()

        # (Opcional) Red
        if enable_network and HAS_NETWORK:
            try:
                net_items = network_extractor.extract_network(final_url)  # type: ignore[attr-defined]
                if net_items:
                    items.extend(net_items)
                method_chain.append("network")
            except Exception as e:
                self.log.debug(f"[orchestrator] Network extractor failed: {e}")
        elif enable_network and not HAS_NETWORK:
            self.log.info(
                "[orchestrator] enable_network=True, pero el extractor de red no está disponible "
                "(instala 'playwright' y ejecuta 'playwright install')."
            )

        # Record de salida
        return {
            "url": final_url,
            "extracted_at": datetime.utcnow().isoformat(),
            "data_items": items,
            "meta": {
                "method_chain": method_chain,
                "count": len(items),
                "source_url": final_url,
            },
        }

