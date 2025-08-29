# crewai_html_extractor/scraper/orchestrator.py

from __future__ import annotations

import logging
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .core import Core

# Extractores
from .extractors import tourism
from .extractors import html_tables
from .extractors import ine as ine_extractor

# Extractor de red (opcional, p.ej. Playwright)
try:
    from .extractors import network as network_extractor  # type: ignore
    HAS_NETWORK = True
except Exception:
    network_extractor = None  # type: ignore
    HAS_NETWORK = False


class Orchestrator:
    """
    Orquesta el pipeline:
      1) fetch (Core)
      2) extractores: turismo JSON-LD, listados genéricos, INE, tablas HTML,
         y opcionalmente extractor de red.
      3) devuelve un 'record' con data_items y metadatos
    """

    def __init__(
        self,
        core: Optional[Core] = None,
        logger: Optional[logging.Logger] = None,
        **kwargs,  # tolera parámetros extra (p.ej. versiones previas pasaban use_crewai)
    ) -> None:
        self.core = core or Core()
        self.log = logger or logging.getLogger("crewai.orchestrator")

    def _pause(self, lo: float = 0.4, hi: float = 0.8) -> None:
        """Pausa breve entre etapas para evitar ráfagas."""
        time.sleep(lo + random.uniform(0, hi - lo))

    def run_once(self, url: str, enable_network: bool = False) -> Dict[str, Any]:
        """
        Ejecuta una pasada de extracción sobre 'url'.

        :param url: URL a procesar.
        :param enable_network: si True e instalado el extractor de red,
                               intenta capturar respuestas (JSON/CSV).
        :return: dict con: url, extracted_at, data_items, meta
        """
        # 1) FETCH
        final_url, html = self.core.fetch(url)

        items: List[Dict[str, Any]] = []
        method_chain: List[str] = ["fetch"]

        # 2) Turismo (JSON-LD / microdatos)
        try:
            t_jsonld = tourism.extract_tourism_entities(html, final_url)
            if t_jsonld:
                items.extend(t_jsonld)
            method_chain.append("tourism-jsonld")
        except Exception as e:
            self.log.debug(f"[orchestrator] Tourism JSON-LD failed: {e}")
        self._pause()

        # 3) Turismo (listados genéricos en portales)
        try:
            t_list = tourism.extract_portal_listings_generic(html, final_url)
            if t_list:
                items.extend(t_list)
            method_chain.append("tourism-listings")
        except Exception as e:
            self.log.debug(f"[orchestrator] Tourism listings failed: {e}")
        self._pause()

        # 4) INE (tablas con normalización específica)
        try:
            ine_items = ine_extractor.extract_ine_tables(html, final_url)
            if ine_items:
                items.extend(ine_items)
            method_chain.append("ine-html")
        except Exception as e:
            self.log.debug(f"[orchestrator] INE extractor failed: {e}")
        self._pause()

        # 5) HTML genérico (todas las tablas)
        try:
            generic_items = html_tables.extract_html_tables(html, final_url)
            if generic_items:
                items.extend(generic_items)
            method_chain.append("html-tables")
        except Exception as e:
            self.log.debug(f"[orchestrator] HTML tables extractor failed: {e}")
        self._pause()

        # 6) (Opcional) EXTRACTOR DE RED
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

        # 7) RECORD DE SALIDA (la CLI se encarga de persistirlo)
        record: Dict[str, Any] = {
            "url": final_url,
            "extracted_at": datetime.utcnow().isoformat(),
            "data_items": items,
            "meta": {
                "method_chain": method_chain,
                "count": len(items),
                "source_url": final_url,
            },
        }
        return record
