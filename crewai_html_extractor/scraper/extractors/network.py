# crewai_html_extractor/scraper/extractors/network.py
from __future__ import annotations

from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
import json
import csv

from playwright.sync_api import sync_playwright


# -----------------------------
# Helpers de DataItem
# -----------------------------
def _make_table(label: str, headers: List[str], rows: List[List[Any]], url: str, method: str, confidence: float = 0.92) -> Dict[str, Any]:
    return {
        "type": "table",
        "label": label,
        "schema": [str(h) for h in headers],
        "data": rows,
        "unit": None,
        "source": {"method": method, "url": url},
        "confidence": confidence,
    }

def _make_series(label: str, rows: List[Tuple[Any, Any, Optional[str]]], url: str, method: str, confidence: float = 0.9) -> Dict[str, Any]:
    # rows: list of (x, y, serie_name)
    headers = ["x", "y", "series"]
    data = [[x, y, sname] for (x, y, sname) in rows]
    return _make_table(f"{label} (series)", headers, data, url, method, confidence)


# -----------------------------
# Parseadores genéricos
# -----------------------------
def _parse_json_body(body: str, src_url: str) -> List[Dict[str, Any]]:
    """Intenta convertir un JSON (objeto/array) en tablas o series estandarizadas."""
    out: List[Dict[str, Any]] = []
    try:
        obj = json.loads(body)
    except Exception:
        return out

    # Caso 1: array de objetos homogéneos -> tabla
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        headers = list({k for row in obj for k in row.keys()})
        rows = [[row.get(h) for h in headers] for row in obj]
        out.append(_make_table("network_json_table", headers, rows, src_url, "network-json", 0.95))
        return out

    # Caso 2: Highcharts-like { series: [ {name, data:[(x,y)|y,...]} ] }
    if isinstance(obj, dict):
        series = obj.get("series") or obj.get("data") or obj.get("datasets")
        # Highcharts
        if isinstance(series, list) and series and isinstance(series[0], dict) and ("data" in series[0] or "values" in series[0]):
            rows: List[Tuple[Any, Any, Optional[str]]] = []
            for s in series:
                name = s.get("name") or s.get("label")
                data = s.get("data") or s.get("values")
                if isinstance(data, list):
                    for dp in data:
                        if isinstance(dp, (list, tuple)) and len(dp) >= 2:
                            x, y = dp[0], dp[1]
                        elif isinstance(dp, dict) and ("x" in dp and "y" in dp):
                            x, y = dp.get("x"), dp.get("y")
                        else:
                            # si es un solo valor, el x es el índice
                            x, y = None, dp
                        rows.append((x, y, name))
            if rows:
                out.append(_make_series("network_json_series", rows, src_url, "network-json", 0.95))
                return out

        # Chart.js datasets (chart.data.labels + chart.data.datasets[].data)
        labels = obj.get("labels")
        datasets = obj.get("datasets") or obj.get("dataSets") or obj.get("data_sets")
        if isinstance(labels, list) and isinstance(datasets, list):
            rows: List[Tuple[Any, Any, Optional[str]]] = []
            for ds in datasets:
                name = ds.get("label") or ds.get("name")
                data = ds.get("data")
                if isinstance(data, list):
                    for i, y in enumerate(data):
                        x = labels[i] if i < len(labels) else i
                        rows.append((x, y, name))
            if rows:
                out.append(_make_series("network_chartjs_series", rows, src_url, "network-json", 0.93))
                return out

    # Caso 3: Si nada encaja, serializamos plano (útil para inspección)
    out.append(
        _make_table(
            "network_json_raw",
            ["json"],
            [[obj]],
            src_url,
            "network-json-raw",
            0.6,
        )
    )
    return out


def _parse_csv_body(body: str, src_url: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        # csv.Sniffer puede fallar con CSV raros; probamos rápido
        f = StringIO(body)
        reader = csv.reader(f)
        rows = list(reader)
        if not rows:
            return out
        headers = [str(h) for h in rows[0]]
        data = rows[1:]
        out.append(_make_table("network_csv_table", headers, data, src_url, "network-csv", 0.95))
    except Exception:
        pass
    return out


def _looks_like_data_content_type(ct: str) -> bool:
    ct = (ct or "").lower()
    # además de json/csv, muchos sitios sirven JSON como text/plain o incluso javascript/html
    return (
        "json" in ct
        or "csv" in ct
        or "text/plain" in ct
        or "application/javascript" in ct
        or "text/javascript" in ct
        or "application/octet-stream" in ct
        or "text/html" in ct  # ⬅️ lo probamos; filtraremos por cuerpo después
    )


def _body_to_items(body: str, url: str, content_type: str) -> List[Dict[str, Any]]:
    ct = (content_type or "").lower()
    # 1) JSON claro
    if "json" in ct or url.lower().endswith(".json"):
        return _parse_json_body(body, url)
    # 2) CSV claro
    if "csv" in ct or url.lower().endswith(".csv"):
        return _parse_csv_body(body, url)
    # 3) Heurística: ¿parece JSON aunque no lo digan?
    text = body.strip()
    if text.startswith("{") or text.startswith("["):
        items = _parse_json_body(text, url)
        if items:
            return items
    # 4) Intento de CSV ligero si detecta separadores
    if ("," in body and "\n" in body) or (";" in body and "\n" in body):
        csv_items = _parse_csv_body(body, url)
        if csv_items:
            return csv_items
    return []
def body_to_items(body: str, url: str, content_type: str) -> list[dict]:
    """Wrapper público para reutilizar el parser de JSON/CSV sin Playwright."""
    return _body_to_items(body, url, content_type)
# -----------------------------
# Hooks de librerías en la página
# -----------------------------
def _extract_charts_via_dom(page) -> List[Dict[str, Any]]:
    """Intenta leer estructuras en vivo de Highcharts, Chart.js y ECharts desde el DOM."""
    items: List[Dict[str, Any]] = []

    # Highcharts
    try:
        high_opts = page.evaluate("""
() => {
  if (!(window.Highcharts && Highcharts.charts)) return [];
  return Highcharts.charts
    .filter(Boolean)
    .map(c => c && c.options ? c.options : null)
    .filter(Boolean);
}""")
        if isinstance(high_opts, list) and high_opts:
            for opt in high_opts:
                # Reutilizamos el parser de JSON (usa misma estructura)
                items.extend(_parse_json_body(json.dumps(opt), "dom://highcharts"))
    except Exception:
        pass

    # Chart.js (v3+ mantiene un registry)
    try:
        chartjs_data = page.evaluate("""
() => {
  const out = [];
  const win = window;
  // Chart.js v3+: Chart.getChart(canvas) o registry._plugins ... pero intentamos localizar instancias:
  const gs = win.Chart && win.Chart.instances 
      ? Array.from(win.Chart.instances) 
      : [];
  // Algunas versiones guardan en Chart.instances como objeto/Map
  const arr = Array.isArray(gs) ? gs : (gs ? Array.from(gs.values ? gs.values() : Object.values(gs)) : []);
  for (const inst of arr) {
    try {
      const d = inst.data || inst.config?.data;
      if (d) out.push({labels: d.labels || [], datasets: d.datasets || []});
    } catch {}
  }
  return out;
}""")
        if isinstance(chartjs_data, list) and chartjs_data:
            for cfg in chartjs_data:
                items.extend(_parse_json_body(json.dumps(cfg), "dom://chartjs"))
    except Exception:
        pass

    # ECharts
    try:
        echarts_opts = page.evaluate("""
() => {
  const out = [];
  if (!(window.echarts)) return out;
  const root = document;
  const nodes = root.querySelectorAll('[id], .echarts, [data-echarts]');
  const uniq = new Set();
  nodes.forEach(n => {
    try {
      const inst = window.echarts.getInstanceByDom(n);
      if (inst) {
        const opt = inst.getOption();
        out.push(opt);
      }
    } catch {}
  });
  return out;
}""")
        if isinstance(echarts_opts, list) and echarts_opts:
            for opt in echarts_opts:
                items.extend(_parse_json_body(json.dumps(opt), "dom://echarts"))
    except Exception:
        pass

    return items


# -----------------------------
# Punto de entrada público
# -----------------------------
def grab_from_page(url: str, wait_selector: Optional[str] = None, max_body_bytes: int = 1_000_000, headless: bool = True, timeout_ms: int = 30_000) -> List[Dict[str, Any]]:
    """
    Abre la página con Playwright, captura respuestas de red (JSON/CSV/plano) y
    además intenta leer gráficas vivas de Highcharts/Chart.js/ECharts vía DOM.

    Devuelve lista de DataItems (tablas/series).
    """
    items: List[Dict[str, Any]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context()
        page = ctx.new_page()

        try:
            ct = (response.headers.get("content-type") or "").lower()
            url_r = response.url
            if not _looks_like_data_content_type(ct) and not url_r.lower().endswith((".json", ".csv")):
                return
            if response.status >= 400:
                return
            # limita tamaño de cuerpo
            body = response.text()
            if not body:
                return
            if len(body) > max_body_bytes:
                body = body[:max_body_bytes]
            items.extend(_body_to_items(body, url_r, ct))
        except Exception:
            pass

        page.on("response", on_response)

        page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        if wait_selector:
            try:
                page.wait_for_selector(wait_selector, timeout=timeout_ms)
            except Exception:
                pass

        # Hooks DOM para Highcharts/Chart.js/ECharts
        items.extend(_extract_charts_via_dom(page))

        browser.close()

    # Dedup ingenuo por (type,label,schema_len,data_len)
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for it in items:
        key = (it.get("type"), it.get("label"), tuple(it.get("schema", [])), len(it.get("data", [])))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(it)
    return uniq
