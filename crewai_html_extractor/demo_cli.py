# crewai_html_extractor/demo_cli.py
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from crewai_html_extractor.scraper.orchestrator import Orchestrator


def _ensure_outdir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _dump_csv_tables(items: List[Dict[str, Any]], outdir: Path) -> int:
    """Guarda cada item type='table' a CSV. Devuelve el número guardado."""
    n = 0
    for idx, it in enumerate(items):
        if it.get("type") != "table":
            continue
        data = it.get("data")
        schema = it.get("schema") or []
        if not isinstance(data, list) or not data:
            continue
        try:
            df = pd.DataFrame(data, columns=schema if schema else None)
        except Exception:
            df = pd.DataFrame(data)
        df.to_csv(outdir / f"dataitem_{idx:03d}.csv", index=False)
        n += 1
    return n


def _export_entities(items: List[Dict[str, Any]], outdir: Path) -> int:
    entities = [it for it in items if it.get("type") == "entity"]
    if not entities:
        return 0

    # normaliza same_as
    for e in entities:
        if isinstance(e.get("same_as"), list):
            e["same_as"] = ";".join(map(str, e["same_as"]))
        for k in ("segment","subtype","segment_source","segment_score"):
            e.setdefault(k, None)

    df = pd.DataFrame(entities)
    df.to_csv(outdir / "entities.csv", index=False)

    # 👉 Nuevos exports por segmento
    if "segment" in df.columns:
        segs = {
            "accommodation": "accommodations.csv",
            "experience": "experiences.csv",
            "business": "businesses.csv",
        }
        for seg, fname in segs.items():
            sdf = df[df["segment"] == seg]
            if not sdf.empty:
                sdf.to_csv(outdir / fname, index=False)
    return len(df)

def _export_long(items: List[Dict[str, Any]], source_url: str, outdir: Path, to_parquet: bool = False) -> Optional[Path]:
    """Convierte tablas a formato largo y exporta long.csv (+ opcional long.parquet)."""
    long_rows: List[pd.DataFrame] = []
    for it in items:
        if it.get("type") != "table":
            continue
        data = it.get("data")
        schema = it.get("schema") or []
        if not isinstance(data, list) or not data:
            continue
        try:
            df = pd.DataFrame(data, columns=schema if schema else None)
        except Exception:
            df = pd.DataFrame(data)
        if df.empty:
            continue

        # Heurística simple: primera columna como id_vars (dimensión/periodo)
        id_col = df.columns[0]
        try:
            long = df.melt(id_vars=[id_col], var_name="variable", value_name="valor")
        except Exception:
            # Si falla (p. ej., 1 columna), conserva tal cual
            long = df.copy()
            long["variable"] = None
            long["valor"] = None

        # Añade metadatos si existen en el item
        if "period" in it and it.get("period"):
            long["periodo"] = it["period"]
        long["fuente"] = source_url
        long_rows.append(long)

    if not long_rows:
        return None

    long_df = pd.concat(long_rows, ignore_index=True)
    csv_path = outdir / "long.csv"
    long_df.to_csv(csv_path, index=False)

    if to_parquet:
        try:
            pq_path = outdir / "long.parquet"
            long_df.to_parquet(pq_path, index=False)
        except Exception as e:
            logging.getLogger("crewai.cli").warning(f"[cli] No se pudo exportar Parquet: {e}")

    return csv_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Demo CLI para crewai-html-extractor")
    ap.add_argument("--url", required=True, help="URL a extraer")
    ap.add_argument("--outdir", default="outputs", help="Directorio de salida (por defecto: outputs)")
    ap.add_argument("--enable-network", action="store_true", help="Capturar respuestas de red (si hay extractor de red)")
    ap.add_argument("--export-long", action="store_true", help="Exportar tablas en formato largo (long.csv)")
    ap.add_argument("--parquet", action="store_true", help="Si --export-long, exportar también long.parquet (requiere pyarrow)")
    ap.add_argument("--log-level", default="WARNING", choices=["CRITICAL","ERROR","WARNING","INFO","DEBUG"], help="Nivel de logging")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))

    outdir = _ensure_outdir(args.outdir)

    orch = Orchestrator()
    record = orch.run_once(args.url, enable_network=args.enable_network)

    # record.json
    record_path = outdir / "record.json"
    with open(record_path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    # CSV por tabla
    saved_tables = _dump_csv_tables(record.get("data_items", []), outdir)

    # Entities
    saved_entities = _export_entities(record.get("data_items", []), outdir)

    # Long/Parquet
    long_path = None
    if args.export_long:
        long_path = _export_long(record.get("data_items", []), record["url"], outdir, to_parquet=args.parquet)

    # Resumen
    print(f"[OK] URL: {record['url']}")
    print(f"[OK] Guardado: {record_path}")
    print(f"[OK] Tablas CSV guardadas: {saved_tables}")
    print(f"[OK] Entidades exportadas: {saved_entities}")
    if long_path:
        print(f"[OK] Largo: {long_path}")
        if args.parquet:
            print(f"[OK] Parquet: {outdir / 'long.parquet'}")


if __name__ == "__main__":
    main()


