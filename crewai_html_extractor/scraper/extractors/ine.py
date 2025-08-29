# crewai_html_extractor/scraper/extractors/ine.py
import re, pandas as pd
from bs4 import BeautifulSoup

def _flatten_columns(cols):
    if isinstance(cols, pd.MultiIndex):
        return [" / ".join([str(x) for x in tup if str(x)!='nan']).strip() for tup in cols.values]
    return [str(c) for c in cols]

def extract_ine_tables(html: str, base_url: str):
    soup = BeautifulSoup(html, "lxml")
    out = []
    for i, tbl in enumerate(soup.select("table")):
        try:
            # intenta header multinivel
            dfs = pd.read_html(str(tbl), header=[0,1])
        except Exception:
            dfs = pd.read_html(str(tbl), header=0)
        for k, df in enumerate(dfs):
            df.columns = _flatten_columns(df.columns)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # normaliza números (coma decimal)
            df = df.replace(r"\.", "", regex=True).replace(",", ".", regex=True)
            df = df.apply(pd.to_numeric, errors="ignore")

            caption = (tbl.find("caption").get_text(" ", strip=True) if tbl.find("caption") else "")
            titulo = soup.title.get_text(strip=True) if soup.title else ""
            meta_txt = " ".join([caption, titulo])

            # periodo (heurística simple)
            m = re.search(r"(20\d{2})(?:[-/](\d{1,2}))?", meta_txt)
            periodo = m.group(0) if m else None

            out.append({
                "type":"table","label": f"ine_table_{i}_{k}",
                "schema": [str(c) for c in df.columns],
                "data": df.where(pd.notnull(df), None).values.tolist(),
                "unit": None,
                "source":{"method":"html-ine","url":base_url},
                "confidence": 0.99,
                "period": periodo
            })
    return out
