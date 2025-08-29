import pandas as pd
from bs4 import BeautifulSoup

def extract_html_tables(html: str, base_url: str):
    soup = BeautifulSoup(html, "lxml")
    items = []
    for i, tbl in enumerate(soup.select("table")):
        try:
            df = pd.read_html(str(tbl), flavor="lxml")[0]
            items.append({
                "type":"table",
                "label": tbl.get("id") or f"table_{i}",
                "schema": [str(c) for c in df.columns],
                "data": df.where(pd.notnull(df), None).values.tolist(),
                "unit": None,
                "source":{"method":"html","selector":"table","url":base_url},
                "confidence": 0.98
            })
        except Exception:
            continue
    return items
