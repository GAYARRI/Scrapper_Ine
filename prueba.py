# demo_cli.py
import json, os
import pandas as pd
from crewai_html_extractor.scraper.orchestrator import Orchestrator

def main():
    url = "https://www.ine.es/"
    orch = Orchestrator(use_crewai=False)
    rec = orch.run_once(url)

    print("URL:", rec.get("url"))
    print("Título:", rec.get("title"))
    print("Meta description:", rec.get("meta_description"))
    print("Tablas detectadas:", len(rec.get("data_items", [])))

    # Guardar JSON completo
    os.makedirs("outputs", exist_ok=True)
    json_path = os.path.join("outputs", "rec.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False, indent=2)
    print("Guardado:", json_path)

    # Guardar tablas a CSV
    tables = [x for x in rec.get("data_items", []) if x.get("type") == "table"]
    for i, t in enumerate(tables):
        try:
            df = pd.DataFrame(t["data"], columns=t["schema"])
            csv_path = os.path.join("outputs", f"table_{i}_{t.get('label','tbl')}.csv".replace('/','_'))
            df.to_csv(csv_path, index=False)
            print("CSV:", csv_path)
        except Exception as e:
            print("No se pudo exportar tabla:", e)

if __name__ == "__main__":
    main()
