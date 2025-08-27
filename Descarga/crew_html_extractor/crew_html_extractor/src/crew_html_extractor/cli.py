import argparse, os, gzip, sys
from crewai import Crew
from .tasks.extract_task import make_task
from .settings import settings
import pathlib

def _write_output(html: str, out_path: str, use_gzip: bool) -> str:
    path = pathlib.Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if use_gzip:
        if not str(path).endswith(".gz"):
            path = path.with_suffix(path.suffix + ".gz")
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(html)
    else:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
    return str(path)

def main():
    p = argparse.ArgumentParser(description="Extraer HTML con CrewAI.")
    p.add_argument("url", help="URL a extraer (http/https)")
    p.add_argument("--render", action="store_true", help="Usar Playwright (HTML renderizado)")
    p.add_argument("--out", type=str, help="Ruta de salida (por defecto en outputs/ page.html o page.html.gz)")
    p.add_argument("--gzip", action="store_true", help="Guardar comprimido .gz")
    p.add_argument("--verbose", action="store_true", help="Modo verbose para el agente")
    args = p.parse_args()

    task = make_task(args.url, renderizado=args.render, verbose=args.verbose)
    crew = Crew(agents=[task.agent], tasks=[task])
    result = crew.kickoff()

    # result puede ser objeto; forzamos a str
    html = str(result)

    default_name = "page.html"
    if args.gzip and not default_name.endswith(".gz"):
        default_name += ".gz"
    out_path = args.out or os.path.join(settings.output_dir, default_name)

    final_path = _write_output(html, out_path, args.gzip)
    print(final_path)

if __name__ == "__main__":
    main()
