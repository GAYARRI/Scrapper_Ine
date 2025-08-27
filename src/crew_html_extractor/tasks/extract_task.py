from crewai import Task
from ..agents.html_extractor import make_html_extractor_agent

def make_task(url: str, renderizado: bool = False, verbose: bool = False) -> Task:
    agent = make_html_extractor_agent(verbose=verbose)
    instr = (
        f"Obtén el HTML completo de la URL: {url}.\n"
        + ("Usa la herramienta 'fetch_rendered_html'."
           if renderizado else
           "Usa la herramienta 'fetch_html'. Devuelve SOLO el HTML como texto bruto.")
    )
    return Task(
        description=instr,
        agent=agent,
        expected_output="Devuelve únicamente el HTML como texto bruto, sin comentarios ni formato adicional."
    )
