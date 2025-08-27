import os
from crewai import Agent
# LLM puede no estar disponible en algunas versiones; hacemos import opcional
try:
    from crewai import LLM  # type: ignore
except Exception:
    LLM = None  # fallback

from ..tools.fetch_html import FetchHTMLTool
from ..tools.fetch_rendered import FetchRenderedHTMLTool

def _maybe_make_llm():
    """Crea un LLM con temperatura baja si la clase está disponible; en caso contrario devuelve None para usar el default."""
    if LLM is None:
        return None
    model = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-mini")
    try:
        return LLM(model=model, temperature=0.1)
    except Exception:
        return None

def make_html_extractor_agent(verbose: bool = False) -> Agent:
    llm = _maybe_make_llm()
    return Agent(
        role="Extractor de HTML",
        goal="Dada una URL válida, usar la herramienta adecuada y devolver EXACTAMENTE el HTML.",
        backstory=(
            "Especialista en recuperar código fuente HTML para análisis. "
            "No alteras ni resumes el contenido."
        ),
        verbose=verbose,
        allow_delegation=False,
        tools=[
            FetchHTMLTool(result_as_answer=True),
            FetchRenderedHTMLTool(result_as_answer=True),
        ],
        **({} if llm is None else {"llm": llm})
    )
