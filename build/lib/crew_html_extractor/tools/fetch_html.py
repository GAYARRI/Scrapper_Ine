from typing import Type
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pydantic import BaseModel, Field
from crewai.tools import BaseTool
from ..settings import settings

class FetchHTMLInput(BaseModel):
    url: str = Field(..., description="URL completa (http/https) a recuperar.")

class FetchHTMLTool(BaseTool):
    """Descarga el HTML bruto de una URL usando HTTP GET."""
    name: str = "fetch_html"
    description: str = "Descarga el HTML bruto de una URL. Devuelve el HTML como texto."
    args_schema: Type[BaseModel] = FetchHTMLInput

    def __init__(self, *args, **kwargs):
        # Permite result_as_answer=True si se pasa desde el agente
        super().__init__(*args, **kwargs)

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((requests.RequestException,))
    )
    def _run(self, url: str) -> str:
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("La URL debe empezar por http:// o https://")
        headers = {
            "User-Agent": settings.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        }
        resp = requests.get(url, headers=headers, timeout=settings.http_timeout_s, verify=settings.verify_tls)
        resp.raise_for_status()
        return resp.text
