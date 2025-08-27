from typing import Type
from pydantic import BaseModel, Field
from crewai.tools import BaseTool
from ..settings import settings

class FetchRenderedHTMLInput(BaseModel):
    url: str = Field(..., description="URL completa (http/https) a recuperar con navegador.")

class FetchRenderedHTMLTool(BaseTool):
    """Abre la URL con Playwright (Chromium headless) y devuelve el HTML renderizado."""
    name: str = "fetch_rendered_html"
    description: str = "Devuelve el HTML renderizado por JavaScript usando Playwright."
    args_schema: Type[BaseModel] = FetchRenderedHTMLInput

    def __init__(self, *args, **kwargs):
        # Permite result_as_answer=True si se pasa desde el agente
        super().__init__(*args, **kwargs)

    def _run(self, url: str) -> str:
        # Import diferido para no requerir playwright si no se usa
        from playwright.sync_api import sync_playwright
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("La URL debe empezar por http:// o https://")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=settings.user_agent)
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=settings.http_timeout_s * 1000)
            page.wait_for_timeout(settings.rendered_wait_ms)
            html = page.content()
            ctx.close()
            browser.close()
            return html
