# crew-html-extractor

Agente CrewAI con herramientas para extraer **todo el HTML** de una página:
- `fetch_html`: usa `requests` para HTML estático.
- `fetch_rendered_html`: usa **Playwright** para HTML renderizado por JavaScript.

## Requisitos

- Python >= 3.10
- Una clave para algún LLM (OpenAI por defecto) o configura otro proveedor siguiendo la guía oficial de CrewAI.
- (Opcional) Playwright para páginas dinámicas.

## Instalación

```bash
# Crear y activar venv (recomendado)
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Instalar el paquete en modo editable
pip install -e .
# Si vas a usar Playwright:
pip install ".[playwright]"
playwright install chromium
```

## Configuración (.env)

Crea un fichero `.env` en la raíz (puedes copiar `.env.example`):

```env
# LLM por defecto (OpenAI); cambia según tu proveedor
OPENAI_API_KEY=sk-...
OPENAI_MODEL_NAME=gpt-4o-mini

# Ajustes del extractor (opcional)
CHE_HTTP_TIMEOUT_S=30
CHE_VERIFY_TLS=true
CHE_RENDERED_WAIT_MS=1500
CHE_OUTPUT_DIR=outputs
```

> Para otros proveedores (Azure OpenAI, Gemini, Ollama, etc.), sigue la documentación de CrewAI y LiteLLM y define las variables de entorno correspondientes.

## Uso rápido (CLI)

```bash
crew-html-extractor https://example.org --out outputs/example.html
# Renderizado con Playwright
crew-html-extractor https://example.org --render --gzip
```

## Uso programático

```python
from crew_html_extractor.tasks.extract_task import make_task
from crewai import Crew

task = make_task("https://example.org", renderizado=False, verbose=True)
crew = Crew(agents=[task.agent], tasks=[task])
result = crew.kickoff()
html = str(result)
```

## Tests

```bash
pip install pytest
pytest -q
```

## Notas legales

Respeta siempre `robots.txt` y los Términos de Uso del sitio. No uses este proyecto para eludir restricciones de acceso.
