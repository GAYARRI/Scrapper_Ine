from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    http_timeout_s: int = 30
    verify_tls: bool = True
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    rendered_wait_ms: int = 1500
    output_dir: str = "outputs"

    model_config = {"env_prefix": "CHE_", "env_file": ".env"}

settings = Settings()
