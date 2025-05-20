from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    SUPABASE_URL: str
    SUPABASE_KEY: str
    SUPABASE_SERVICE_KEY: str = ""  # Service role key with higher privileges

    model_config = SettingsConfigDict(env_file=".env", extra='ignore')

settings = Settings()