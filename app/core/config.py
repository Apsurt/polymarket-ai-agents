from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DATABASE_URL: str
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    NEWS_API_KEY: str = "your_news_api_key"
    # Add other settings from your .env file

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
