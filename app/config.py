from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payment:payment@localhost:5432/payment"
    rabbitmq_url: str = "amqp://payment:payment@localhost:5672/"
    api_key: SecretStr = SecretStr("local-development-key")
    outbox_poll_interval: float = Field(default=1, gt=0)
    outbox_batch_size: int = Field(default=50, ge=1, le=1000)
    retry_base_seconds: float = Field(default=3, gt=0)
    gateway_min_seconds: float = Field(default=2, ge=0)
    gateway_max_seconds: float = Field(default=5, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    webhook_timeout: float = Field(default=10, gt=0)

    @model_validator(mode="after")
    def validate_settings(self) -> "Settings":
        if self.gateway_max_seconds < self.gateway_min_seconds:
            raise ValueError("gateway_max_seconds must be >= gateway_min_seconds")
        if not self.api_key.get_secret_value().strip():
            raise ValueError("api_key must not be empty")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
