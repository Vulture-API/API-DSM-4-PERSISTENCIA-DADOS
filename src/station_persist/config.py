from __future__ import annotations

import os
import socket

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PERSIST_", env_file=".env", extra="ignore"
    )

    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")
    database_url: SecretStr
    stream: str = Field(default="telemetry:ingest", min_length=1)
    group: str = Field(default="postgres-persister", min_length=1)
    consumer: str = Field(
        default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}", min_length=1
    )
    batch_size: int = Field(default=500, ge=1)
    block_ms: int = Field(default=1000, ge=1)
    socket_timeout_seconds: float = Field(default=5, gt=0)
    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=4, ge=1)
    retry_initial_seconds: float = Field(default=0.25, gt=0)
    retry_max_seconds: float = Field(default=30, gt=0)
    shutdown_timeout_seconds: float = Field(default=10, gt=0)
    log_level: str = "INFO"

    @model_validator(mode="after")
    def validate_limits(self) -> Settings:
        if self.pool_min_size > self.pool_max_size:
            raise ValueError("pool_min_size cannot exceed pool_max_size")
        if self.retry_initial_seconds > self.retry_max_seconds:
            raise ValueError("retry_initial_seconds cannot exceed retry_max_seconds")
        return self
