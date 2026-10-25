from __future__ import annotations

import os
import socket
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from station_persist.validation import stream_id_parts

# Opções aceitas pela libpq que o asyncpg repassaria como parâmetro de sessão,
# derrubando a conexão (ex.: a URL do Neon traz channel_binding=require).
LIBPQ_ONLY_OPTIONS = frozenset({"channel_binding"})


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
    legacy_max_id: str | None = None
    source_id: str | None = None
    recovery_interval_seconds: float = Field(default=10, gt=0)
    recovery_min_idle_ms: int = Field(default=120000, ge=1)
    recovery_batch_size: int = Field(default=100, ge=1, le=1000)
    lease_refresh_seconds: float = Field(default=10, gt=0)
    lease_ttl_seconds: float = Field(default=60, gt=0)
    processing_timeout_seconds: float = Field(default=30, gt=0)
    metrics_interval_seconds: float = Field(default=30, gt=0)

    @field_validator("database_url", mode="before")
    @classmethod
    def drop_libpq_only_options(cls, value: object) -> object:
        raw = value.get_secret_value() if isinstance(value, SecretStr) else value
        if not isinstance(raw, str):
            return value
        parts = urlsplit(raw)
        if not parts.query:
            return raw
        query = parse_qsl(parts.query, keep_blank_values=True)
        kept = [(key, item) for key, item in query if key not in LIBPQ_ONLY_OPTIONS]
        if len(kept) == len(query):
            return raw
        return urlunsplit(parts._replace(query=urlencode(kept)))

    @field_validator("stream", "group", "consumer", "source_id")
    @classmethod
    def validate_storage_identifiers(cls, value: str | None) -> str | None:
        if value is not None:
            if "\x00" in value:
                raise ValueError("Identifiers cannot contain NUL")
            value.encode("utf-8")
        return value

    @field_validator("source_id")
    @classmethod
    def validate_source_id(cls, value: str | None) -> str | None:
        if value is not None and (not value.strip() or "\x00" in value):
            raise ValueError("source_id must be a non-empty stable identity")
        return value

    @field_validator("legacy_max_id", mode="before")
    @classmethod
    def validate_legacy_cutoff(cls, value: str | None) -> str | None:
        if value in (None, ""):
            return None
        stream_id_parts(value)
        return value

    @model_validator(mode="after")
    def validate_limits(self) -> Settings:
        if self.pool_min_size > self.pool_max_size:
            raise ValueError("pool_min_size cannot exceed pool_max_size")
        if self.retry_initial_seconds > self.retry_max_seconds:
            raise ValueError("retry_initial_seconds cannot exceed retry_max_seconds")
        if (
            self.lease_ttl_seconds
            < self.processing_timeout_seconds + 2 * self.lease_refresh_seconds
        ):
            raise ValueError(
                "lease TTL must cover processing timeout and two refreshes"
            )
        if self.recovery_min_idle_ms < self.lease_ttl_seconds * 1000:
            raise ValueError("recovery idle must not be shorter than lease TTL")
        return self
