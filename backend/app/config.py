from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AliasChoices
from typing import Optional
import os


class Settings(BaseSettings):
    """Application configuration using Pydantic v2 settings.

    - Parses comma-separated CORS origins into a list
    - Reads environment from APP_ENV or ENVIRONMENT
    - Ignores unknown env keys
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Bayan WebApp API"
    environment: str = Field(default="dev", validation_alias=AliasChoices("APP_ENV", "ENVIRONMENT"))

    # CORS (comma-separated string)
    cors_origins: str = Field(default="http://localhost:3000,http://127.0.0.1:3000")

    # Secrets
    secret_key: str = Field(default="BayanSecretKey")

    # Local analytical store (DuckDB)
    duckdb_path: str = Field(default=".data/local.duckdb")

    # Metadata store (SQLite for dev)
    metadata_db_path: str = Field(default=".data/meta.sqlite")

    # Postgres / Supabase DSNs
    postgres_dsn: Optional[str] = None
    supabase_postgres_dsn: Optional[str] = None

    # Frontend base URL (Next.js) used for server-side snapshots
    frontend_base_url: str = Field(default="http://localhost:3000")
    # Actor id used when calling embed route to bypass dashboard permission in dev
    snapshot_actor_id: str = Field(default="dev_user")
    # Backend base URL for internal API calls (e.g., snapshot service)
    backend_base_url: str = Field(default="http://localhost:8000/api")

    # App version and Updates
    app_version: str = Field(default="0.0.0", validation_alias=AliasChoices("APP_VERSION"))
    updates_enabled: bool = Field(default=False, validation_alias=AliasChoices("UPDATES_ENABLED"))
    update_repo_owner: Optional[str] = None
    update_repo_name: Optional[str] = None
    update_channel: str = Field(default="stable")
    github_token: Optional[str] = Field(default=None, validation_alias=AliasChoices("GITHUB_TOKEN"))
    update_manifest_name: str = Field(default="bayan-manifest.json")
    # Optional seed for frontend current version when file is missing
    frontend__env: Optional[str] = Field(default=None, validation_alias=AliasChoices("FRONTEND_VERSION"))

    # Admin bootstrap on startup
    admin_email: Optional[str] = Field(default=None, validation_alias=AliasChoices("ADMIN_EMAIL"))
    admin_password: Optional[str] = Field(default=None, validation_alias=AliasChoices("ADMIN_PASSWORD"))
    admin_name: Optional[str] = Field(default=None, validation_alias=AliasChoices("ADMIN_NAME"))

    # Prefer routing queries to local DuckDB when available
    prefer_local_duckdb: bool = Field(default=False, validation_alias=AliasChoices("PREFER_LOCAL_DUCKDB"))

    # SQLGlot feature flag (experimental dual-mode SQL generation)
    enable_sqlglot: bool = Field(
        default=False,
        validation_alias=AliasChoices("ENABLE_SQLGLOT"),
        description="Enable SQLGlot SQL generation (runs side-by-side with legacy)"
    )
    sqlglot_users: str = Field(
        default="",
        validation_alias=AliasChoices("SQLGLOT_USERS"),
        description="Comma-separated user IDs for SQLGlot (empty=none, *=all)"
    )
    enable_legacy_fallback: bool = Field(
        default=True,
        validation_alias=AliasChoices("ENABLE_LEGACY_FALLBACK"),
        description="Allow fallback to legacy SQL builder when SQLGlot fails (disable for 100% SQLGlot)"
    )

    pivot_join_debug: bool = Field(
        default=False,
        validation_alias=AliasChoices("PIVOT_JOIN_DEBUG"),
        description="Enable detailed pivot join diagnostics (debug only)"
    )

    @property
    def cors_origins_list(self) -> list[str]:
        return [x.strip() for x in str(self.cors_origins).split(",") if x.strip()]


settings = Settings()
