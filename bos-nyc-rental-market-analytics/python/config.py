"""
File: config.py
Author: Shankar Veludandi
Created: 2025-03-01
Last Updated: 2025-12-08

Description:
    Centralized configuration loader for the rental ETL project.
    Loads secrets from environment variables so credentials never
    live in source code or logs. Designed to work locally and in CI.

Usage:
    from config import settings
    print(settings.get_sqlalchemy_url())
"""
import os, re
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=False)

@dataclass(frozen=True)
class _Settings:
    # Core
    ENV: str = os.getenv("ENV", "dev")  # dev | prod | ci

    # RapidAPI key
    API_KEY: Optional[str] = os.getenv("API_KEY")

    # Database (either DATABASE_URL or parts)
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")

    DB_HOST: Optional[str] = os.getenv("DB_HOST")
    DB_PORT: Optional[str] = os.getenv("DB_PORT")
    DB_NAME: Optional[str] = os.getenv("DB_NAME")
    DB_USER: Optional[str] = os.getenv("DB_USER")
    DB_PASSWORD: Optional[str] = os.getenv("DB_PASSWORD")

    def get_sqlalchemy_url(self) -> str:
        """Return a SQLAlchemy-ready PostgreSQL URL."""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        if all([self.DB_HOST, self.DB_PORT, self.DB_NAME, self.DB_USER, self.DB_PASSWORD]):
            return (
                f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}"
                f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            )
        raise RuntimeError("Database configuration is missing. Set DATABASE_URL or DB_* parts.")

    def get_psycopg2_kwargs(self) -> dict:
        """Return kwargs suitable for psycopg2.connect(**kwargs)."""
        if all([self.DB_HOST, self.DB_PORT, self.DB_NAME, self.DB_USER, self.DB_PASSWORD]):
            return {
                "host": self.DB_HOST,
                "port": int(self.DB_PORT),
                "database": self.DB_NAME,
                "user": self.DB_USER,
                "password": self.DB_PASSWORD,
            }
        if self.DATABASE_URL and self.DATABASE_URL.startswith("postgresql"):
            import re as _re
            m = _re.match(
                r"postgresql\+psycopg2://([^:]+):([^@]+)@([^:]+):(\d+)/(\S+)",
                self.DATABASE_URL
            )
            if not m:
                raise RuntimeError("DATABASE_URL is set but not in expected format for kwargs parsing.")
            user, pwd, host, port, db = m.groups()
            return {
                "host": host,
                "port": int(port),
                "database": db,
                "user": user,
                "password": pwd,
            }
        raise RuntimeError("psycopg2 configuration is missing. Set DB_* vars or a parseable DATABASE_URL.")


settings = _Settings()
