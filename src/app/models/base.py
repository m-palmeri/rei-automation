import os
from contextlib import contextmanager
from typing import Any, Generator

import psycopg
from loguru import logger


class PostgresBase:
    """Base class for PostgreSQL database interactions."""

    def __init__(self) -> None:
        self.db_url = os.getenv("DATABASE_URL")
        if not self.db_url:
            raise RuntimeError("DATABASE_URL not set")

    @contextmanager
    def cursor(self, autocommit: bool = True) -> Generator[Any, None, None]:
        """Context manager for database cursor with proper connection handling."""
        with psycopg.connect(self.db_url, autocommit=autocommit) as conn, conn.cursor() as cur:  # type: ignore
            yield cur

    def execute_ddl(self, ddl_statements: list[str]) -> None:
        """Execute DDL statements to initialize or alter tables."""
        with self.cursor() as cur:
            for stmt in ddl_statements:
                cur.execute(stmt)
                logger.debug(f"Executed DDL: {stmt}")
