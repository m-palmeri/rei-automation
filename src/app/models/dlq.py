from datetime import datetime
from typing import Optional

from .base import PostgresBase


class DLQ(PostgresBase):
    """Handles interactions with the Dead Letter Queue (dlq) table."""

    DDL = """
    CREATE TABLE IF NOT EXISTS dlq (
      id BIGSERIAL PRIMARY KEY,
      page_id TEXT NOT NULL,
      edit_ts TIMESTAMPTZ NOT NULL,
      error TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    def __init__(self) -> None:
        super().__init__()
        self.init_table()

    def init_table(self) -> None:
        """Initialize the dlq table."""
        self.execute_ddl([self.DDL])

    def put(self, page_id: str, edit_ts_iso: str, error: str) -> None:
        """Add an error entry to the DLQ."""
        from .page_state import PageState  # Import here to avoid circular dependency

        edit_dt = PageState._parse_iso_z(edit_ts_iso)
        with self.cursor() as cur:
            cur.execute(
                "INSERT INTO dlq (page_id, edit_ts, error) VALUES (%s, %s, %s)",
                (page_id, edit_dt, error[:8000]),  # Truncate very long error messages
            )

    def get_errors(
        self, page_id: Optional[str] = None, limit: int = 100
    ) -> list[tuple[str, datetime, str]]:
        """Get recent errors, optionally filtered by page_id."""
        query = "SELECT page_id, edit_ts, error FROM dlq"
        params: tuple = ()
        if page_id:
            query += " WHERE page_id = %s"
            params = (page_id,)
        query += " ORDER BY created_at DESC LIMIT %s"
        params = params + (limit,)

        with self.cursor(autocommit=False) as cur:
            cur.execute(query, params)
            return cur.fetchall()
