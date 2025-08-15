from datetime import datetime, timezone
from typing import Optional

from .base import PostgresBase


class PageState(PostgresBase):
    """Handles interactions with the page_state table."""

    DDL = """
    CREATE TABLE IF NOT EXISTS page_state (
      page_id TEXT PRIMARY KEY,
      last_processed_edit TIMESTAMPTZ,
      last_seen_edit TIMESTAMPTZ,
      drive_folder_id TEXT,
      drive_link TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    ALTERS = [
        "ALTER TABLE page_state ADD COLUMN IF NOT EXISTS drive_folder_id TEXT;",
        "ALTER TABLE page_state ADD COLUMN IF NOT EXISTS drive_link TEXT;",
    ]

    def __init__(self) -> None:
        super().__init__()
        self.init_table()

    def init_table(self) -> None:
        """Initialize the page_state table."""
        self.execute_ddl([self.DDL] + self.ALTERS)

    def get_drive_info(self, page_id: str) -> tuple[Optional[str], Optional[str]]:
        """Get drive folder ID and link for a page."""
        with self.cursor(autocommit=False) as cur:
            cur.execute(
                "SELECT drive_folder_id, drive_link FROM page_state WHERE page_id=%s", (page_id,)
            )
            row = cur.fetchone()
            return (row[0], row[1]) if row else (None, None)

    def set_drive_info(self, page_id: str, folder_id: str, link: str) -> None:
        """Set drive folder ID and link for a page."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO page_state (page_id, drive_folder_id, drive_link)
                VALUES (%s, %s, %s)
                ON CONFLICT (page_id)
                DO UPDATE SET drive_folder_id = EXCLUDED.drive_folder_id,
                              drive_link = EXCLUDED.drive_link,
                              updated_at = NOW()
                """,
                (page_id, folder_id, link),
            )

    def already_processed(self, page_id: str, edit_ts_iso: str) -> bool:
        """Return True if edit_ts <= last_processed_edit; also update last_seen_edit."""
        edit_dt = self._parse_iso_z(edit_ts_iso)
        with self.cursor() as cur:
            cur.execute(
                """
                WITH upsert AS (
                    INSERT INTO page_state (page_id, last_seen_edit)
                    VALUES (%s, %s)
                    ON CONFLICT (page_id)
                    DO UPDATE SET last_seen_edit = GREATEST(page_state.last_seen_edit,
                                                          EXCLUDED.last_seen_edit),
                                updated_at = NOW()
                    RETURNING last_processed_edit
                )
                SELECT last_processed_edit FROM upsert;
                """,
                (page_id, edit_dt),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return False
            return row[0] >= edit_dt

    def mark_processed(self, page_id: str, edit_ts_iso: str) -> None:
        """Mark a page as processed at a specific edit timestamp."""
        edit_dt = self._parse_iso_z(edit_ts_iso)
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO page_state (page_id, last_processed_edit, last_seen_edit)
                VALUES (%s, %s, %s)
                ON CONFLICT (page_id)
                DO UPDATE SET last_processed_edit = GREATEST(page_state.last_processed_edit,
                                                         EXCLUDED.last_processed_edit),
                              last_seen_edit = GREATEST(page_state.last_seen_edit,
                                                    EXCLUDED.last_seen_edit),
                              updated_at = NOW()
                """,
                (page_id, edit_dt, edit_dt),
            )

    @staticmethod
    def _parse_iso_z(ts: str) -> datetime:
        """Parse ISO8601 timestamp with 'Z' timezone indicator."""
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
