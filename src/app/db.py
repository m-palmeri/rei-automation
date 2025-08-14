import os
from datetime import datetime, timezone
from typing import Optional

import psycopg
from loguru import logger

DB_URL = os.getenv("DATABASE_URL")

DDL_PAGE_STATE = """
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

DDL_DLQ = """
CREATE TABLE IF NOT EXISTS dlq (
  id BIGSERIAL PRIMARY KEY,
  page_id TEXT NOT NULL,
  edit_ts TIMESTAMPTZ NOT NULL,
  error TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def init_db() -> None:
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    with psycopg.connect(DB_URL, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(DDL_PAGE_STATE)
        for stmt in ALTERS:
            cur.execute(stmt)
    logger.info("DB initialized (page_state, dlq).")


def get_drive_info(page_id: str) -> tuple[Optional[str], Optional[str]]:
    with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:  # type: ignore
        cur.execute(
            "SELECT drive_folder_id, drive_link FROM page_state WHERE page_id=%s", (page_id,)
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)


def set_drive_info(page_id: str, folder_id: str, link: str) -> None:
    with psycopg.connect(DB_URL, autocommit=True) as conn, conn.cursor() as cur:  # type: ignore
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


def _parse_iso_z(ts: str) -> datetime:
    # Notion gives ISO8601 with 'Z'. Convert to aware datetime.
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def already_processed(page_id: str, edit_ts_iso: str) -> bool:
    """Return True if edit_ts <= last_processed_edit; also update last_seen_edit."""
    edit_dt = _parse_iso_z(edit_ts_iso)
    with psycopg.connect(DB_URL, autocommit=True) as conn, conn.cursor() as cur:  # type: ignore
        cur.execute("SELECT last_processed_edit FROM page_state WHERE page_id=%s", (page_id,))
        row = cur.fetchone()
        processed = bool(row and row[0] and row[0] >= edit_dt)
        # upsert last_seen_edit
        cur.execute(
            """
            INSERT INTO page_state (page_id, last_seen_edit)
            VALUES (%s, %s)
            ON CONFLICT (page_id)
            DO UPDATE SET last_seen_edit = GREATEST(page_state.last_seen_edit,
                                                    EXCLUDED.last_seen_edit),
                          updated_at = NOW()
        """,
            (page_id, edit_dt),
        )
    return processed


def mark_processed(page_id: str, edit_ts_iso: str) -> None:
    edit_dt = _parse_iso_z(edit_ts_iso)
    with psycopg.connect(DB_URL, autocommit=True) as conn, conn.cursor() as cur:  # type: ignore
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


def dlq_put(page_id: str, edit_ts_iso: str, error: str) -> None:
    edit_dt = _parse_iso_z(edit_ts_iso)
    with psycopg.connect(DB_URL, autocommit=True) as conn, conn.cursor() as cur:  # type: ignore
        cur.execute(
            "INSERT INTO dlq (page_id, edit_ts, error) VALUES (%s, %s, %s)",
            (page_id, edit_dt, error[:8000]),
        )
