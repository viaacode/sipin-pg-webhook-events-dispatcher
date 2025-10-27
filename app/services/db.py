import datetime as dt
from typing import Any

from psycopg_pool import ConnectionPool
from psycopg import Cursor
from viaa.configuration import ConfigParser
from viaa.observability import logging

FETCH_SQL = """
WITH picked AS (
  SELECT id
  FROM webhook_events
  WHERE status = 'pending'
    AND next_attempt_at <= now()
  ORDER BY id
  LIMIT %s
  FOR UPDATE SKIP LOCKED
)
UPDATE webhook_events w
SET status = 'sending'
FROM picked p
WHERE w.id = p.id
RETURNING
  w.id,
  w.event_type,
  w.payload,
  w.attempts,
  w.s3_bucket;
"""

MAX_ATTEMPTS = 20
BATCH_LIMIT = 100


class DbClient:
    def __init__(
        self, host: str, port: int, db_name: str, username: str, password: str
    ):
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pool = ConnectionPool(
            f"host={host} port={str(port)} dbname={db_name} user={username} password={password}"
        )

    def fetch_batch(self, cur: Cursor) -> list[dict[str, Any]]:
        """Fetch records to process.

        The open cursor has a dict_row as row_factory.
        """
        cur.execute(FETCH_SQL, (BATCH_LIMIT,))
        return cur.fetchall()

    def mark_skipped(self, cur: Cursor, event_id: int) -> int:
        cur.execute(
            """
            UPDATE webhook_events
               SET status='skipped'
             WHERE id=%s
            """,
            (event_id,),
        )
        return cur.rowcount

    def mark_sent(self, cur: Cursor, event_id: int, svix_id: str):
        cur.execute(
            "UPDATE webhook_events SET status='sent', sent_at=now(), error=NULL, svix_id=%s WHERE id=%s",
            (
                svix_id,
                event_id,
            ),
        )

    def mark_pending(self, cur: Cursor, event_id: int) -> int:
        cur.execute(
            "UPDATE webhook_events SET status='pending' WHERE id=%s",
            (event_id,),
        )
        return cur.rowcount

    def mark_dead(self, cur: Cursor, event_id: int, attempts: int, err: str) -> int:
        cur.execute(
            """
            UPDATE webhook_events
            SET status='dead', attempts=%s, error=left(%s, 1000)
            WHERE id=%s
            """,
            (attempts, err, event_id),
        )
        return cur.rowcount

    def mark_retry(
        self,
        cur: Cursor,
        event_id: int,
        attempts: int,
        err: str,
        next_attempt_at: dt.datetime,
    ) -> int:
        if attempts >= MAX_ATTEMPTS:
            return self.mark_dead(cur, event_id, attempts, err)
        else:
            cur.execute(
                """
                UPDATE webhook_events
                SET status='pending',
                    attempts=%s,
                    next_attempt_at=%s,
                    error=left(%s, 1000)
                WHERE id=%s
                """,
                (attempts, next_attempt_at, err, event_id),
            )
            return cur.rowcount
