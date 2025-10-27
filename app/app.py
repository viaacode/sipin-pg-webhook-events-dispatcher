import datetime as dt
import random
import signal
import time

from psycopg.rows import dict_row
from psycopg import Cursor
from viaa.configuration import ConfigParser
from viaa.observability import logging
from svix.exceptions import HttpError, HTTPValidationError
from .services.db import DbClient
from .services.svix import SvixClient
from .helpers.svix_router import SvixRouter


BACKOFF_CAP_S = 900

SLEEP: int = 120


class PgEventsPoller:
    def __init__(self):
        config_parser = ConfigParser()
        self.config = config_parser.app_cfg
        self.log = logging.get_logger(__name__, config=config_parser)
        db_config = self.config["db"]
        self.db_client = DbClient(
            db_config["host"],
            db_config["port"],
            db_config["dbname"],
            db_config["username"],
            db_config["password"],
        )
        self.svix_client = SvixClient(
            self.config["svix"]["auth_token"], self.config["svix"]["base_url"]
        )
        self.svix_router = SvixRouter(self.config["svix"]["bucket_application_map"])
        self.should_continue = True

    def stop(self, *_) -> None:
        self.should_continue = False

    def _backoff_seconds(self, attempts: int) -> int:
        """Calculate the backoff to retry a failed attempt to send to Svix.

        Args:
            attempts: The amount of attempts of sending an event to Svix.

        Returns:
            Time to wait to try again, in seconds.
        """
        base = min(BACKOFF_CAP_S, 2 ** max(0, attempts))
        return max(1, int(base * random.uniform(0.8, 1.2)))

    def calculate_next_timestamp_to_retry(self, attempts: int) -> dt.datetime:
        return dt.datetime.now(dt.UTC) + dt.timedelta(
            seconds=self._backoff_seconds(attempts)
        )

    def _handle_webhook_event(self, cur: Cursor, row: dict[str, str]):
        """Handle a webhook event record.

        This event will be sent to the Svix server.
        """
        row_id: int = int(row["id"])
        attempts: int = int(row["attempts"])
        event_type = row["event_type"]
        s3_bucket = row["s3_bucket"]
        payload: dict[str, str] = row["payload"]

        # Check mapping to Svix application
        app_id = self.svix_router.route(s3_bucket)
        if not app_id:
            self.db_client.mark_skipped(
                cur,
                row_id,
            )
            self.log.debug(
                "Unknown bucket, cannot be routed to an application in Svix",
                id=row_id,
            )
            return

        try:
            response = self.svix_client.post_event(app_id, row_id, event_type, payload)
        except HTTPValidationError as http_val_e:
            # This mean invalid body, no reason to retry
            status_code = http_val_e.status_code
            self.db_client.mark_dead(
                cur,
                row_id,
                attempts + 1,
                repr(http_val_e),
            )
            self.log.error(
                "Validation error when delivering event",
                id=row_id,
                status_code=status_code,
                error=repr(http_val_e)
            )
            return
        except HttpError as http_e:
            status_code = http_e.status_code
            if status_code == 401:
                self.stop()
                self.db_client.mark_pending(
                    cur,
                    row_id,
                )
                self.log.error(
                    "Invalid auth header",
                )
                return

            next_at = self.calculate_next_timestamp_to_retry(attempts)
            self.db_client.mark_retry(
                cur,
                row_id,
                attempts + 1,
                repr(http_e),
                next_at,
            )
            self.log.error(
                "Error when delivering event",
                id=row_id,
                status_code=status_code,
                error=repr(http_e)
            )
            return
        except Exception as e:
            next_at = self.calculate_next_timestamp_to_retry(attempts)
            self.db_client.mark_retry(
                cur,
                row_id,
                attempts + 1,
                repr(e),
                next_at,
            )
            self.log.error("Something went wrong", error=repr(e))
            return

        self.db_client.mark_sent(cur, row_id, response.id)
        self.log.info(
            "Event delivered",
            id=row_id,
        )

    def start_polling(self) -> None:
        """The main polling loop.

        Fetch a fixed amount of records that should be processed.
        """
        # Graceful shutdown signals
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        self.log.info("Start polling Postgres for events")

        while self.should_continue:
            try:
                with self.db_client.pool.connection() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        rows = self.db_client.fetch_batch(cur)
                        if not rows:
                            conn.rollback()  # Clear the open (UPDATE … RETURNING) transaction
                            time.sleep(SLEEP)  # Sleep some time
                            continue

                        for row in rows:
                            self._handle_webhook_event(cur, row)
                        conn.commit()

            except Exception as e:
                self.log.error("Error during executing polling loop", error=repr(e))
                time.sleep(1)

        self.log.info("Poller stopped gracefully")
