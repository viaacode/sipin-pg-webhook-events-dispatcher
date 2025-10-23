from svix.api import Svix, SvixOptions, MessageCreateOptions
from svix.models import MessageIn, MessageOut

from viaa.configuration import ConfigParser
from viaa.observability import logging


class SvixClient:
    def __init__(self, auth_token: str, base_url: str):
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)

        self.svix = Svix(auth_token, SvixOptions(server_url=base_url))

    def post_event(
        self, app_id: str, event_id: int, event_type: str, payload: dict[str, str]
    ) -> MessageOut:
        """Posts an event to svix

        Args:
            app_id: The ID of the application in Svix to send the event to.
            event_id: The event ID hat will be used as idempotency key.
            event_type: The mandatory event_type.
            payload: The actual payload of the event.
        
        Returns:
            MessageOut: The response message from Svix.
        """
        message = MessageIn(event_type=event_type, payload=payload)
        return self.svix.message.create(
            app_id, message, MessageCreateOptions(idempotency_key=f"webhook_events:{event_id}")
        )
