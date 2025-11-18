import json


class SvixRouter:
    """Routes S3 bucket names to Svix application IDs.

    This class provides a simple routing mechanism that maps S3 bucket names
    to Svix application IDs based on a JSON mapping provided at initialization.
    If a given bucket name is not found in the mapping, `route()` returns None.

    Attributes:
        map (dict[str, str]): A dictionary where keys are S3 bucket names and
            values are Svix application IDs.
    """

    def __init__(self, mapping: str):
        """Initialize the SvixRouter with a JSON mapping.

        Args:
            mapping: A JSON string representing the mapping between
                S3 bucket names and Svix application IDs. For example:
                '{"bucket-a": "app_123", "bucket-b": "app_456"}'

        Raises:
            ValueError: If the provided mapping string is not valid JSON or
                does not represent a valid dictionary.
        """
        try:
            self.map: dict[str, str] = json.loads(mapping)
        except Exception as e:
            raise ValueError(
                f"Invalid mapping of buckets to Svix applications: {e}"
            ) from e

    def route(self, s3_bucket: str) -> str | None:
        """Return the Svix application ID for the given S3 bucket.

        Args:
            s3_bucket: The name of the S3 bucket to route.

        Returns:
            str : The Svix application ID associated with the bucket,
            None: if the bucket is not found in the mapping.
        """
        app_id = self.map.get(s3_bucket)
        if not app_id:
            return None

        return app_id
