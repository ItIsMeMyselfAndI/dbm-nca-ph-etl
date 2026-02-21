from typing import Protocol


class NotificationProvider(Protocol):
    def send_notification(self, message: str) -> None:
        """Send a notification with the given message."""
        ...
