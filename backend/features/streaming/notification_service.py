"""Persist and deliver notifications for live-stream events."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from typing import Any
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.notification import NotificationType
from repositories.notification_repository import NotificationRepository

SubscriberIdsProvider = Callable[[uuid.UUID], Awaitable[Iterable[uuid.UUID]]]


class NotificationManager:
    """Tracks active WebSocket connections for real-time notifications."""

    def __init__(self) -> None:
        self._connections: dict[uuid.UUID, set[Any]] = {}

    async def connect(self, user_id: uuid.UUID, websocket: Any) -> None:
        self._connections.setdefault(user_id, set()).add(websocket)

    async def disconnect(self, user_id: uuid.UUID, websocket: Any) -> None:
        connections = self._connections.get(user_id)
        if connections is None:
            return
        connections.discard(websocket)
        if not connections:
            del self._connections[user_id]

    async def send_notification(self, user_id: uuid.UUID, payload: dict[str, Any]) -> None:
        connections = tuple(self._connections.get(user_id, ()))
        for websocket in connections:
            try:
                await websocket.send_json(payload)
            except Exception:
                await self.disconnect(user_id, websocket)


class NotificationService:
    """Creates durable notifications and optionally pushes them to WebSockets."""

    def __init__(
        self,
        db: AsyncSession,
        subscriber_ids_provider: SubscriberIdsProvider,
        manager: NotificationManager | None = None,
    ) -> None:
        self._notification_repository = NotificationRepository(db)
        self._subscriber_ids_provider = subscriber_ids_provider
        self._manager = manager or NotificationManager()
        self._db = db

    async def stream_started(
        self,
        creator_id: uuid.UUID,
        stream_id: uuid.UUID,
        stream_title: str,
    ) -> int:
        """Notify every follower that a creator's stream has started."""
        title = stream_title.strip()
        if not title:
            raise ValueError("stream_title is required")

        subscriber_ids = await self._subscriber_ids_provider(creator_id)
        delivered = 0
        for subscriber_id in subscriber_ids:
            notification = await self._notification_repository.create_notification(
                user_id=subscriber_id,
                type=NotificationType.SYSTEM,
                title="Live Now",
                body=f"{title} is now live!",
                actor_id=creator_id,
                event_key=f"stream_started:{stream_id}",
                data={"stream_id": str(stream_id), "creator_id": str(creator_id)},
            )
            if notification is None:
                continue

            delivered += 1
            await self._manager.send_notification(
                subscriber_id,
                {
                    "type": "stream_started",
                    "notification_id": str(notification.id),
                    "title": notification.title,
                    "body": notification.body,
                    "data": notification.data,
                },
            )

        await self._db.commit()
        return delivered
