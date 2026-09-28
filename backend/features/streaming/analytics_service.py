class AnalyticsService:
    """Track lightweight, process-local metrics for active streams."""

    def __init__(self) -> None:
        self.active_viewers: dict[object, int] = {}
        self.message_count: dict[object, int] = {}
        self.subscription_count: dict[tuple[object, str], int] = {}

    async def viewer_join(self, stream_id: object) -> int:
        count = self.active_viewers.get(stream_id, 0) + 1
        self.active_viewers[stream_id] = count
        return count

    async def viewer_leave(self, stream_id: object) -> int:
        count = max(0, self.active_viewers.get(stream_id, 0) - 1)
        self.active_viewers[stream_id] = count
        return count

    async def get_viewer_count(self, stream_id: object) -> int:
        return self.active_viewers.get(stream_id, 0)

    async def track_viewer_join(self, stream_id: object, user_id: object) -> int:
        del user_id
        return await self.viewer_join(stream_id)

    async def track_viewer_leave(self, stream_id: object, user_id: object) -> int:
        del user_id
        return await self.viewer_leave(stream_id)

    async def track_message(self, stream_id: object) -> int:
        count = self.message_count.get(stream_id, 0) + 1
        self.message_count[stream_id] = count
        return count

    async def track_chat_message(self, stream_id: object) -> int:
        return await self.track_message(stream_id)

    async def track_subscription(self, stream_id: object, tier: str = "free") -> int:
        key = (stream_id, tier)
        count = self.subscription_count.get(key, 0) + 1
        self.subscription_count[key] = count
        return count

    async def generate_stream_summary(self, stream_id: object) -> dict[str, int | object]:
        subscriptions = sum(
            count
            for (tracked_stream_id, _tier), count in self.subscription_count.items()
            if tracked_stream_id == stream_id
        )
        return {
            "stream_id": stream_id,
            "active_viewers": await self.get_viewer_count(stream_id),
            "messages": self.message_count.get(stream_id, 0),
            "subscriptions": subscriptions,
        }