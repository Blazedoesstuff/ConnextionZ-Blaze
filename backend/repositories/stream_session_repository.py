import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.streaming import StreamSession, StreamSessionStatus
from repositories.base import BaseRepository


class StreamSessionRepository(BaseRepository[StreamSession]):
    def __init__(self, db: AsyncSession):
        super().__init__(db, StreamSession)

    async def get_active(self, limit: int = 20) -> list[StreamSession]:
        result = await self.db.execute(
            select(StreamSession)
            .where(StreamSession.status == StreamSessionStatus.ACTIVE)
            .order_by(StreamSession.started_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_for_owner(self, owner_id: uuid.UUID) -> list[StreamSession]:
        result = await self.db.execute(
            select(StreamSession)
            .where(StreamSession.owner_id == owner_id)
            .order_by(StreamSession.created_at.desc())
        )
        return list(result.scalars().all())

    async def end(self, stream_id: uuid.UUID) -> StreamSession | None:
        stream = await self.get_by_id(stream_id)
        if stream is None:
            return None

        stream.status = StreamSessionStatus.ENDED
        stream.ended_at = datetime.now(timezone.utc)
        await self.db.flush()
        return stream