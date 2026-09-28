from sqlalchemy import delete, select

from app.models.social import Follow


class SubscriptionService:

    def __init__(self, db, notification_service=None):
        self.db = db
        self.notification_service = notification_service

    async def subscribe(self, creator_id, subscriber_id, tier="free"):
        existing = await self.db.execute(
            select(Follow).where(
                Follow.following_id == creator_id,
                Follow.follower_id == subscriber_id
            )
        )

        if existing.scalar_one_or_none():
            raise ValueError(
                "Already subscribed"
            )

        follow = Follow(
            follower_id=subscriber_id,
            following_id=creator_id,
        )

        self.db.add(follow)
        await self.db.commit()
        await self.db.refresh(follow)

        return follow

    async def unsubscribe(self, creator_id, subscriber_id) -> bool:
        result = await self.db.execute(
            delete(Follow).where(
                Follow.following_id == creator_id,
                Follow.follower_id == subscriber_id,
            )
        )
        await self.db.commit()
        return result.rowcount > 0

    async def get_subscribers(self, creator_id) -> list[Follow]:
        result = await self.db.execute(
            select(Follow)
            .where(Follow.following_id == creator_id)
            .order_by(Follow.created_at.desc())
        )
        return list(result.scalars().all())