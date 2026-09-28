class FollowerService:

    async def follow(
        self,
        creator_id,
        follower_id
    ):
        ...

    async def unfollow(
        self,
        creator_id,
        follower_id
    ):
        ...

    async def get_followers(
        self,
        creator_id
    ):
        ...

    async def get_following(
        self,
        follower_id
    ):
        ...

async def notify_live(
    self,
    creator_id,
    stream_title
):
    followers = await self.followers.get_followers(
        creator_id
    )

    for follower in followers:
        await self.send_notification(
            follower.id,
            f"{stream_title} is live!"
        )