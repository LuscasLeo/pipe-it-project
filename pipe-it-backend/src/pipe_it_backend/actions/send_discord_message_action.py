from dataclasses import dataclass
import requests


@dataclass
class SendDiscordMessage:
    webhook_url: str
    message: str
    name: str | None = None
    avatar_url: str | None = None


def send_discord_message(payload: SendDiscordMessage):
    requests.post(
        payload.webhook_url,
        json={
            "content": payload.message,
            "username": payload.name,
            "avatar_url": payload.avatar_url,
        },
    )


