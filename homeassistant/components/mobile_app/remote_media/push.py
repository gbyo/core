"""Send one Remote Now Playing update through the mobile push relay."""

import asyncio
from enum import Enum
from http import HTTPStatus
import logging
from typing import Any

from aiohttp import ClientError, ClientResponseError

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from ..const import (
    ATTR_APP_DATA,
    ATTR_APP_ID,
    ATTR_APP_VERSION,
    ATTR_OS_VERSION,
    ATTR_PUSH_TOKEN,
    ATTR_PUSH_URL,
    ATTR_WEBHOOK_ID,
)
from .const import (
    ATTR_ATTRIBUTES,
    ATTR_EVENT,
    ATTR_NOW_PLAYING,
    ATTR_NOW_PLAYING_TOKEN,
    ATTR_REGISTRATION_INFO,
    ATTR_TIMESTAMP,
    ERROR_INVALID_TOKEN,
)

_LOGGER = logging.getLogger(__name__)

RELAY_TIMEOUT_SECONDS = 15


class PushOutcome(Enum):
    """The outcomes the subscription adapter needs to distinguish."""

    DELIVERED = "delivered"
    INVALID_TOKEN = "invalid_token"
    FAILED = "failed"


def build_request(
    entry: ConfigEntry,
    now_playing_token: str,
    event: str,
    timestamp: int,
    attributes: dict[str, Any],
) -> dict[str, Any]:
    """Return the physically tested relay request body."""
    app_data = entry.data[ATTR_APP_DATA]
    registration_info = {
        ATTR_APP_ID: entry.data[ATTR_APP_ID],
        ATTR_APP_VERSION: entry.data[ATTR_APP_VERSION],
        ATTR_WEBHOOK_ID: entry.data[ATTR_WEBHOOK_ID],
    }
    if ATTR_OS_VERSION in entry.data:
        registration_info[ATTR_OS_VERSION] = entry.data[ATTR_OS_VERSION]

    return {
        ATTR_PUSH_TOKEN: app_data[ATTR_PUSH_TOKEN],
        ATTR_NOW_PLAYING_TOKEN: now_playing_token,
        ATTR_NOW_PLAYING: {
            ATTR_EVENT: event,
            ATTR_TIMESTAMP: timestamp,
            ATTR_ATTRIBUTES: attributes,
        },
        ATTR_REGISTRATION_INFO: registration_info,
    }


async def async_send(
    hass: HomeAssistant,
    entry: ConfigEntry,
    session_id: str,
    now_playing_token: str,
    event: str,
    timestamp: int,
    attributes: dict[str, Any],
) -> PushOutcome:
    """Send one update and distinguish only a permanently invalid token."""
    app_data = entry.data.get(ATTR_APP_DATA, {})
    if not app_data.get(ATTR_PUSH_URL) or not app_data.get(ATTR_PUSH_TOKEN):
        return PushOutcome.FAILED

    body = build_request(entry, now_playing_token, event, timestamp, attributes)
    session = async_get_clientsession(hass)

    try:
        async with asyncio.timeout(RELAY_TIMEOUT_SECONDS):
            response = await session.post(app_data[ATTR_PUSH_URL], json=body)
            status = response.status
            try:
                result = await response.json()
            except ValueError, ClientResponseError:
                result = {}
    except TimeoutError, ClientError:
        _LOGGER.debug(
            "Failed to send Remote Now Playing update for session %s",
            session_id,
            exc_info=True,
        )
        return PushOutcome.FAILED

    if status in (HTTPStatus.OK, HTTPStatus.CREATED, HTTPStatus.ACCEPTED):
        return PushOutcome.DELIVERED

    error_type = result.get("errorType") if isinstance(result, dict) else None
    _LOGGER.debug(
        "Remote Now Playing update for session %s returned %s (%s)",
        session_id,
        status,
        error_type,
    )
    return (
        PushOutcome.INVALID_TOKEN
        if error_type == ERROR_INVALID_TOKEN
        else PushOutcome.FAILED
    )
