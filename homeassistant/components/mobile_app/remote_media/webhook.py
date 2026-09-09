"""Thin Remote Now Playing adapters over generic push subscriptions."""
# pylint: disable=home-assistant-use-runtime-data  # Uses legacy hass.data[DOMAIN] pattern

import hashlib
import logging
from typing import Any

from aiohttp.web import Response
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_ENTITY_ID, CONF_WEBHOOK_ID
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv

from ..const import (
    DATA_PUSH_SUBSCRIPTIONS,
    DOMAIN,
    PUSH_SUBSCRIPTION_DATA,
    PUSH_SUBSCRIPTION_ENTITY_IDS,
    PUSH_SUBSCRIPTION_KIND,
    PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA,
    PUSH_SUBSCRIPTION_TOKEN,
)
from ..helpers import empty_okay_response
from ..push_subscription.store import (
    async_schedule_subscription_push,
    remove_push_subscription,
    store_push_subscription,
)
from ..webhook import WEBHOOK_COMMANDS, WEBHOOK_PAYLOAD_REDACTORS, validate_schema
from .const import (
    ATTR_GENERATION,
    ATTR_GENERATION_SEQUENCE,
    ATTR_PUSH_TOKEN,
    ATTR_SCHEMA_VERSION,
    ATTR_SERVER_ID_KEY,
    ATTR_SESSION_ID,
    COALESCE_SECONDS,
    CONTEXT_GENERATION,
    CONTEXT_GENERATION_SEQUENCE,
    CONTEXT_LAST_SNAPSHOT,
    CONTEXT_LAST_TIMESTAMP,
    CONTEXT_SCHEMA_VERSION,
    CONTEXT_SERVER_ID,
    MAX_GENERATION_LENGTH,
    MAX_GENERATION_SEQUENCE,
    MAX_PUSH_TOKEN_LENGTH,
    MAX_SERVER_ID_LENGTH,
    MAX_SESSION_ID_LENGTH,
    SUPPORTED_SCHEMA_VERSIONS,
    WEBHOOK_TYPE_DISMISSED,
    WEBHOOK_TYPE_TOKEN,
)
from .mapper import MEDIA_PLAYER_PREFIX

_LOGGER = logging.getLogger(__name__)

TOKEN_FINGERPRINT_LENGTH = 16


def _bounded_string(maximum: int) -> vol.All:
    """Return a validator for a non-empty bounded string."""
    return vol.All(cv.string, vol.Length(min=1, max=maximum))


def _media_player_entity_id(value: Any) -> str:
    """Validate that a subscription follows exactly one media player."""
    entity_id = cv.entity_id(value)
    if not entity_id.startswith(MEDIA_PLAYER_PREFIX):
        raise vol.Invalid("Remote Now Playing can only follow a media_player entity")
    return entity_id


def valid_token(value: Any) -> str | None:
    """Return a usable APNs token without exposing invalid input to schema logs."""
    if not isinstance(value, str) or not value or len(value) > MAX_PUSH_TOKEN_LENGTH:
        return None
    if len(value) % 2 or not all(
        character in "0123456789abcdefABCDEF" for character in value
    ):
        return None
    return value


def token_fingerprint(token: str) -> str:
    """Return a short non-reversible label for a session token."""
    return hashlib.sha256(token.encode()).hexdigest()[:TOKEN_FINGERPRINT_LENGTH]


@WEBHOOK_PAYLOAD_REDACTORS.register(WEBHOOK_TYPE_TOKEN)
def redact_token_payload(payload: Any) -> Any:
    """Replace the session token before the decrypted payload is logged."""
    if not isinstance(payload, dict) or ATTR_PUSH_TOKEN not in payload:
        return payload
    token = payload[ATTR_PUSH_TOKEN]
    redacted = (
        f"**REDACTED:{token_fingerprint(token)}**"
        if isinstance(token, str)
        else "**REDACTED**"
    )
    return {**payload, ATTR_PUSH_TOKEN: redacted}


def _remote_subscription(
    hass: HomeAssistant, webhook_id: str, session_id: str
) -> dict[str, Any] | None:
    """Return the stored RemoteMedia subscription, if this id names one."""
    subscription = (
        hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {}).get(session_id)
    )
    if (
        subscription is None
        or subscription.get(PUSH_SUBSCRIPTION_KIND)
        != PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA
    ):
        return None
    return subscription


def _describes_same_lifetime(
    context: dict[str, Any], generation: str, sequence: int
) -> bool:
    """Return whether both ordering fields name the current Follow lifetime."""
    return (
        context.get(CONTEXT_GENERATION) == generation
        and context.get(CONTEXT_GENERATION_SEQUENCE) == sequence
    )


@WEBHOOK_COMMANDS.register(WEBHOOK_TYPE_TOKEN)
@validate_schema(
    {
        vol.Required(ATTR_SESSION_ID): _bounded_string(MAX_SESSION_ID_LENGTH),
        vol.Required(ATTR_SERVER_ID_KEY): _bounded_string(MAX_SERVER_ID_LENGTH),
        vol.Required(ATTR_ENTITY_ID): _media_player_entity_id,
        vol.Required(ATTR_GENERATION): _bounded_string(MAX_GENERATION_LENGTH),
        vol.Required(ATTR_GENERATION_SEQUENCE): vol.All(
            cv.positive_int, vol.Range(min=1, max=MAX_GENERATION_SEQUENCE)
        ),
        vol.Required(ATTR_PUSH_TOKEN): object,
        vol.Required(ATTR_SCHEMA_VERSION): vol.All(
            cv.positive_int, vol.In(SUPPORTED_SCHEMA_VERSIONS)
        ),
    }
)
async def webhook_remote_media_session_token(
    hass: HomeAssistant, config_entry: ConfigEntry, data: dict[str, Any]
) -> Response:
    """Create or update the generic subscription for one Follow relationship."""
    webhook_id = config_entry.data[CONF_WEBHOOK_ID]
    session_id = data[ATTR_SESSION_ID]
    entity_id = data[ATTR_ENTITY_ID]
    generation = data[ATTR_GENERATION]
    sequence = data[ATTR_GENERATION_SEQUENCE]

    if (token := valid_token(data[ATTR_PUSH_TOKEN])) is None:
        _LOGGER.warning(
            "Ignoring a Remote Now Playing registration for %s: its update token is not"
            " hexadecimal data of a usable length",
            entity_id,
        )
        return empty_okay_response()

    previous = _remote_subscription(hass, webhook_id, session_id)
    previous_context: dict[str, Any] = (
        previous.get(PUSH_SUBSCRIPTION_DATA, {}) if previous is not None else {}
    )
    previous_sequence = previous_context.get(CONTEXT_GENERATION_SEQUENCE)

    if isinstance(previous_sequence, int):
        if sequence < previous_sequence:
            _LOGGER.debug(
                "Ignoring a stale Remote Now Playing registration for session %s: it names"
                " Follow %s, and %s is current",
                session_id,
                sequence,
                previous_sequence,
            )
            return empty_okay_response()
        if (
            sequence == previous_sequence
            and previous_context.get(CONTEXT_GENERATION) != generation
        ):
            _LOGGER.warning(
                "Ignoring a conflicting Remote Now Playing registration for %s: Follow %s is"
                " already held by a different session lifetime",
                entity_id,
                sequence,
            )
            return empty_okay_response()

    same_lifetime = _describes_same_lifetime(previous_context, generation, sequence)
    if (
        previous is not None
        and same_lifetime
        and previous[PUSH_SUBSCRIPTION_TOKEN] == token
        and previous[PUSH_SUBSCRIPTION_ENTITY_IDS] == [entity_id]
        and previous_context.get(CONTEXT_SERVER_ID) == data[ATTR_SERVER_ID_KEY]
        and previous_context.get(CONTEXT_SCHEMA_VERSION) == data[ATTR_SCHEMA_VERSION]
    ):
        return empty_okay_response()

    context: dict[str, object] = {
        CONTEXT_GENERATION: generation,
        CONTEXT_GENERATION_SEQUENCE: sequence,
        CONTEXT_SERVER_ID: data[ATTR_SERVER_ID_KEY],
        CONTEXT_SCHEMA_VERSION: data[ATTR_SCHEMA_VERSION],
        CONTEXT_LAST_TIMESTAMP: (
            previous_context.get(CONTEXT_LAST_TIMESTAMP) if same_lifetime else None
        ),
        CONTEXT_LAST_SNAPSHOT: (
            previous_context.get(CONTEXT_LAST_SNAPSHOT) if same_lifetime else None
        ),
    }
    store_push_subscription(
        hass,
        webhook_id,
        session_id,
        token,
        [entity_id],
        None,
        kind=PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA,
        data=context,
        debounce_seconds=COALESCE_SECONDS,
    )
    async_schedule_subscription_push(hass, webhook_id, session_id)

    _LOGGER.debug(
        "Following %s for Remote Now Playing (session %s, Follow %s/%s, token %s)",
        entity_id,
        session_id,
        generation,
        sequence,
        token_fingerprint(token),
    )
    return empty_okay_response()


@WEBHOOK_COMMANDS.register(WEBHOOK_TYPE_DISMISSED)
@validate_schema(
    {
        vol.Required(ATTR_SESSION_ID): _bounded_string(MAX_SESSION_ID_LENGTH),
        vol.Required(ATTR_GENERATION): _bounded_string(MAX_GENERATION_LENGTH),
        vol.Required(ATTR_GENERATION_SEQUENCE): vol.All(
            cv.positive_int, vol.Range(min=1, max=MAX_GENERATION_SEQUENCE)
        ),
    }
)
async def webhook_remote_media_session_dismissed(
    hass: HomeAssistant, config_entry: ConfigEntry, data: dict[str, Any]
) -> Response:
    """Remove the generic subscription only for the current Follow lifetime."""
    webhook_id = config_entry.data[CONF_WEBHOOK_ID]
    session_id = data[ATTR_SESSION_ID]
    if (subscription := _remote_subscription(hass, webhook_id, session_id)) is None:
        return empty_okay_response()

    context = subscription.get(PUSH_SUBSCRIPTION_DATA, {})
    if not _describes_same_lifetime(
        context, data[ATTR_GENERATION], data[ATTR_GENERATION_SEQUENCE]
    ):
        return empty_okay_response()

    remove_push_subscription(hass, webhook_id, session_id)
    _LOGGER.debug(
        "Stopped following %s for Remote Now Playing",
        subscription[PUSH_SUBSCRIPTION_ENTITY_IDS][0],
    )
    return empty_okay_response()
