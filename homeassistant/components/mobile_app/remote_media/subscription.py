"""RemoteMedia-specific delivery for a generic push subscription."""
# pylint: disable=home-assistant-use-runtime-data  # Uses legacy hass.data[DOMAIN] pattern

import time
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_ID
from homeassistant.core import HomeAssistant

from ..const import (
    DATA_PUSH_SUBSCRIPTIONS,
    DOMAIN,
    PUSH_SUBSCRIPTION_DATA,
    PUSH_SUBSCRIPTION_ENTITY_IDS,
    PUSH_SUBSCRIPTION_TOKEN,
)
from ..push_subscription.store import (
    remove_push_subscription,
    update_push_subscription_data,
)
from .const import (
    ATTR_GENERATION,
    ATTR_SNAPSHOT,
    ATTR_WIRE_GENERATION_SEQUENCE,
    CONTEXT_GENERATION,
    CONTEXT_GENERATION_SEQUENCE,
    CONTEXT_LAST_SNAPSHOT,
    CONTEXT_LAST_TIMESTAMP,
    CONTEXT_SERVER_ID,
    EVENT_UPDATE,
    SEEK_TOLERANCE_SECONDS,
    VOLUME_CHANGE_THRESHOLD,
)
from .mapper import snapshot_from_state
from .model import RemoteMediaSnapshot
from .push import PushOutcome, async_send
from .reducer import reduce_snapshot


def is_meaningful_change(
    published: RemoteMediaSnapshot | None, candidate: RemoteMediaSnapshot
) -> bool:
    """Return whether the candidate conveys information worth waking the phone for."""
    if published is None:
        return True
    if published.entity_id != candidate.entity_id:
        return True
    if published.server_id != candidate.server_id:
        return True
    if published.track_id != candidate.track_id:
        return True
    if published.artwork_url != candidate.artwork_url:
        return True
    if published.playback != candidate.playback:
        return True
    if published.features != candidate.features:
        return True
    if published.duration != candidate.duration:
        return True
    if published.device_name != candidate.device_name:
        return True
    if published.device_class != candidate.device_class:
        return True
    if published.is_muted != candidate.is_muted:
        return True
    if _volume_moved(published.volume, candidate.volume):
        return True
    return _position_jumped(published, candidate)


def _volume_moved(previous: float | None, current: float | None) -> bool:
    """Return whether volume changed enough to matter."""
    if previous is None or current is None:
        return previous is not current
    return abs(previous - current) >= VOLUME_CHANGE_THRESHOLD


def _position_jumped(
    published: RemoteMediaSnapshot, candidate: RemoteMediaSnapshot
) -> bool:
    """Return whether position moved somewhere playback would not have taken it."""
    if candidate.position is None:
        return published.position is not None
    if published.position is None:
        return True

    elapsed = 0.0
    if published.playback == "playing":
        if (
            published.position_updated_at_unix is None
            or candidate.position_updated_at_unix is None
        ):
            return False
        elapsed = max(
            0.0,
            candidate.position_updated_at_unix - published.position_updated_at_unix,
        )

    expected = published.position + elapsed
    return abs(candidate.position - expected) > SEEK_TOLERANCE_SECONDS


def _stored_snapshot(data: dict[str, object]) -> RemoteMediaSnapshot | None:
    """Load the last successfully delivered snapshot from opaque context."""
    stored = data.get(CONTEXT_LAST_SNAPSHOT)
    return (
        RemoteMediaSnapshot.from_storage(stored) if isinstance(stored, dict) else None
    )


def _next_timestamp(data: dict[str, object]) -> int:
    """Return a strictly increasing APNs ordering timestamp."""
    now = int(time.time())
    previous = data.get(CONTEXT_LAST_TIMESTAMP)
    return now if not isinstance(previous, int) else max(now, previous + 1)


def _attributes(
    session_id: str,
    data: dict[str, object],
    snapshot: RemoteMediaSnapshot,
) -> dict[str, Any]:
    """Build the exact attributes object decoded by the iOS extension."""
    return {
        ATTR_ID: session_id,
        ATTR_GENERATION: data[CONTEXT_GENERATION],
        ATTR_WIRE_GENERATION_SEQUENCE: data[CONTEXT_GENERATION_SEQUENCE],
        ATTR_SNAPSHOT: snapshot.as_wire(),
    }


async def async_deliver_subscription(
    hass: HomeAssistant,
    entry: ConfigEntry,
    webhook_id: str,
    sub_id: str,
    subscription: dict[str, Any],
) -> None:
    """Map current state, filter it, and deliver one best-effort update."""
    [entity_id] = subscription[PUSH_SUBSCRIPTION_ENTITY_IDS]
    if (state := hass.states.get(entity_id)) is None:
        return

    data = dict(subscription[PUSH_SUBSCRIPTION_DATA])
    previous = _stored_snapshot(data)
    incoming = snapshot_from_state(state, data[CONTEXT_SERVER_ID])
    if incoming is None or (candidate := reduce_snapshot(previous, incoming)) is None:
        return
    if not is_meaningful_change(previous, candidate):
        return

    timestamp = _next_timestamp(data)
    data[CONTEXT_LAST_TIMESTAMP] = timestamp
    if not update_push_subscription_data(hass, webhook_id, sub_id, subscription, data):
        return

    outcome = await async_send(
        hass,
        entry,
        sub_id,
        subscription[PUSH_SUBSCRIPTION_TOKEN],
        EVENT_UPDATE,
        timestamp,
        _attributes(sub_id, data, candidate),
    )

    current = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {}).get(sub_id)
    if current is not subscription:
        return
    if outcome is PushOutcome.INVALID_TOKEN:
        remove_push_subscription(hass, webhook_id, sub_id)
    elif outcome is PushOutcome.DELIVERED:
        # Only the newest concurrent attempt may update what the phone is believed to show.
        current_data = current[PUSH_SUBSCRIPTION_DATA]
        if current_data.get(CONTEXT_LAST_TIMESTAMP) != timestamp:
            return
        data[CONTEXT_LAST_SNAPSHOT] = candidate.as_storage()
        update_push_subscription_data(hass, webhook_id, sub_id, subscription, data)
