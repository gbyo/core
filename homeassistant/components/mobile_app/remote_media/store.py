"""Which generic push subscriptions are Remote Now Playing's, and where their order is kept.

Every Remote Now Playing webhook names the relationship it belongs to - a generation and the
sequence the app gave it - and the two arrive over an unordered transport, from a phone that may
have queued them before going to sleep. The Follow cursor is what places them: it remembers the
newest relationship a registration has claimed, and whether it has already been stopped.

The cursor is the RemoteMedia kind's opaque device state rather than something inside one
subscription, because it has to outlive them. A dismissal that reaches Core before the
registration it stops leaves no subscription behind to hold the decision, and with nowhere to put
it the registration that follows would start the card the user has already dismissed.
"""
# pylint: disable=home-assistant-use-runtime-data  # Uses legacy hass.data[DOMAIN] pattern

from typing import Any

from homeassistant.core import HomeAssistant, callback

from ..const import (
    DATA_PUSH_SUBSCRIPTIONS,
    DOMAIN,
    PUSH_SUBSCRIPTION_DATA,
    PUSH_SUBSCRIPTION_KIND,
    PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA,
)
from ..push_subscription.store import (
    get_device_subscription_data,
    remove_push_subscription,
    set_device_subscription_data,
)
from .const import CONTEXT_GENERATION, CONTEXT_GENERATION_SEQUENCE
from .model import RemoteMediaFollowCursor


@callback
def async_follows(
    hass: HomeAssistant, webhook_id: str
) -> list[tuple[str, dict[str, Any]]]:
    """Return one registration's Follow subscriptions as (session id, subscription).

    A registration's other push subscriptions - a widget's, say - share this storage and are none
    of Remote Now Playing's business, so every lifecycle decision here is filtered by kind.
    """
    device_subs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {})
    return [
        (session_id, subscription)
        for session_id, subscription in device_subs.items()
        if subscription.get(PUSH_SUBSCRIPTION_KIND)
        == PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA
    ]


def follow_context(subscription: dict[str, Any]) -> dict[str, Any]:
    """Return one Follow subscription's opaque RemoteMedia context."""
    return subscription.get(PUSH_SUBSCRIPTION_DATA) or {}


@callback
def async_retire_follows(
    hass: HomeAssistant, webhook_id: str, *, keep: str | None = None
) -> None:
    """Drop this registration's Follow subscriptions, optionally sparing one.

    Nothing is pushed on the way out. The relationship being retired is one the phone has already
    replaced, so the only card that could be told about it is not on screen any more.
    """
    for session_id, _ in async_follows(hass, webhook_id):
        if session_id != keep:
            remove_push_subscription(hass, webhook_id, session_id)


@callback
def async_load_follow_cursor(
    hass: HomeAssistant, webhook_id: str
) -> RemoteMediaFollowCursor | None:
    """Return one registration's Follow cursor, deriving it once if it has none.

    Storage written before the cursor existed still knows the ordering - it is in the context of
    the subscriptions themselves, and the newest of those is what the phone last told us. A
    derived cursor was never stopped, because a relationship that had been dismissed would not
    have left a subscription behind.
    """
    if (
        stored := get_device_subscription_data(
            hass, webhook_id, PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA
        )
    ) is not None:
        return RemoteMediaFollowCursor.from_storage(stored)

    newest: RemoteMediaFollowCursor | None = None
    for _, subscription in async_follows(hass, webhook_id):
        context = follow_context(subscription)
        generation = context.get(CONTEXT_GENERATION)
        sequence = context.get(CONTEXT_GENERATION_SEQUENCE)
        if (
            not isinstance(generation, str)
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
        ):
            continue
        if newest is None or sequence > newest.generation_sequence:
            newest = RemoteMediaFollowCursor(generation, sequence, False)

    if newest is not None:
        async_save_follow_cursor(hass, webhook_id, newest)
    return newest


@callback
def async_save_follow_cursor(
    hass: HomeAssistant, webhook_id: str, cursor: RemoteMediaFollowCursor
) -> None:
    """Persist one registration's Follow cursor."""
    set_device_subscription_data(
        hass, webhook_id, PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA, cursor.as_storage()
    )
