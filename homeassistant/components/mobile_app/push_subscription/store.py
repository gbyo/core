"""Push-subscription storage, state tracking, and debounce.

A push subscription maps a push token to a set of entity_ids. Internal callers
may also persist an opaque delivery kind, context, and debounce interval; the
public generic registration continues to use the original defaults.

State changes are debounced per subscription: a burst of rapid changes within
PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS collapses to a single push (trailing edge),
so a chatty entity does not exhaust the device's background-push budget.

A subscription follows the entity, not the string that names it: renaming a
tracked entity in the entity registry rewrites the stored entity_ids and pushes,
so the app learns the new identifier rather than silently going quiet.

Three runtime structures, all keyed [webhook_id][sub_id]:
- DATA_PUSH_SUBSCRIPTIONS: persisted token/entities/target mapping.
- DATA_PUSH_SUBSCRIPTION_UNSUBS: listener cancels, one per subscription, covering
  both its state-change and its entity-registry listener (runtime only).
- DATA_PUSH_SUBSCRIPTION_DEBOUNCE: pending debounce-timer cancels (runtime only).

A fourth, DATA_PUSH_SUBSCRIPTION_DEVICE_DATA, is keyed [webhook_id][kind] and
holds opaque state a delivery kind keeps for a whole device. It is persisted and
is not tied to any one subscription, so a kind can remember a decision after its
last subscription is gone; it is dropped only when the entry is removed.

Two lifecycle paths:
- async_teardown_device_subscriptions: on unload/reload, cancel listeners and
  pending timers but KEEP the stored mapping so it survives a restart.
- remove_stored_device_subscriptions: on entry removal, drop the mapping too.
"""
# pylint: disable=home-assistant-use-runtime-data  # Uses legacy hass.data[DOMAIN] pattern

from collections.abc import Iterable
from datetime import datetime
from functools import partial
import logging

from homeassistant.core import Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.entity_registry import EventEntityRegistryUpdatedData
from homeassistant.helpers.event import (
    async_call_later,
    async_track_entity_registry_updated_event,
    async_track_state_change_event,
)
from homeassistant.helpers.start import async_at_started

from ..const import (
    ATTR_APP_DATA,
    ATTR_PUSH_URL,
    DATA_CONFIG_ENTRIES,
    DATA_PUSH_SUBSCRIPTION_DEBOUNCE,
    DATA_PUSH_SUBSCRIPTION_DEVICE_DATA,
    DATA_PUSH_SUBSCRIPTION_UNSUBS,
    DATA_PUSH_SUBSCRIPTIONS,
    DATA_STORE,
    DOMAIN,
    PUSH_SUBSCRIPTION_DATA,
    PUSH_SUBSCRIPTION_DEBOUNCE,
    PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS,
    PUSH_SUBSCRIPTION_ENTITY_IDS,
    PUSH_SUBSCRIPTION_KIND,
    PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA,
    PUSH_SUBSCRIPTION_MAX_PER_DEVICE,
    PUSH_SUBSCRIPTION_TARGET,
    PUSH_SUBSCRIPTION_TOKEN,
    STORAGE_SAVE_DELAY_SECONDS,
)
from ..helpers import savable_state

_LOGGER = logging.getLogger(__name__)


@callback
def _async_cancel_debounce(hass: HomeAssistant, webhook_id: str, sub_id: str) -> None:
    """Cancel a pending debounce timer for one subscription, if present."""
    device_timers = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEBOUNCE].get(webhook_id)
    if device_timers and (cancel := device_timers.pop(sub_id, None)) is not None:
        cancel()
    if device_timers is not None and not device_timers:
        del hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEBOUNCE][webhook_id]


@callback
def async_schedule_subscription_push(
    hass: HomeAssistant, webhook_id: str, sub_id: str
) -> None:
    """Schedule a debounced push, resetting any in-flight timer.

    Trailing edge: each call restarts the clock, so the push only fires once the
    subscription has been quiet for its configured interval.
    """
    # Local import to avoid a circular import at module load.
    from .notify import async_send_subscription_push  # noqa: PLC0415

    _async_cancel_debounce(hass, webhook_id, sub_id)

    @callback
    def _fire(_now: datetime) -> None:
        # Clear our own timer handle first so cancel paths stay consistent.
        device_timers = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEBOUNCE].get(
            webhook_id
        )
        if device_timers is not None:
            device_timers.pop(sub_id, None)
            if not device_timers:
                del hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEBOUNCE][webhook_id]
        async_send_subscription_push(hass, webhook_id, sub_id)

    subscription = (
        hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {}).get(sub_id)
    )
    if subscription is None:
        return
    delay = subscription.get(
        PUSH_SUBSCRIPTION_DEBOUNCE, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS
    )
    cancel = async_call_later(hass, delay, _fire)
    hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEBOUNCE].setdefault(webhook_id, {})[
        sub_id
    ] = cancel


@callback
def _async_unsub_tracker(hass: HomeAssistant, webhook_id: str, sub_id: str) -> None:
    """Cancel the state-change listener and any pending timer for one sub."""
    device_unsubs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS].get(webhook_id)
    if device_unsubs and (unsub := device_unsubs.pop(sub_id, None)) is not None:
        unsub()
    if device_unsubs is not None and not device_unsubs:
        del hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][webhook_id]
    _async_cancel_debounce(hass, webhook_id, sub_id)


@callback
def _async_setup_tracker(
    hass: HomeAssistant, webhook_id: str, sub_id: str, entity_ids: Iterable[str]
) -> None:
    """Start (or restart) the listeners for one subscription."""
    # Replace any existing listener so an updated entity set takes effect.
    _async_unsub_tracker(hass, webhook_id, sub_id)

    # Only arm listeners for registrations that can send a cloud push; others
    # would schedule a debounce timer on every state change that never sends.
    entry = hass.data[DOMAIN][DATA_CONFIG_ENTRIES].get(webhook_id)
    if entry is None or ATTR_PUSH_URL not in entry.data.get(ATTR_APP_DATA, {}):
        return

    tracked = list(entity_ids)

    @callback
    def _handle_state_change(event: Event[EventStateChangedData]) -> None:
        # Ignore changes fired while HA is still starting so a restart does not
        # push for every tracked entity as it is restored. Once running, every
        # change - including an entity first appearing (old_state is None) -
        # should refresh the subscribed surface.
        if not hass.is_running:
            return
        async_schedule_subscription_push(hass, webhook_id, sub_id)

    @callback
    def _handle_registry_update(event: Event[EventEntityRegistryUpdatedData]) -> None:
        # Only a rename is acted on. A state going to None is transient - an
        # integration reload removes and restores its entities - and a registry
        # removal is left alone, because what a surface with no entity should do
        # is the app's decision and it can always remove the subscription.
        data = event.data
        if data["action"] != "update":
            return
        if (old_entity_id := data.get("old_entity_id")) is None:
            return
        _async_follow_rename(hass, webhook_id, sub_id, old_entity_id, data["entity_id"])

    unsub_state = async_track_state_change_event(hass, tracked, _handle_state_change)
    unsub_registry = async_track_entity_registry_updated_event(
        hass, tracked, _handle_registry_update
    )

    @callback
    def _unsub() -> None:
        unsub_state()
        unsub_registry()

    hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS].setdefault(webhook_id, {})[
        sub_id
    ] = _unsub


@callback
def _async_follow_rename(
    hass: HomeAssistant,
    webhook_id: str,
    sub_id: str,
    old_entity_id: str,
    new_entity_id: str,
) -> None:
    """Move one subscription onto an entity's new identifier.

    The subscription is with the entity, so nothing else about it changes: not
    the push token, not the delivery kind, and not the opaque context - which is
    how a Remote Now Playing Follow keeps its Apple session id and its place in
    the ordering across a rename. The push that follows is what tells the app
    which identifier to ask about from now on.
    """
    subscription = (
        hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {}).get(sub_id)
    )
    if subscription is None:
        return
    entity_ids: list[str] = subscription[PUSH_SUBSCRIPTION_ENTITY_IDS]
    if old_entity_id not in entity_ids:
        return

    _LOGGER.debug(
        "%s was renamed to %s; moving push subscription %s onto it",
        old_entity_id,
        new_entity_id,
        sub_id,
    )
    # dict.fromkeys collapses the case where the new name is already tracked, so
    # a rename cannot arm the same entity's listener twice.
    updated = list(
        dict.fromkeys(
            new_entity_id if entity_id == old_entity_id else entity_id
            for entity_id in entity_ids
        )
    )
    subscription[PUSH_SUBSCRIPTION_ENTITY_IDS] = updated
    _async_setup_tracker(hass, webhook_id, sub_id, updated)
    hass.data[DOMAIN][DATA_STORE].async_delay_save(
        partial(savable_state, hass), STORAGE_SAVE_DELAY_SECONDS
    )
    async_schedule_subscription_push(hass, webhook_id, sub_id)


@callback
def store_push_subscription(
    hass: HomeAssistant,
    webhook_id: str,
    sub_id: str,
    token: str,
    entity_ids: list[str],
    target: str | None,
    *,
    kind: str | None = None,
    data: dict[str, object] | None = None,
    debounce_seconds: float | None = None,
) -> None:
    """Persist a subscription and (re)arm its state listener.

    The number of subscriptions retained per device is capped at
    PUSH_SUBSCRIPTION_MAX_PER_DEVICE; registering a new one past the cap evicts
    the oldest (FIFO), so the listener count a device can arm stays bounded.
    """
    device_subs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].setdefault(webhook_id, {})
    if (
        sub_id not in device_subs
        and len(device_subs) >= PUSH_SUBSCRIPTION_MAX_PER_DEVICE
    ):
        oldest_sub_id = next(iter(device_subs))
        _LOGGER.debug(
            "Push subscription cap reached for %s; evicting oldest %s",
            webhook_id,
            oldest_sub_id,
        )
        del device_subs[oldest_sub_id]
        _async_unsub_tracker(hass, webhook_id, oldest_sub_id)
    subscription: dict[str, object] = {
        PUSH_SUBSCRIPTION_TOKEN: token,
        PUSH_SUBSCRIPTION_ENTITY_IDS: entity_ids,
        PUSH_SUBSCRIPTION_TARGET: target,
    }
    if kind is not None:
        subscription[PUSH_SUBSCRIPTION_KIND] = kind
    if data is not None:
        subscription[PUSH_SUBSCRIPTION_DATA] = data
    if debounce_seconds is not None:
        subscription[PUSH_SUBSCRIPTION_DEBOUNCE] = debounce_seconds
    device_subs[sub_id] = subscription
    _async_setup_tracker(hass, webhook_id, sub_id, entity_ids)
    hass.data[DOMAIN][DATA_STORE].async_delay_save(
        partial(savable_state, hass), STORAGE_SAVE_DELAY_SECONDS
    )


@callback
def update_push_subscription_data(
    hass: HomeAssistant,
    webhook_id: str,
    sub_id: str,
    subscription: dict[str, object],
    data: dict[str, object],
) -> bool:
    """Persist opaque data if `subscription` is still the current value."""
    current = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {}).get(sub_id)
    if current is not subscription:
        return False
    subscription[PUSH_SUBSCRIPTION_DATA] = data
    hass.data[DOMAIN][DATA_STORE].async_delay_save(
        partial(savable_state, hass), STORAGE_SAVE_DELAY_SECONDS
    )
    return True


@callback
def get_device_subscription_data(
    hass: HomeAssistant, webhook_id: str, kind: str
) -> dict[str, object] | None:
    """Return one delivery kind's opaque device-wide state, if it has any."""
    data = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEVICE_DATA].get(webhook_id, {})
    stored = data.get(kind)
    return stored if isinstance(stored, dict) else None


@callback
def set_device_subscription_data(
    hass: HomeAssistant, webhook_id: str, kind: str, data: dict[str, object]
) -> None:
    """Persist one delivery kind's opaque device-wide state."""
    hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEVICE_DATA].setdefault(webhook_id, {})[
        kind
    ] = data
    hass.data[DOMAIN][DATA_STORE].async_delay_save(
        partial(savable_state, hass), STORAGE_SAVE_DELAY_SECONDS
    )


@callback
def remove_push_subscription(hass: HomeAssistant, webhook_id: str, sub_id: str) -> None:
    """Remove one stored subscription and cancel its listener + timer."""
    subscriptions = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS]
    if (device_subs := subscriptions.get(webhook_id)) is None or device_subs.pop(
        sub_id, None
    ) is None:
        return
    if not device_subs:
        del subscriptions[webhook_id]

    _async_unsub_tracker(hass, webhook_id, sub_id)
    hass.data[DOMAIN][DATA_STORE].async_delay_save(
        partial(savable_state, hass), STORAGE_SAVE_DELAY_SECONDS
    )


@callback
def async_teardown_device_subscriptions(hass: HomeAssistant, webhook_id: str) -> None:
    """Cancel all listeners + pending timers for a device on unload/reload.

    Keeps the stored mapping so the subscription is restored on next setup.
    """
    device_unsubs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS].pop(
        webhook_id, None
    )
    if device_unsubs:
        for unsub in device_unsubs.values():
            unsub()
    device_timers = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEBOUNCE].pop(
        webhook_id, None
    )
    if device_timers:
        for cancel in device_timers.values():
            cancel()


@callback
def remove_stored_device_subscriptions(hass: HomeAssistant, webhook_id: str) -> None:
    """Drop all subscriptions, and each kind's device state, on entry removal.

    Unlike a restart or a reload, the registration itself is gone: there is
    nothing left for a kind's remembered ordering to be about, and keeping it
    would decide the fate of a re-registration that has nothing to do with it.
    """
    hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].pop(webhook_id, None)
    hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEVICE_DATA].pop(webhook_id, None)
    async_teardown_device_subscriptions(hass, webhook_id)


@callback
def async_restore_push_subscriptions(hass: HomeAssistant, webhook_id: str) -> None:
    """Re-arm listeners for one device's subscriptions after setup.

    Called from async_setup_entry once the entry is in DATA_CONFIG_ENTRIES, so a
    subscription survives a Home Assistant restart without the app re-registering.
    """
    if (entry := hass.data[DOMAIN][DATA_CONFIG_ENTRIES].get(webhook_id)) is None:
        return
    device_subs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {})
    for sub_id, sub in device_subs.items():
        _async_setup_tracker(
            hass, webhook_id, sub_id, sub[PUSH_SUBSCRIPTION_ENTITY_IDS]
        )
        if sub.get(
            PUSH_SUBSCRIPTION_KIND
        ) == PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA and ATTR_PUSH_URL in entry.data.get(
            ATTR_APP_DATA, {}
        ):
            entry.async_on_unload(
                async_at_started(
                    hass,
                    partial(
                        async_schedule_subscription_push,
                        webhook_id=webhook_id,
                        sub_id=sub_id,
                    ),
                )
            )
