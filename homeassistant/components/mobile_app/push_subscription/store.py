"""Push-subscription storage, state tracking, and debounce.

A push subscription maps a push token to a set of entity_ids. The integration
owns the mapping and the state tracking; it has no knowledge of what the app
does with the resulting push.

A subscription created inside this integration rather than by the app may also
carry a delivery kind, opaque data of its own, and its own debounce interval.
None of those are reachable from the public webhook, and a subscription without
them behaves exactly as before: see `delivery.py`.

State changes are debounced per subscription: a burst of rapid changes within
PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS collapses to a single push (trailing edge),
so a chatty entity does not exhaust the device's background-push budget.

A subscription is with the entity_ids it named, and a rename in the entity
registry leaves it listening for an identifier nothing will report again. For
some relationships that is wrong - they are with the entity, not with the string
- so an internal caller may ask for the subscription to be moved onto the new
identifier and pushed. Whether that is the right policy is the consumer's to
decide, so it is off unless asked for.

Three runtime structures, all keyed [webhook_id][sub_id]:
- DATA_PUSH_SUBSCRIPTIONS: persisted token/entities/target mapping.
- DATA_PUSH_SUBSCRIPTION_UNSUBS: listener cancels, one per subscription, covering
  its state-change listener and, where it opted in, its registry listener
  (runtime only).
- DATA_PUSH_SUBSCRIPTION_DEBOUNCE: pending debounce-timer cancels (runtime only).

Two lifecycle paths:
- async_teardown_device_subscriptions: on unload/reload, cancel listeners and
  pending timers but KEEP the stored mapping so it survives a restart.
- remove_stored_device_subscriptions: on entry removal, drop the mapping too.
"""
# pylint: disable=home-assistant-use-runtime-data  # Uses legacy hass.data[DOMAIN] pattern

from datetime import datetime
from functools import partial
import logging
from typing import Any

from homeassistant.core import Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.entity_registry import EventEntityRegistryUpdatedData
from homeassistant.helpers.event import (
    async_call_later,
    async_track_entity_registry_updated_event,
    async_track_state_change_event,
)
from homeassistant.helpers.start import async_at_started
from homeassistant.util.json import JsonObjectType

from ..const import (
    ATTR_APP_DATA,
    ATTR_PUSH_URL,
    DATA_CONFIG_ENTRIES,
    DATA_PUSH_SUBSCRIPTION_DEBOUNCE,
    DATA_PUSH_SUBSCRIPTION_UNSUBS,
    DATA_PUSH_SUBSCRIPTIONS,
    DATA_STORE,
    DOMAIN,
    PUSH_SUBSCRIPTION_DATA,
    PUSH_SUBSCRIPTION_DEBOUNCE,
    PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS,
    PUSH_SUBSCRIPTION_ENTITY_IDS,
    PUSH_SUBSCRIPTION_FOLLOW_RENAMES,
    PUSH_SUBSCRIPTION_KIND,
    PUSH_SUBSCRIPTION_MAX_PER_DEVICE,
    PUSH_SUBSCRIPTION_TARGET,
    PUSH_SUBSCRIPTION_TOKEN,
    STORAGE_SAVE_DELAY_SECONDS,
)
from ..helpers import savable_state
from .delivery import PUSH_SUBSCRIPTION_DELIVERIES

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
    subscription has been quiet for its interval. That is
    PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS unless the subscription was created with
    one of its own, which only a delivery kind inside this integration can do.
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
    hass: HomeAssistant, webhook_id: str, sub_id: str, subscription: dict[str, Any]
) -> None:
    """Start (or restart) the listeners for one subscription.

    The subscription itself says which listeners it wants, so re-arming one
    after a rename or a restart cannot lose what it asked for.
    """
    # Replace any existing listener so an updated entity set takes effect.
    _async_unsub_tracker(hass, webhook_id, sub_id)

    # Only arm listeners for registrations that can send a cloud push; others
    # would schedule a debounce timer on every state change that never sends.
    entry = hass.data[DOMAIN][DATA_CONFIG_ENTRIES].get(webhook_id)
    if entry is None or ATTR_PUSH_URL not in entry.data.get(ATTR_APP_DATA, {}):
        return

    tracked: list[str] = list(subscription[PUSH_SUBSCRIPTION_ENTITY_IDS])

    @callback
    def _handle_state_change(event: Event[EventStateChangedData]) -> None:
        # Ignore changes fired while HA is still starting so a restart does not
        # push for every tracked entity as it is restored. Once running, every
        # change - including an entity first appearing (old_state is None) -
        # should refresh the subscribed surface.
        if not hass.is_running:
            return
        async_schedule_subscription_push(hass, webhook_id, sub_id)

    unsub_state = async_track_state_change_event(hass, tracked, _handle_state_change)

    if not subscription.get(PUSH_SUBSCRIPTION_FOLLOW_RENAMES):
        # This subscription named entity_ids and meant them. Nothing watches the
        # registry for it, so a rename leaves the mapping exactly as registered.
        hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS].setdefault(webhook_id, {})[
            sub_id
        ] = unsub_state
        return

    @callback
    def _handle_registry_update(event: Event[EventEntityRegistryUpdatedData]) -> None:
        # Only a rename is acted on. A state going to None is transient - an
        # integration reload removes and restores its entities - and a registry
        # removal is left alone, because what a surface with no entity should do
        # is the consumer's decision and it can always remove the subscription.
        data = event.data
        if data["action"] != "update":
            return
        if (old_entity_id := data.get("old_entity_id")) is None:
            return
        _async_follow_rename(hass, webhook_id, sub_id, old_entity_id, data["entity_id"])

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

    Only reached for a subscription that asked to follow the entity rather than
    the string. Nothing else about it changes: not the push token, not the
    delivery kind, and not any opaque data that kind keeps. The push that
    follows is what tells the consumer which identifier to ask about from now
    on.
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
    _async_setup_tracker(hass, webhook_id, sub_id, subscription)
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
    data: JsonObjectType | None = None,
    debounce_seconds: float | None = None,
    follow_entity_renames: bool = False,
) -> None:
    """Persist a subscription and (re)arm its state listener.

    The number of subscriptions retained per device is capped at
    PUSH_SUBSCRIPTION_MAX_PER_DEVICE; registering a new one past the cap evicts
    the oldest (FIFO), so the listener count a device can arm stays bounded.

    The keyword arguments are for an internal caller and are absent from a
    subscription the app registered, which is what keeps the stored shape and
    the debounce of a public registration unchanged. `follow_entity_renames`
    says the subscription is with the entity rather than with the entity_id
    naming it today; it is stored only when asked for, so nothing is written for
    the subscriptions that do not want it.
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
    subscription: JsonObjectType = {
        PUSH_SUBSCRIPTION_TOKEN: token,
        # Copied into a JSON list because that is what is persisted; the caller
        # keeps whatever list it validated.
        PUSH_SUBSCRIPTION_ENTITY_IDS: list(entity_ids),
        PUSH_SUBSCRIPTION_TARGET: target,
    }
    if kind is not None:
        subscription[PUSH_SUBSCRIPTION_KIND] = kind
    if data is not None:
        subscription[PUSH_SUBSCRIPTION_DATA] = data
    if debounce_seconds is not None:
        subscription[PUSH_SUBSCRIPTION_DEBOUNCE] = debounce_seconds
    if follow_entity_renames:
        subscription[PUSH_SUBSCRIPTION_FOLLOW_RENAMES] = True
    device_subs[sub_id] = subscription
    _async_setup_tracker(hass, webhook_id, sub_id, subscription)
    hass.data[DOMAIN][DATA_STORE].async_delay_save(
        partial(savable_state, hass), STORAGE_SAVE_DELAY_SECONDS
    )


@callback
def update_push_subscription_data(
    hass: HomeAssistant,
    webhook_id: str,
    sub_id: str,
    subscription: JsonObjectType,
    data: JsonObjectType,
) -> bool:
    """Persist opaque data if `subscription` is still the current value.

    Delivery is asynchronous, so the subscription a delivery started from can be
    replaced while it is in flight. Identity, not the subscription id, is what
    says whether the data still belongs anywhere.
    """
    current = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {}).get(sub_id)
    if current is not subscription:
        return False
    subscription[PUSH_SUBSCRIPTION_DATA] = data
    hass.data[DOMAIN][DATA_STORE].async_delay_save(
        partial(savable_state, hass), STORAGE_SAVE_DELAY_SECONDS
    )
    return True


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
    """Drop all subscriptions for a device on entry removal."""
    hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].pop(webhook_id, None)
    async_teardown_device_subscriptions(hass, webhook_id)


@callback
def async_restore_push_subscriptions(hass: HomeAssistant, webhook_id: str) -> None:
    """Re-arm listeners for one device's subscriptions after setup.

    Called from async_setup_entry once the entry is in DATA_CONFIG_ENTRIES, so a
    subscription survives a Home Assistant restart without the app re-registering.
    """
    if (entry := hass.data[DOMAIN][DATA_CONFIG_ENTRIES].get(webhook_id)) is None:
        return
    can_push = ATTR_PUSH_URL in entry.data.get(ATTR_APP_DATA, {})
    device_subs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS].get(webhook_id, {})
    for sub_id, sub in device_subs.items():
        _async_setup_tracker(hass, webhook_id, sub_id, sub)
        # A generic subscription stays quiet until something changes: it carries
        # no state, so there is nothing for the app to be out of date about. A
        # delivery kind sends the state itself, and the device has been holding
        # whatever it was sent before the restart, so it is given the chance to
        # reconcile that with what is true now. A kind nothing has registered is
        # not deliverable, so scheduling one would only start a timer that ends
        # in a dropped push.
        if can_push and sub.get(PUSH_SUBSCRIPTION_KIND) in PUSH_SUBSCRIPTION_DELIVERIES:
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
