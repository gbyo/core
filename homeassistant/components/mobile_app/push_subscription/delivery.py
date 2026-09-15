"""How a specialized push subscription is delivered.

An ordinary subscription posts the generic trigger in `notify.py` and that is
the whole of it. Some subscriptions are created by this integration rather than
by the app, for a surface that needs the state itself rather than a nudge to go
and fetch it; those carry a `kind`, and the module that owns that kind registers
how to deliver it here.

The registry is the only thing `push_subscription` knows about them: it never
imports a kind's module, and a kind is only present once whatever owns it has
been imported. A subscription whose kind is not registered is therefore not
deliverable, and `notify.py` drops it rather than falling back to the generic
push - a specialized surface expects the state in the payload, and the generic
trigger is not that.
"""

from collections.abc import Callable, Coroutine
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.util.decorator import Registry
from homeassistant.util.json import JsonObjectType

type PushSubscriptionDelivery = Callable[
    [HomeAssistant, ConfigEntry, str, str, JsonObjectType],
    Coroutine[Any, Any, None],
]

PUSH_SUBSCRIPTION_DELIVERIES: Registry[str, PushSubscriptionDelivery] = Registry()
