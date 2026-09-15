"""Tests for mobile_app push subscriptions."""

from collections.abc import Generator
from datetime import timedelta
from http import HTTPStatus
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import ClientError
from aiohttp.test_utils import TestClient
from freezegun.api import FrozenDateTimeFactory
import pytest

from homeassistant.components.mobile_app.const import (
    DATA_PUSH_SUBSCRIPTION_DEBOUNCE,
    DATA_PUSH_SUBSCRIPTION_UNSUBS,
    DATA_PUSH_SUBSCRIPTIONS,
    DOMAIN,
    PUSH_SUBSCRIPTION_DATA,
    PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS,
    PUSH_SUBSCRIPTION_ENTITY_IDS,
    PUSH_SUBSCRIPTION_FOLLOW_RENAMES,
    PUSH_SUBSCRIPTION_ID,
    PUSH_SUBSCRIPTION_MAX_PER_DEVICE,
    PUSH_SUBSCRIPTION_TARGET,
    PUSH_SUBSCRIPTION_TOKEN,
    PUSH_SUBSCRIPTION_TRIGGER,
)
from homeassistant.components.mobile_app.push_subscription.delivery import (
    PUSH_SUBSCRIPTION_DELIVERIES,
)
from homeassistant.components.mobile_app.push_subscription.notify import (
    _send_subscription_push,
)
from homeassistant.components.mobile_app.push_subscription.store import (
    async_restore_push_subscriptions,
    store_push_subscription,
    update_push_subscription_data,
)
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import CONF_WEBHOOK_ID
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.setup import async_setup_component
from homeassistant.util.json import JsonObjectType

from .const import REGISTER_CLEARTEXT, UPDATE

from tests.common import async_fire_time_changed

PUSH_URL = "https://mobile-push.home-assistant.dev/push"
PUSH_APP_DATA = {"push_url": PUSH_URL, "push_token": "device-token"}
TRACKED_ENTITY = "light.living_room"
SUB_ID = "sub-1"
SUB_TOKEN = "push-token-abc"

# Patch target for the inner coroutine that performs the HTTP POST, letting the
# debounce/scheduling logic run under test while the network call is stubbed.
SEND_PUSH = (
    "homeassistant.components.mobile_app.push_subscription"
    ".notify._send_subscription_push"
)
# async_get_clientsession as looked up inside notify.py - patched in the two
# direct _send_subscription_push tests so the POST never touches the network.
GET_SESSION = (
    "homeassistant.components.mobile_app.push_subscription"
    ".notify.async_get_clientsession"
)


def _mock_session_post(
    *,
    status: HTTPStatus | None = None,
    side_effect: Exception | type[Exception] | None = None,
) -> MagicMock:
    """Return a session whose .post() behaves as an async context manager.

    Mirrors aiohttp's ClientSession.post, which returns a context manager the
    caller enters with ``async with`` to obtain (and later release) the response.
    """
    cm = MagicMock()
    if side_effect is not None:
        cm.__aenter__ = AsyncMock(side_effect=side_effect)
    else:
        response = MagicMock()
        response.status = status
        cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.post = MagicMock(return_value=cm)
    return session


@pytest.fixture
async def push_webhook_id(hass: HomeAssistant, webhook_client: TestClient) -> str:
    """Register a cleartext, push-enabled device and return its webhook_id."""
    await async_setup_component(hass, DOMAIN, {DOMAIN: {}})

    resp = await webhook_client.post(
        "/api/mobile_app/registrations",
        json={**REGISTER_CLEARTEXT, "app_data": PUSH_APP_DATA},
    )
    assert resp.status == HTTPStatus.CREATED
    await hass.async_block_till_done()
    return (await resp.json())[CONF_WEBHOOK_ID]


async def _register_subscription(
    client: TestClient,
    webhook_id: str,
    *,
    sub_id: str = SUB_ID,
    token: str = SUB_TOKEN,
    entity_ids: list[str] | None = None,
    target: str | None = "lock_screen",
) -> None:
    """POST a register_push_subscription webhook command."""
    data: dict[str, Any] = {
        "subscription_id": sub_id,
        "push_token": token,
        "entity_ids": entity_ids or [TRACKED_ENTITY],
    }
    if target is not None:
        data["target"] = target
    resp = await client.post(
        f"/api/webhook/{webhook_id}",
        json={"type": "register_push_subscription", "data": data},
    )
    assert resp.status == HTTPStatus.OK


async def _update_registration(
    client: TestClient, webhook_id: str, app_data: dict[str, Any]
) -> None:
    """POST an update_registration webhook command."""
    response = await client.post(
        f"/api/webhook/{webhook_id}",
        json={"type": "update_registration", "data": {**UPDATE, "app_data": app_data}},
    )
    assert response.status == HTTPStatus.OK


KIND = "test_kind"
KIND_DEBOUNCE_SECONDS = 0.2


def _store_internal(
    hass: HomeAssistant,
    webhook_id: str,
    *,
    sub_id: str = SUB_ID,
    entity_ids: list[str] | None = None,
    kind: str | None = None,
    data: JsonObjectType | None = None,
    debounce_seconds: float | None = None,
    follow_entity_renames: bool = False,
) -> None:
    """Store a subscription the way a caller inside the integration does.

    Nothing here is reachable from the public webhook; these are the options an
    internal consumer of push subscriptions has.
    """
    store_push_subscription(
        hass,
        webhook_id,
        sub_id,
        SUB_TOKEN,
        entity_ids or [TRACKED_ENTITY],
        None,
        kind=kind,
        data=data,
        debounce_seconds=debounce_seconds,
        follow_entity_renames=follow_entity_renames,
    )


async def _settle(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, seconds: float
) -> None:
    """Advance past a debounce interval and let any delivery task finish."""
    await hass.async_block_till_done()
    freezer.tick(timedelta(seconds=seconds))
    async_fire_time_changed(hass)
    await hass.async_block_till_done(wait_background_tasks=True)


async def test_register_stores_subscription(
    hass: HomeAssistant, webhook_client: TestClient, push_webhook_id: str
) -> None:
    """Registering a subscription persists the mapping and arms a listener."""
    await _register_subscription(webhook_client, push_webhook_id)

    stored = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]
    # Exactly the public shape: a registration from the app carries no delivery
    # kind, no opaque data and no debounce of its own.
    assert stored == {
        PUSH_SUBSCRIPTION_TOKEN: SUB_TOKEN,
        PUSH_SUBSCRIPTION_ENTITY_IDS: [TRACKED_ENTITY],
        PUSH_SUBSCRIPTION_TARGET: "lock_screen",
    }
    assert SUB_ID in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][push_webhook_id]


async def test_no_listener_without_push_url(
    hass: HomeAssistant, webhook_client: TestClient
) -> None:
    """A registration with no cloud push URL stores the sub but arms no listener."""
    await async_setup_component(hass, DOMAIN, {DOMAIN: {}})
    resp = await webhook_client.post(
        "/api/mobile_app/registrations", json=REGISTER_CLEARTEXT
    )
    assert resp.status == HTTPStatus.CREATED
    webhook_id = (await resp.json())[CONF_WEBHOOK_ID]
    await hass.async_block_till_done()

    await _register_subscription(webhook_client, webhook_id)

    # Mapping is stored, but no state-change listener is armed since this
    # registration can never send a push.
    assert SUB_ID in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][webhook_id]
    assert webhook_id not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS]


async def test_a_pending_push_survives_an_unrelated_update_registration(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """An app version change is not a reason to lose a push the device is owed.

    Re-arming a subscription replaces its listener, and that cancels the debounce timer
    already counting down for it, so a change the app was about to be told about would
    simply never arrive.
    """
    await _register_subscription(webhook_client, push_webhook_id)
    freezer.move_to("2026-01-01 00:00:00+00:00")

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await hass.async_block_till_done()
        # Mid-debounce, and about something that has nothing to do with pushing.
        await _update_registration(webhook_client, push_webhook_id, PUSH_APP_DATA)
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    assert mock_send.call_count == 1


async def test_gaining_a_push_url_arms_a_stored_subscription(
    hass: HomeAssistant,
    webhook_client: TestClient,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A subscription registered before there was anywhere to send it starts working."""
    await async_setup_component(hass, DOMAIN, {DOMAIN: {}})
    response = await webhook_client.post(
        "/api/mobile_app/registrations", json=REGISTER_CLEARTEXT
    )
    assert response.status == HTTPStatus.CREATED
    webhook_id = (await response.json())[CONF_WEBHOOK_ID]
    await hass.async_block_till_done()
    await _register_subscription(webhook_client, webhook_id)
    assert webhook_id not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS]
    freezer.move_to("2026-01-01 00:00:00+00:00")

    await _update_registration(webhook_client, webhook_id, PUSH_APP_DATA)
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][webhook_id]) == 1

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    assert mock_send.call_count == 1


async def test_losing_a_push_url_stops_listening_but_keeps_the_subscription(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """There is nowhere to deliver any more, but the app should not have to re-register."""
    await _register_subscription(webhook_client, push_webhook_id)
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][push_webhook_id]) == 1
    freezer.move_to("2026-01-01 00:00:00+00:00")

    await _update_registration(webhook_client, push_webhook_id, {"foo": "bar"})

    assert push_webhook_id not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS]
    assert push_webhook_id not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEBOUNCE]
    # The mapping survives, so a push URL coming back resumes it.
    assert hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID][
        PUSH_SUBSCRIPTION_ENTITY_IDS
    ] == [TRACKED_ENTITY]

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    assert mock_send.call_count == 0


async def test_repeated_push_capable_update_registrations_do_not_duplicate_listeners(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
) -> None:
    """Staying push-capable leaves the subscription's listeners exactly as they are."""
    await _register_subscription(webhook_client, push_webhook_id)
    armed = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][push_webhook_id][SUB_ID]

    for _ in range(2):
        await _update_registration(webhook_client, push_webhook_id, PUSH_APP_DATA)
        device_unsubs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][
            push_webhook_id
        ]
        assert len(device_unsubs) == 1
        # The same listener, not a replacement: nothing was torn down and re-armed.
        assert device_unsubs[SUB_ID] is armed


async def test_register_is_idempotent(
    hass: HomeAssistant, webhook_client: TestClient, push_webhook_id: str
) -> None:
    """Re-registering the same id updates token/entities in place."""
    await _register_subscription(webhook_client, push_webhook_id)
    await _register_subscription(
        webhook_client,
        push_webhook_id,
        token="rotated-token",
        entity_ids=["switch.fan"],
    )

    device_subs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id]
    assert len(device_subs) == 1
    assert device_subs[SUB_ID][PUSH_SUBSCRIPTION_TOKEN] == "rotated-token"
    assert device_subs[SUB_ID][PUSH_SUBSCRIPTION_ENTITY_IDS] == ["switch.fan"]


async def test_register_dedupes_entity_ids(
    hass: HomeAssistant, webhook_client: TestClient, push_webhook_id: str
) -> None:
    """Duplicate entity_ids collapse so a listener is not armed twice."""
    await _register_subscription(
        webhook_client,
        push_webhook_id,
        entity_ids=[TRACKED_ENTITY, TRACKED_ENTITY, "switch.fan"],
    )

    stored = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]
    assert stored[PUSH_SUBSCRIPTION_ENTITY_IDS] == [TRACKED_ENTITY, "switch.fan"]


async def test_subscription_count_capped_per_device(
    hass: HomeAssistant, webhook_client: TestClient, push_webhook_id: str
) -> None:
    """Registering past the per-device cap evicts the oldest subscription."""
    for i in range(PUSH_SUBSCRIPTION_MAX_PER_DEVICE + 1):
        await _register_subscription(
            webhook_client,
            push_webhook_id,
            sub_id=f"sub-{i}",
            entity_ids=[f"light.l{i}"],
        )

    device_subs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id]
    device_unsubs = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][push_webhook_id]
    # Count stays at the cap; the oldest was evicted and the newest kept.
    assert len(device_subs) == PUSH_SUBSCRIPTION_MAX_PER_DEVICE
    assert len(device_unsubs) == PUSH_SUBSCRIPTION_MAX_PER_DEVICE
    assert "sub-0" not in device_subs
    assert "sub-0" not in device_unsubs
    assert f"sub-{PUSH_SUBSCRIPTION_MAX_PER_DEVICE}" in device_subs


async def test_state_change_sends_push_after_debounce(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A state change sends exactly one silent push after the debounce window."""
    freezer.move_to("2026-01-01 00:00:00+00:00")
    await _register_subscription(webhook_client, push_webhook_id)

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await hass.async_block_till_done()
        # Nothing sent yet - still inside the debounce window.
        assert mock_send.call_count == 0

        freezer.tick(timedelta(seconds=PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1))
        async_fire_time_changed(hass)
        await hass.async_block_till_done(wait_background_tasks=True)

    assert mock_send.call_count == 1
    # _send_subscription_push(hass, entry, sub_id, sub)
    _, _, sub_id, sub = mock_send.call_args.args
    assert sub_id == SUB_ID
    assert sub[PUSH_SUBSCRIPTION_TOKEN] == SUB_TOKEN
    assert sub[PUSH_SUBSCRIPTION_TARGET] == "lock_screen"


async def test_burst_collapses_to_single_push(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A burst of changes within the window collapses to one push."""
    freezer.move_to("2026-01-01 00:00:00+00:00")
    await _register_subscription(webhook_client, push_webhook_id)

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        for i in range(5):
            hass.states.async_set(TRACKED_ENTITY, f"level-{i}")
            await hass.async_block_till_done()
            freezer.tick(timedelta(seconds=1))  # < window, restarts the clock
            async_fire_time_changed(hass)
            await hass.async_block_till_done()

        assert mock_send.call_count == 0

        freezer.tick(timedelta(seconds=PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1))
        async_fire_time_changed(hass)
        await hass.async_block_till_done(wait_background_tasks=True)

    assert mock_send.call_count == 1


async def test_separated_changes_send_separate_pushes(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Two changes spaced beyond the window send two pushes."""
    freezer.move_to("2026-01-01 00:00:00+00:00")
    await _register_subscription(webhook_client, push_webhook_id)

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        for value in ("on", "off"):
            hass.states.async_set(TRACKED_ENTITY, value)
            await hass.async_block_till_done()
            freezer.tick(timedelta(seconds=PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1))
            async_fire_time_changed(hass)
            await hass.async_block_till_done(wait_background_tasks=True)

    assert mock_send.call_count == 2


async def test_untracked_entity_does_not_push(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A change to an entity not in the subscription sends nothing."""
    freezer.move_to("2026-01-01 00:00:00+00:00")
    await _register_subscription(webhook_client, push_webhook_id)

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set("sensor.unrelated", "123")
        await hass.async_block_till_done()
        freezer.tick(timedelta(seconds=PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1))
        async_fire_time_changed(hass)
        await hass.async_block_till_done(wait_background_tasks=True)

    assert mock_send.call_count == 0


async def test_remove_subscription(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Removing a subscription stops pushes and clears the mapping."""
    freezer.move_to("2026-01-01 00:00:00+00:00")
    await _register_subscription(webhook_client, push_webhook_id)

    resp = await webhook_client.post(
        f"/api/webhook/{push_webhook_id}",
        json={
            "type": "remove_push_subscription",
            "data": {"subscription_id": SUB_ID},
        },
    )
    assert resp.status == HTTPStatus.OK
    assert push_webhook_id not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS]

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await hass.async_block_till_done()
        freezer.tick(timedelta(seconds=PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1))
        async_fire_time_changed(hass)
        await hass.async_block_till_done(wait_background_tasks=True)

    assert mock_send.call_count == 0


async def test_pending_push_cancelled_on_unload(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Unloading the entry cancels an in-flight debounce timer."""
    freezer.move_to("2026-01-01 00:00:00+00:00")
    await _register_subscription(webhook_client, push_webhook_id)

    hass.states.async_set(TRACKED_ENTITY, "on")
    await hass.async_block_till_done()
    # A debounce timer is pending.
    assert push_webhook_id in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_DEBOUNCE]

    entry = hass.config_entries.async_entries(DOMAIN)[0]
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        freezer.tick(timedelta(seconds=PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1))
        async_fire_time_changed(hass)
        await hass.async_block_till_done(wait_background_tasks=True)

    assert mock_send.call_count == 0


async def test_subscription_restored_after_reload(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A stored subscription survives an entry reload and still pushes."""
    freezer.move_to("2026-01-01 00:00:00+00:00")
    await _register_subscription(webhook_client, push_webhook_id)

    entry = hass.config_entries.async_entries(DOMAIN)[0]
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED

    # Mapping persisted and listener re-armed.
    assert SUB_ID in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id]

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await hass.async_block_till_done()
        freezer.tick(timedelta(seconds=PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1))
        async_fire_time_changed(hass)
        await hass.async_block_till_done(wait_background_tasks=True)

    assert mock_send.call_count == 1


async def test_send_push_posts_payload(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
) -> None:
    """The push POST carries the token, trigger marker and registration info."""
    await _register_subscription(webhook_client, push_webhook_id)

    entry = hass.config_entries.async_entries(DOMAIN)[0]
    sub = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]

    session = _mock_session_post(status=HTTPStatus.CREATED)

    with patch(GET_SESSION, return_value=session):
        await _send_subscription_push(hass, entry, SUB_ID, sub)

    assert session.post.call_count == 1
    url = session.post.call_args.args[0]
    payload = session.post.call_args.kwargs["json"]
    assert url == PUSH_URL
    assert payload[PUSH_SUBSCRIPTION_TOKEN] == SUB_TOKEN
    assert payload[PUSH_SUBSCRIPTION_TRIGGER][PUSH_SUBSCRIPTION_ID] == SUB_ID
    assert payload[PUSH_SUBSCRIPTION_TRIGGER][PUSH_SUBSCRIPTION_TARGET] == "lock_screen"
    assert payload["registration_info"]["webhook_id"] == push_webhook_id


async def test_send_push_omits_target_when_unset(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
) -> None:
    """The payload leaves out target entirely when it was not registered."""
    await _register_subscription(webhook_client, push_webhook_id, target=None)

    entry = hass.config_entries.async_entries(DOMAIN)[0]
    sub = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]

    session = _mock_session_post(status=HTTPStatus.CREATED)

    with patch(GET_SESSION, return_value=session):
        await _send_subscription_push(hass, entry, SUB_ID, sub)

    payload = session.post.call_args.kwargs["json"]
    assert PUSH_SUBSCRIPTION_TARGET not in payload[PUSH_SUBSCRIPTION_TRIGGER]
    assert payload[PUSH_SUBSCRIPTION_TRIGGER][PUSH_SUBSCRIPTION_ID] == SUB_ID


async def test_send_push_swallows_client_error(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
) -> None:
    """A transport error is swallowed - silent pushes are best-effort."""
    await _register_subscription(webhook_client, push_webhook_id)

    entry = hass.config_entries.async_entries(DOMAIN)[0]
    sub = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]

    session = _mock_session_post(side_effect=ClientError())

    with patch(GET_SESSION, return_value=session):
        # Must not raise.
        await _send_subscription_push(hass, entry, SUB_ID, sub)

    assert session.post.call_count == 1


async def test_a_public_subscription_does_not_follow_an_entity_rename(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    entity_registry: er.EntityRegistry,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A registration named entity_ids and meant them.

    Whether a relationship is with the entity or with the string that names it is the
    consumer's policy, and the public webhook has no way to say. So a rename leaves the
    mapping exactly as registered, which is what the app already expects.
    """
    entry = entity_registry.async_get_or_create(
        "light", "test", "lamp-1", suggested_object_id="living_room"
    )
    assert entry.entity_id == TRACKED_ENTITY
    await _register_subscription(webhook_client, push_webhook_id)

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        entity_registry.async_update_entity(
            TRACKED_ENTITY, new_entity_id="light.lounge"
        )
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    # Unchanged, and still exactly the shape a public registration has always stored:
    # opting out is the absence of a key, not a stored false.
    assert hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID] == {
        PUSH_SUBSCRIPTION_TOKEN: SUB_TOKEN,
        PUSH_SUBSCRIPTION_ENTITY_IDS: [TRACKED_ENTITY],
        PUSH_SUBSCRIPTION_TARGET: "lock_screen",
    }
    assert mock_send.call_count == 0


async def test_an_opted_in_subscription_moves_onto_the_new_entity_id(
    hass: HomeAssistant,
    push_webhook_id: str,
    entity_registry: er.EntityRegistry,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A subscription that is with the entity is moved onto its new identifier.

    Leaving it alone would have it listening for an identifier nothing will ever report
    again, with no way for the consumer to find out.
    """
    entry = entity_registry.async_get_or_create(
        "light", "test", "lamp-1", suggested_object_id="living_room"
    )
    assert entry.entity_id == TRACKED_ENTITY
    _store_internal(
        hass,
        push_webhook_id,
        entity_ids=[TRACKED_ENTITY, "light.hall"],
        follow_entity_renames=True,
    )

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        entity_registry.async_update_entity(
            TRACKED_ENTITY, new_entity_id="light.lounge"
        )
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    stored = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]
    assert stored[PUSH_SUBSCRIPTION_ENTITY_IDS] == ["light.lounge", "light.hall"]
    assert stored[PUSH_SUBSCRIPTION_TOKEN] == SUB_TOKEN
    # Re-arming must not lose what the subscription asked for.
    assert stored[PUSH_SUBSCRIPTION_FOLLOW_RENAMES] is True
    # One listener entry for the subscription, and the rename is worth telling it about.
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][push_webhook_id]) == 1
    assert mock_send.call_count == 1

    # The state listener moved with it: the old identifier is no longer watched.
    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)
        assert mock_send.call_count == 0

        hass.states.async_set("light.lounge", "on")
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    assert mock_send.call_count == 1


async def test_a_rename_onto_an_already_tracked_entity_id_does_not_duplicate_it(
    hass: HomeAssistant,
    push_webhook_id: str,
    entity_registry: er.EntityRegistry,
) -> None:
    """Two tracked entities collapsing into one must not arm the same listener twice."""
    entry = entity_registry.async_get_or_create(
        "light", "test", "lamp-1", suggested_object_id="living_room"
    )
    assert entry.entity_id == TRACKED_ENTITY
    _store_internal(
        hass,
        push_webhook_id,
        entity_ids=[TRACKED_ENTITY, "light.hall"],
        follow_entity_renames=True,
    )

    entity_registry.async_update_entity(TRACKED_ENTITY, new_entity_id="light.hall")
    await hass.async_block_till_done()

    stored = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]
    assert stored[PUSH_SUBSCRIPTION_ENTITY_IDS] == ["light.hall"]
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][push_webhook_id]) == 1


async def test_the_rename_opt_in_survives_a_restore(
    hass: HomeAssistant,
    push_webhook_id: str,
    entity_registry: er.EntityRegistry,
    freezer: FrozenDateTimeFactory,
) -> None:
    """The opt-in is persisted, so restoring a subscription re-arms what it asked for."""
    entry = entity_registry.async_get_or_create(
        "light", "test", "lamp-1", suggested_object_id="living_room"
    )
    assert entry.entity_id == TRACKED_ENTITY
    _store_internal(hass, push_webhook_id, follow_entity_renames=True)

    async_restore_push_subscriptions(hass, push_webhook_id)

    with patch(SEND_PUSH, new_callable=AsyncMock):
        entity_registry.async_update_entity(
            TRACKED_ENTITY, new_entity_id="light.lounge"
        )
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    stored = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]
    assert stored[PUSH_SUBSCRIPTION_ENTITY_IDS] == ["light.lounge"]
    assert stored[PUSH_SUBSCRIPTION_FOLLOW_RENAMES] is True


@pytest.fixture
def deliveries() -> Generator[list[tuple[str, str, dict[str, Any]]]]:
    """Register a delivery kind for the duration of one test."""
    calls: list[tuple[str, str, dict[str, Any]]] = []

    async def _deliver(
        hass: HomeAssistant,
        entry: ConfigEntry,
        webhook_id: str,
        sub_id: str,
        subscription: dict[str, Any],
    ) -> None:
        calls.append((webhook_id, sub_id, subscription))

    PUSH_SUBSCRIPTION_DELIVERIES[KIND] = _deliver
    yield calls
    del PUSH_SUBSCRIPTION_DELIVERIES[KIND]


async def test_a_kind_is_delivered_by_its_own_registered_implementation(
    hass: HomeAssistant,
    push_webhook_id: str,
    deliveries: list[tuple[str, str, dict[str, Any]]],
    freezer: FrozenDateTimeFactory,
) -> None:
    """A subscription with a kind is handed to that kind, not pushed generically."""
    _store_internal(hass, push_webhook_id, kind=KIND, data={"seen": 1})

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    assert mock_send.call_count == 0
    assert deliveries == [
        (
            push_webhook_id,
            SUB_ID,
            hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID],
        )
    ]


async def test_an_unregistered_kind_is_dropped_rather_than_pushed_generically(
    hass: HomeAssistant,
    push_webhook_id: str,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Nothing knows how to build this payload, and the generic push is not it."""
    _store_internal(hass, push_webhook_id, kind="kind_nothing_registered")

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    assert mock_send.call_count == 0


async def test_a_kind_may_coalesce_on_a_shorter_interval_of_its_own(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    deliveries: list[tuple[str, str, dict[str, Any]]],
    freezer: FrozenDateTimeFactory,
) -> None:
    """A kind's interval is its own; the generic default is untouched by it."""
    await _register_subscription(webhook_client, push_webhook_id, sub_id="generic")
    _store_internal(
        hass, push_webhook_id, kind=KIND, debounce_seconds=KIND_DEBOUNCE_SECONDS
    )

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        hass.states.async_set(TRACKED_ENTITY, "on")
        await _settle(hass, freezer, KIND_DEBOUNCE_SECONDS + 0.1)
        assert len(deliveries) == 1
        assert mock_send.call_count == 0

        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS)

    assert len(deliveries) == 1
    assert mock_send.call_count == 1


async def test_restoring_a_kind_reconciles_what_the_device_was_last_sent(
    hass: HomeAssistant,
    webhook_client: TestClient,
    push_webhook_id: str,
    deliveries: list[tuple[str, str, dict[str, Any]]],
    freezer: FrozenDateTimeFactory,
) -> None:
    """A kind sends state, so restoring one asks it to bring the device up to date.

    A generic subscription carries no state and stays quiet until something changes.
    """
    await _register_subscription(webhook_client, push_webhook_id, sub_id="generic")
    _store_internal(hass, push_webhook_id, kind=KIND)

    with patch(SEND_PUSH, new_callable=AsyncMock) as mock_send:
        async_restore_push_subscriptions(hass, push_webhook_id)
        await _settle(hass, freezer, PUSH_SUBSCRIPTION_DEBOUNCE_SECONDS + 1)

    assert len(deliveries) == 1
    assert mock_send.call_count == 0


async def test_opaque_data_is_not_written_back_to_a_replaced_subscription(
    hass: HomeAssistant,
    push_webhook_id: str,
) -> None:
    """Delivery is asynchronous, so what it started from may no longer be stored."""
    _store_internal(hass, push_webhook_id, kind=KIND, data={"seen": 1})
    started_from = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]

    assert update_push_subscription_data(
        hass, push_webhook_id, SUB_ID, started_from, {"seen": 2}
    )
    _store_internal(hass, push_webhook_id, kind=KIND, data={"seen": 3})

    assert not update_push_subscription_data(
        hass, push_webhook_id, SUB_ID, started_from, {"seen": 4}
    )
    stored = hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][push_webhook_id][SUB_ID]
    assert stored[PUSH_SUBSCRIPTION_DATA] == {"seen": 3}
