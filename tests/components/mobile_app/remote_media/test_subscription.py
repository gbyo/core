"""Remote Now Playing on top of generic mobile_app push subscriptions."""

import asyncio
from datetime import timedelta
from http import HTTPStatus
import logging
from typing import Any
from unittest.mock import patch

from aiohttp.test_utils import TestClient
import pytest

from homeassistant.components.mobile_app.const import (
    DATA_CONFIG_ENTRIES,
    DATA_DELETED_IDS,
    DATA_LIVE_ACTIVITY_TOKENS,
    DATA_PUSH_SUBSCRIPTION_UNSUBS,
    DATA_PUSH_SUBSCRIPTIONS,
    DOMAIN,
    PUSH_SUBSCRIPTION_DATA,
    PUSH_SUBSCRIPTION_DEBOUNCE,
    PUSH_SUBSCRIPTION_ENTITY_IDS,
    PUSH_SUBSCRIPTION_KIND,
    PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA,
    PUSH_SUBSCRIPTION_TARGET,
    PUSH_SUBSCRIPTION_TOKEN,
    STORAGE_KEY,
    STORAGE_VERSION,
    STORAGE_VERSION_MINOR,
)
from homeassistant.components.mobile_app.remote_media.const import (
    COALESCE_SECONDS,
    CONTEXT_GENERATION,
    CONTEXT_GENERATION_SEQUENCE,
    CONTEXT_LAST_SNAPSHOT,
    CONTEXT_LAST_TIMESTAMP,
    CONTEXT_SCHEMA_VERSION,
    CONTEXT_SERVER_ID,
)
from homeassistant.components.mobile_app.remote_media.push import PushOutcome
from homeassistant.components.mobile_app.remote_media.subscription import (
    async_deliver_subscription,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.setup import async_setup_component
from homeassistant.util import dt as dt_util

from .const import (
    ENTITY_ID,
    GENERATION,
    LATER_GENERATION,
    LATER_PUSH_TOKEN,
    LATER_SEQUENCE,
    PLAYING_ATTRIBUTES,
    PUSH_TOKEN,
    PUSH_URL,
    SEQUENCE,
    SERVER_ID,
    SESSION_ID,
    dismissal_payload,
    registration_payload,
)

from tests.common import MockConfigEntry, MockUser, async_fire_time_changed
from tests.test_util.aiohttp import AiohttpClientMocker

NEXT_TRACK = {
    **PLAYING_ATTRIBUTES,
    "media_content_id": "track-2",
    "media_title": "Second",
    "media_artist": "Another Artist",
    "media_position": 0,
    "media_position_updated_at": "2026-09-07T00:04:00+00:00",
}
SEND_REMOTE_MEDIA = (
    "homeassistant.components.mobile_app.remote_media.subscription.async_send"
)


async def _post(
    client: TestClient, webhook_id: str, webhook_type: str, data: dict[str, Any]
) -> None:
    """Post one mobile_app webhook command."""
    response = await client.post(
        f"/api/webhook/{webhook_id}", json={"type": webhook_type, "data": data}
    )
    assert response.status == HTTPStatus.OK


async def _register(client: TestClient, webhook_id: str, **overrides: Any) -> None:
    """Register one RemoteMedia Follow relationship."""
    await _post(
        client,
        webhook_id,
        "remote_media_session_token",
        registration_payload(**overrides),
    )


async def _settle(hass: HomeAssistant) -> None:
    """Let the subscription debounce expire and its background send finish."""
    async_fire_time_changed(
        hass, dt_util.utcnow() + timedelta(seconds=COALESCE_SECONDS + 0.1)
    )
    await hass.async_block_till_done(wait_background_tasks=True)


def _subscription(hass: HomeAssistant, webhook_id: str) -> dict[str, Any]:
    """Return the generic subscription representing the Follow relationship."""
    return hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][webhook_id][SESSION_ID]


def _payloads(mock: AiohttpClientMocker) -> list[dict[str, Any]]:
    """Return JSON bodies sent to the push relay."""
    return [call[2] for call in mock.mock_calls]


async def _deliver_current_state(hass: HomeAssistant, webhook_id: str) -> None:
    """Deliver the current entity state through the RemoteMedia adapter."""
    await async_deliver_subscription(
        hass,
        hass.data[DOMAIN][DATA_CONFIG_ENTRIES][webhook_id],
        webhook_id,
        SESSION_ID,
        _subscription(hass, webhook_id),
    )


async def _run_overlapping_deliveries(
    hass: HomeAssistant,
    webhook_id: str,
    second_outcome: PushOutcome,
) -> list[int]:
    """Complete a newer delivery before releasing an older one."""
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    timestamps: list[int] = []

    async def controlled_send(*args: Any) -> PushOutcome:
        timestamps.append(args[5])
        if len(timestamps) == 1:
            first_started.set()
            await release_first.wait()
            return PushOutcome.DELIVERED
        return second_outcome

    with patch(SEND_REMOTE_MEDIA, side_effect=controlled_send):
        hass.states.async_set(ENTITY_ID, "paused", PLAYING_ATTRIBUTES)
        first_delivery = asyncio.create_task(
            _deliver_current_state(hass, webhook_id), name="older_remote_media_delivery"
        )
        await first_started.wait()

        hass.states.async_set(ENTITY_ID, "playing", NEXT_TRACK)
        await _deliver_current_state(hass, webhook_id)

        release_first.set()
        await first_delivery

    return timestamps


@pytest.fixture
async def registration(
    hass: HomeAssistant,
    create_registrations: tuple[dict[str, Any], dict[str, Any]],
    aioclient_mock: AiohttpClientMocker,
) -> str:
    """Return a push-capable registration with a current media player state."""
    aioclient_mock.post(PUSH_URL, status=HTTPStatus.CREATED, json={})
    hass.states.async_set(ENTITY_ID, "playing", PLAYING_ATTRIBUTES)
    webhook_id = create_registrations[1]["webhook_id"]
    entry = hass.data[DOMAIN][DATA_CONFIG_ENTRIES][webhook_id]
    hass.config_entries.async_update_entry(
        entry,
        data={
            **entry.data,
            "app_data": {"push_token": "COMPANION_TOKEN", "push_url": PUSH_URL},
        },
    )
    await hass.async_block_till_done()
    return webhook_id


@pytest.fixture
async def following(
    hass: HomeAssistant,
    registration: str,
    webhook_client: TestClient,
    aioclient_mock: AiohttpClientMocker,
) -> str:
    """Return a Follow whose initial snapshot was successfully delivered."""
    await _register(webhook_client, registration)
    await _settle(hass)
    assert len(_payloads(aioclient_mock)) == 1
    aioclient_mock.clear_requests()
    aioclient_mock.post(PUSH_URL, status=HTTPStatus.CREATED, json={})
    return registration


async def test_registration_is_a_generic_subscription(
    hass: HomeAssistant, registration: str, webhook_client: TestClient
) -> None:
    """RemoteMedia stores no parallel session or listener structure."""
    await _register(webhook_client, registration)

    subscription = _subscription(hass, registration)
    assert subscription[PUSH_SUBSCRIPTION_TOKEN] == PUSH_TOKEN
    assert subscription[PUSH_SUBSCRIPTION_ENTITY_IDS] == [ENTITY_ID]
    assert subscription[PUSH_SUBSCRIPTION_TARGET] is None
    assert subscription[PUSH_SUBSCRIPTION_KIND] == PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA
    assert subscription[PUSH_SUBSCRIPTION_DEBOUNCE] == COALESCE_SECONDS
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][registration]) == 1
    assert "remote_media_sessions" not in hass.data[DOMAIN]
    assert "remote_media_manager" not in hass.data[DOMAIN]

    assert subscription[PUSH_SUBSCRIPTION_DATA] == {
        CONTEXT_GENERATION: GENERATION,
        CONTEXT_GENERATION_SEQUENCE: SEQUENCE,
        CONTEXT_SERVER_ID: SERVER_ID,
        CONTEXT_SCHEMA_VERSION: 1,
        CONTEXT_LAST_TIMESTAMP: None,
        CONTEXT_LAST_SNAPSHOT: None,
    }


async def test_initial_registration_reconciles_through_relay_contract(
    hass: HomeAssistant,
    registration: str,
    webhook_client: TestClient,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """Registration schedules the current state without another entity event."""
    await _register(webhook_client, registration)
    assert _payloads(aioclient_mock) == []
    await _settle(hass)

    [payload] = _payloads(aioclient_mock)
    assert payload["push_token"] == "COMPANION_TOKEN"
    assert payload["now_playing_token"] == PUSH_TOKEN
    assert payload["registration_info"]["webhook_id"] == registration
    assert set(payload) == {
        "push_token",
        "now_playing_token",
        "now_playing",
        "registration_info",
    }
    now_playing = payload["now_playing"]
    assert set(now_playing) == {"event", "timestamp", "attributes"}
    assert now_playing["event"] == "update"
    assert isinstance(now_playing["timestamp"], int)
    attributes = now_playing["attributes"]
    assert attributes["id"] == SESSION_ID
    assert attributes["generation"] == GENERATION
    assert attributes["generationSequence"] == SEQUENCE
    assert attributes["snapshot"]["selection"] == {
        "serverId": SERVER_ID,
        "entityId": ENTITY_ID,
    }


async def test_one_state_change_uses_one_generic_debounced_delivery(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """One state event produces one RemoteMedia delivery after 0.2 seconds."""
    hass.states.async_set(ENTITY_ID, "playing", NEXT_TRACK)
    await hass.async_block_till_done()
    assert _payloads(aioclient_mock) == []
    await _settle(hass)
    assert len(_payloads(aioclient_mock)) == 1


async def test_ordinary_playback_progress_does_not_push(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """Expected position advancement carries no new information."""
    progressed = {
        **PLAYING_ATTRIBUTES,
        "media_position": 20,
        "media_position_updated_at": "2026-09-07T00:00:10+00:00",
    }
    hass.states.async_set(ENTITY_ID, "playing", progressed)
    await _settle(hass)
    assert _payloads(aioclient_mock) == []


async def test_track_change_pushes(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """A new content identity produces an update."""
    hass.states.async_set(ENTITY_ID, "playing", NEXT_TRACK)
    await _settle(hass)
    [payload] = _payloads(aioclient_mock)
    assert payload["now_playing"]["attributes"]["snapshot"]["title"] == "Second"


async def test_play_to_pause_pushes(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """A collapsed playback transition produces an update."""
    hass.states.async_set(ENTITY_ID, "paused", PLAYING_ATTRIBUTES)
    await _settle(hass)
    [payload] = _payloads(aioclient_mock)
    assert payload["now_playing"]["attributes"]["snapshot"]["state"] == "paused"


async def test_real_seek_pushes(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """A position far from expected playback is treated as a seek."""
    sought = {
        **PLAYING_ATTRIBUTES,
        "media_position": 80,
        "media_position_updated_at": "2026-09-07T00:00:10+00:00",
    }
    hass.states.async_set(ENTITY_ID, "playing", sought)
    await _settle(hass)
    [payload] = _payloads(aioclient_mock)
    assert payload["now_playing"]["attributes"]["snapshot"]["position"] == 80


async def test_transient_blank_does_not_erase_media(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """A brief blank idle report settles to the meaningful track that follows it."""
    hass.states.async_set(ENTITY_ID, "idle", {"friendly_name": "Speaker"})
    await hass.async_block_till_done()
    hass.states.async_set(ENTITY_ID, "playing", NEXT_TRACK)
    await _settle(hass)

    [payload] = _payloads(aioclient_mock)
    snapshot = payload["now_playing"]["attributes"]["snapshot"]
    assert snapshot["title"] == "Second"
    assert snapshot["state"] == "playing"


async def test_same_lifetime_is_idempotent(
    hass: HomeAssistant,
    following: str,
    webhook_client: TestClient,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """The same registration neither duplicates nor reconciles again."""
    previous = _subscription(hass, following)
    await _register(webhook_client, following)
    await _settle(hass)
    assert _subscription(hass, following) is previous
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][following]) == 1
    assert _payloads(aioclient_mock) == []


async def test_token_rotation_updates_one_subscription_and_keeps_context(
    hass: HomeAssistant,
    following: str,
    webhook_client: TestClient,
) -> None:
    """A token replacement preserves the current lifetime's published state."""
    old_context = _subscription(hass, following)[PUSH_SUBSCRIPTION_DATA]
    await _register(webhook_client, following, push_token=LATER_PUSH_TOKEN)

    stored = _subscription(hass, following)
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][following]) == 1
    assert stored[PUSH_SUBSCRIPTION_TOKEN] == LATER_PUSH_TOKEN
    assert stored[PUSH_SUBSCRIPTION_DATA] == old_context


async def test_stale_registration_is_rejected(
    hass: HomeAssistant,
    registration: str,
    webhook_client: TestClient,
) -> None:
    """An older Follow cannot replace a newer lifetime that arrived first."""
    await _register(
        webhook_client,
        registration,
        generation=LATER_GENERATION,
        generation_sequence=LATER_SEQUENCE,
        push_token=LATER_PUSH_TOKEN,
    )
    await _register(webhook_client, registration)

    stored = _subscription(hass, registration)
    assert stored[PUSH_SUBSCRIPTION_TOKEN] == LATER_PUSH_TOKEN
    assert stored[PUSH_SUBSCRIPTION_DATA][CONTEXT_GENERATION_SEQUENCE] == LATER_SEQUENCE


async def test_conflicting_same_sequence_registration_is_rejected(
    hass: HomeAssistant,
    registration: str,
    webhook_client: TestClient,
) -> None:
    """Two generations claiming one sequence cannot replace each other."""
    await _register(webhook_client, registration)
    await _register(
        webhook_client,
        registration,
        generation=LATER_GENERATION,
        push_token=LATER_PUSH_TOKEN,
    )

    stored = _subscription(hass, registration)
    assert stored[PUSH_SUBSCRIPTION_TOKEN] == PUSH_TOKEN
    assert stored[PUSH_SUBSCRIPTION_DATA][CONTEXT_GENERATION] == GENERATION


async def test_newer_lifetime_replaces_subscription_without_old_snapshot(
    hass: HomeAssistant,
    following: str,
    webhook_client: TestClient,
) -> None:
    """A genuinely newer Follow starts with a fresh clock and comparison state."""
    await _register(
        webhook_client,
        following,
        generation=LATER_GENERATION,
        generation_sequence=LATER_SEQUENCE,
        push_token=LATER_PUSH_TOKEN,
    )

    stored = _subscription(hass, following)
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][following]) == 1
    assert stored[PUSH_SUBSCRIPTION_TOKEN] == LATER_PUSH_TOKEN
    assert stored[PUSH_SUBSCRIPTION_DATA][CONTEXT_LAST_TIMESTAMP] is None
    assert stored[PUSH_SUBSCRIPTION_DATA][CONTEXT_LAST_SNAPSHOT] is None


async def test_invalid_update_token_is_rejected_without_logging_it(
    hass: HomeAssistant,
    registration: str,
    webhook_client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A malformed APNs destination is neither stored nor written to logs."""
    invalid_token = "not-a-valid-remote-media-token"
    with caplog.at_level(logging.DEBUG):
        await _register(webhook_client, registration, push_token=invalid_token)
        await hass.async_block_till_done()

    assert invalid_token not in caplog.text
    assert registration not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS]


async def test_dismissal_only_removes_current_lifetime(
    hass: HomeAssistant,
    registration: str,
    webhook_client: TestClient,
) -> None:
    """A stale dismissal cannot remove a newer relationship."""
    await _register(
        webhook_client,
        registration,
        generation=LATER_GENERATION,
        generation_sequence=LATER_SEQUENCE,
    )
    await _post(
        webhook_client,
        registration,
        "remote_media_session_dismissed",
        dismissal_payload(),
    )
    assert SESSION_ID in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][registration]

    await _post(
        webhook_client,
        registration,
        "remote_media_session_dismissed",
        dismissal_payload(
            generation=LATER_GENERATION, generation_sequence=LATER_SEQUENCE
        ),
    )
    assert registration not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS]
    assert registration not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS]


async def test_reload_restores_listener_context_and_delivery(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """Generic lifecycle restoration retains RemoteMedia comparison state."""
    entry = hass.data[DOMAIN][DATA_CONFIG_ENTRIES][following]
    context = _subscription(hass, following)[PUSH_SUBSCRIPTION_DATA]
    assert context[CONTEXT_LAST_SNAPSHOT] is not None

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    assert _subscription(hass, following)[PUSH_SUBSCRIPTION_DATA] == context
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][following]) == 1

    hass.states.async_set(ENTITY_ID, "playing", NEXT_TRACK)
    await _settle(hass)
    assert len(_payloads(aioclient_mock)) == 1


async def test_restart_restores_listener_and_reconciles_current_state(
    hass: HomeAssistant,
    hass_storage: dict[str, Any],
    hass_admin_user: MockUser,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """Startup restores and reconciles without a new entity state event."""
    webhook_id = "restored-remote-media"
    last_snapshot = {
        "server_id": SERVER_ID,
        "entity_id": ENTITY_ID,
        "device_name": "Speaker",
        "state": "playing",
        "device_class": None,
        "title": "First",
        "artist": "Artist",
        "album": "Album",
        "content_id": "track-1",
        "duration": 240.0,
        "position": 10.0,
        "position_updated_at_unix": 1788739200.0,
        "volume": 0.4,
        "is_muted": False,
        "features": 84037,
        "artwork_url": None,
    }
    context = {
        CONTEXT_GENERATION: GENERATION,
        CONTEXT_GENERATION_SEQUENCE: SEQUENCE,
        CONTEXT_SERVER_ID: SERVER_ID,
        CONTEXT_SCHEMA_VERSION: 1,
        CONTEXT_LAST_TIMESTAMP: 1788739200,
        CONTEXT_LAST_SNAPSHOT: last_snapshot,
    }
    hass_storage[STORAGE_KEY] = {
        "key": STORAGE_KEY,
        "version": STORAGE_VERSION,
        "minor_version": STORAGE_VERSION_MINOR,
        "data": {
            DATA_DELETED_IDS: [],
            DATA_LIVE_ACTIVITY_TOKENS: {},
            DATA_PUSH_SUBSCRIPTIONS: {
                webhook_id: {
                    SESSION_ID: {
                        PUSH_SUBSCRIPTION_TOKEN: PUSH_TOKEN,
                        PUSH_SUBSCRIPTION_ENTITY_IDS: [ENTITY_ID],
                        PUSH_SUBSCRIPTION_TARGET: None,
                        PUSH_SUBSCRIPTION_KIND: PUSH_SUBSCRIPTION_KIND_REMOTE_MEDIA,
                        PUSH_SUBSCRIPTION_DATA: context,
                        PUSH_SUBSCRIPTION_DEBOUNCE: COALESCE_SECONDS,
                    }
                }
            },
        },
    }
    entry = MockConfigEntry(
        data={
            "app_data": {
                "push_token": "COMPANION_TOKEN",
                "push_url": PUSH_URL,
            },
            "app_id": "io.robbie.HomeAssistant",
            "app_name": "Home Assistant",
            "app_version": "2026.9.1",
            "device_id": "device-1",
            "device_name": "Test iPhone",
            "manufacturer": "Apple",
            "model": "iPhone",
            "os_name": "iOS",
            "os_version": "27.0",
            "supports_encryption": False,
            "user_id": hass_admin_user.id,
            "webhook_id": webhook_id,
        },
        domain=DOMAIN,
        source="registration",
        title="Test iPhone",
        version=1,
    )
    entry.add_to_hass(hass)
    aioclient_mock.post(PUSH_URL, status=HTTPStatus.CREATED, json={})
    hass.states.async_set(ENTITY_ID, "playing", NEXT_TRACK)

    await async_setup_component(hass, DOMAIN, {DOMAIN: {}})
    await hass.async_block_till_done()

    restored = _subscription(hass, webhook_id)
    assert restored[PUSH_SUBSCRIPTION_DATA] == context
    assert len(hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS][webhook_id]) == 1

    await _settle(hass)
    [payload] = _payloads(aioclient_mock)
    assert payload["now_playing"]["attributes"]["snapshot"]["title"] == "Second"


async def test_older_success_cannot_overwrite_newer_success(
    hass: HomeAssistant,
    following: str,
) -> None:
    """An older successful request cannot overwrite newer delivered state."""
    timestamps = await _run_overlapping_deliveries(
        hass, following, PushOutcome.DELIVERED
    )

    context = _subscription(hass, following)[PUSH_SUBSCRIPTION_DATA]
    assert timestamps[1] > timestamps[0]
    assert context[CONTEXT_LAST_TIMESTAMP] == timestamps[1]
    assert context[CONTEXT_LAST_SNAPSHOT]["title"] == "Second"
    assert SESSION_ID in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][following]


async def test_older_success_cannot_overwrite_newer_failure(
    hass: HomeAssistant,
    following: str,
) -> None:
    """An older completion cannot claim stale state after a newer failure."""
    previous_snapshot = _subscription(hass, following)[PUSH_SUBSCRIPTION_DATA][
        CONTEXT_LAST_SNAPSHOT
    ]
    timestamps = await _run_overlapping_deliveries(hass, following, PushOutcome.FAILED)

    context = _subscription(hass, following)[PUSH_SUBSCRIPTION_DATA]
    assert timestamps[1] > timestamps[0]
    assert context[CONTEXT_LAST_TIMESTAMP] == timestamps[1]
    assert context[CONTEXT_LAST_SNAPSHOT] == previous_snapshot
    assert SESSION_ID in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS][following]


async def test_ordering_timestamp_increases_within_one_second(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """Several meaningful changes never reuse an APNs ordering value."""
    hass.states.async_set(ENTITY_ID, "paused", PLAYING_ATTRIBUTES)
    await _settle(hass)
    first = _payloads(aioclient_mock)[0]["now_playing"]["timestamp"]
    aioclient_mock.clear_requests()
    aioclient_mock.post(PUSH_URL, status=HTTPStatus.CREATED, json={})

    hass.states.async_set(ENTITY_ID, "playing", PLAYING_ATTRIBUTES)
    await _settle(hass)
    second = _payloads(aioclient_mock)[0]["now_playing"]["timestamp"]
    assert second > first


async def test_failed_send_advances_clock_but_not_published_snapshot(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """Best effort failure cannot reuse ordering or claim delivery."""
    before = dict(_subscription(hass, following)[PUSH_SUBSCRIPTION_DATA])
    aioclient_mock.clear_requests()
    aioclient_mock.post(PUSH_URL, status=HTTPStatus.BAD_GATEWAY, json={})

    hass.states.async_set(ENTITY_ID, "playing", NEXT_TRACK)
    await _settle(hass)

    after = _subscription(hass, following)[PUSH_SUBSCRIPTION_DATA]
    assert after[CONTEXT_LAST_TIMESTAMP] > before[CONTEXT_LAST_TIMESTAMP]
    assert after[CONTEXT_LAST_SNAPSHOT] == before[CONTEXT_LAST_SNAPSHOT]


async def test_invalid_token_removes_generic_subscription(
    hass: HomeAssistant,
    following: str,
    aioclient_mock: AiohttpClientMocker,
) -> None:
    """The relay's InvalidToken result removes its owning generic subscription."""
    aioclient_mock.clear_requests()
    aioclient_mock.post(
        PUSH_URL,
        status=HTTPStatus.GONE,
        json={"errorType": "InvalidToken"},
    )
    hass.states.async_set(ENTITY_ID, "playing", NEXT_TRACK)
    await _settle(hass)

    assert following not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTIONS]
    assert following not in hass.data[DOMAIN][DATA_PUSH_SUBSCRIPTION_UNSUBS]
