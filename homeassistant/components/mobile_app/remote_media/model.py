"""JSON-safe values that make up a Remote Now Playing session.

The snapshot is made only of primitives because the `mobile_app` store serializes it and the iOS
app decodes the same shape on the wire.
"""

from dataclasses import dataclass
from typing import Any, Self

from homeassistant.const import ATTR_STATE

from .const import (
    ATTR_ALBUM,
    ATTR_ARTIST,
    ATTR_ARTWORK,
    ATTR_ARTWORK_URL,
    ATTR_CONTENT_ID,
    ATTR_DEVICE_CLASS,
    ATTR_DEVICE_NAME,
    ATTR_DURATION,
    ATTR_FEATURES,
    ATTR_IS_MUTED,
    ATTR_POSITION,
    ATTR_POSITION_UPDATED_AT_UNIX,
    ATTR_SELECTION,
    ATTR_SERVER_ID,
    ATTR_TITLE,
    ATTR_VOLUME,
    ATTR_WIRE_ENTITY_ID,
)

# Playback states the iOS `RemoteMediaPlaybackState` collapses to "not reporting". Anything the
# reducer sees that is not a known media player state lands here too.
INDETERMINATE_STATES = frozenset({"unavailable", "unknown"})
# Collapsed to "stopped": the card still shows its media, it is just not advancing.
STOPPED_STATES = frozenset({"idle", "off", "standby"})
PLAYING_STATES = frozenset({"playing", "buffering"})
PAUSED_STATES = frozenset({"paused"})


def collapsed_playback(state: str) -> str:
    """Return the playback state the card renders, ignoring how the integration spelled it.

    Mirrors Swift `RemoteMediaPlaybackState(homeAssistantState:)`. Integrations differ — some pass
    through `idle` on the way from `playing` to `paused` — and collapsing that here keeps the
    variation out of the decision to push.
    """
    if state in PLAYING_STATES:
        return "playing"
    if state in PAUSED_STATES:
        return "paused"
    if state in STOPPED_STATES:
        return "stopped"
    return "indeterminate"


@dataclass(frozen=True, slots=True)
class RemoteMediaSnapshot:
    """One followed player's state, in the exact shape the iOS app decodes.

    Field names are Python; `as_wire()` produces the camelCase keys of Swift `RemoteMediaSnapshot`.
    """

    server_id: str
    entity_id: str
    device_name: str
    state: str
    device_class: str | None = None
    title: str | None = None
    artist: str | None = None
    album: str | None = None
    content_id: str | None = None
    duration: float | None = None
    position: float | None = None
    position_updated_at_unix: float | None = None
    volume: float | None = None
    is_muted: bool | None = None
    features: int = 0
    # A credential-free absolute HTTPS source the phone may fetch artwork from, when the
    # integration exposes one. Never a Home Assistant proxy path: see `mapper._public_artwork`.
    artwork_url: str | None = None

    @property
    def has_meaningful_media(self) -> bool:
        """Whether this describes an actual piece of media.

        Mirrors Swift `hasMeaningfulMedia`. The card hangs on this rather than on playback state,
        because a paused player still has something to show.
        """
        return any(
            value is not None
            for value in (
                self.content_id,
                self.title,
                self.artist,
                self.album,
                self.duration,
            )
        )

    @property
    def track_id(self) -> str:
        """A deterministic identity for the current track.

        Byte-for-byte the Swift `trackId`: each component length-prefixed by its UTF-8 byte count,
        so no combination of values can collide with another. Metadata joins the content id because
        integrations reuse a stream URL across tracks.
        """
        return "".join(
            f"{len((value or '').encode())}:{value or ''}"
            for value in (self.content_id, self.title, self.artist, self.album)
        )

    @property
    def playback(self) -> str:
        """The collapsed playback state."""
        return collapsed_playback(self.state)

    def as_wire(self) -> dict[str, Any]:
        """Return the snapshot as the iOS decoder expects it.

        `selection`, `deviceName`, `state` and `features` are always present because Swift decodes
        them unconditionally; everything else is omitted when absent, which keeps the payload inside
        the relay's 4096-byte ceiling. `artwork` carries a credential-free source and never a token:
        see `mapper._public_artwork`.
        """
        wire: dict[str, Any] = {
            ATTR_SELECTION: {
                ATTR_SERVER_ID: self.server_id,
                ATTR_WIRE_ENTITY_ID: self.entity_id,
            },
            ATTR_DEVICE_NAME: self.device_name,
            ATTR_STATE: self.state,
            ATTR_FEATURES: self.features,
        }
        optional: dict[str, Any] = {
            ATTR_DEVICE_CLASS: self.device_class,
            ATTR_TITLE: self.title,
            ATTR_ARTIST: self.artist,
            ATTR_ALBUM: self.album,
            ATTR_CONTENT_ID: self.content_id,
            ATTR_DURATION: self.duration,
            ATTR_POSITION: self.position,
            ATTR_POSITION_UPDATED_AT_UNIX: self.position_updated_at_unix,
            ATTR_VOLUME: self.volume,
            ATTR_IS_MUTED: self.is_muted,
        }
        wire.update(
            {key: value for key, value in optional.items() if value is not None}
        )
        if self.artwork_url is not None:
            wire[ATTR_ARTWORK] = {ATTR_ARTWORK_URL: self.artwork_url}
        return wire

    def as_storage(self) -> dict[str, Any]:
        """Return the snapshot as persisted, so sticky state survives a restart."""
        return {
            "server_id": self.server_id,
            "entity_id": self.entity_id,
            "device_name": self.device_name,
            "state": self.state,
            "device_class": self.device_class,
            "title": self.title,
            "artist": self.artist,
            "album": self.album,
            "content_id": self.content_id,
            "duration": self.duration,
            "position": self.position,
            "position_updated_at_unix": self.position_updated_at_unix,
            "volume": self.volume,
            "is_muted": self.is_muted,
            "features": self.features,
            "artwork_url": self.artwork_url,
        }

    @classmethod
    def from_storage(cls, data: dict[str, Any]) -> Self | None:
        """Rebuild a persisted snapshot, or return None when it cannot be trusted."""
        try:
            return cls(
                server_id=data["server_id"],
                entity_id=data["entity_id"],
                device_name=data["device_name"],
                state=data["state"],
                device_class=data.get("device_class"),
                title=data.get("title"),
                artist=data.get("artist"),
                album=data.get("album"),
                content_id=data.get("content_id"),
                duration=data.get("duration"),
                position=data.get("position"),
                position_updated_at_unix=data.get("position_updated_at_unix"),
                volume=data.get("volume"),
                is_muted=data.get("is_muted"),
                features=data.get("features", 0),
                artwork_url=data.get("artwork_url"),
            )
        except KeyError, TypeError:
            return None
