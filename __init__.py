"""AriaCast Receiver Plugin — native Python implementation of the AriaCast protocol."""

from __future__ import annotations

import asyncio
import hashlib
import json
import socket
import time
from collections.abc import AsyncGenerator
from contextlib import suppress
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import aiohttp
from aiohttp import ClientTimeout, web
from music_assistant_models.config_entries import ConfigEntry, ConfigValueOption
from music_assistant_models.enums import (
    ConfigEntryType,
    ContentType,
    ImageType,
    MediaType,
    PlaybackState,
    ProviderFeature,
    SourceControl,
    StreamType,
)
from music_assistant_models.errors import AudioError, MediaNotFoundError, SetupFailedError
from music_assistant_models.media_items import (
    AudioFormat,
    AudioSource,
    MediaItemImage,
    ProviderMapping,
)
from music_assistant_models.streamdetails import StreamDetails, StreamMetadata

from music_assistant.constants import CONF_ENTRY_WARN_PREVIEW
from music_assistant.models.plugin import PluginProvider

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ConfigValueType, ProviderConfig
    from music_assistant_models.provider import ProviderManifest

    from music_assistant.mass import MusicAssistant
    from music_assistant.models import ProviderInstanceType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONF_MASS_PLAYER_ID = "mass_player_id"
PLAYER_ID_AUTO = "__auto__"
SUPPORTED_FEATURES = {ProviderFeature.AUDIO_SOURCE}
AUDIO_SOURCE_ID = "main"

ARIACAST_PORT = 12889
DISCOVERY_PORT = 12888
FRAME_SIZE = 3840  # 20 ms of PCM S16LE 48 kHz stereo


# ---------------------------------------------------------------------------
# Provider entry points
# ---------------------------------------------------------------------------


async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> ProviderInstanceType:
    """Return a new provider instance."""
    return AriaCastReceiver(mass, manifest, config)


async def get_config_entries(
    mass: MusicAssistant,
    instance_id: str | None = None,  # noqa: ARG001
    action: str | None = None,  # noqa: ARG001
    values: dict[str, ConfigValueType] | None = None,  # noqa: ARG001
) -> tuple[ConfigEntry, ...]:
    """Return configuration entries."""
    return (
        CONF_ENTRY_WARN_PREVIEW,
        ConfigEntry(
            key=CONF_MASS_PLAYER_ID,
            type=ConfigEntryType.STRING,
            label="Connected Music Assistant Player",
            description="The player to route AriaCast audio to.",
            default_value=PLAYER_ID_AUTO,
            options=[
                ConfigValueOption(PLAYER_ID_AUTO, title="Auto (prefer playing player)"),
                *(
                    ConfigValueOption(p.player_id, title=p.display_name)
                    for p in sorted(
                        mass.players.all_players(False, False),
                        key=lambda p: p.display_name.lower(),
                    )
                ),
            ],
            required=True,
        ),
    )


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------


class AriaCastReceiver(PluginProvider):
    """
    Native Python AriaCast protocol server for Music Assistant.

    Listens on port 12889 and implements the AriaCast v1.1 wire protocol
    directly — no external binary or named pipe required.  Audio frames
    received from the Android sender flow into an asyncio.Queue and are
    yielded by get_audio_stream exactly like the VBAN receiver.
    """

    @property
    def supported_features(self) -> set[ProviderFeature]:
        return SUPPORTED_FEATURES

    def __init__(
        self, mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
    ) -> None:
        super().__init__(mass, manifest, config, SUPPORTED_FEATURES)
        self._default_player_id = str(config.get_value(CONF_MASS_PLAYER_ID))

        # Audio pipeline: one asyncio.Queue, drained per stream (VBAN pattern)
        self._audio_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=100)

        # AriaCast protocol state
        self._control_senders: set[web.WebSocketResponse] = set()
        self._meta_sockets: set[web.WebSocketResponse] = set()
        self._artwork_bytes: bytes | None = None
        self._last_artwork_url: str | None = None
        self._is_playing: bool = False

        # MA stream-routing state
        self._active_player_id: str | None = None
        self._in_use_by_queue: str | None = None
        self._active_session_id: str | None = None

        # Metadata pushed to the consuming MA queue
        self._stream_meta = StreamMetadata(title="AriaCast Ready")

        # aiohttp server handles
        self._runner: web.AppRunner | None = None
        self._discovery_transport: asyncio.BaseTransport | None = None

        self._audio_format = AudioFormat(
            content_type=ContentType.PCM_S16LE,
            sample_rate=48000,
            bit_depth=16,
            channels=2,
        )
        self._audio_source = AudioSource(
            item_id=AUDIO_SOURCE_ID,
            provider=self.instance_id,
            name=self.name,
            provider_mappings={
                ProviderMapping(
                    item_id=AUDIO_SOURCE_ID,
                    provider_domain=self.domain,
                    provider_instance=self.instance_id,
                    audio_format=self._audio_format,
                )
            },
            can_play_pause=True,
            can_seek=False,
            can_next_previous=True,
            exclusive=True,
            allow_external_trigger=True,
            # Source only appears when an Android sender connects and starts playing
            can_initiate=False,
        )

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def handle_async_init(self) -> None:
        """Start the AriaCast WebSocket server."""
        app = web.Application()
        app.router.add_get("/audio", self._ws_audio)
        app.router.add_get("/control", self._ws_control)
        app.router.add_get("/metadata", self._ws_metadata)
        app.router.add_post("/metadata", self._http_metadata)
        app.router.add_post("/api/command", self._http_command)
        app.router.add_get("/image/artwork", self._http_artwork)
        app.router.add_get("/artwork", self._http_artwork)

        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", ARIACAST_PORT)
        try:
            await site.start()
        except OSError as err:
            raise SetupFailedError(
                f"Cannot bind AriaCast server on port {ARIACAST_PORT}: {err}"
            ) from err

        self.logger.info("AriaCast server listening on port %d", ARIACAST_PORT)
        self.mass.create_task(self._run_udp_discovery())

    async def unload(self, is_removed: bool = False) -> None:
        """Tear down the server and close all connections."""
        for ws in list(self._control_senders) | list(self._meta_sockets):
            with suppress(Exception):
                await ws.close()
        if self._runner:
            await self._runner.cleanup()
        if self._discovery_transport:
            self._discovery_transport.close()

    # -----------------------------------------------------------------------
    # PluginProvider audio-source contract
    # -----------------------------------------------------------------------

    async def get_audio_sources(self) -> list[AudioSource]:
        return [self._audio_source]

    async def get_stream_details(self, source_id: str, queue_id: str) -> StreamDetails:
        if source_id != AUDIO_SOURCE_ID:
            raise MediaNotFoundError(f"Unknown AudioSource: {source_id}")
        # Allow through if currently playing OR if a player has played before (resume path)
        if not self._is_playing and not self._active_player_id:
            raise AudioError(
                "No AriaCast sender is streaming — open the AriaCast app on your device first"
            )
        return StreamDetails(
            provider=self.instance_id,
            item_id=source_id,
            audio_format=self._audio_format,
            media_type=MediaType.AUDIO_SOURCE,
            stream_type=StreamType.CUSTOM,
            stream_metadata=self._stream_meta,
        )

    async def get_audio_stream(
        self, streamdetails: StreamDetails, seek_position: int = 0
    ) -> AsyncGenerator[bytes]:
        """Yield raw PCM frames from the AriaCast sender queue (VBAN-style)."""
        consumer_queue = self._in_use_by_queue
        captured_session = self._active_session_id
        acquired = False

        # Drain any stale frames accumulated while the stream was idle
        # (avoids playing silence that built up during a pause)
        while not self._audio_queue.empty():
            with suppress(asyncio.QueueEmpty):
                self._audio_queue.get_nowait()

        self.logger.debug("Audio stream started: queue=%s", consumer_queue)

        try:
            while True:
                if (
                    self._in_use_by_queue != consumer_queue
                    or self._active_session_id != captured_session
                ):
                    self.logger.debug("Stream ownership changed, stopping")
                    break

                try:
                    async with asyncio.timeout(1):
                        frame = await self._audio_queue.get()
                    if not acquired:
                        acquired = True
                        self.logger.debug("First frame received from sender")
                    yield frame
                except TimeoutError:
                    # Cold-start check: fail fast if sender never starts sending
                    if not acquired and not self._is_playing:
                        raise AudioError(
                            "AriaCast sender is not streaming audio"
                        ) from None
                    continue
        finally:
            self.logger.debug("Audio stream ended: queue=%s", consumer_queue)
            # Drain queue so the next stream starts clean
            while not self._audio_queue.empty():
                with suppress(asyncio.QueueEmpty):
                    self._audio_queue.get_nowait()
            if (
                self._in_use_by_queue == consumer_queue
                and self._active_session_id == captured_session
            ):
                self._in_use_by_queue = None

    async def on_source_selected(
        self, source_id: str, player_id: str, queue_id: str, stream_session_id: str
    ) -> None:
        if source_id != AUDIO_SOURCE_ID:
            return
        self._in_use_by_queue = queue_id
        self._active_session_id = stream_session_id
        self._active_player_id = queue_id

    async def on_source_unselected(
        self, source_id: str, queue_id: str, stream_session_id: str
    ) -> None:
        if source_id != AUDIO_SOURCE_ID:
            return
        if self._active_session_id != stream_session_id:
            return
        self._active_session_id = None
        if self._in_use_by_queue == queue_id:
            self._in_use_by_queue = None

    async def on_source_control(
        self, source_id: str, action: SourceControl, value: int | None = None
    ) -> None:
        if source_id != AUDIO_SOURCE_ID:
            return
        if action == SourceControl.PLAY:
            await self._cmd_play()
        elif action == SourceControl.PAUSE:
            await self._cmd_pause()
        elif action == SourceControl.NEXT:
            await self._forward_action("next")
        elif action == SourceControl.PREVIOUS:
            await self._forward_action("previous")

    async def resolve_image(self, path: str) -> bytes:
        if path.startswith("artwork_") and self._artwork_bytes:
            return self._artwork_bytes
        return b""

    # -----------------------------------------------------------------------
    # AriaCast protocol — WebSocket handlers
    # -----------------------------------------------------------------------

    async def _ws_audio(self, request: web.Request) -> web.WebSocketResponse:
        """Receive raw PCM frames from the AriaCast sender."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Protocol handshake
        await ws.send_json({
            "status": "READY",
            "sample_rate": 48000,
            "channels": 2,
            "frame_size": FRAME_SIZE,
        })
        self.logger.info("AriaCast sender connected from %s", request.remote)

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                if len(msg.data) == FRAME_SIZE:
                    try:
                        self._audio_queue.put_nowait(msg.data)
                    except asyncio.QueueFull:
                        # Drop the oldest frame to make room for the new one
                        with suppress(asyncio.QueueEmpty):
                            self._audio_queue.get_nowait()
                        with suppress(asyncio.QueueFull):
                            self._audio_queue.put_nowait(msg.data)
            elif msg.type in (
                aiohttp.WSMsgType.ERROR,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.CLOSED,
            ):
                break

        self.logger.info("AriaCast sender disconnected from %s", request.remote)
        return ws

    async def _ws_control(self, request: web.Request) -> web.WebSocketResponse:
        """Register a sender for command delivery."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._control_senders.add(ws)
        self.logger.info("Control client connected from %s", request.remote)

        try:
            async for msg in ws:
                # Per spec the Go server only sends to /control clients, never reads.
                # We follow the same model: forward only.
                if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSING):
                    break
        finally:
            self._control_senders.discard(ws)

        return ws

    async def _ws_metadata(self, request: web.Request) -> web.WebSocketResponse:
        """Stream metadata updates to subscribers."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._meta_sockets.add(ws)

        # Immediately push current state on connect (spec requirement)
        await ws.send_json({"type": "metadata", "data": self._meta_dict()})

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    with suppress(Exception):
                        payload = json.loads(msg.data)
                        ptype = payload.get("type")
                        if ptype == "update":
                            await self._apply_meta(payload.get("data", {}))
                            await ws.send_json({"type": "ack", "success": True})
                        elif ptype == "get":
                            await ws.send_json({"type": "metadata", "data": self._meta_dict()})
                        elif ptype == "clear":
                            self._stream_meta = StreamMetadata(title="AriaCast Ready")
                            await ws.send_json({"type": "ack", "success": True})
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSING):
                    break
        finally:
            self._meta_sockets.discard(ws)

        return ws

    # -----------------------------------------------------------------------
    # AriaCast protocol — HTTP handlers
    # -----------------------------------------------------------------------

    async def _http_metadata(self, request: web.Request) -> web.Response:
        """POST /metadata — sender pushes track info."""
        try:
            body = await request.json()
            # Spec: sender may wrap payload in {"data": {...}}
            data = body.get("data", body)
            await self._apply_meta(data)
        except Exception as exc:
            return web.Response(status=400, text=str(exc))
        return web.Response(status=200)

    async def _http_command(self, request: web.Request) -> web.Response:
        """POST /api/command — MA (or web dashboard) triggers a playback action."""
        try:
            body = await request.json()
            action = body.get("action")
            if not action:
                return web.Response(status=400, text="Missing action")
            if action == "play":
                await self._cmd_play()
            elif action == "pause":
                await self._cmd_pause()
            else:
                await self._forward_action(action)
            return web.Response(status=200)
        except Exception as exc:
            return web.Response(status=400, text=str(exc))

    async def _http_artwork(self, _request: web.Request) -> web.Response:
        """GET /image/artwork or /artwork — serve cached artwork."""
        if not self._artwork_bytes:
            return web.Response(status=404, text="No artwork available")
        return web.Response(body=self._artwork_bytes, content_type="image/jpeg")

    # -----------------------------------------------------------------------
    # Metadata helpers
    # -----------------------------------------------------------------------

    def _meta_dict(self) -> dict[str, Any]:
        """Serialise current metadata to the canonical AriaCast wire format."""
        m = self._stream_meta
        return {
            "title": m.title,
            "artist": m.artist,
            "album": m.album,
            "artwork_url": m.image_url,
            "duration_ms": int(m.duration * 1000) if m.duration else None,
            "position_ms": int(m.elapsed_time * 1000) if m.elapsed_time else None,
            "is_playing": self._is_playing,
        }

    async def _apply_meta(self, data: dict[str, Any]) -> None:
        """Merge a partial metadata update from the sender into local state."""
        m = self._stream_meta

        if "title" in data:
            m.title = data["title"]
        if "artist" in data:
            m.artist = data["artist"]
        if "album" in data:
            m.album = data["album"]

        # Accept both camelCase (Android) and snake_case (spec broadcast) per interop rule
        duration = data.get("durationMs") or data.get("duration_ms")
        if duration is not None:
            m.duration = int(duration) / 1000

        position = data.get("positionMs") or data.get("position_ms")
        if position is not None:
            m.elapsed_time = int(position) / 1000
            m.elapsed_time_last_updated = time.time()

        artwork = data.get("artworkUrl") or data.get("artwork_url")
        if artwork and artwork != self._last_artwork_url:
            self._last_artwork_url = artwork
            self._artwork_bytes = None
            m.image_url = None
            self.mass.create_task(self._fetch_artwork(artwork))

        # Handle is_playing in both casings
        if "isPlaying" in data:
            is_playing = bool(data["isPlaying"])
        elif "is_playing" in data:
            is_playing = bool(data["is_playing"])
        else:
            is_playing = None

        if is_playing is not None:
            await self._handle_playback_state(is_playing)
        else:
            await self._broadcast_meta()

    async def _handle_playback_state(self, is_playing: bool) -> None:
        """React to is_playing transitions from the sender."""
        was_playing = self._is_playing
        self._is_playing = is_playing

        if is_playing and not self._in_use_by_queue:
            target = self._active_player_id or self._get_target_player_id()
            if target:
                self._active_player_id = target
                self._in_use_by_queue = target  # optimistic guard vs duplicate events
                self.mass.create_task(self._safe_play_media(target))
        elif not is_playing and was_playing and self._in_use_by_queue:
            self._active_player_id = self._in_use_by_queue
            target = self._in_use_by_queue
            self.mass.create_task(self.mass.players.cmd_stop(target))

        await self._broadcast_meta()

    async def _broadcast_meta(self) -> None:
        """Push current metadata to all /metadata WebSocket subscribers and to MA."""
        msg = {"type": "metadata", "data": self._meta_dict()}
        dead: set[web.WebSocketResponse] = set()
        for ws in list(self._meta_sockets):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.add(ws)
        self._meta_sockets -= dead

        if self._in_use_by_queue:
            self.mass.streams.update_stream_metadata(
                self._in_use_by_queue, AUDIO_SOURCE_ID, self.instance_id, self._stream_meta
            )

    async def _fetch_artwork(self, url: str) -> None:
        """Download artwork from the sender's HTTP server and cache it."""
        await asyncio.sleep(0.2)  # let the sender stabilise the image
        try:
            async with self.mass.http_session.get(
                url, timeout=ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    if data:
                        self._artwork_bytes = data
                        img_hash = hashlib.md5(data).hexdigest()[:8]
                        image = MediaItemImage(
                            type=ImageType.THUMB,
                            path=f"artwork_{img_hash}",
                            provider=self.instance_id,
                            remotely_accessible=False,
                        )
                        self._stream_meta.image_url = self.mass.metadata.get_image_url(image)
                        await self._broadcast_meta()
        except Exception as exc:
            self.logger.debug("Artwork fetch failed: %s", exc)

    # -----------------------------------------------------------------------
    # Playback commands
    # -----------------------------------------------------------------------

    async def _cmd_play(self) -> None:
        self.logger.info("PLAY")
        # Optimistically mark playing before the sender confirms so that
        # get_stream_details passes on an immediate resume.
        self._is_playing = True
        await self._forward_action("play")
        if not self._in_use_by_queue and self._active_player_id:
            target = self._active_player_id
            self._in_use_by_queue = target
            self.mass.create_task(self._safe_play_media(target))

    async def _cmd_pause(self) -> None:
        self.logger.info("PAUSE")
        if self._in_use_by_queue:
            self._active_player_id = self._in_use_by_queue
            await self.mass.players.cmd_stop(self._in_use_by_queue)
        await self._forward_action("pause")
        self._is_playing = False
        await self._broadcast_meta()

    async def _forward_action(self, action: str) -> None:
        """Send an action to all connected /control WebSocket senders."""
        msg = {"action": action}
        dead: set[web.WebSocketResponse] = set()
        for ws in list(self._control_senders):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.add(ws)
        self._control_senders -= dead

    async def _safe_play_media(self, target: str) -> None:
        try:
            await self.mass.player_queues.play_media(target, str(self._audio_source.uri))
        except Exception as exc:
            self.logger.warning("play_media failed for %s: %s", target, exc)
            if self._in_use_by_queue == target:
                self._in_use_by_queue = None

    # -----------------------------------------------------------------------
    # UDP discovery (AriaCast v1.1 spec)
    # -----------------------------------------------------------------------

    async def _run_udp_discovery(self) -> None:
        """Respond to DISCOVER_AUDIOCAST UDP broadcasts on port 12888."""
        loop = asyncio.get_running_loop()

        local_ip = self._get_local_ip()

        response_payload = json.dumps({
            "server_name": "MusicAssistant AriaCast Receiver",
            "ip": local_ip,
            "port": ARIACAST_PORT,
            "samplerate": 48000,
            "channels": 2,
        }).encode()

        class _Proto(asyncio.DatagramProtocol):
            def __init__(self, transport_holder: list, payload: bytes, logger: Any) -> None:
                self._holder = transport_holder
                self._payload = payload
                self._log = logger

            def connection_made(self, transport: asyncio.DatagramTransport) -> None:
                self._holder.append(transport)

            def datagram_received(self, data: bytes, addr: tuple) -> None:
                if data.strip() == b"DISCOVER_AUDIOCAST":
                    self._log.debug("Discovery from %s", addr)
                    transport = self._holder[0] if self._holder else None
                    if transport:
                        with suppress(Exception):
                            transport.sendto(self._payload, addr)

        holder: list[asyncio.DatagramTransport] = []
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _Proto(holder, response_payload, self.logger),
                local_addr=("0.0.0.0", DISCOVERY_PORT),
                allow_broadcast=True,
            )
            self._discovery_transport = transport
            self.logger.info("UDP discovery active on port %d", DISCOVERY_PORT)
        except Exception as exc:
            self.logger.warning("UDP discovery unavailable (port %d in use?): %s", DISCOVERY_PORT, exc)

    @staticmethod
    def _get_local_ip() -> str:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"

    # -----------------------------------------------------------------------
    # Player selection helper
    # -----------------------------------------------------------------------

    def _get_target_player_id(self) -> str | None:
        if self._active_player_id:
            if self.mass.players.get_player(self._active_player_id):
                return self._active_player_id
            self._active_player_id = None

        if self._default_player_id == PLAYER_ID_AUTO:
            for player in self.mass.players.all_players(False, False):
                if player.state.playback_state == PlaybackState.PLAYING:
                    return player.player_id
            players = list(self.mass.players.all_players(False, False))
            return players[0].player_id if players else None

        return self._default_player_id
