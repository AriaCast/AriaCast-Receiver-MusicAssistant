"""AriaCast Receiver Plugin Provider."""

from __future__ import annotations

import asyncio
import hashlib
import os
import platform
import stat
import tempfile
import time
from collections import deque
from collections.abc import AsyncGenerator
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
from aiohttp import ClientTimeout
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
from music_assistant_models.errors import AudioError, MediaNotFoundError
from music_assistant_models.media_items import (
    AudioFormat,
    AudioSource,
    MediaItemImage,
    ProviderMapping,
)
from music_assistant_models.streamdetails import StreamDetails, StreamMetadata

from music_assistant.constants import CONF_ENTRY_WARN_PREVIEW
from music_assistant.helpers.named_pipe import AsyncNamedPipeWriter
from music_assistant.helpers.process import AsyncProcess
from music_assistant.models.plugin import PluginProvider

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ConfigValueType, ProviderConfig
    from music_assistant_models.provider import ProviderManifest

    from music_assistant.mass import MusicAssistant
    from music_assistant.models import ProviderInstanceType

CONF_MASS_PLAYER_ID = "mass_player_id"

PLAYER_ID_AUTO = "__auto__"
SUPPORTED_FEATURES = {ProviderFeature.AUDIO_SOURCE}

# Stable id for the single AudioSource this provider exposes.
# Combined with provider instance_id this forms the persistent URI.
AUDIO_SOURCE_ID = "main"


async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> ProviderInstanceType:
    """Initialize provider(instance) with given configuration."""
    return AriaCastBridge(mass, manifest, config)


async def get_config_entries(
    mass: MusicAssistant,
    _instance_id: str | None = None,
    _action: str | None = None,
    _values: dict[str, ConfigValueType] | None = None,
) -> tuple[ConfigEntry, ...]:
    """Return Config entries to setup this provider."""
    return (
        CONF_ENTRY_WARN_PREVIEW,
        ConfigEntry(
            key=CONF_MASS_PLAYER_ID,
            type=ConfigEntryType.STRING,
            label="Connected Music Assistant Player",
            description="The player to use for playback.",
            default_value=PLAYER_ID_AUTO,
            options=[
                ConfigValueOption("Auto (prefer playing player)", PLAYER_ID_AUTO),
                *(
                    ConfigValueOption(x.display_name, x.player_id)
                    for x in sorted(
                        mass.players.all(False, False), key=lambda p: p.display_name.lower()
                    )
                ),
            ],
            required=True,
        ),
    )


class AriaCastBridge(PluginProvider):
    """Bridge for the AriaCast Go Binary."""

    def __init__(
        self, mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
    ) -> None:
        """Initialize AriaCast Receiver."""
        super().__init__(mass, manifest, config, SUPPORTED_FEATURES)
        self._default_player_id = str(config.get_value(CONF_MASS_PLAYER_ID))

        # Process & Pipe
        self._binary_process: AsyncProcess | None = None
        pipe_path = Path(tempfile.gettempdir()) / f"ariacast_{self.instance_id}"
        self._pipe = AsyncNamedPipeWriter(str(pipe_path))

        # Internal State
        # _active_player_id remembers the player/queue that last consumed our stream
        # so we can reclaim it when the external app resumes after a pause.
        self._active_player_id: str | None = None
        # _in_use_by_queue is the queue currently streaming us (set in
        # on_source_selected, used to detect stream cancellation from inside
        # get_audio_stream and to gate metadata pushes to the consumer queue).
        self._in_use_by_queue: str | None = None
        # _active_session_id is the controller-provided token for the current
        # stream request — used to reject stale on_source_unselected callbacks
        # after a same-queue reconnect supersedes the previous request.
        self._active_session_id: str | None = None

        self._metadata_task: asyncio.Task[None] | None = None
        self._pipe_reader_task: asyncio.Task[None] | None = None
        self._stop_called = False
        self._binary_is_playing: bool = False
        self._current_track_title: str | None = None

        # Mutable metadata mirrored to the active queue via update_stream_metadata
        self._stream_metadata = StreamMetadata(title="AriaCast Ready")

        # Audio buffer — larger for high-latency players like Sendspin
        self.max_frames = 75  # 1.5 second buffer (75 frames × 20 ms)
        self.frame_queue: deque[bytes] = deque(maxlen=self.max_frames)
        self.frame_available = asyncio.Event()

        # Artwork storage
        self._artwork_bytes: bytes | None = None
        self._last_artwork_identifier: str | None = None

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
            # Passive: only flows when an external Cast client is connected
            can_initiate=False,
        )

    async def handle_async_init(self) -> None:
        """Start the provider."""
        await self._pipe.create()

        binary_path = await self._get_binary_path()
        args = [binary_path, "--pipe", self._pipe.path]

        self.logger.info("Starting AriaCast binary: %s", binary_path)
        self._binary_process = AsyncProcess(args, name="ariacast")
        await self._binary_process.start()

        # Give the binary a moment to bind its WebSocket port
        await asyncio.sleep(1)
        self._metadata_task = self.mass.create_task(self._monitor_metadata())
        self._pipe_reader_task = self.mass.create_task(self._read_pipe_to_queue())

    async def unload(self, is_removed: bool = False) -> None:
        """Cleanup resources."""
        self._stop_called = True

        if self._metadata_task:
            self._metadata_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._metadata_task

        if self._pipe_reader_task:
            self._pipe_reader_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._pipe_reader_task

        if self._binary_process:
            self.logger.info("Stopping AriaCast binary...")
            await self._binary_process.close()

        await self._pipe.remove()

    # ---------------------------------------------------------------------------
    # PluginProvider audio-source contract
    # ---------------------------------------------------------------------------

    async def get_audio_sources(self) -> list[AudioSource]:
        """Return the AudioSources this plugin currently exposes."""
        return [self._audio_source]

    async def get_stream_details(self, source_id: str, queue_id: str) -> StreamDetails:
        """
        Return StreamDetails for the AriaCast audio source.

        Side-effect-free: ownership is claimed in on_source_selected, which the
        streams controller fires before this method on a real stream request.
        Keeping this idempotent lets queue preload fetch StreamDetails without
        blocking a subsequent cross-queue handoff.
        """
        if source_id != AUDIO_SOURCE_ID:
            raise MediaNotFoundError(f"Unknown AudioSource: {source_id}")
        if not self._binary_is_playing:
            raise AudioError(
                "AriaCast has no active Cast client — start playback from your "
                "Cast-capable device first"
            )
        return StreamDetails(
            provider=self.instance_id,
            item_id=source_id,
            audio_format=self._audio_format,
            media_type=MediaType.AUDIO_SOURCE,
            stream_type=StreamType.CUSTOM,
            stream_metadata=self._stream_metadata,
        )

    async def get_audio_stream(
        self, streamdetails: StreamDetails, seek_position: int = 0
    ) -> AsyncGenerator[bytes]:
        """Stream PCM audio frames from the named-pipe pump."""
        consumer_queue = self._in_use_by_queue
        # Capture session id so a same-queue reconnect (which rolls
        # _active_session_id forward but keeps _in_use_by_queue) causes
        # this generator to exit without clobbering the new session's claim.
        captured_session_id = self._active_session_id
        self.logger.debug("Audio stream requested by queue %s", consumer_queue)

        # Pre-buffer before handing frames to the player to avoid underruns
        # on high-latency targets like Sendspin.
        min_buffer_size = int(self.max_frames * 0.6)
        self.logger.info("Pre-buffering: waiting for %d frames…", min_buffer_size)
        buffer_start = time.time()
        while len(self.frame_queue) < min_buffer_size and not self._stop_called:
            if time.time() - buffer_start > 5:
                self.logger.warning(
                    "Pre-buffering timeout, starting with %d frames", len(self.frame_queue)
                )
                break
            await asyncio.sleep(0.05)

        self.logger.info("Starting playback with %d frames buffered", len(self.frame_queue))

        try:
            while not self._stop_called:
                # Exit when: the queue changed (cross-queue handoff), or
                # a same-queue reconnect rolled the session id forward.
                if (
                    self._in_use_by_queue != consumer_queue
                    or self._active_session_id != captured_session_id
                ):
                    self.logger.debug("Stream lock released or superseded, stopping stream")
                    break

                if self.frame_queue:
                    try:
                        yield self.frame_queue.popleft()
                    except IndexError:
                        continue
                else:
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.frame_available.wait(), timeout=1.0)
                        if not self.frame_queue:
                            self.frame_available.clear()
        finally:
            self.logger.debug("Audio stream ended for queue %s", consumer_queue)
            self.frame_queue.clear()
            # Only clear the claim if this is still the active session so a stale
            # generator teardown after a same-queue reconnect doesn't wipe the
            # live session's state.
            if (
                self._in_use_by_queue == consumer_queue
                and self._active_session_id == captured_session_id
            ):
                self._in_use_by_queue = None

    async def on_source_selected(
        self,
        source_id: str,
        player_id: str,
        queue_id: str,
        stream_session_id: str,
    ) -> None:
        """Claim ownership when MA routes this source to a queue."""
        if source_id != AUDIO_SOURCE_ID:
            return
        # Claim here (not in get_stream_details) so preload paths stay
        # side-effect-free and cross-queue handoffs work correctly.
        self._in_use_by_queue = queue_id
        self._active_session_id = stream_session_id
        # Cache queue_id as the active player; queue_id == player_id for
        # direct players and is more stable than the protocol-level player_id
        # for bridges like Sendspin that can tear down between streams.
        self._active_player_id = queue_id

    async def on_source_unselected(
        self, source_id: str, queue_id: str, stream_session_id: str
    ) -> None:
        """Release the queue-scoped claim when MA tears down the stream."""
        if source_id != AUDIO_SOURCE_ID:
            return
        # Guard on session id, not just queue_id: a same-queue reconnect
        # (player drops + reopens the same stream URL before the original
        # request's finally fires) must not let the stale callback clear the
        # live claim of the new stream.
        if self._active_session_id != stream_session_id:
            return
        self._active_session_id = None
        if self._in_use_by_queue == queue_id:
            self._in_use_by_queue = None

    async def on_source_control(
        self,
        source_id: str,
        action: SourceControl,
        value: int | None = None,
    ) -> None:
        """Proxy playback control commands to the AriaCast binary HTTP API."""
        if source_id != AUDIO_SOURCE_ID:
            return
        if action == SourceControl.PLAY:
            await self._cmd_play()
        elif action == SourceControl.PAUSE:
            await self._cmd_pause()
        elif action == SourceControl.NEXT:
            await self._send_api_command("next")
        elif action == SourceControl.PREVIOUS:
            await self._send_api_command("previous")

    # ---------------------------------------------------------------------------
    # Binary lifecycle helpers
    # ---------------------------------------------------------------------------

    async def _get_binary_path(self) -> str:
        """Locate the correct binary for the current OS/Arch."""
        base_dir = os.path.join(os.path.dirname(__file__), "bin")
        system = platform.system().lower()
        machine = platform.machine().lower()

        if machine in ("x86_64", "amd64"):
            arch = "amd64"
        elif machine in ("aarch64", "arm64"):
            arch = "arm64"
        else:
            raise RuntimeError(f"Unsupported architecture: {machine}")

        binary_name = f"ariacast_{system}_{arch}"
        binary_path = os.path.join(base_dir, binary_name)

        if not os.path.exists(binary_path):
            raise FileNotFoundError(f"Binary not found at {binary_path}")

        Path(binary_path).chmod(Path(binary_path).stat().st_mode | stat.S_IEXEC)
        return binary_path

    # ---------------------------------------------------------------------------
    # Metadata WebSocket monitor
    # ---------------------------------------------------------------------------

    async def _monitor_metadata(self) -> None:
        """Connect to the Go binary WebSocket and receive metadata updates."""
        url = "ws://127.0.0.1:12889/metadata"
        retry_delay = 1

        while not self._stop_called:
            try:
                async with self.mass.http_session.ws_connect(url, heartbeat=30) as ws:
                    self.logger.info("Connected to AriaCast metadata stream")
                    retry_delay = 1
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = msg.json()
                            if payload.get("type") == "metadata":
                                self._update_metadata(payload.get("data", {}))
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except Exception as exc:
                if not self._stop_called:
                    self.logger.debug(
                        "AriaCast metadata WebSocket error: %s. Retrying in %ds…",
                        exc,
                        retry_delay,
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60)

    def _update_metadata(self, data: dict[str, Any]) -> None:
        """Parse a metadata payload and update internal state + active queue."""
        meta = self._stream_metadata

        new_title = data.get("title", "Unknown")
        if self._current_track_title and new_title != self._current_track_title:
            if self._binary_is_playing:
                self.logger.info(
                    "Song changed '%s' → '%s', clearing audio queue",
                    self._current_track_title,
                    new_title,
                )
                self.frame_queue.clear()
                self.frame_available.clear()
        self._current_track_title = new_title

        meta.title = new_title
        meta.artist = data.get("artist", "Unknown")
        meta.album = data.get("album", "Unknown")

        if data.get("artwork_url"):
            artwork_identifier = f"{data['artwork_url']}_{meta.title}_{meta.artist}"
            if artwork_identifier != self._last_artwork_identifier:
                self._last_artwork_identifier = artwork_identifier
                self._artwork_bytes = None
                meta.image_url = None
                self.mass.create_task(self._download_artwork())

        if duration_ms := data.get("duration_ms"):
            meta.duration = int(duration_ms / 1000)

        if position_ms := data.get("position_ms"):
            meta.elapsed_time = int(position_ms / 1000)
            meta.elapsed_time_last_updated = time.time()

        self._handle_playback_state_update(data.get("is_playing", False))

        # Push updated metadata to the consuming queue's stream
        if self._in_use_by_queue:
            self.mass.streams.update_stream_metadata(
                self._in_use_by_queue, AUDIO_SOURCE_ID, self.instance_id, meta
            )

    def _handle_playback_state_update(self, is_playing: bool) -> None:
        """React to binary play/pause transitions."""
        was_playing = self._binary_is_playing
        self.logger.debug(
            "Playback state: is_playing=%s was_playing=%s active_player=%s in_use_by_queue=%s",
            is_playing,
            was_playing,
            self._active_player_id,
            self._in_use_by_queue,
        )
        self._binary_is_playing = is_playing

        if is_playing and not self._in_use_by_queue:
            # External app started or resumed — route to a player
            target = self._active_player_id or self._get_target_player_id()
            if target:
                self.logger.info("External playback started, routing to player %s", target)
                self.frame_queue.clear()
                self.frame_available.clear()
                self._active_player_id = target
                self.mass.create_task(
                    self.mass.player_queues.play_media(target, str(self._audio_source.uri))
                )
        elif not is_playing and was_playing and self._in_use_by_queue:
            # External app paused — stop the MA player so it can serve other content
            self.logger.info("External playback paused, releasing player")
            self._active_player_id = self._in_use_by_queue
            target_player = self._in_use_by_queue
            self.frame_queue.clear()
            self.frame_available.clear()
            self.mass.create_task(self.mass.players.cmd_stop(target_player))

    # ---------------------------------------------------------------------------
    # Play / Pause commands (called via on_source_control)
    # ---------------------------------------------------------------------------

    async def _cmd_play(self) -> None:
        """Resume playback: re-route to the last active player if needed."""
        self.logger.info("PLAY command")
        if not self._in_use_by_queue and self._active_player_id:
            self.frame_queue.clear()
            self.frame_available.clear()
            await self.mass.player_queues.play_media(
                self._active_player_id, str(self._audio_source.uri)
            )
        await self._send_api_command("play")

    async def _cmd_pause(self) -> None:
        """Pause playback: stop the MA player and tell the binary to pause."""
        self.logger.info("PAUSE command")
        if self._in_use_by_queue:
            self._active_player_id = self._in_use_by_queue
            target_player = self._in_use_by_queue
            self.frame_queue.clear()
            self.frame_available.clear()
            await self.mass.players.cmd_stop(target_player)
        await self._send_api_command("pause")

    async def _send_api_command(self, action: str) -> None:
        """POST a control command to the Go binary HTTP API."""
        url = "http://127.0.0.1:12889/api/command"
        try:
            async with self.mass.http_session.post(url, json={"action": action}) as response:
                body = await response.text()
                if not 200 <= response.status < 300:
                    self.logger.warning(
                        "Command '%s' failed HTTP %s: %s", action, response.status, body
                    )
        except Exception as e:
            self.logger.warning("Failed to send command '%s': %s", action, e)

    # ---------------------------------------------------------------------------
    # Artwork
    # ---------------------------------------------------------------------------

    async def _download_artwork(self) -> None:
        """Fetch artwork bytes from the Go binary and push to the active queue."""
        await asyncio.sleep(0.2)  # let the binary rotate to the new image
        artwork_url = "http://127.0.0.1:12889/image/artwork"
        try:
            async with self.mass.http_session.get(
                artwork_url, timeout=ClientTimeout(total=5)
            ) as response:
                if response.status == 200:
                    img_data = await response.read()
                    if img_data:
                        self._artwork_bytes = img_data
                        img_hash = hashlib.md5(img_data).hexdigest()[:8]
                        image = MediaItemImage(
                            type=ImageType.THUMB,
                            path=f"artwork_{img_hash}",
                            provider=self.instance_id,
                            remotely_accessible=False,
                        )
                        self._stream_metadata.image_url = self.mass.metadata.get_image_url(image)
                        if self._in_use_by_queue:
                            self.mass.streams.update_stream_metadata(
                                self._in_use_by_queue,
                                AUDIO_SOURCE_ID,
                                self.instance_id,
                                self._stream_metadata,
                            )
        except Exception as e:
            self.logger.debug("Failed to download artwork: %s", e)

    async def resolve_image(self, path: str) -> bytes:
        """Return raw artwork bytes to Music Assistant."""
        if path.startswith("artwork") and self._artwork_bytes:
            return self._artwork_bytes
        return b""

    # ---------------------------------------------------------------------------
    # Pipe reader (audio pump)
    # ---------------------------------------------------------------------------

    async def _read_pipe_to_queue(self) -> None:
        """Read PCM frames from the named pipe into the frame queue."""
        frame_size = 3840  # 20 ms of 48 kHz stereo 16-bit PCM
        loop = asyncio.get_event_loop()

        while not self._stop_called:
            try:
                if not os.path.exists(self._pipe.path):
                    await asyncio.sleep(0.1)
                    continue

                self.logger.debug("Opening pipe for reading: %s", self._pipe.path)
                pipe_fd = await loop.run_in_executor(None, open, self._pipe.path, "rb")
                try:
                    while not self._stop_called:
                        data = await loop.run_in_executor(None, pipe_fd.read, frame_size)
                        if not data:
                            self.logger.debug("Pipe closed")
                            break
                        self.frame_queue.append(data)
                        self.frame_available.set()
                finally:
                    await loop.run_in_executor(None, pipe_fd.close)

            except Exception as e:
                self.logger.debug("Error reading from pipe: %s", e)
                await asyncio.sleep(0.5)

    # ---------------------------------------------------------------------------
    # Player selection helper
    # ---------------------------------------------------------------------------

    def _get_target_player_id(self) -> str | None:
        """Return the best available player to route new playback to."""
        if self._active_player_id:
            if self.mass.players.get(self._active_player_id):
                return self._active_player_id
            self._active_player_id = None

        if self._default_player_id == PLAYER_ID_AUTO:
            for player in self.mass.players.all(False, False):
                if player.state.playback_state == PlaybackState.PLAYING:
                    return player.player_id
            players = list(self.mass.players.all(False, False))
            return players[0].player_id if players else None

        return str(self._default_player_id)
