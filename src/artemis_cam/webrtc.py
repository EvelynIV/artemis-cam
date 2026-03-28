from __future__ import annotations

import asyncio
import fractions
import logging
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from .capure import EncodedVideoChunk, GStreamerCapture, WebRTCEncodedStreamSink

logger = logging.getLogger(__name__)

_TRACK_STOP = object()

try:
    from aiortc import (
        RTCConfiguration,
        RTCIceCandidate,
        RTCPeerConnection,
        RTCSessionDescription,
    )
    from aiortc.mediastreams import MediaStreamError, MediaStreamTrack
    from aiortc.rtcrtpsender import RTCRtpSender
    from av.packet import Packet

    _WEBRTC_IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - depends on local native runtime
    RTCConfiguration = Any  # type: ignore[assignment]
    RTCIceCandidate = Any  # type: ignore[assignment]
    RTCPeerConnection = Any  # type: ignore[assignment]
    RTCRtpSender = Any  # type: ignore[assignment]
    RTCSessionDescription = Any  # type: ignore[assignment]
    Packet = Any  # type: ignore[assignment]
    _WEBRTC_IMPORT_ERROR = exc

    class MediaStreamError(Exception):
        pass

    class MediaStreamTrack:  # type: ignore[no-redef]
        kind = "video"

        def __init__(self) -> None:
            self.readyState = "live"
            self.id = str(uuid.uuid4())

        def stop(self) -> None:
            self.readyState = "ended"


def _ensure_webrtc_runtime() -> None:
    if _WEBRTC_IMPORT_ERROR is not None:
        raise RuntimeError(
            "WebRTC runtime is not available. Install aiortc, av and their native "
            "dependencies before creating peer sessions."
        ) from _WEBRTC_IMPORT_ERROR


@dataclass(slots=True)
class SignalingDescription:
    type: str
    sdp: str


class _RelaySubscription:
    def __init__(self, loop: asyncio.AbstractEventLoop, max_queue_size: int) -> None:
        self.loop = loop
        self.queue: asyncio.Queue[EncodedVideoChunk | object] = asyncio.Queue(
            maxsize=max_queue_size
        )
        self.closed = False

    def publish(self, chunk: EncodedVideoChunk) -> None:
        if self.closed or self.loop.is_closed():
            return
        self.loop.call_soon_threadsafe(self._push_nowait, chunk)

    def close(self) -> None:
        if self.closed or self.loop.is_closed():
            self.closed = True
            return
        self.closed = True
        self.loop.call_soon_threadsafe(self._push_nowait, _TRACK_STOP)

    def _push_nowait(self, item: EncodedVideoChunk | object) -> None:
        if self.closed and item is not _TRACK_STOP:
            return

        if self.queue.full():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

        self.queue.put_nowait(item)


class EncodedVideoRelay(WebRTCEncodedStreamSink):
    """
    Fan out GStreamer H.264 chunks to multiple WebRTC tracks.
    """

    def __init__(self, max_queue_size: int = 120) -> None:
        self.max_queue_size = max_queue_size
        self._lock = threading.Lock()
        self._subscriptions: set[_RelaySubscription] = set()

    def push_h264_chunk(self, chunk: EncodedVideoChunk) -> None:
        with self._lock:
            subscriptions = list(self._subscriptions)

        for subscription in subscriptions:
            subscription.publish(chunk)

    def subscribe(self, loop: asyncio.AbstractEventLoop) -> _RelaySubscription:
        subscription = _RelaySubscription(loop=loop, max_queue_size=self.max_queue_size)
        with self._lock:
            self._subscriptions.add(subscription)
        return subscription

    def unsubscribe(self, subscription: _RelaySubscription) -> None:
        with self._lock:
            self._subscriptions.discard(subscription)
        subscription.close()

    def close(self) -> None:
        with self._lock:
            subscriptions = list(self._subscriptions)
            self._subscriptions.clear()

        for subscription in subscriptions:
            subscription.close()


class H264EncodedStreamTrack(MediaStreamTrack):
    """
    aiortc track that forwards pre-encoded Annex-B H.264 packets.
    """

    kind = "video"

    def __init__(
        self,
        relay: EncodedVideoRelay,
        *,
        framerate: int = 30,
    ) -> None:
        _ensure_webrtc_runtime()
        super().__init__()
        self._relay = relay
        self._time_base = fractions.Fraction(1, 1_000_000_000)
        self._frame_duration_ns = int(1_000_000_000 / max(framerate, 1))
        self._last_pts_ns: int | None = None

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        self._subscription = relay.subscribe(loop)

    async def recv(self) -> Packet:
        if getattr(self, "readyState", "live") != "live":
            raise MediaStreamError

        item = await self._subscription.queue.get()
        if item is _TRACK_STOP:
            self.stop()
            raise MediaStreamError

        assert isinstance(item, EncodedVideoChunk)
        packet = Packet(item.data)

        pts_ns = self._resolve_pts(item)
        packet.pts = pts_ns
        packet.dts = item.dts_ns if item.dts_ns is not None else pts_ns
        packet.time_base = self._time_base
        return packet

    def stop(self) -> None:
        self._relay.unsubscribe(self._subscription)
        base_stop = getattr(super(), "stop", None)
        if callable(base_stop):
            base_stop()

    def _resolve_pts(self, chunk: EncodedVideoChunk) -> int:
        if chunk.pts_ns is not None:
            pts_ns = chunk.pts_ns
        elif self._last_pts_ns is not None:
            duration_ns = chunk.duration_ns or self._frame_duration_ns
            pts_ns = self._last_pts_ns + duration_ns
        else:
            pts_ns = 0

        if self._last_pts_ns is not None and pts_ns <= self._last_pts_ns:
            pts_ns = self._last_pts_ns + max(chunk.duration_ns or self._frame_duration_ns, 1)

        self._last_pts_ns = pts_ns
        return pts_ns


class WebRTCPeerSession:
    """
    Single WebRTC publishing session for one remote client.

    Signaling transport is intentionally left outside this module. Future gRPC
    handlers can call `create_offer`, `accept_offer`, `accept_answer` and
    `add_remote_ice_candidate` directly.
    """

    def __init__(
        self,
        relay: EncodedVideoRelay,
        *,
        rtc_configuration: RTCConfiguration | None = None,
        framerate: int = 30,
        on_close: Callable[[str], None] | None = None,
    ) -> None:
        _ensure_webrtc_runtime()
        self.id = str(uuid.uuid4())
        self._relay = relay
        self._closed = False
        self._on_close = on_close
        self._pc = RTCPeerConnection(configuration=rtc_configuration)
        self._track = H264EncodedStreamTrack(relay, framerate=framerate)
        self._pc.addTrack(self._track)
        self._prefer_h264()

        @self._pc.on("connectionstatechange")
        async def _on_connectionstatechange() -> None:
            state = self.connection_state
            logger.info("Peer session %s connection state -> %s", self.id, state)
            if state in {"failed", "closed"}:
                await self.close()

    @property
    def connection_state(self) -> str:
        return self._pc.connectionState

    @property
    def local_description(self) -> SignalingDescription | None:
        description = self._pc.localDescription
        if description is None:
            return None
        return SignalingDescription(type=description.type, sdp=description.sdp)

    @property
    def remote_description(self) -> SignalingDescription | None:
        description = self._pc.remoteDescription
        if description is None:
            return None
        return SignalingDescription(type=description.type, sdp=description.sdp)

    async def create_offer(self) -> SignalingDescription:
        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(offer)
        return self.local_description_or_raise()

    async def accept_offer(
        self,
        description: RTCSessionDescription | SignalingDescription | dict[str, str],
    ) -> SignalingDescription:
        await self._pc.setRemoteDescription(self._coerce_description(description))
        answer = await self._pc.createAnswer()
        await self._pc.setLocalDescription(answer)
        return self.local_description_or_raise()

    async def accept_answer(
        self,
        description: RTCSessionDescription | SignalingDescription | dict[str, str],
    ) -> None:
        await self._pc.setRemoteDescription(self._coerce_description(description))

    async def add_remote_ice_candidate(
        self,
        candidate: RTCIceCandidate | dict[str, Any] | None,
    ) -> None:
        if candidate is None:
            await self._pc.addIceCandidate(None)
            return

        if isinstance(candidate, dict):
            candidate = RTCIceCandidate(**candidate)

        await self._pc.addIceCandidate(candidate)

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        self._track.stop()
        await self._pc.close()
        if self._on_close is not None:
            self._on_close(self.id)

    def local_description_or_raise(self) -> SignalingDescription:
        description = self.local_description
        if description is None:
            raise RuntimeError("Local WebRTC description is not available yet.")
        return description

    def _coerce_description(
        self,
        description: RTCSessionDescription | SignalingDescription | dict[str, str],
    ) -> RTCSessionDescription:
        if isinstance(description, dict):
            return RTCSessionDescription(sdp=description["sdp"], type=description["type"])
        if isinstance(description, SignalingDescription):
            return RTCSessionDescription(sdp=description.sdp, type=description.type)
        return description

    def _prefer_h264(self) -> None:
        capabilities = RTCRtpSender.getCapabilities("video").codecs
        preferred = [
            codec
            for codec in capabilities
            if codec.mimeType.lower() == "video/h264"
        ]
        if not preferred:
            raise RuntimeError("No H.264 WebRTC codec capability is available.")

        for transceiver in self._pc.getTransceivers():
            if transceiver.sender.track is self._track:
                transceiver.setCodecPreferences(preferred)
                return

        raise RuntimeError("Failed to find the transceiver for the H.264 video track.")


class WebRTCCameraBroadcaster:
    """
    Bridge `GStreamerCapture` encoded output to one or more WebRTC peer sessions.
    """

    def __init__(
        self,
        capture: GStreamerCapture,
        *,
        rtc_configuration: RTCConfiguration | None = None,
        framerate: int = 30,
        auto_start_capture: bool = True,
    ) -> None:
        self.capture = capture
        self.rtc_configuration = rtc_configuration
        self.framerate = framerate
        self.auto_start_capture = auto_start_capture
        self.relay = EncodedVideoRelay()
        self.capture.set_webrtc_output(self.relay)
        self._sessions: dict[str, WebRTCPeerSession] = {}

    async def create_session(self) -> WebRTCPeerSession:
        if self.auto_start_capture and not self.capture.is_running:
            self.capture.start()

        session = WebRTCPeerSession(
            self.relay,
            rtc_configuration=self.rtc_configuration,
            framerate=self.framerate,
            on_close=lambda session_id: self._sessions.pop(session_id, None),
        )
        self._sessions[session.id] = session
        return session

    async def accept_offer(
        self,
        description: RTCSessionDescription | SignalingDescription | dict[str, str],
    ) -> tuple[WebRTCPeerSession, SignalingDescription]:
        session = await self.create_session()
        answer = await session.accept_offer(description)
        return session, answer

    async def close_session(self, session_id: str) -> None:
        session = self._sessions.pop(session_id, None)
        if session is not None:
            await session.close()

    async def close(self) -> None:
        session_ids = list(self._sessions)
        for session_id in session_ids:
            await self.close_session(session_id)
        self.relay.close()
