from __future__ import annotations

import asyncio
import fractions
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .capure import EncodedStreamSink, EncodedVideoChunk, GStreamerCapture

_TRACK_STOP = object()

if TYPE_CHECKING:
    from av.packet import Packet
    from aiortc import RTCConfiguration, RTCIceCandidate, RTCSessionDescription

try:
    from aiortc import (
        RTCPeerConnection,
        RTCIceCandidate as _RTCIceCandidate,
        RTCSessionDescription as _RTCSessionDescription,
    )
    from aiortc.mediastreams import MediaStreamError, MediaStreamTrack
    from aiortc.rtcrtpsender import RTCRtpSender
    from av.packet import Packet

    _WEBRTC_IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - depends on local native runtime
    RTCPeerConnection = None
    RTCRtpSender = None
    _RTCIceCandidate = None
    _RTCSessionDescription = None
    _WEBRTC_IMPORT_ERROR = exc

    class MediaStreamError(Exception):
        pass

    class MediaStreamTrack:  # type: ignore[no-redef]
        kind = "video"

        def __init__(self) -> None:
            self.readyState = "live"

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
        self.closed = False
        self.queue: asyncio.Queue[EncodedVideoChunk | object] = asyncio.Queue(
            maxsize=max_queue_size
        )

    def publish(self, chunk: EncodedVideoChunk) -> None:
        if self.closed or self.loop.is_closed():
            return
        self.loop.call_soon_threadsafe(self._push, chunk)

    def close(self) -> None:
        if self.closed or self.loop.is_closed():
            self.closed = True
            return
        self.closed = True
        self.loop.call_soon_threadsafe(self._push, _TRACK_STOP)

    def _push(self, item: EncodedVideoChunk | object) -> None:
        if self.queue.full():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self.queue.put_nowait(item)


class EncodedVideoRelay(EncodedStreamSink):
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
        subscription = _RelaySubscription(loop, self.max_queue_size)
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


class H264Track(MediaStreamTrack):
    kind = "video"

    def __init__(self, relay: EncodedVideoRelay, framerate: int) -> None:
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
        if chunk.pts_ns is None:
            base = self._last_pts_ns or 0
            pts_ns = base + (chunk.duration_ns or self._frame_duration_ns)
        else:
            pts_ns = chunk.pts_ns

        if self._last_pts_ns is not None and pts_ns <= self._last_pts_ns:
            pts_ns = self._last_pts_ns + max(chunk.duration_ns or self._frame_duration_ns, 1)

        self._last_pts_ns = pts_ns
        return pts_ns


class WebRTCSignalingSession:
    def __init__(
        self,
        capture: GStreamerCapture,
        *,
        rtc_configuration: RTCConfiguration | None = None,
        framerate: int = 30,
        auto_start_capture: bool = True,
    ) -> None:
        _ensure_webrtc_runtime()
        self.capture = capture
        self.auto_start_capture = auto_start_capture
        self._closed = False
        self._relay = EncodedVideoRelay()
        self.capture.set_encoded_output(self._relay)
        self._pc = RTCPeerConnection(configuration=rtc_configuration)
        self._track = H264Track(self._relay, framerate)
        self._pc.addTrack(self._track)
        self._prefer_h264()

    async def answer_offer(
        self,
        offer: RTCSessionDescription | SignalingDescription | dict[str, str],
    ) -> SignalingDescription:
        if self.auto_start_capture and not self.capture.is_running:
            self.capture.start()

        await self._pc.setRemoteDescription(self._coerce_description(offer))
        answer = await self._pc.createAnswer()
        await self._pc.setLocalDescription(answer)
        description = self._pc.localDescription
        if description is None:
            raise RuntimeError("Local WebRTC description is not available.")
        return SignalingDescription(type=description.type, sdp=description.sdp)

    async def add_ice_candidate(
        self,
        candidate: RTCIceCandidate | dict[str, Any] | None,
    ) -> None:
        if candidate is None:
            await self._pc.addIceCandidate(None)
            return
        if isinstance(candidate, dict):
            candidate = _RTCIceCandidate(**candidate)
        await self._pc.addIceCandidate(candidate)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._track.stop()
        await self._pc.close()
        self._relay.close()

    def _coerce_description(
        self,
        description: RTCSessionDescription | SignalingDescription | dict[str, str],
    ) -> RTCSessionDescription:
        if isinstance(description, dict):
            return _RTCSessionDescription(
                sdp=description["sdp"],
                type=description["type"],
            )
        if isinstance(description, SignalingDescription):
            return _RTCSessionDescription(sdp=description.sdp, type=description.type)
        return description

    def _prefer_h264(self) -> None:
        codecs = RTCRtpSender.getCapabilities("video").codecs
        preferred = [codec for codec in codecs if codec.mimeType.lower() == "video/h264"]
        if not preferred:
            raise RuntimeError("No H.264 WebRTC codec capability is available.")

        for transceiver in self._pc.getTransceivers():
            if transceiver.sender.track is self._track:
                transceiver.setCodecPreferences(preferred)
                return

        raise RuntimeError("Failed to find the H.264 video transceiver.")
