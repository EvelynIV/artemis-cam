from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Any

import grpc

from .capure import GStreamerCapture
from .proto import signaling_pb2, signaling_pb2_grpc
from .webrtc import SignalingDescription, WebRTCSignalingSession

if TYPE_CHECKING:
    from aiortc import RTCConfiguration


def _description_to_proto(description: SignalingDescription) -> signaling_pb2.SessionDescription:
    return signaling_pb2.SessionDescription(type=description.type, sdp=description.sdp)


def _candidate_to_dict(candidate: signaling_pb2.IceCandidate) -> dict[str, Any]:
    return {
        "foundation": candidate.foundation,
        "component": candidate.component,
        "ip": candidate.ip,
        "port": candidate.port,
        "priority": candidate.priority,
        "protocol": candidate.protocol,
        "type": candidate.type,
        "relatedAddress": candidate.related_address or None,
        "relatedPort": candidate.related_port or None,
        "sdpMid": candidate.sdp_mid or None,
        "sdpMLineIndex": candidate.sdp_mline_index,
        "tcpType": candidate.tcp_type or None,
    }


class SignalingService(signaling_pb2_grpc.SignalingServiceServicer):
    """
    Single-stream signaling service.

    The current design intentionally supports one active signaling stream at a
    time, matching the single-client assumption in the proto and WebRTC layer.
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
        self._stream_lock = asyncio.Lock()

    async def Signal(
        self,
        request_iterator: AsyncIterable[signaling_pb2.SignalMessage],
        context: grpc.aio.ServicerContext,
    ):
        if self._stream_lock.locked():
            await context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                "Only one signaling stream is supported.",
            )

        async with self._stream_lock:
            session = WebRTCSignalingSession(
                self.capture,
                rtc_configuration=self.rtc_configuration,
                framerate=self.framerate,
                auto_start_capture=self.auto_start_capture,
            )

            try:
                async for request in request_iterator:
                    payload = request.WhichOneof("payload")
                    if payload is None:
                        await context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "Signal message payload is required.",
                        )

                    if payload == "offer":
                        answer = await session.answer_offer(
                            {
                                "type": request.offer.type,
                                "sdp": request.offer.sdp,
                            }
                        )
                        yield signaling_pb2.SignalMessage(
                            answer=_description_to_proto(answer)
                        )
                        continue

                    if payload == "candidate":
                        await session.add_ice_candidate(
                            _candidate_to_dict(request.candidate)
                        )
                        continue

                    if payload == "end_of_candidates":
                        if request.end_of_candidates:
                            await session.add_ice_candidate(None)
                        continue

                    if payload == "close":
                        break

                    await context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        f"Unsupported signal payload: {payload}",
                    )
            finally:
                await session.close()


def create_server(
    capture: GStreamerCapture,
    *,
    host: str = "0.0.0.0",
    port: int = 50051,
    rtc_configuration: RTCConfiguration | None = None,
    framerate: int = 30,
    auto_start_capture: bool = True,
    grpc_options: list[tuple[str, int | str]] | None = None,
) -> grpc.aio.Server:
    server = grpc.aio.server(options=grpc_options)
    signaling_pb2_grpc.add_SignalingServiceServicer_to_server(
        SignalingService(
            capture,
            rtc_configuration=rtc_configuration,
            framerate=framerate,
            auto_start_capture=auto_start_capture,
        ),
        server,
    )
    server.add_insecure_port(f"{host}:{port}")
    return server


async def serve(
    capture: GStreamerCapture,
    *,
    host: str = "0.0.0.0",
    port: int = 50051,
    rtc_configuration: RTCConfiguration | None = None,
    framerate: int = 30,
    auto_start_capture: bool = True,
    grpc_options: list[tuple[str, int | str]] | None = None,
) -> grpc.aio.Server:
    server = create_server(
        capture,
        host=host,
        port=port,
        rtc_configuration=rtc_configuration,
        framerate=framerate,
        auto_start_capture=auto_start_capture,
        grpc_options=grpc_options,
    )
    await server.start()
    return server
