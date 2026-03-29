from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import cv2
import grpc
from aiortc import RTCPeerConnection, RTCRtpReceiver, RTCSessionDescription

from artemis_cam.capure import GStreamerCapture
from artemis_cam.proto import signaling_pb2, signaling_pb2_grpc
from artemis_cam.server import create_server


async def _wait_for_ice_complete(pc: RTCPeerConnection) -> None:
    if pc.iceGatheringState == "complete":
        return

    done = asyncio.Event()

    @pc.on("icegatheringstatechange")
    def _on_icegatheringstatechange() -> None:
        if pc.iceGatheringState == "complete":
            done.set()

    await done.wait()


def _prefer_h264(pc: RTCPeerConnection) -> None:
    transceiver = pc.addTransceiver("video", direction="recvonly")
    codecs = RTCRtpReceiver.getCapabilities("video").codecs
    preferred = [codec for codec in codecs if codec.mimeType.lower() == "video/h264"]
    if preferred:
        transceiver.setCodecPreferences(preferred)


async def _display_video(track, stop_event: asyncio.Event) -> None:
    window_name = "artemis-cam-demo"
    cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)

    try:
        while not stop_event.is_set():
            frame = await track.recv()
            image = frame.to_ndarray(format="bgr24")
            cv2.imshow(window_name, image)

            key = cv2.waitKey(1) & 0xFF
            if key in (27, ord("q")):
                stop_event.set()
                break
    finally:
        cv2.destroyAllWindows()


async def _handle_signaling(
    call,
    pc: RTCPeerConnection,
    answer_ready: asyncio.Event,
    stop_event: asyncio.Event,
) -> None:
    async for message in call:
        payload = message.WhichOneof("payload")

        if payload == "answer":
            await pc.setRemoteDescription(
                RTCSessionDescription(
                    type=message.answer.type,
                    sdp=message.answer.sdp,
                )
            )
            answer_ready.set()
            continue

        if payload == "close":
            stop_event.set()
            return

    stop_event.set()


async def run_demo(args: argparse.Namespace) -> None:
    capture = GStreamerCapture(
        width=args.width,
        height=args.height,
        framerate=args.framerate,
        bitrate=args.bitrate,
    )
    server = create_server(
        capture,
        host=args.host,
        port=args.port,
        framerate=args.framerate,
    )
    await server.start()

    channel = grpc.aio.insecure_channel(f"{args.host}:{args.port}")
    await channel.channel_ready()
    stub = signaling_pb2_grpc.SignalingServiceStub(channel)
    pc = RTCPeerConnection()
    request_queue: asyncio.Queue[signaling_pb2.SignalMessage | None] = asyncio.Queue()
    stop_event = asyncio.Event()
    answer_ready = asyncio.Event()
    display_task: asyncio.Task[None] | None = None

    async def request_iterator():
        while True:
            message = await request_queue.get()
            if message is None:
                return
            yield message

    @pc.on("track")
    def _on_track(track) -> None:
        nonlocal display_task
        if track.kind == "video" and display_task is None:
            display_task = asyncio.create_task(_display_video(track, stop_event))

    @pc.on("connectionstatechange")
    async def _on_connectionstatechange() -> None:
        if pc.connectionState in {"failed", "closed"}:
            stop_event.set()

    _prefer_h264(pc)
    call = stub.Signal(request_iterator())
    signal_task = asyncio.create_task(
        _handle_signaling(call, pc, answer_ready, stop_event)
    )

    try:
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        await _wait_for_ice_complete(pc)

        local_offer = pc.localDescription
        if local_offer is None:
            raise RuntimeError("Local offer is not available.")

        await request_queue.put(
            signaling_pb2.SignalMessage(
                offer=signaling_pb2.SessionDescription(
                    type=local_offer.type,
                    sdp=local_offer.sdp,
                )
            )
        )

        await asyncio.wait_for(answer_ready.wait(), timeout=30.0)
        await stop_event.wait()
    finally:
        await request_queue.put(
            signaling_pb2.SignalMessage(
                close=signaling_pb2.CloseSessionRequest()
            )
        )
        await request_queue.put(None)

        await pc.close()
        await channel.close()
        await server.stop(0)
        capture.stop()

        if display_task is not None:
            await asyncio.gather(display_task, return_exceptions=True)
        await asyncio.gather(signal_task, return_exceptions=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="artemis-cam WebRTC demo client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=720)
    parser.add_argument("--framerate", type=int, default=30)
    parser.add_argument("--bitrate", type=int, default=2_000_000)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_demo(parse_args()))
