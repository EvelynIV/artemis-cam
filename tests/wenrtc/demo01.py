from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

import cv2
import grpc
from aiortc import RTCPeerConnection, RTCRtpReceiver, RTCSessionDescription

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from artemis_cam.proto import signaling_pb2, signaling_pb2_grpc


def log(message: str) -> None:
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


async def wait_for_ice_complete(pc: RTCPeerConnection) -> None:
    if pc.iceGatheringState == "complete":
        log("ICE gathering already complete.")
        return

    done = asyncio.Event()

    @pc.on("icegatheringstatechange")
    def on_icegatheringstatechange() -> None:
        log(f"ICE gathering state -> {pc.iceGatheringState}")
        if pc.iceGatheringState == "complete":
            done.set()

    await done.wait()


def prefer_h264(pc: RTCPeerConnection) -> None:
    transceiver = pc.addTransceiver("video", direction="recvonly")
    codecs = RTCRtpReceiver.getCapabilities("video").codecs
    preferred = [codec for codec in codecs if codec.mimeType.lower() == "video/h264"]
    if preferred:
        transceiver.setCodecPreferences(preferred)
        codec_names = ", ".join(codec.mimeType for codec in preferred)
        log(f"Preferred codecs: {codec_names}")
    else:
        log("No explicit H.264 codec preference found, using default codec order.")


async def display_video(track, stop_event: asyncio.Event) -> None:
    window_name = "artemis-cam-webrtc-client"
    cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
    frame_count = 0
    stats_started_at = time.monotonic()

    log(f"Video display loop started for track id={track.id}")

    try:
        while not stop_event.is_set():
            frame = await track.recv()
            frame_count += 1
            image = frame.to_ndarray(format="bgr24")

            if frame_count == 1:
                log(
                    "First frame received: "
                    f"{image.shape[1]}x{image.shape[0]}, pts={frame.pts}, time_base={frame.time_base}"
                )

            elapsed = time.monotonic() - stats_started_at
            if elapsed >= 1.0:
                fps = frame_count / elapsed
                log(f"Receiving video: {frame_count} frames in {elapsed:.1f}s ({fps:.1f} fps)")
                frame_count = 0
                stats_started_at = time.monotonic()

            cv2.imshow(window_name, image)

            key = cv2.waitKey(1) & 0xFF
            if key in (27, ord("q")):
                log("Exit requested from OpenCV window.")
                stop_event.set()
                break
    except Exception as exc:
        log(f"display_video stopped with error: {exc!r}")
        stop_event.set()
        raise
    finally:
        log("Closing OpenCV window.")
        cv2.destroyAllWindows()


async def handle_signaling(
    call,
    pc: RTCPeerConnection,
    answer_ready: asyncio.Event,
    stop_event: asyncio.Event,
) -> None:
    async for message in call:
        payload = message.WhichOneof("payload")
        log(f"Received signaling payload: {payload}")

        if payload == "answer":
            await pc.setRemoteDescription(
                RTCSessionDescription(
                    type=message.answer.type,
                    sdp=message.answer.sdp,
                )
            )
            log("Remote answer applied.")
            answer_ready.set()
            continue

        if payload == "close":
            log("Received remote close signal.")
            stop_event.set()
            return

    log("Signaling stream closed by server.")
    stop_event.set()


async def run_client(args: argparse.Namespace) -> None:
    log(f"Connecting to signaling server at {args.host}:{args.port}")
    loop = asyncio.get_running_loop()
    channel = grpc.aio.insecure_channel(f"{args.host}:{args.port}")
    await channel.channel_ready()
    log("gRPC channel is ready.")
    stub = signaling_pb2_grpc.SignalingServiceStub(channel)
    pc = RTCPeerConnection()
    request_queue: asyncio.Queue[signaling_pb2.SignalMessage | None] = asyncio.Queue()
    stop_event = asyncio.Event()
    answer_ready = asyncio.Event()
    display_task: asyncio.Task[None] | None = None

    def on_background_task_done(task: asyncio.Task[None], name: str) -> None:
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            log(f"Background task cancelled: {name}")
            return

        if exception is None:
            log(f"Background task finished: {name}")
            return

        log(f"Background task failed: {name}: {exception!r}")
        stop_event.set()

    def start_display_task(track) -> None:
        nonlocal display_task
        if display_task is not None:
            log("Display task already exists, ignoring duplicate video track.")
            return

        log(f"Creating display task for video track id={track.id}")
        display_task = loop.create_task(display_video(track, stop_event))
        display_task.add_done_callback(
            lambda task: on_background_task_done(task, "display_video")
        )

    async def request_iterator():
        while True:
            message = await request_queue.get()
            if message is None:
                log("Request iterator received sentinel and will stop.")
                return
            log(f"Sending signaling payload: {message.WhichOneof('payload')}")
            yield message

    @pc.on("track")
    def on_track(track) -> None:
        log(f"Received remote track: kind={track.kind}, id={track.id}")
        if track.kind == "video":
            loop.call_soon_threadsafe(start_display_task, track)

    @pc.on("connectionstatechange")
    async def on_connectionstatechange() -> None:
        log(f"Peer connection state -> {pc.connectionState}")
        if pc.connectionState in {"failed", "closed"}:
            stop_event.set()

    @pc.on("iceconnectionstatechange")
    async def on_iceconnectionstatechange() -> None:
        log(f"ICE connection state -> {pc.iceConnectionState}")

    @pc.on("icegatheringstatechange")
    async def on_icegatheringstatechange_for_debug() -> None:
        log(f"Observed ICE gathering state -> {pc.iceGatheringState}")

    prefer_h264(pc)
    call = stub.Signal(request_iterator())
    signal_task = asyncio.create_task(
        handle_signaling(call, pc, answer_ready, stop_event)
    )

    try:
        log("Creating local SDP offer.")
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        log("Local offer created and applied.")
        await wait_for_ice_complete(pc)

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

        log("Waiting for remote SDP answer.")
        await asyncio.wait_for(answer_ready.wait(), timeout=args.timeout)
        log("Remote SDP answer received, waiting for stop event.")
        await stop_event.wait()
        log("Stop event received.")
    finally:
        log("Shutting down client.")
        await request_queue.put(
            signaling_pb2.SignalMessage(
                close=signaling_pb2.CloseSessionRequest()
            )
        )
        await request_queue.put(None)

        await pc.close()
        await channel.close()
        log("Peer connection and channel closed.")

        if display_task is not None:
            await asyncio.gather(display_task, return_exceptions=True)
        await asyncio.gather(signal_task, return_exceptions=True)
        log("Client shutdown complete.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Standalone WebRTC test client for artemis-cam"
    )
    parser.add_argument("--host", default="127.0.0.1", help="gRPC signaling host")
    parser.add_argument("--port", type=int, default=50051, help="gRPC signaling port")
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="seconds to wait for the WebRTC answer",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_client(parse_args()))
