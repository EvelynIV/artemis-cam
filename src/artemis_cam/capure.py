from __future__ import annotations

import logging
import queue
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Protocol

logger = logging.getLogger(__name__)

_STOP_SENTINEL = object()
_GST_INIT_LOCK = threading.Lock()
_GST_MODULE: Any | None = None


def _load_gstreamer() -> Any:
    global _GST_MODULE
    if _GST_MODULE is not None:
        return _GST_MODULE

    with _GST_INIT_LOCK:
        if _GST_MODULE is not None:
            return _GST_MODULE

        try:
            import gi
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "GStreamer Python bindings are not available. Install PyGObject "
                "and the GStreamer runtime before starting capture."
            ) from exc

        gi.require_version("Gst", "1.0")
        from gi.repository import Gst

        Gst.init(None)
        _GST_MODULE = Gst
        return _GST_MODULE


class CaptureState(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    FAILED = "failed"


@dataclass(slots=True)
class EncodedVideoChunk:
    data: bytes
    codec: str
    pts_ns: int | None
    dts_ns: int | None
    duration_ns: int | None
    is_keyframe: bool


class EncodedStreamSink(Protocol):
    def push_h264_chunk(self, chunk: EncodedVideoChunk) -> None:
        ...


EncodedChunkConsumer = Callable[[EncodedVideoChunk], None]


class GStreamerCapture:
    """
    Capture camera video with GStreamer, encode it to H.264 and expose the
    encoded byte stream to upper layers such as WebRTC.
    """

    def __init__(
        self,
        *,
        width: int = 1280,
        height: int = 720,
        framerate: int = 30,
        bitrate: int = 2_000_000,
        source_factory: str = "autovideosrc",
        source_properties: dict[str, Any] | None = None,
        encoder_factory: str | None = None,
        output_queue_size: int = 30,
        state_change_timeout_seconds: float = 5.0,
    ) -> None:
        self.width = width
        self.height = height
        self.framerate = framerate
        self.bitrate = bitrate
        self.source_factory = source_factory
        self.source_properties = source_properties or {}
        self.encoder_factory = encoder_factory
        self.output_queue_size = output_queue_size
        self.state_change_timeout_seconds = state_change_timeout_seconds

        self._gst = None
        self._pipeline = None
        self._bus = None
        self._bus_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._state = CaptureState.STOPPED
        self._last_error: Exception | None = None
        self._encoded_chunks: queue.Queue[EncodedVideoChunk | object] = queue.Queue(
            maxsize=output_queue_size
        )
        self._encoded_consumer: EncodedChunkConsumer | None = None

    @property
    def state(self) -> CaptureState:
        with self._state_lock:
            return self._state

    @property
    def is_running(self) -> bool:
        return self.state is CaptureState.RUNNING

    def set_encoded_output(
        self,
        sink: EncodedStreamSink | EncodedChunkConsumer | None,
    ) -> None:
        if sink is None:
            self._encoded_consumer = None
            return
        if callable(sink):
            self._encoded_consumer = sink
            return
        if hasattr(sink, "push_h264_chunk"):
            self._encoded_consumer = sink.push_h264_chunk
            return
        raise TypeError("Encoded sink must be callable or implement push_h264_chunk().")

    def start(self) -> None:
        with self._state_lock:
            if self._state is CaptureState.RUNNING:
                return
            self._state = CaptureState.STARTING
            self._last_error = None

        self._stop_event.clear()
        self._reset_queue()

        try:
            self._gst = _load_gstreamer()
            self._build_pipeline()

            self._bus_thread = threading.Thread(
                target=self._poll_bus,
                name="gstreamer-capture-bus",
                daemon=True,
            )
            self._bus_thread.start()

            self._pipeline.set_state(self._gst.State.PLAYING)
            result, _, _ = self._pipeline.get_state(
                int(self.state_change_timeout_seconds * 1_000_000_000)
            )
            if result == self._gst.StateChangeReturn.FAILURE:
                raise RuntimeError("Failed to transition GStreamer pipeline to PLAYING.")

            with self._state_lock:
                self._state = CaptureState.RUNNING
        except Exception as exc:
            self._last_error = exc
            self._stop_event.set()
            with self._state_lock:
                self._state = CaptureState.FAILED
            self._signal_shutdown()
            self._teardown_pipeline()
            raise

    def stop(self) -> None:
        self._stop_event.set()
        self._signal_shutdown()
        self._teardown_pipeline()
        with self._state_lock:
            if self._state is not CaptureState.FAILED:
                self._state = CaptureState.STOPPED

    def pull_encoded_chunk(self, timeout: float | None = None) -> EncodedVideoChunk:
        try:
            item = self._encoded_chunks.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError("Timed out while waiting for encoded video.") from exc

        if item is _STOP_SENTINEL:
            self._enqueue(_STOP_SENTINEL)
            if self._last_error is not None:
                raise RuntimeError("Capture pipeline stopped because of an error.") from self._last_error
            raise RuntimeError("Capture pipeline is not running.")

        return item

    def close(self) -> None:
        self.stop()

    def __enter__(self) -> GStreamerCapture:
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    def _build_pipeline(self) -> None:
        gst = self._gst
        pipeline = gst.Pipeline.new("artemis-capture")
        if pipeline is None:
            raise RuntimeError("Failed to create GStreamer pipeline.")

        encoder_factory = self._resolve_encoder_factory()

        source = self._make_element(self.source_factory, "camera-source")
        videoconvert = self._make_element("videoconvert", "video-convert")
        videoscale = self._make_element("videoscale", "video-scale")
        videorate = self._make_element("videorate", "video-rate")
        capsfilter = self._make_element("capsfilter", "capture-caps")
        encoder = self._make_element(encoder_factory, "h264-encoder")
        parser = self._make_element("h264parse", "h264-parser")
        sink = self._make_element("appsink", "encoded-sink")

        self._configure_source(source)
        self._configure_caps(capsfilter)
        self._configure_encoder(encoder, encoder_factory)
        self._configure_parser(parser)
        self._configure_encoded_sink(sink)

        for element in (
            source,
            videoconvert,
            videoscale,
            videorate,
            capsfilter,
            encoder,
            parser,
            sink,
        ):
            pipeline.add(element)

        if not source.link(videoconvert):
            raise RuntimeError("Failed to link source -> videoconvert.")
        if not videoconvert.link(videoscale):
            raise RuntimeError("Failed to link videoconvert -> videoscale.")
        if not videoscale.link(videorate):
            raise RuntimeError("Failed to link videoscale -> videorate.")
        if not videorate.link(capsfilter):
            raise RuntimeError("Failed to link videorate -> capsfilter.")
        if not capsfilter.link(encoder):
            raise RuntimeError("Failed to link capsfilter -> encoder.")
        if not encoder.link(parser):
            raise RuntimeError("Failed to link encoder -> parser.")
        if not parser.link(sink):
            raise RuntimeError("Failed to link parser -> appsink.")

        sink.connect("new-sample", self._on_encoded_sample)

        self._pipeline = pipeline
        self._bus = pipeline.get_bus()

    def _teardown_pipeline(self) -> None:
        bus_thread = self._bus_thread
        if self._pipeline is not None:
            self._pipeline.set_state(self._gst.State.NULL)

        self._pipeline = None
        self._bus = None

        if bus_thread is not None and bus_thread.is_alive():
            bus_thread.join(timeout=1.0)
        self._bus_thread = None

    def _poll_bus(self) -> None:
        if self._bus is None:
            return

        message_types = self._gst.MessageType.ERROR | self._gst.MessageType.EOS

        while not self._stop_event.is_set():
            message = self._bus.timed_pop_filtered(100 * self._gst.MSECOND, message_types)
            if message is None:
                continue

            if message.type == self._gst.MessageType.ERROR:
                err, debug = message.parse_error()
                self._last_error = RuntimeError(str(err))
                logger.error("GStreamer pipeline error: %s (%s)", err, debug)
                with self._state_lock:
                    self._state = CaptureState.FAILED
                self._stop_event.set()
                self._signal_shutdown()
                return

            if message.type == self._gst.MessageType.EOS:
                with self._state_lock:
                    self._state = CaptureState.STOPPED
                self._stop_event.set()
                self._signal_shutdown()
                return

    def _on_encoded_sample(self, sink: Any) -> Any:
        sample = sink.emit("pull-sample")
        if sample is None:
            return self._gst.FlowReturn.ERROR

        chunk = self._extract_encoded_chunk(sample)
        self._enqueue(chunk)

        if self._encoded_consumer is not None:
            try:
                self._encoded_consumer(chunk)
            except Exception:
                logger.exception("Encoded consumer failed while handling H.264 chunk.")

        return self._gst.FlowReturn.OK

    def _extract_encoded_chunk(self, sample: Any) -> EncodedVideoChunk:
        buffer = sample.get_buffer()
        return EncodedVideoChunk(
            data=self._read_buffer(buffer),
            codec="h264",
            pts_ns=self._normalize_gst_time(buffer.pts),
            dts_ns=self._normalize_gst_time(buffer.dts),
            duration_ns=self._normalize_gst_time(buffer.duration),
            is_keyframe=not buffer.has_flags(self._gst.BufferFlags.DELTA_UNIT),
        )

    def _read_buffer(self, buffer: Any) -> bytes:
        ok, map_info = buffer.map(self._gst.MapFlags.READ)
        if not ok:
            raise RuntimeError("Failed to map GStreamer buffer for reading.")
        try:
            return bytes(map_info.data)
        finally:
            buffer.unmap(map_info)

    def _normalize_gst_time(self, value: int) -> int | None:
        if value == self._gst.CLOCK_TIME_NONE:
            return None
        return int(value)

    def _signal_shutdown(self) -> None:
        self._enqueue(_STOP_SENTINEL)

    def _reset_queue(self) -> None:
        while True:
            try:
                self._encoded_chunks.get_nowait()
            except queue.Empty:
                return

    def _enqueue(self, item: EncodedVideoChunk | object) -> None:
        try:
            self._encoded_chunks.put_nowait(item)
        except queue.Full:
            try:
                self._encoded_chunks.get_nowait()
            except queue.Empty:
                pass
            self._encoded_chunks.put_nowait(item)

    def _resolve_encoder_factory(self) -> str:
        if self.encoder_factory is not None:
            if self._gst.ElementFactory.find(self.encoder_factory) is None:
                raise RuntimeError(
                    f"Requested H.264 encoder '{self.encoder_factory}' is not available."
                )
            return self.encoder_factory

        for candidate in ("x264enc", "openh264enc"):
            if self._gst.ElementFactory.find(candidate) is not None:
                return candidate

        raise RuntimeError(
            "No supported H.264 encoder was found. Install x264enc or openh264enc."
        )

    def _make_element(self, factory: str, name: str) -> Any:
        element = self._gst.ElementFactory.make(factory, name)
        if element is None:
            raise RuntimeError(f"GStreamer element '{factory}' is not available.")
        return element

    def _configure_source(self, source: Any) -> None:
        for key, value in self.source_properties.items():
            self._set_property_if_supported(source, key, value)

    def _configure_caps(self, capsfilter: Any) -> None:
        caps = self._gst.Caps.from_string(
            "video/x-raw,"
            f"format=I420,width={self.width},height={self.height},"
            f"framerate={self.framerate}/1"
        )
        capsfilter.set_property("caps", caps)

    def _configure_encoder(self, encoder: Any, encoder_factory: str) -> None:
        if encoder_factory == "x264enc":
            self._set_property_if_supported(encoder, "tune", "zerolatency")
            self._set_property_if_supported(encoder, "speed-preset", "veryfast")
            self._set_property_if_supported(encoder, "bitrate", max(self.bitrate // 1000, 1))
            self._set_property_if_supported(encoder, "key-int-max", max(self.framerate, 1))
            self._set_property_if_supported(encoder, "byte-stream", True)
            return

        if encoder_factory == "openh264enc":
            self._set_property_if_supported(encoder, "bitrate", max(self.bitrate, 1))
            self._set_property_if_supported(encoder, "gop-size", max(self.framerate, 1))

    def _configure_parser(self, parser: Any) -> None:
        self._set_property_if_supported(parser, "config-interval", -1)

    def _configure_encoded_sink(self, sink: Any) -> None:
        caps = self._gst.Caps.from_string(
            "video/x-h264,stream-format=byte-stream,alignment=au"
        )
        sink.set_property("caps", caps)
        sink.set_property("emit-signals", True)
        sink.set_property("sync", False)
        sink.set_property("drop", True)
        sink.set_property("max-buffers", self.output_queue_size)

    def _set_property_if_supported(self, element: Any, name: str, value: Any) -> None:
        if value is None:
            return
        if hasattr(element, "find_property") and element.find_property(name) is None:
            return
        element.set_property(name, value)
