from __future__ import annotations

import asyncio
import logging
import sys
from typing import Optional

import typer

from artemis_cam.capure import GStreamerCapture
from artemis_cam.server import serve as serve_server

app = typer.Typer(no_args_is_help=True)


@app.callback()
def main() -> None:
    """artemis-cam command line interface."""


@app.command("serve-grpc")
def serve_grpc(
    host: str = typer.Option(
        "0.0.0.0",
        help="gRPC server bind host.",
        envvar="ARTEMIS_CAM_HOST",
    ),
    port: int = typer.Option(
        50051,
        help="gRPC server listen port.",
        envvar="ARTEMIS_CAM_PORT",
    ),
    framerate: int = typer.Option(
        30,
        help="Capture frame rate.",
        envvar="ARTEMIS_CAM_FRAMERATE",
    ),
    bitrate: int = typer.Option(
        2_000_000,
        help="H.264 target bitrate in bps.",
        envvar="ARTEMIS_CAM_BITRATE",
    ),
    source_factory: str = typer.Option(
        "autovideosrc",
        help="GStreamer camera source element.",
        envvar="ARTEMIS_CAM_SOURCE",
    ),
    encoder_factory: Optional[str] = typer.Option(
        None,
        help="Optional GStreamer H.264 encoder element.",
        envvar="ARTEMIS_CAM_ENCODER",
    ),
):
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    if port <= 0:
        raise typer.BadParameter("port must be > 0")
    if framerate <= 0:
        raise typer.BadParameter("framerate must be > 0")
    if bitrate <= 0:
        raise typer.BadParameter("bitrate must be > 0")

    logger.info("Starting gRPC camera server with the following configuration:")
    logger.info("  Host: %s", host)
    logger.info("  Port: %s", port)
    logger.info("  Framerate: %s", framerate)
    logger.info("  Bitrate: %s", bitrate)
    logger.info("  Source: %s", source_factory)
    logger.info("  Encoder: %s", encoder_factory or "auto")

    async def _run() -> None:
        capture = GStreamerCapture(
            framerate=framerate,
            bitrate=bitrate,
            source_factory=source_factory,
            encoder_factory=encoder_factory,
        )
        server = await serve_server(
            capture,
            host=host,
            port=port,
            framerate=framerate,
        )
        typer.echo(f"gRPC camera server listening on {host}:{port}")

        try:
            await server.wait_for_termination()
        finally:
            await server.stop(0)
            capture.stop()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.append("serve-grpc")
    app()
