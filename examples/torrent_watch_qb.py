"""
Simple torrent watcher using AutoWorkflow and qBittorrent.

This script watches a directory for new .torrent files. For each new torrent:
- Adds it to qBittorrent to download into a specified directory
- Waits until the download finishes
- If total size > 0.5 GiB, compresses the result using 7-Zip

Requirements:
- pip install qbittorrent-api
- 7z CLI available in PATH (install p7zip / 7-Zip)

Usage example:
    python examples/torrent_watch_qb.py \
        --watch-dir "/path/to/incoming" \
        --download-dir "/path/to/downloads" \
        --qb-host "localhost" --qb-port 8080 \
        --qb-username "admin" --qb-password "adminadmin"
"""

from __future__ import annotations

import argparse
import uuid
import os
import time
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, cast
import logging

import qbittorrentapi

from autoworkflow.api import flow, task, switch
from autoworkflow.core import BaseContext, CONTEXT
from autoworkflow.services.engine import Engine
from autoworkflow.services.triggers import DirectoryWatchTrigger
from autoworkflow_plugins.qbittorrent import QBittorrentPlugin, HasQBittorrent

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Watch directory and download torrents via qBittorrent"
    )
    parser.add_argument(
        "--watch-dir", required=True, help="Directory to watch for .torrent files"
    )
    parser.add_argument(
        "--download-dir", required=True, help="Directory to save downloaded content"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Polling interval seconds for watcher",
    )
    parser.add_argument(
        "--size-threshold-gib",
        type=float,
        default=0.5,
        help="GiB threshold for compression",
    )

    # qBittorrent connection
    parser.add_argument("--qb-host", default="localhost")
    parser.add_argument("--qb-port", type=int, default=8080)
    parser.add_argument("--qb-username", default="admin")
    parser.add_argument("--qb-password", default="adminadmin")
    parser.add_argument("--qb-use-https", action="store_true")
    parser.add_argument("--qb-verify-ssl", action="store_true")
    parser.add_argument("--qb-timeout", type=int, default=30)

    return parser.parse_args()


@dataclass
class AppContext(BaseContext, HasQBittorrent):
    """Execution context with qBittorrent client injection points."""

    qbittorrent: qbittorrentapi.Client | None = None


def build_workflow(size_threshold_bytes: int):
    with flow("Torrent Watch & Compress") as f:

        @task
        def create_category() -> str:
            return f"aw-{uuid.uuid4().hex}"

        @task
        def add_torrent(
            ctx: AppContext = cast(AppContext, CONTEXT),
            torrent_path: str | None = None,
            download_dir: str | None = None,
            category: str | None = None,
        ) -> str:
            category = category or create_category()
            logger.info(
                "Adding torrent %s to %s in category %s",
                torrent_path,
                download_dir,
                category,
            )

            client = ctx.qbittorrent
            if client is None:
                raise RuntimeError("qBittorrent client not available in context")
            client.torrents_add(
                torrent_files=torrent_path,
                save_path=download_dir,
                category=category,
            )
            return str(category)

        @task
        def wait_for_completion(
            ctx: AppContext = cast(AppContext, CONTEXT),
            category: str = "",
            poll_interval: float = 2.0,
            timeout_seconds: float = 7 * 24 * 3600,
        ) -> Dict[str, Any]:
            logger.info("Waiting for completion of torrent in category %s", category)
            client = ctx.qbittorrent
            if client is None:
                raise RuntimeError("qBittorrent client not available in context")
            deadline = time.time() + timeout_seconds
            last_progress: float = 0.0

            displayed = -1
            while True:
                infos = list(client.torrents_info(category=category) or [])
                if not infos:
                    if time.time() > deadline:
                        raise TimeoutError(
                            f"Download did not complete in time (last progress={last_progress:.2%})"
                        )
                    time.sleep(poll_interval)
                    continue
                info = infos[0]
                name = getattr(info, "name", None) or category
                download_progress = float(getattr(info, "progress", 0.0))
                last_progress = max(last_progress, download_progress)
                percent_complete = int(max(0.0, min(1.0, download_progress)) * 100)
                if percent_complete != displayed:
                    bar_len = 30
                    filled = int(bar_len * percent_complete / 100)
                    bar = "#" * filled + "-" * (bar_len - filled)
                    print(
                        f"\rDownloading: {name} [{bar}] {percent_complete:3d}%",
                        end="",
                        flush=True,
                    )
                    displayed = percent_complete
                if download_progress >= 1.0:
                    print()  # newline after finishing
                    content_path = getattr(info, "content_path", None) or getattr(
                        info, "save_path", None
                    )
                    total_size = int(
                        getattr(info, "total_size", 0) or getattr(info, "size", 0)
                    )
                    return {
                        "content_path": content_path,
                        "total_size": total_size,
                        "category": category,
                    }
                if time.time() > deadline:
                    raise TimeoutError(
                        f"Download did not complete in time (last progress={last_progress:.2%})"
                    )
                time.sleep(poll_interval)

        @task
        def choose_compress(result: Dict[str, Any]) -> str:
            total_size = int(result.get("total_size", 0))
            decision = "compress" if total_size > size_threshold_bytes else "skip"
            logger.info(
                "Compression decision: %s (size=%s, threshold=%s)",
                decision,
                total_size,
                size_threshold_bytes,
            )
            return decision

        @task
        def compress_7z(input_path: str) -> str:
            base_name = os.path.basename(input_path.rstrip(os.sep))
            parent_dir = os.path.dirname(input_path.rstrip(os.sep)) or os.getcwd()
            archive_path = os.path.join(parent_dir, f"{base_name}.7z")

            cmd: List[str] = ["7z", "a", "-t7z", "-mx=5", archive_path, input_path]
            logger.info("Running compression: %s", " ".join(cmd))
            try:
                subprocess.run(cmd, check=True)
            except FileNotFoundError as exc:
                raise RuntimeError(
                    "7z command not found. Please install 7-Zip/p7zip and ensure '7z' is in PATH."
                ) from exc

            return archive_path

        # Wire dependencies using call-style
        category = create_category()
        added = add_torrent(category=category)
        waited = wait_for_completion(category=added)

        compress_bound = compress_7z(
            input_path=waited["content_path"]
        )  # pre-bind for case
        decision = choose_compress(result=waited)
        case_final_path = (
            switch(decision).when("compress", compress_bound).otherwise("skip")
        )

        @task
        def assemble_result(selected: Any, result: Dict[str, Any]) -> Dict[str, Any]:
            if selected == "skip":
                final_path = str(result.get("content_path"))
            else:
                final_path = str(selected)
            return {"final_path": final_path, **result}

        assembled = assemble_result(case_final_path, waited)

        @task
        def rename_with_torrent(
            data: Dict[str, Any], torrent_path: str | None = None
        ) -> Dict[str, Any]:
            final_path = str(data.get("final_path"))
            if not torrent_path:
                logger.info("No torrent_path provided, skip renaming: %s", final_path)
                return data

            if os.path.isdir(final_path):
                logger.info("Final path is a directory, skip renaming: %s", final_path)
                return data

            torrent_base = os.path.splitext(os.path.basename(torrent_path))[0]
            parent_dir = os.path.dirname(final_path) or os.getcwd()
            base_without_ext, ext = os.path.splitext(os.path.basename(final_path))
            new_name = f"{base_without_ext}_{torrent_base}{ext}"
            new_path = os.path.join(parent_dir, new_name)

            if new_path != final_path:
                try:
                    os.replace(final_path, new_path)
                    logger.info(
                        "Renamed final artifact: %s -> %s", final_path, new_path
                    )
                    return {**data, "final_path": new_path}
                except Exception as exc:
                    logger.warning(
                        "Failed to rename %s -> %s: %s", final_path, new_path, exc
                    )
                    return data
            return data

        _ = rename_with_torrent(assembled)  # connect terminal node

    return f.workflow


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    size_threshold_bytes = int(args.size_threshold_gib * (1024**3))
    workflow = build_workflow(size_threshold_bytes=size_threshold_bytes)

    engine = Engine(config={})

    engine.register_workflow(workflow)
    engine.register_plugin(
        QBittorrentPlugin(
            host=args.qb_host,
            port=args.qb_port,
            username=args.qb_username,
            password=args.qb_password,
            use_https=bool(args.qb_use_https),
            verify_ssl=bool(args.qb_verify_ssl),
            timeout=int(args.qb_timeout),
        )
    )
    engine.set_context_factory(lambda: AppContext())
    engine.enable_web_ui(host="127.0.0.1", port=8008)

    trigger = DirectoryWatchTrigger(
        watch_dir=args.watch_dir,
        interval_seconds=args.interval,
        stable_seconds=1.0,
        suffix=".torrent",
        payload_key="torrent_path",
    )
    engine.add_trigger(
        trigger,
        workflow_name=workflow.name,
        initial_data={"download_dir": os.path.abspath(args.download_dir)},
    )

    engine.run()


if __name__ == "__main__":
    main()
