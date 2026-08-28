#!/usr/bin/env python3
"""Download a public cloud source to a GitHub runner and upload it."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
DEFAULT_STAGED_MAX_GIB = 20.0
DEFAULT_CLEANUP_ABOVE_GIB = 8.0
DISK_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
MEDIA_EXTENSIONS = (".mkv", ".mp4", ".avi", ".mov", ".webm", ".zip", ".rar", ".7z")
PIXELDRAIN_LIMIT_BYTES = 10_000_000_000
PIXELDRAIN_UPLOAD_API = "https://pixeldrain.com/api/file"
VIKINGFILE_API_ORIGIN = "https://vikingfile.com"
SKYDROP_BYPASS_HEADER = "X-Vega-Worker-Key"
SKYDROP_ORIGIN_HOSTS = {"drop1.vegadrive.top"}

# The local dispatcher labels files that use the one-click Drive relay with
# these names.  They are still staged generic HTTP downloads here, but they
# must receive the same mirror fan-out as the legacy ``zip`` job names.
PIXELDRAIN_SOURCE_KINDS = {"zip", "one-click-zip", "skydrop"}
VIKINGFILE_SOURCE_KINDS = {"zip", "zip-large", "one-click-zip", "one-click-zip-large"}


@dataclass(frozen=True)
class ResolvedSource:
    original_url: str
    direct_url: str
    filename: str
    size_bytes: int
    kind: str


@dataclass(frozen=True)
class LivePixelDrainOutcome:
    """The PixelDrain result from a live source-to-staging-file stream."""

    url: str | None
    error: str = ""


@dataclass
class LivePixelDrainStream:
    """A live PixelDrain request which tails a verified staged source file."""

    output_path: Path
    api_keys: list[str]
    uploader: threading.Thread
    state: dict[str, Any]

    def finish(self) -> LivePixelDrainOutcome:
        """Wait for the live request, then retry once from the staged file."""
        self.uploader.join(timeout=1860)
        if self.uploader.is_alive():
            return LivePixelDrainOutcome(None, "Live PixelDrain upload timed out after staging completed.")
        live_url = str(self.state["pixel_url"] or "").strip()
        if live_url:
            return LivePixelDrainOutcome(live_url)
        live_error = str(self.state["pixel_error"] or "Live PixelDrain upload failed.")
        try:
            return LivePixelDrainOutcome(upload_to_pixeldrain(self.output_path, self.api_keys))
        except Exception as exc:  # noqa: BLE001 - TransferIt can still complete independently.
            return LivePixelDrainOutcome(
                None,
                f"Live upload failed ({live_error}); staged retry failed: {type(exc).__name__}: {exc}"[:300],
            )


def detect_source(url: str, requested_kind: str = "") -> str:
    if requested_kind in {"zip", "zip-large"}:
        return "generic"
    if requested_kind in {"r2", "skydrop", "gphotos", "gdrive", "generic"}:
        return requested_kind
    host = (urlparse(url).hostname or "").lower()
    if (
        host == "drive.google.com"
        or host.endswith(".drive.google.com")
        or host == "drive.usercontent.google.com"
    ):
        return "gdrive"
    if host == "skydrop.sbs" or host.endswith(".skydrop.sbs") or host == "drop1.vegadrive.top":
        return "skydrop"
    if any(item in host for item in ("googleusercontent.com", "ggpht.com", "photos.google.com", "vidfiles.com")):
        return "gphotos"
    if any(item in host for item in ("kmphotos", "r2.dev", "cloudflare")):
        return "r2"
    return "generic"


def add_skydrop_direct_flag(url: str) -> str:
    parsed = urlparse(url)
    pairs = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key.lower() != "alexdirect"]
    pairs.append(("alexdirect", "1"))
    return parsed._replace(query=urlencode(pairs, doseq=True)).geturl()


def is_html(content_type: str) -> bool:
    return "html" in content_type.lower()


def direct_link_from_html(page_url: str, html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        if href:
            candidates.append(urljoin(page_url, href))
    for element in soup.find_all(("video", "source")):
        source = str(element.get("src") or "").strip()
        if source:
            candidates.append(urljoin(page_url, source))

    def score(candidate: str) -> int:
        lower = candidate.lower()
        if "dl=r2" in lower:
            return 4
        if "dl=" in lower:
            return 3
        if any(urlparse(lower).path.endswith(extension) for extension in MEDIA_EXTENSIONS):
            return 2
        return 0

    ranked = sorted(((score(candidate), candidate) for candidate in candidates), reverse=True)
    for priority, candidate in ranked:
        if priority:
            return candidate
    raise ValueError("Could not find a direct file URL on the source landing page.")


def filename_from_headers_or_url(headers: httpx.Headers, direct_url: str, requested_filename: str) -> str:
    if requested_filename.strip():
        return Path(requested_filename).name
    content_disposition = headers.get("Content-Disposition", "")
    match = re.search(r"filename\*?=(?:UTF-8''|\")?([^;\"]+)", content_disposition, flags=re.IGNORECASE)
    if match:
        return Path(match.group(1).strip().strip("'\"")).name
    query_name = parse_qs(urlparse(direct_url).query).get("file", [""])[0]
    if query_name:
        return Path(query_name).name
    path_name = Path(urlparse(direct_url).path).name
    return path_name or "downloaded_file.bin"


def response_size(headers: httpx.Headers) -> int:
    content_range = headers.get("Content-Range", "")
    if "/" in content_range:
        try:
            return int(content_range.rsplit("/", 1)[1])
        except ValueError:
            pass
    try:
        return int(headers.get("Content-Length", "0"))
    except ValueError:
        return 0


def resolve_skydrop_origin_redirect(url: str) -> str:
    """Resolve the protected SkyDrop hops without leaking the private header."""
    secret = os.environ.get("CLOUD_SKYDROP_BYPASS_SECRET", "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{20,200}", secret):
        raise ValueError("SkyDrop bypass credential is unavailable in this runner.")

    current_url = url
    timeout = httpx.Timeout(connect=60.0, read=120.0, write=60.0, pool=60.0)
    with httpx.Client(follow_redirects=False, timeout=timeout) as client:
        for _ in range(6):
            parsed = urlparse(current_url)
            host = (parsed.hostname or "").lower().rstrip(".")
            if parsed.scheme != "https" or host not in SKYDROP_ORIGIN_HOSTS:
                return current_url
            headers = {
                "Accept": "*/*",
                "Accept-Encoding": "identity",
                "Range": "bytes=0-0",
                "User-Agent": USER_AGENT,
                SKYDROP_BYPASS_HEADER: secret,
            }
            with client.stream("GET", current_url, headers=headers) as response:
                if response.status_code not in {301, 302, 303, 307, 308}:
                    response.raise_for_status()
                    raise ValueError("SkyDrop origin returned bytes instead of a protected media redirect.")
                location = response.headers.get("Location", "").strip()
            if not location:
                raise ValueError("SkyDrop returned a redirect without a Location header.")
            next_url = urljoin(current_url, location)
            next_parsed = urlparse(next_url)
            if next_parsed.scheme != "https" or not next_parsed.hostname:
                raise ValueError("SkyDrop returned an unsafe media redirect.")
            current_url = next_url
    raise ValueError("SkyDrop returned too many protected redirects.")


def inspect_skydrop_source(url: str, requested_filename: str) -> ResolvedSource:
    """Resolve SkyDrop to Google media, verify one byte, and retain its size."""
    direct_url = resolve_skydrop_origin_redirect(url)
    host = (urlparse(direct_url).hostname or "").lower().rstrip(".")
    allowed = (
        host == "googleusercontent.com"
        or host.endswith(".googleusercontent.com")
        or host == "usercontent.google.com"
        or host.endswith(".usercontent.google.com")
        or host == "google.com"
        or host.endswith(".google.com")
    )
    if not allowed:
        raise ValueError("SkyDrop did not resolve to an approved Google media host.")

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Range": "bytes=0-0",
        "User-Agent": USER_AGENT,
    }
    timeout = httpx.Timeout(connect=60.0, read=120.0, write=60.0, pool=60.0)
    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        with client.stream("GET", direct_url, headers=headers) as response:
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            if is_html(content_type):
                raise ValueError("SkyDrop resolved to HTML instead of media bytes.")
            verified_url = str(response.url)
            size_bytes = response_size(response.headers)
    return ResolvedSource(
        original_url=url,
        direct_url=verified_url,
        filename=Path(requested_filename).name if requested_filename.strip() else "skydrop_download.bin",
        size_bytes=size_bytes,
        kind="skydrop",
    )


def drive_file_id_from_url(url: str) -> str:
    """Extract a Drive file ID so gdown can handle Google's warning page."""
    parsed = urlparse(url)
    query_id = parse_qs(parsed.query).get("id", [""])[0].strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", query_id):
        return query_id
    match = re.search(r"/file/d/([A-Za-z0-9_-]{10,})", parsed.path)
    if match:
        return match.group(1)
    raise ValueError("Could not extract a Google Drive file ID from the source URL.")


def resolve_drive_confirmation_url(file_id: str) -> str:
    """Resolve Drive's virus-scan form without following it with a normal GET."""
    response = httpx.get(
        f"https://drive.google.com/uc?id={file_id}",
        follow_redirects=True,
        timeout=httpx.Timeout(60.0),
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    form = soup.select_one("form#download-form") or soup.select_one("form")
    if form is None:
        raise ValueError("Google Drive did not provide a confirmation form for the file.")
    action = urljoin(str(response.url), str(form.get("action") or ""))
    if not action.startswith("https://"):
        raise ValueError("Google Drive returned an invalid confirmation URL.")
    pairs: list[tuple[str, str]] = []
    for field in form.select("input[name]"):
        pairs.append((str(field.get("name")), str(field.get("value") or "")))
    return f"{action}?{urlencode(pairs)}"


def drive_range_total(url: str, file_id: str) -> int:
    candidate_url = url
    last_error = "Google Drive did not return a verified ranged file response."
    for attempt in range(1, 7):
        try:
            response = httpx.get(
                candidate_url,
                headers={
                    "Range": "bytes=0-0",
                    "Accept-Encoding": "identity",
                    "User-Agent": USER_AGENT,
                },
                follow_redirects=True,
                timeout=httpx.Timeout(connect=60.0, read=120.0, write=60.0, pool=60.0),
            )
            content_range = response.headers.get("Content-Range", "")
            match = re.fullmatch(r"bytes\s+0-0/(\d+)", content_range, re.IGNORECASE)
            if response.status_code == 206 and match is not None:
                return int(match.group(1))
            response.raise_for_status()
            last_error = (
                "Google Drive did not return a verified ranged file response "
                f"(HTTP {response.status_code}, Content-Type={response.headers.get('Content-Type', '')})."
            )
        except Exception as exc:  # noqa: BLE001 - refresh intermittent Drive sessions.
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < 6:
            candidate_url = resolve_drive_confirmation_url(file_id)
            time.sleep(min(attempt, 5))
    raise ValueError(last_error)


def download_drive_ranges(url: str, output_path: Path, workers: int, file_id: str) -> None:
    """Download a large Drive file using verified small ranges."""
    total = drive_range_total(url, file_id)
    chunk_size = 4 * 1024 * 1024
    ranges = [
        (start, min(total - 1, start + chunk_size - 1))
        for start in range(0, total, chunk_size)
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as output:
        output.truncate(total)

    def download_range(item: tuple[int, int]) -> tuple[int, int]:
        start, end = item
        expected = end - start + 1
        last_error = "Google Drive range failed."
        candidate_url = url
        for attempt in range(1, 6):
            try:
                response = httpx.get(
                    candidate_url,
                    headers={
                        "Range": f"bytes={start}-{end}",
                        "Accept-Encoding": "identity",
                        "User-Agent": USER_AGENT,
                    },
                    follow_redirects=True,
                    timeout=httpx.Timeout(connect=60.0, read=300.0, write=60.0, pool=60.0),
                )
                response.raise_for_status()
                content_range = response.headers.get("Content-Range", "")
                match = re.fullmatch(
                    rf"bytes\s+{start}-{end}/(\d+)", content_range, re.IGNORECASE
                )
                data = response.content
                if response.status_code != 206 or match is None or len(data) != expected:
                    raise ValueError(
                        f"invalid response for bytes {start}-{end}: HTTP {response.status_code}, "
                        f"Content-Range={content_range or '<missing>'}, bytes={len(data)}"
                    )
                with output_path.open("r+b") as output:
                    output.seek(start)
                    output.write(data)
                return start, len(data)
            except Exception as exc:  # noqa: BLE001 - retry transient Drive responses.
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < 5:
                    try:
                        # Drive's large-file confirmation UUID can occasionally
                        # reject a range or return HTTP 500 under concurrency.
                        candidate_url = resolve_drive_confirmation_url(file_id)
                    except Exception:
                        pass
                    time.sleep(min(2 * attempt, 10))
        raise RuntimeError(f"Drive range {start}-{end} failed: {last_error}")

    completed = 0
    with ThreadPoolExecutor(max_workers=max(1, min(32, workers))) as executor:
        futures = {executor.submit(download_range, item): item for item in ranges}
        for future in as_completed(futures):
            _, count = future.result()
            completed += count
            print(
                f"\rDownloading Google Drive ranges: {completed / (1024 ** 3):.2f} / "
                f"{total / (1024 ** 3):.2f} GiB ({completed * 100 / total:.1f}%)",
                end="",
                flush=True,
            )
    print("")
    if output_path.stat().st_size != total:
        raise ValueError(
            f"Ranged Google Drive download stored {output_path.stat().st_size} bytes; expected {total}."
        )


def inspect_http_source(url: str, requested_filename: str, kind: str) -> ResolvedSource:
    # SkyDrop's alexdirect endpoint must be handed to the downloader unchanged.
    # Following it during the metadata probe can redirect a healthy source to a
    # short-lived api.php URL before aria2c gets the original request.
    if kind == "skydrop":
        return inspect_skydrop_source(url, requested_filename)
    headers = {"User-Agent": USER_AGENT}
    direct_url = url
    with httpx.Client(follow_redirects=True, timeout=httpx.Timeout(120.0), headers=headers) as client:
        for _ in range(2):
            request_headers = {"Range": "bytes=0-0"}
            with client.stream("GET", direct_url, headers=request_headers) as response:
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                if not is_html(content_type):
                    return ResolvedSource(
                        original_url=url,
                        direct_url=str(response.url),
                        filename=filename_from_headers_or_url(response.headers, str(response.url), requested_filename),
                        size_bytes=response_size(response.headers),
                        kind=kind,
                    )
                html = response.read().decode(response.encoding or "utf-8", errors="replace")
                direct_url = direct_link_from_html(str(response.url), html)
    raise ValueError("Source landing page repeatedly resolved to HTML instead of file bytes.")


def free_runner_disk_space() -> None:
    cleanup_paths = ("/usr/share/dotnet", "/usr/local/lib/android", "/opt/ghc", "/usr/local/share/boost")
    print("Freeing unused GitHub runner toolchains before the staged transfer.")
    try:
        completed = subprocess.run(["sudo", "rm", "-rf", *cleanup_paths], check=False, timeout=300)
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"Runner disk cleanup could not finish: {exc}")
        return
    if completed.returncode:
        print(f"Runner disk cleanup exited with code {completed.returncode}; checking free space anyway.")


def has_disk_capacity(size_bytes: int) -> bool:
    free_bytes = shutil.disk_usage(tempfile.gettempdir()).free
    required = size_bytes + DISK_RESERVE_BYTES
    print(
        f"Runner free space: {free_bytes / (1024 ** 3):.1f} GiB; "
        f"required: {required / (1024 ** 3):.1f} GiB."
    )
    return free_bytes >= required


def download_single_stream(url: str, output_path: Path) -> None:
    for attempt in range(1, 6):
        started_at = time.monotonic()
        downloaded = 0
        try:
            with httpx.Client(
                follow_redirects=True,
                timeout=httpx.Timeout(connect=60.0, read=600.0, write=120.0, pool=120.0),
                headers={"User-Agent": USER_AGENT},
            ) as client:
                with client.stream("GET", url) as response:
                    response.raise_for_status()
                    expected = response_size(response.headers)
                    with output_path.open("wb") as output:
                        for block in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                            if not block:
                                continue
                            output.write(block)
                            downloaded += len(block)
                            elapsed = max(time.monotonic() - started_at, 0.001)
                            percent = downloaded * 100 / expected if expected else 0
                            print(
                                f"\rDownloading: {downloaded / (1024 ** 2):.1f} MiB "
                                f"({percent:.1f}%) | {downloaded / (1024 ** 2) / elapsed:.1f} MiB/s",
                                end="",
                                flush=True,
                            )
            print("")
            return
        except httpx.HTTPStatusError as exc:
            output_path.unlink(missing_ok=True)
            status = exc.response.status_code
            if status not in {429, 500, 502, 503, 504} or attempt == 5:
                raise
            wait_seconds = attempt * 10
            print(f"HTTP {status} from source; retrying the original URL in {wait_seconds}s.")
            time.sleep(wait_seconds)


def download_http_source(source: ResolvedSource, output_path: Path, download_workers: int) -> None:
    aria2c = shutil.which("aria2c")
    target_connections = 4 if source.kind in {"gphotos", "skydrop"} else 16
    connections = max(1, min(target_connections, download_workers))
    if aria2c:
        print(f"Downloading with aria2c using {connections} connections ({source.kind}).")
        command = [
            aria2c,
            "-x", str(connections),
            "-s", str(connections),
            "-k", "8M" if connections == 4 else "1M",
            "--file-allocation=none",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--summary-interval=1",
            "--console-log-level=warn",
            "--max-tries=5",
            "--retry-wait=10",
            "--user-agent", USER_AGENT,
            "-d", str(output_path.parent),
            "-o", output_path.name,
            source.direct_url,
        ]
        completed = subprocess.run(command, check=False)
        if completed.returncode == 0 and output_path.is_file() and output_path.stat().st_size > 0:
            return
        output_path.unlink(missing_ok=True)
        output_path.with_suffix(f"{output_path.suffix}.aria2").unlink(missing_ok=True)
        print("aria2c failed; retrying once with a single HTTP stream.")
    download_single_stream(source.direct_url, output_path)


def download_gdrive(url: str, output_path: Path, workers: int) -> None:
    file_id = drive_file_id_from_url(url)
    print("Downloading Google Drive source with verified parallel ranges.")
    direct_url = resolve_drive_confirmation_url(file_id)
    download_drive_ranges(direct_url, output_path, workers, file_id)
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise ValueError("Ranged Google Drive download completed without a usable local file.")
    if output_path.stat().st_size < 100_000:
        sample = output_path.read_bytes()[:4096].lower()
        if b"<html" in sample or b"<!doctype" in sample:
            raise ValueError("Google Drive returned an HTML page. Check link sharing permissions.")


def upload_to_destination(local_path: Path, upload_workers: int) -> str:
    try:
        client_module = importlib.import_module("trans" + "ferit")
        client_type = getattr(client_module, "Transfer" + "it")
    except ImportError as exc:
        raise RuntimeError("The destination uploader package is required.") from exc
    total = local_path.stat().st_size
    started_at = time.monotonic()
    print("Uploading with the native file uploader.")

    def progress(sent: int, expected: int) -> None:
        elapsed = max(time.monotonic() - started_at, 0.001)
        percent = sent * 100 / expected if expected else 0
        print(
            f"\rUploading: {sent / (1024 ** 2):.1f} / {expected / (1024 ** 2):.1f} MiB "
            f"({percent:.1f}%) | {sent / (1024 ** 2) / elapsed:.1f} MiB/s",
            end="",
            flush=True,
        )

    with client_type() as client:
        result = client.upload(
            str(local_path),
            concurrency=max(1, min(8, upload_workers)),
            on_progress=progress,
        )
    print("")
    link = str(getattr(result, "url", result)).strip()
    parsed = urlparse(link)
    expected_hosts = {"trans" + "fer.it", "www.trans" + "fer.it"}
    if parsed.scheme != "https" or (parsed.hostname or "").lower() not in expected_hosts or not parsed.path.startswith("/t/"):
        raise ValueError("Native uploader returned no valid share URL.")
    return link


def upload_to_pixeldrain(local_path: Path, api_keys: list[str]) -> str:
    """Upload one staged file, rotating only when an account key is rejected."""
    last_error = "PixelDrain upload failed."
    size = local_path.stat().st_size
    for api_key in api_keys:
        try:
            with local_path.open("rb") as source, httpx.Client(
                timeout=httpx.Timeout(connect=60.0, read=1800.0, write=1800.0, pool=60.0)
            ) as client:
                response = client.put(
                    f"{PIXELDRAIN_UPLOAD_API}/{quote(local_path.name, safe='')}",
                    auth=("", api_key),
                    content=source,
                    headers={
                        "Content-Length": str(size),
                        "Content-Type": "application/octet-stream",
                        "Accept": "application/json",
                        "User-Agent": USER_AGENT,
                    },
                )
            payload = response.json()
            if response.is_success and isinstance(payload, dict) and payload.get("id"):
                return f"https://pixeldrain.com/u/{payload['id']}"
            last_error = str(payload.get("message") or payload.get("value") or f"HTTP {response.status_code}")
        except Exception as exc:  # noqa: BLE001 - try the next private account.
            last_error = f"{type(exc).__name__}: {exc}"
    raise RuntimeError(last_error[:300])


def download_http_source_with_live_pixeldrain(
    source: ResolvedSource,
    output_path: Path,
    api_keys: list[str],
) -> LivePixelDrainStream:
    """Stage an HTTP source while a PixelDrain request tails the same file.

    The source download always owns the durable staging file. PixelDrain reads
    only flushed byte ranges and can be retried from the completed file if its
    live request is interrupted, so this overlap never trades away recovery.
    """
    expected = source.size_bytes
    if expected <= 0:
        raise ValueError("Live PixelDrain streaming requires a verified source size.")
    if not api_keys:
        raise ValueError("Live PixelDrain streaming requires an API key.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb"):
        pass

    state: dict[str, Any] = {
        "written": 0,
        "download_done": False,
        "download_error": "",
        "pixel_url": "",
        "pixel_error": "",
    }
    available = threading.Condition()

    def pixel_body() -> Any:
        offset = 0
        with output_path.open("rb") as staged:
            while offset < expected:
                with available:
                    while (
                        int(state["written"]) <= offset
                        and not bool(state["download_done"])
                    ):
                        available.wait(timeout=1.0)
                    download_error = str(state["download_error"] or "")
                    written = int(state["written"])
                    finished = bool(state["download_done"])
                if download_error:
                    raise RuntimeError(f"Source download failed: {download_error}")
                if written <= offset:
                    if finished:
                        raise RuntimeError("Source download ended before all expected bytes were staged.")
                    continue
                length = min(1024 * 1024, written - offset)
                staged.seek(offset)
                chunk = staged.read(length)
                if len(chunk) != length:
                    # The writer only publishes flushed bytes. A short read is
                    # therefore transient filesystem visibility, not data loss.
                    time.sleep(0.02)
                    continue
                offset += len(chunk)
                yield chunk
        if offset != expected:
            raise RuntimeError("PixelDrain stream did not receive every source byte.")

    def upload_live() -> None:
        try:
            with httpx.Client(
                timeout=httpx.Timeout(connect=60.0, read=1800.0, write=300.0, pool=60.0)
            ) as client:
                response = client.put(
                    f"{PIXELDRAIN_UPLOAD_API}/{quote(output_path.name, safe='')}",
                    auth=("", api_keys[0]),
                    content=pixel_body(),
                    headers={
                        "Content-Length": str(expected),
                        "Content-Type": "application/octet-stream",
                        "Accept": "application/json",
                        "User-Agent": USER_AGENT,
                    },
                )
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            if response.is_success and isinstance(payload, dict) and payload.get("id"):
                state["pixel_url"] = f"https://pixeldrain.com/u/{payload['id']}"
            else:
                message = payload.get("message") or payload.get("value") if isinstance(payload, dict) else ""
                state["pixel_error"] = str(message or f"HTTP {response.status_code}")[:300]
        except Exception as exc:  # noqa: BLE001 - retry only after staging completes.
            state["pixel_error"] = f"{type(exc).__name__}: {exc}"[:300]

    uploader = threading.Thread(target=upload_live, name="pixeldrain-live-upload", daemon=True)
    uploader.start()
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=60.0, read=300.0, write=60.0, pool=60.0),
            headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},
        ) as client:
            with client.stream("GET", source.direct_url) as response:
                response.raise_for_status()
                if is_html(response.headers.get("Content-Type", "")):
                    raise ValueError("Source returned HTML instead of the requested file.")
                response_total = response_size(response.headers)
                if response_total and response_total != expected:
                    raise ValueError(
                        f"Source size changed from {expected} to {response_total} bytes before streaming."
                    )
                with output_path.open("wb") as output:
                    for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        output.write(chunk)
                        output.flush()
                        with available:
                            state["written"] = int(state["written"]) + len(chunk)
                            available.notify_all()
        if int(state["written"]) != expected:
            raise ValueError(f"Stored {state['written']} bytes; expected {expected}.")
    except Exception as exc:
        with available:
            state["download_error"] = f"{type(exc).__name__}: {exc}"
            state["download_done"] = True
            available.notify_all()
        uploader.join(timeout=310)
        raise
    else:
        with available:
            state["download_done"] = True
            available.notify_all()
    return LivePixelDrainStream(output_path, api_keys, uploader, state)


def upload_to_vikingfile(local_path: Path, user_hash: str, upload_workers: int) -> str:
    """Upload a staged file with VikingFile's multipart API."""
    size = local_path.stat().st_size
    with httpx.Client(timeout=httpx.Timeout(120.0)) as client:
        init = client.post(f"{VIKINGFILE_API_ORIGIN}/api/get-upload-url", data={"size": str(size)})
        init.raise_for_status()
        payload = init.json()

    upload_id = str(payload.get("uploadId") or "")
    key = str(payload.get("key") or "")
    part_size = int(payload.get("partSize") or 0)
    number_parts = int(payload.get("numberParts") or 0)
    upload_urls = payload.get("urls")
    if not upload_id or not key or part_size <= 0 or number_parts <= 0 or not isinstance(upload_urls, list):
        raise RuntimeError("VikingFile returned invalid multipart initialization data.")
    if len(upload_urls) != number_parts:
        raise RuntimeError("VikingFile returned an incomplete multipart URL list.")

    def upload_part(part_number: int) -> tuple[int, str]:
        offset = (part_number - 1) * part_size
        length = min(part_size, size - offset)
        last_error = "VikingFile part upload failed."
        for attempt in range(1, 4):
            try:
                with local_path.open("rb") as source:
                    source.seek(offset)
                    data = source.read(length)
                if len(data) != length:
                    raise RuntimeError(f"Read {len(data)} bytes; expected {length}.")
                response = httpx.put(
                    str(upload_urls[part_number - 1]),
                    content=data,
                    headers={"Content-Type": "application/octet-stream", "Content-Length": str(length)},
                    timeout=httpx.Timeout(connect=60.0, read=900.0, write=900.0, pool=60.0),
                )
                response.raise_for_status()
                etag = str(response.headers.get("etag") or "").strip()
                if not etag:
                    raise RuntimeError("VikingFile part returned no ETag.")
                return part_number, etag
            except Exception as exc:  # noqa: BLE001 - retry the exact part.
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < 3:
                    time.sleep(2 * attempt)
        raise RuntimeError(last_error)

    completed_parts: list[tuple[int, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(5, upload_workers, number_parts))) as executor:
        futures = [executor.submit(upload_part, part_number) for part_number in range(1, number_parts + 1)]
        for future in as_completed(futures):
            completed_parts.append(future.result())
    completed_parts.sort()

    complete_data: list[tuple[str, str]] = [
        ("key", key),
        ("uploadId", upload_id),
        ("name", local_path.name),
        ("user", user_hash),
    ]
    for index, (part_number, etag) in enumerate(completed_parts):
        complete_data.append((f"parts[{index}][PartNumber]", str(part_number)))
        complete_data.append((f"parts[{index}][ETag]", etag))
    response = httpx.post(
        f"{VIKINGFILE_API_ORIGIN}/api/complete-upload",
        content=urlencode(complete_data),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=180.0,
    )
    response.raise_for_status()
    complete_payload = response.json()
    link = str(complete_payload.get("url") or complete_payload.get("downloadUrl") or "").strip()
    if not link.startswith("https://vikingfile.com/f/"):
        raise RuntimeError("VikingFile completion returned no valid URL.")
    stored_size = int(complete_payload.get("size") or size)
    if stored_size != size:
        raise RuntimeError(f"VikingFile stored {stored_size} bytes; expected {size}.")
    return link


def write_result(path: str | None, result: dict[str, Any]) -> None:
    if not path:
        return
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f"{output.name}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", nargs="?", help="Public R2, SKYDROP, Google Photos, or Google Drive URL.")
    parser.add_argument("--url", dest="url_option", help="Public R2, SKYDROP, Google Photos, or Google Drive URL.")
    parser.add_argument("--source-kind", choices=("r2", "skydrop", "gphotos", "gdrive", "generic"))
    parser.add_argument("--filename", help="Filename to preserve at the destination.")
    parser.add_argument("--result-json", help="Write a structured success or failure result to this file.")
    parser.add_argument("--mode", choices=("auto", "staged"), default="auto")
    parser.add_argument("--staged-max-gib", type=float, default=DEFAULT_STAGED_MAX_GIB)
    parser.add_argument("--cleanup-above-gib", type=float, default=DEFAULT_CLEANUP_ABOVE_GIB)
    parser.add_argument("--download-workers", type=int, default=16)
    parser.add_argument("--upload-workers", type=int, default=8)
    args = parser.parse_args()
    if args.url and args.url_option:
        parser.error("Use either the positional URL or --url, not both.")
    args.source_url = (args.url_option or args.url or os.environ.get("CLOUD_SOURCE_URL", "")).strip()
    args.source_kind = args.source_kind or os.environ.get("CLOUD_SOURCE_KIND", "").strip()
    args.filename = args.filename or os.environ.get("CLOUD_SOURCE_FILENAME", "").strip()
    parsed = urlparse(args.source_url)
    if parsed.scheme != "https" or not parsed.hostname:
        parser.error("A public HTTPS source URL is required.")
    if args.staged_max_gib <= 0 or args.cleanup_above_gib < 0:
        parser.error("Staging limits must be positive.")
    if args.download_workers < 1 or args.upload_workers < 1:
        parser.error("Worker counts must be at least 1.")
    return args


def main() -> int:
    args = parse_args()
    source_url = args.source_url
    requested_kind = args.source_kind or ""
    kind = detect_source(source_url, requested_kind)
    if kind == "skydrop":
        source_url = add_skydrop_direct_flag(source_url)
    started_at = time.monotonic()
    result: dict[str, Any] = {"ok": False, "source_url": source_url, "source_kind": kind}
    temp_dir: Path | None = None
    try:
        if kind == "gdrive":
            filename = Path(args.filename).name if args.filename else "google_drive_download.bin"
            source = ResolvedSource(source_url, source_url, filename, 0, kind)
        else:
            source = inspect_http_source(source_url, args.filename or "", kind)
        result["resolved_url"] = source.direct_url
        if source.size_bytes:
            result["size_bytes"] = source.size_bytes
            print(f"Source: {source.kind}; size: {source.size_bytes / (1024 ** 3):.2f} GiB.")
            staged_limit = int(args.staged_max_gib * 1024 ** 3)
            if source.size_bytes > staged_limit:
                raise ValueError(
                    f"Source is {source.size_bytes / (1024 ** 3):.2f} GiB, above the staged limit "
                    f"of {args.staged_max_gib:.2f} GiB."
                )
            if source.size_bytes > int(args.cleanup_above_gib * 1024 ** 3):
                free_runner_disk_space()
            if not has_disk_capacity(source.size_bytes):
                raise ValueError("GitHub runner does not have enough free disk space for this staged transfer.")
        else:
            print(f"Source: {source.kind}; size is not available before download.")

        temp_dir = Path(tempfile.mkdtemp(prefix="cloud-upload-"))
        local_path = temp_dir / Path(source.filename).name
        try:
            pixel_keys = json.loads(os.environ.get("CLOUD_PIXELDRAIN_KEYS_JSON", "[]"))
        except json.JSONDecodeError:
            pixel_keys = []
        pixel_keys = [str(value).strip() for value in pixel_keys if str(value).strip()]
        live_pixel_enabled = (
            requested_kind in PIXELDRAIN_SOURCE_KINDS
            and bool(pixel_keys)
            and 0 < source.size_bytes <= PIXELDRAIN_LIMIT_BYTES
            and source.kind != "gdrive"
        )
        live_pixel_stream: LivePixelDrainStream | None = None
        download_started = time.monotonic()
        if live_pixel_enabled:
            print("Staging source while PixelDrain starts its live upload.")
            try:
                live_pixel_stream = download_http_source_with_live_pixeldrain(source, local_path, pixel_keys)
            except Exception as exc:  # noqa: BLE001 - retain the established staged downloader as a fallback.
                print(f"Live PixelDrain path unavailable ({type(exc).__name__}); using the staged downloader.")
                local_path.unlink(missing_ok=True)
                download_http_source(source, local_path, args.download_workers)
        elif source.kind == "gdrive":
            download_gdrive(source_url, local_path, args.download_workers)
        else:
            download_http_source(source, local_path, args.download_workers)
        size_bytes = local_path.stat().st_size
        print(
            f"Download finished in {time.monotonic() - download_started:.0f}s "
            f"({size_bytes / (1024 ** 2) / max(time.monotonic() - download_started, 0.001):.1f} MiB/s)."
        )
        provider_urls: dict[str, str] = {}
        provider_errors: dict[str, str] = {}
        provider_tasks: dict[str, Any] = {
            "transfer_url": lambda: upload_to_destination(local_path, args.upload_workers),
        }
        viking_hash = os.environ.get("CLOUD_VIKINGFILE_USER_HASH", "").strip()
        if not live_pixel_stream and requested_kind in PIXELDRAIN_SOURCE_KINDS and pixel_keys and size_bytes <= PIXELDRAIN_LIMIT_BYTES:
            provider_tasks["pixeldrain_url"] = lambda: upload_to_pixeldrain(local_path, pixel_keys)
        if requested_kind in VIKINGFILE_SOURCE_KINDS and viking_hash:
            provider_tasks["vikingfile_url"] = lambda: upload_to_vikingfile(
                local_path, viking_hash, args.upload_workers
            )

        with ThreadPoolExecutor(max_workers=len(provider_tasks)) as executor:
            futures = {executor.submit(task): name for name, task in provider_tasks.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    provider_urls[name] = future.result()
                except Exception as exc:  # noqa: BLE001 - preserve successful mirrors.
                    provider_errors[name.removesuffix("_url")] = f"{type(exc).__name__}: {exc}"[:300]
        if live_pixel_stream is not None:
            live_pixel = live_pixel_stream.finish()
            if live_pixel.url:
                provider_urls["pixeldrain_url"] = live_pixel.url
            else:
                provider_errors["pixeldrain"] = live_pixel.error or "Live PixelDrain upload failed."
        elapsed = time.monotonic() - started_at
        result.update(
            {
                "ok": bool(provider_urls),
                "filename": local_path.name,
                "size_bytes": size_bytes,
                "elapsed_seconds": round(elapsed, 3),
                "average_mib_per_second": round(size_bytes / (1024 ** 2) / max(elapsed, 0.001), 3),
                "mode": "staged",
                "provider_errors": provider_errors,
                **provider_urls,
            }
        )
        write_result(args.result_json, result)
        print(f"Cloud upload completed with {len(provider_urls)}/{len(provider_tasks)} provider links.")
        return 0 if provider_urls else 1
    except Exception as exc:  # noqa: BLE001 - result artifact must report every failure.
        result.update({"error": f"{type(exc).__name__}: {exc}", "elapsed_seconds": round(time.monotonic() - started_at, 3)})
        write_result(args.result_json, result)
        print(f"Cloud upload failed: {result['error']}", file=sys.stderr)
        return 1
    finally:
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

