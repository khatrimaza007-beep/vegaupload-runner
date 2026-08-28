#!/usr/bin/env python3
"""Run one opaque broker job without exposing media details in Actions logs."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker-url", required=True)
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()
    args.broker_url = args.broker_url.rstrip("/")
    return args


def workflow_oidc_token(audience: str) -> str:
    request_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
    request_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
    if not request_url or not request_token:
        raise RuntimeError("GitHub OIDC is unavailable; id-token: write is required.")
    separator = "&" if "?" in request_url else "?"
    response = httpx.get(
        f"{request_url}{separator}audience={quote(audience, safe='')}",
        headers={"Authorization": f"Bearer {request_token}"},
        timeout=30,
    )
    response.raise_for_status()
    value = response.json().get("value")
    if not isinstance(value, str) or not value:
        raise RuntimeError("GitHub OIDC did not return a token.")
    return value


def broker_request(
    method: str,
    broker_url: str,
    path: str,
    oidc_token: str,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    response = httpx.request(
        method,
        f"{broker_url}{path}",
        headers={"Authorization": f"Bearer {oidc_token}"},
        json=payload,
        timeout=60,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Broker returned HTTP {response.status_code}.")
    value = response.json()
    if not isinstance(value, dict):
        raise RuntimeError("Broker returned an invalid response.")
    return value


def add_mask(value: str) -> None:
    if value:
        print(f"::add-mask::{value}")


def run_transfer(job: dict[str, object]) -> dict[str, object]:
    source_url = str(job.get("source_url") or "")
    source_kind = str(job.get("source_kind") or "")
    filename = str(job.get("filename") or "")
    pixel_keys = [str(value).strip() for value in job.get("pixeldrain_api_keys", []) if str(value).strip()]
    viking_hash = str(job.get("vikingfile_user_hash") or "").strip()
    skydrop_bypass_secret = str(job.get("skydrop_bypass_secret") or "").strip()
    if not source_url or not source_kind or not filename:
        raise RuntimeError("Broker job is incomplete.")
    if source_kind == "skydrop" and len(skydrop_bypass_secret) < 20:
        raise RuntimeError("Broker job has no valid SkyDrop bypass credential.")

    add_mask(source_url)
    add_mask(filename)
    env = os.environ.copy()
    env["CLOUD_SOURCE_URL"] = source_url
    env["CLOUD_SOURCE_KIND"] = source_kind
    env["CLOUD_SOURCE_FILENAME"] = filename
    env["CLOUD_PIXELDRAIN_KEYS_JSON"] = json.dumps(pixel_keys)
    env["CLOUD_VIKINGFILE_USER_HASH"] = viking_hash
    env["CLOUD_SKYDROP_BYPASS_SECRET"] = skydrop_bypass_secret
    for value in pixel_keys:
        add_mask(value)
    add_mask(viking_hash)
    add_mask(skydrop_bypass_secret)

    with tempfile.TemporaryDirectory(prefix="broker-cloud-") as temporary_dir:
        directory = Path(temporary_dir)
        result_path = directory / "result.json"
        log_path = directory / "transfer.log"
        command = [
            sys.executable,
            str(Path(__file__).with_name("cloud_to_cloud_upload.py")),
            "--result-json",
            str(result_path),
            "--mode",
            "auto",
            "--staged-max-gib",
            "20",
            "--cleanup-above-gib",
            "8",
            "--download-workers",
            "16",
            "--upload-workers",
            "8",
        ]
        with log_path.open("w", encoding="utf-8") as log_file:
            completed = subprocess.run(
                command,
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )
        if not result_path.is_file():
            return {"ok": False, "error": "Transfer job did not create a result."}
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"ok": False, "error": "Transfer job returned an unreadable result."}
        if completed.returncode or result.get("ok") is not True:
            error = str(result.get("error") or "Transfer failed in the GitHub runner.")
            return {"ok": False, "error": error[:500]}

        provider_urls = {
            "transfer_url": str(result.get("transfer_url") or ""),
            "pixeldrain_url": str(result.get("pixeldrain_url") or ""),
            "vikingfile_url": str(result.get("vikingfile_url") or ""),
        }
        if not any(provider_urls.values()):
            return {"ok": False, "error": "Cloud job finished without a provider URL."}
        for value in provider_urls.values():
            add_mask(value)
        response = {
            "ok": True,
            "size_bytes": result.get("size_bytes", 0),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
            "provider_errors": result.get("provider_errors", {}),
        }
        response.update({key: value for key, value in provider_urls.items() if value})
        return response


def main() -> int:
    args = parse_args()
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    try:
        oidc_token = workflow_oidc_token(args.broker_url)
        job = broker_request(
            "POST",
            args.broker_url,
            f"/v1/jobs/{args.job_id}/claim",
            oidc_token,
            {"run_id": run_id},
        )
        result = run_transfer(job)
        result["run_id"] = run_id
        broker_request(
            "POST",
            args.broker_url,
            f"/v1/jobs/{args.job_id}/result",
            oidc_token,
            result,
        )
        print("Transfer job completed." if result["ok"] else "Transfer job failed.")
        return 0 if result["ok"] else 1
    except Exception as exc:  # noqa: BLE001 - do not print source data in Actions output.
        print(f"Broker transfer failed: {type(exc).__name__}.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

