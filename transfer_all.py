"""
transfer_all.py - transfer ownership of all files and folders from source to target.

Transfer order:
  1. Items with a known size, largest first.
  2. Folders and Google Docs/Sheets/Slides with no size, at the end.

Run:
  .venv/bin/python transfer_all.py

.env options:
  DRY_RUN=1              - print the plan, do not transfer anything
  REMOVE_SOURCE_ACCESS=1 - remove source access after transfer
  STREAM=1               - start transferring while pages are still being fetched,
                           then finish the remaining items after full pagination
"""

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))

from src.auth import get_credentials
from src.drive_client import DriveClient
from src.logger import get_logger

logger = get_logger("transfer_all")

DELAY_BETWEEN_FILES = 0.3
PAGE_SIZE = 500
FOLDER_MIME = "application/vnd.google-apps.folder"


# ------------------------------------------------------------------
# Page iteration
# ------------------------------------------------------------------

def iter_pages(client: DriveClient):
    """Yield file pages with PAGE_SIZE items each."""
    page_token = None
    page_num = 0

    while True:
        page_num += 1
        kwargs = dict(
            pageSize=PAGE_SIZE,
            q="'me' in owners and trashed = false",
            fields="nextPageToken, files(id, name, modifiedTime, mimeType, size)",
        )
        if page_token:
            kwargs["pageToken"] = page_token

        response = client.service.files().list(**kwargs).execute()
        batch = response.get("files", [])
        logger.info("Page %d: %d items", page_num, len(batch))
        yield batch

        page_token = response.get("nextPageToken")
        if not page_token:
            break


def get_all_owned_items(client: DriveClient) -> list[dict]:
    """Load all owned items into memory in non-stream mode."""
    items = []
    for batch in iter_pages(client):
        items.extend(batch)
    logger.info("Total items fetched: %d", len(items))
    return items


# ------------------------------------------------------------------
# Sorting
# ------------------------------------------------------------------

def sort_items(items: list[dict]) -> list[dict]:
    with_size = sorted(
        (f for f in items if f.get("size")),
        key=lambda f: int(f["size"]),
        reverse=True,
    )
    without_size = [f for f in items if not f.get("size")]
    return with_size + without_size


def get_remaining_stream_items(
    all_items: list[dict], processed_ids: set[str]
) -> list[dict]:
    return [f for f in sort_items(all_items) if f["id"] not in processed_ids]


def get_next_stream_item(
    seen_items: list[dict], processed_ids: set[str]
) -> dict | None:
    candidates = [f for f in seen_items if f["id"] not in processed_ids]
    if not candidates:
        return None
    return max(candidates, key=lambda f: int(f.get("size") or 0))


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def fmt_size(size: str | None) -> str:
    if size is None:
        return "folder"
    b = int(size)
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def save_failed(failed: list[dict]) -> Path:
    logs_dir = Path(__file__).parent / "logs"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = logs_dir / f"failed_{ts}.json"
    path.write_text(json.dumps(failed, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ------------------------------------------------------------------
# Single item transfer
# ------------------------------------------------------------------

def transfer_item(
    f: dict,
    index: int,
    total: str,
    source_client: DriveClient,
    target_client: DriveClient,
    source_email: str,
    target_email: str,
    remove_source_access: bool,
    failed: list[dict],
) -> bool:
    file_id = f["id"]
    file_name = f.get("name", file_id)
    staging: dict | None = None
    transfer_completed = False

    logger.info("[%s/%s] %s  '%s'", index, total, fmt_size(f.get("size")), file_name)

    try:
        staging = source_client.stage_item_if_needed(file_id, target_email)
        source_client.initiate_ownership_transfer(file_id, target_email)
        target_client.accept_ownership_transfer(file_id, source_email)

        if staging and staging.get("staged"):
            source_client.restore_item_parents(
                file_id,
                staging["original_parents"],
                staging["staging_parent"],
            )
        transfer_completed = True

        if remove_source_access:
            removed = target_client.remove_access(file_id, source_email)
            if removed:
                logger.info("  ✓ transferred, source access removed")
            else:
                logger.info("  ✓ transferred, source retained inherited access only")
        else:
            logger.info("  ✓ transferred")

        return True
    except Exception as exc:
        if staging and staging.get("staged") and not transfer_completed:
            try:
                source_client.restore_item_parents(
                    file_id,
                    staging["original_parents"],
                    staging["staging_parent"],
                )
                logger.warning("  ↺ staging rollback completed")
            except Exception as rollback_exc:
                logger.error("  ! staging rollback failed: %s", rollback_exc)
        logger.warning("  ✗ error: %s", exc)
        failed.append({
            "id": file_id,
            "name": file_name,
            "size": f.get("size"),
            "is_folder": f.get("mimeType") == FOLDER_MIME,
            "error": str(exc),
        })
        return False


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

def main() -> None:
    source_email = os.getenv("SOURCE_EMAIL")
    target_email = os.getenv("TARGET_EMAIL")
    dry_run = os.getenv("DRY_RUN", "0") == "1"
    remove_source_access = os.getenv("REMOVE_SOURCE_ACCESS", "0") == "1"
    stream = os.getenv("STREAM", "0") == "1"

    if not source_email or not target_email:
        logger.error("Set SOURCE_EMAIL and TARGET_EMAIL in .env")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("SOURCE:                %s", source_email)
    logger.info("TARGET:                %s", target_email)
    logger.info("DRY_RUN:               %s", dry_run)
    logger.info("REMOVE_SOURCE_ACCESS:  %s", remove_source_access)
    logger.info("STREAM:                %s", stream)
    logger.info("=" * 60)

    logger.info("Authenticating source account...")
    source_creds = get_credentials("source")
    source_client = DriveClient(source_creds, account_label="source")

    logger.info("Authenticating target account...")
    target_creds = get_credentials("target")
    target_client = DriveClient(target_creds, account_label="target")

    transferred = 0
    failed: list[dict] = []

    if stream:
        # ------------------------------------------------------------------
        # Stream mode:
        #   1. After each page, transfer the largest seen-so-far item.
        #   2. After pagination completes, transfer the remaining items
        #      in global size order.
        # ------------------------------------------------------------------
        logger.info(
            "STREAM mode: transfer the largest seen-so-far item after each page, "
            "then finish the remaining items..."
        )
        index = 0
        all_items: list[dict] = []
        processed_ids: set[str] = set()

        for batch in iter_pages(source_client):
            all_items.extend(batch)
            if not batch:
                continue

            largest = get_next_stream_item(all_items, processed_ids)
            if largest is None:
                continue
            index += 1
            processed_ids.add(largest["id"])

            if dry_run:
                logger.info("%5d. %-10s  %s", index, fmt_size(largest.get("size")), largest.get("name", "—"))
                continue

            ok = transfer_item(
                largest, index, "?",
                source_client, target_client,
                source_email, target_email,
                remove_source_access, failed,
            )
            if ok:
                transferred += 1
            time.sleep(DELAY_BETWEEN_FILES)

        remaining = get_remaining_stream_items(all_items, processed_ids)
        if remaining:
            logger.info(
                "STREAM: pagination finished, transferring %d remaining items in global size order...",
                len(remaining),
            )

        total = len(all_items)
        for f in remaining:
            index += 1

            if dry_run:
                logger.info("%5d. %-10s  %s", index, fmt_size(f.get("size")), f.get("name", "—"))
                continue

            ok = transfer_item(
                f, index, total,
                source_client, target_client,
                source_email, target_email,
                remove_source_access, failed,
            )
            if ok:
                transferred += 1
            time.sleep(DELAY_BETWEEN_FILES)

    else:
        # ------------------------------------------------------------------
        # Full-load mode: fetch all items first, then transfer
        # ------------------------------------------------------------------
        logger.info("Fetching the full list of source-owned files and folders...")
        all_items = get_all_owned_items(source_client)
        selected = sort_items(all_items)

        with_size = sum(1 for f in selected if f.get("size"))
        without_size = len(selected) - with_size
        logger.info(
            "Will transfer %d items (%d with size + %d folders/Google Docs)",
            len(selected), with_size, without_size,
        )

        if not selected:
            logger.info("Nothing to transfer. Finished.")
            return

        if dry_run:
            logger.info("--- Dry run list ---")
            for i, f in enumerate(selected, 1):
                logger.info("%5d. %-10s  %s  [%s]", i, fmt_size(f.get("size")), f.get("name", "—"), f.get("id", ""))
            return

        total = len(selected)
        for i, f in enumerate(selected, 1):
            ok = transfer_item(
                f, i, total,
                source_client, target_client,
                source_email, target_email,
                remove_source_access, failed,
            )
            if ok:
                transferred += 1
            time.sleep(DELAY_BETWEEN_FILES)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("SUMMARY:")
    logger.info("  Transferred: %d", transferred)
    logger.info("  Failed:      %d", len(failed))
    if failed:
        failed_path = save_failed(failed)
        logger.info("  Errors:      %s", failed_path)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
