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
from googleapiclient.errors import HttpError

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))

from src.auth import get_credentials
from src.drive_client import DriveClient, SharingQuotaExceededError, get_http_error_reason
from src.logger import get_logger

logger = get_logger("transfer_all")

DELAY_BETWEEN_FILES = 0.3
PAGE_SIZE = 500
FOLDER_MIME = "application/vnd.google-apps.folder"
QUOTA_RECOVERY_BATCH_SIZE = 200
QUOTA_RECOVERY_ATTEMPTS = 5


# ------------------------------------------------------------------
# Page iteration
# ------------------------------------------------------------------

def iter_pages(client: DriveClient):
    """Yield file pages with PAGE_SIZE items each."""
    page_token = None
    page_num = 0

    while True:
        page_num += 1
        response = client.list_owned_items_page(
            page_size=PAGE_SIZE,
            page_token=page_token,
        )
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


def is_owned_by_source(source_client: DriveClient, file_id: str, source_email: str) -> bool:
    try:
        meta = source_client.get_file(file_id, "id, owners(emailAddress)")
    except HttpError as exc:
        reason = get_http_error_reason(exc)
        status = getattr(exc.resp, "status", None)
        if status == 404 or reason in {"notFound", "fileNotFound", "insufficientFilePermissions"}:
            return False
        raise

    source_email_lower = source_email.lower()
    return any(
        owner.get("emailAddress", "").lower() == source_email_lower
        for owner in meta.get("owners", [])
    )


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
        if not is_owned_by_source(source_client, file_id, source_email):
            logger.info("  - skipped, source no longer owns this item")
            return False

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
    except SharingQuotaExceededError as exc:
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
        logger.warning("  ✗ fatal quota error: %s", exc)
        failed.append({
            "id": file_id,
            "name": file_name,
            "size": f.get("size"),
            "is_folder": f.get("mimeType") == FOLDER_MIME,
            "error": str(exc),
        })
        raise
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


def transfer_or_stop(
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
    for attempt in range(1, QUOTA_RECOVERY_ATTEMPTS + 1):
        try:
            return transfer_item(
                f,
                index,
                total,
                source_client,
                target_client,
                source_email,
                target_email,
                remove_source_access,
                failed,
            )
        except SharingQuotaExceededError:
            logger.warning(
                "Sharing quota exceeded while processing '%s' (attempt %d/%d). "
                "Cleaning up source access from target-side shared items...",
                f.get("name", f["id"]),
                attempt,
                QUOTA_RECOVERY_ATTEMPTS,
            )
            removed = target_client.cleanup_source_access(
                source_email,
                limit=QUOTA_RECOVERY_BATCH_SIZE,
            )
            if removed <= 0:
                logger.error(
                    "Stopping the run because Google Drive returned sharingRateLimitExceeded "
                    "and cleanup did not free any target-owned shared items from source access. Retry later."
                )
                raise

            logger.info(
                "Cleanup removed %d target-side source shares. Retrying the current item...",
                removed,
            )
            time.sleep(DELAY_BETWEEN_FILES)

    logger.error(
        "Stopping the run because sharingRateLimitExceeded persisted after %d cleanup attempts.",
        QUOTA_RECOVERY_ATTEMPTS,
    )
    raise SharingQuotaExceededError("sharing quota still exceeded after cleanup retries")


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
    stopped_due_to_quota = False

    try:
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

                ok = transfer_or_stop(
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

                ok = transfer_or_stop(
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
                ok = transfer_or_stop(
                    f, i, total,
                    source_client, target_client,
                    source_email, target_email,
                    remove_source_access, failed,
                )
                if ok:
                    transferred += 1
                time.sleep(DELAY_BETWEEN_FILES)
    except SharingQuotaExceededError:
        stopped_due_to_quota = True

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
    if stopped_due_to_quota:
        logger.info("  Stopped:     sharing quota exceeded, retry later")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
