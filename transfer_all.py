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

import logging
import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from googleapiclient.errors import HttpError

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent / "src"))

from auth import get_credentials
from drive_client import DriveClient, SharingQuotaExceededError, get_http_error_reason
from logger import get_logger

logger = logging.getLogger("transfer_all")

DELAY_BETWEEN_FILES = 0.3
PAGE_SIZE = 500
FOLDER_MIME = "application/vnd.google-apps.folder"
QUOTA_RECOVERY_BATCH_SIZE = 200
QUOTA_RECOVERY_ATTEMPTS = 5
DisplayTotal = int | str


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


def build_failed_item(item: dict, file_id: str, file_name: str, error: Exception) -> dict:
    return {
        "id": file_id,
        "name": file_name,
        "size": item.get("size"),
        "is_folder": item.get("mimeType") == FOLDER_MIME,
        "error": str(error),
    }


def record_failed_item(
    failed: list[dict],
    item: dict,
    file_id: str,
    file_name: str,
    error: Exception,
) -> None:
    failed.append(build_failed_item(item, file_id, file_name, error))


def rollback_staging_if_needed(
    source_client: DriveClient,
    file_id: str,
    staging: dict | None,
    transfer_completed: bool,
) -> None:
    if not staging or not staging.get("staged") or transfer_completed:
        return

    try:
        source_client.restore_item_parents(
            file_id,
            staging["original_parents"],
            staging["staging_parent"],
        )
        logger.warning("  ↺ staging rollback completed")
    except Exception as rollback_exc:
        logger.error("  ! staging rollback failed: %s", rollback_exc)


def log_transfer_completion(
    target_client: DriveClient,
    file_id: str,
    source_email: str,
    remove_source_access: bool,
) -> None:
    if not remove_source_access:
        logger.info("  ✓ transferred")
        return

    removed = target_client.remove_access(file_id, source_email)
    if removed:
        logger.info("  ✓ transferred, source access removed")
        return

    logger.info("  ✓ transferred, source retained inherited access only")


def process_transfer_candidate(
    item: dict,
    index: int,
    total: DisplayTotal,
    *,
    dry_run: bool,
    source_client: DriveClient,
    target_client: DriveClient,
    source_email: str,
    target_email: str,
    remove_source_access: bool,
    failed: list[dict],
) -> int:
    if dry_run:
        logger.info(
            "%5d. %-10s  %s",
            index,
            fmt_size(item.get("size")),
            item.get("name", "—"),
        )
        return 0

    ok = transfer_or_stop(
        item,
        index,
        total,
        source_client,
        target_client,
        source_email,
        target_email,
        remove_source_access,
        failed,
    )
    time.sleep(DELAY_BETWEEN_FILES)
    return int(ok)


def accept_pending_item(
    target_client: DriveClient,
    item: dict,
    source_email: str,
    *,
    dry_run: bool,
    remove_source_access: bool,
    failed: list[dict],
) -> bool:
    file_id = item["id"]
    file_name = item.get("name", file_id)

    if dry_run:
        logger.info(
            "  pending: %s  '%s'",
            fmt_size(item.get("size")),
            file_name,
        )
        return False

    logger.info(
        "Accepting manual ownership transfer: %s  '%s'",
        fmt_size(item.get("size")),
        file_name,
    )
    try:
        target_client.accept_ownership_transfer(file_id, source_email)
        log_transfer_completion(
            target_client,
            file_id,
            source_email,
            remove_source_access,
        )
        return True
    except Exception as exc:
        logger.warning("  ✗ manual transfer accept error: %s", exc)
        record_failed_item(failed, item, file_id, file_name, exc)
        return False


def read_runtime_config() -> tuple[str, str, bool, bool, bool]:
    source_email = os.getenv("SOURCE_EMAIL")
    target_email = os.getenv("TARGET_EMAIL")
    dry_run = os.getenv("DRY_RUN", "0") == "1"
    remove_source_access = os.getenv("REMOVE_SOURCE_ACCESS", "0") == "1"
    stream = os.getenv("STREAM", "0") == "1"

    if not source_email or not target_email:
        logger.error("Set SOURCE_EMAIL and TARGET_EMAIL in .env")
        sys.exit(1)

    return source_email, target_email, dry_run, remove_source_access, stream


def log_run_configuration(
    source_email: str,
    target_email: str,
    *,
    dry_run: bool,
    remove_source_access: bool,
    stream: bool,
) -> None:
    logger.info("=" * 60)
    logger.info("SOURCE:                %s", source_email)
    logger.info("TARGET:                %s", target_email)
    logger.info("DRY_RUN:               %s", dry_run)
    logger.info("REMOVE_SOURCE_ACCESS:  %s", remove_source_access)
    logger.info("STREAM:                %s", stream)
    logger.info("=" * 60)


def create_drive_clients() -> tuple[DriveClient, DriveClient]:
    logger.info("Authenticating source account...")
    source_creds = get_credentials("source")
    source_client = DriveClient(source_creds, account_label="source")

    logger.info("Authenticating target account...")
    target_creds = get_credentials("target")
    target_client = DriveClient(target_creds, account_label="target")
    return source_client, target_client


def log_selected_items_summary(selected: list[dict]) -> None:
    with_size = sum(1 for item in selected if item.get("size"))
    without_size = len(selected) - with_size
    logger.info(
        "Will transfer %d items (%d with size + %d folders/Google Docs)",
        len(selected),
        with_size,
        without_size,
    )


def log_dry_run_list(items: list[dict]) -> None:
    logger.info("--- Dry run list ---")
    for index, item in enumerate(items, 1):
        logger.info(
            "%5d. %-10s  %s  [%s]",
            index,
            fmt_size(item.get("size")),
            item.get("name", "—"),
            item.get("id", ""),
        )


def run_stream_mode(
    source_client: DriveClient,
    target_client: DriveClient,
    *,
    dry_run: bool,
    source_email: str,
    target_email: str,
    remove_source_access: bool,
    failed: list[dict],
) -> int:
    logger.info(
        "STREAM mode: transfer the largest seen-so-far item after each page, "
        "then finish the remaining items..."
    )
    transferred = 0
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
        transferred += process_transfer_candidate(
            largest,
            index,
            "?",
            dry_run=dry_run,
            source_client=source_client,
            target_client=target_client,
            source_email=source_email,
            target_email=target_email,
            remove_source_access=remove_source_access,
            failed=failed,
        )

    remaining = get_remaining_stream_items(all_items, processed_ids)
    if remaining:
        logger.info(
            "STREAM: pagination finished, transferring %d remaining items in global size order...",
            len(remaining),
        )

    total = len(all_items)
    for item in remaining:
        index += 1
        transferred += process_transfer_candidate(
            item,
            index,
            total,
            dry_run=dry_run,
            source_client=source_client,
            target_client=target_client,
            source_email=source_email,
            target_email=target_email,
            remove_source_access=remove_source_access,
            failed=failed,
        )

    return transferred


def run_full_mode(
    source_client: DriveClient,
    target_client: DriveClient,
    *,
    dry_run: bool,
    source_email: str,
    target_email: str,
    remove_source_access: bool,
    failed: list[dict],
) -> int:
    logger.info("Fetching the full list of source-owned files and folders...")
    selected = sort_items(get_all_owned_items(source_client))
    log_selected_items_summary(selected)

    if not selected:
        logger.info("Nothing to transfer. Finished.")
        return 0

    if dry_run:
        log_dry_run_list(selected)
        return 0

    transferred = 0
    total = len(selected)
    for index, item in enumerate(selected, 1):
        transferred += process_transfer_candidate(
            item,
            index,
            total,
            dry_run=False,
            source_client=source_client,
            target_client=target_client,
            source_email=source_email,
            target_email=target_email,
            remove_source_access=remove_source_access,
            failed=failed,
        )
    return transferred


def log_summary(
    *,
    accepted_manual: int,
    transferred: int,
    failed: list[dict],
    stopped_due_to_quota: bool,
) -> None:
    logger.info("=" * 60)
    logger.info("SUMMARY:")
    logger.info("  Manual accepted: %d", accepted_manual)
    logger.info("  Transferred: %d", transferred)
    logger.info("  Failed:      %d", len(failed))
    if failed:
        failed_path = save_failed(failed)
        logger.info("  Errors:      %s", failed_path)
    if stopped_due_to_quota:
        logger.info("  Stopped:     sharing quota exceeded, retry later")
    logger.info("=" * 60)


# ------------------------------------------------------------------
# Single item transfer
# ------------------------------------------------------------------

def transfer_item(
    f: dict,
    index: int,
    total: DisplayTotal,
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

        log_transfer_completion(
            target_client,
            file_id,
            source_email,
            remove_source_access,
        )
        return True
    except SharingQuotaExceededError as exc:
        rollback_staging_if_needed(source_client, file_id, staging, transfer_completed)
        logger.warning("  ✗ fatal quota error: %s", exc)
        record_failed_item(failed, f, file_id, file_name, exc)
        raise
    except Exception as exc:
        rollback_staging_if_needed(source_client, file_id, staging, transfer_completed)
        logger.warning("  ✗ error: %s", exc)
        record_failed_item(failed, f, file_id, file_name, exc)
        return False


def transfer_or_stop(
    f: dict,
    index: int,
    total: DisplayTotal,
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


def accept_pending_ownership_transfers(
    target_client: DriveClient,
    source_email: str,
    *,
    dry_run: bool,
    remove_source_access: bool,
    failed: list[dict],
) -> int:
    accepted = 0
    page_token = None
    page_num = 0

    while True:
        page_num += 1
        pending_items, page_token = target_client.list_pending_ownership_items(
            source_email,
            page_token=page_token,
        )
        if pending_items:
            logger.info(
                "Manual pending ownership page %d: %d item(s)",
                page_num,
                len(pending_items),
            )

        for item in pending_items:
            accepted += int(
                accept_pending_item(
                    target_client,
                    item,
                    source_email,
                    dry_run=dry_run,
                    remove_source_access=remove_source_access,
                    failed=failed,
                )
            )

            time.sleep(DELAY_BETWEEN_FILES)

        if not page_token:
            break

    return accepted


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

def main() -> None:
    get_logger("transfer_all")

    source_email, target_email, dry_run, remove_source_access, stream = read_runtime_config()
    log_run_configuration(
        source_email,
        target_email,
        dry_run=dry_run,
        remove_source_access=remove_source_access,
        stream=stream,
    )
    source_client, target_client = create_drive_clients()

    transferred = 0
    accepted_manual = 0
    failed: list[dict] = []
    stopped_due_to_quota = False

    logger.info("Checking manually pending ownership transfers...")
    accepted_manual = accept_pending_ownership_transfers(
        target_client,
        source_email,
        dry_run=dry_run,
        remove_source_access=remove_source_access,
        failed=failed,
    )
    if accepted_manual:
        logger.info("Accepted manual ownership transfers: %d", accepted_manual)

    try:
        if stream:
            transferred = run_stream_mode(
                source_client,
                target_client,
                dry_run=dry_run,
                source_email=source_email,
                target_email=target_email,
                remove_source_access=remove_source_access,
                failed=failed,
            )
        else:
            transferred = run_full_mode(
                source_client,
                target_client,
                dry_run=dry_run,
                source_email=source_email,
                target_email=target_email,
                remove_source_access=remove_source_access,
                failed=failed,
            )
    except SharingQuotaExceededError:
        stopped_due_to_quota = True

    log_summary(
        accepted_manual=accepted_manual,
        transferred=transferred,
        failed=failed,
        stopped_due_to_quota=stopped_due_to_quota,
    )


if __name__ == "__main__":
    main()
