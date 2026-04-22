"""
test_connection.py - smoke test script.

What it does:
  1. Authenticates the source account.
  2. Prints the latest 10 files.
  3. Initiates ownership transfer for the newest file.
  4. If credentials/client_secret_target.json exists, authenticates the target
     account and accepts the transfer automatically.
     Otherwise, prints short manual acceptance instructions.

Requirements:
  - Fill in .env from .env.example.
  - Put the source OAuth client at credentials/client_secret_source.json
  - Optionally put the target OAuth client at credentials/client_secret_target.json
"""

import sys
from pathlib import Path

from dotenv import load_dotenv
import os

load_dotenv()

# Add the project root so `src.*` imports work when the script is run directly.
sys.path.insert(0, str(Path(__file__).parent))

from src.auth import get_credentials, CREDENTIALS_DIR
from src.drive_client import DriveClient
from src.logger import get_logger

logger = get_logger("test_connection")


def fmt_size(size: str | None) -> str:
    if size is None:
        return "-"
    b = int(size)
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def main() -> None:
    source_email = os.getenv("SOURCE_EMAIL")
    target_email = os.getenv("TARGET_EMAIL")

    if not source_email or not target_email:
        logger.error("Set SOURCE_EMAIL and TARGET_EMAIL in .env")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("SOURCE: %s", source_email)
    logger.info("TARGET: %s", target_email)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: authenticate the source account
    # ------------------------------------------------------------------
    logger.info("Step 1/4: authenticating the source account...")
    source_creds = get_credentials("source")
    source_client = DriveClient(source_creds, account_label="source")

    # Confirm that the cached token belongs to the expected source account.
    about = source_client._execute(
        source_client.service.about().get(fields="user(emailAddress, displayName)"),
        "Get source account profile",
    )
    logged_as = about["user"]["emailAddress"]
    display = about["user"].get("displayName", "")
    logger.info("Authenticated as: %s (%s)", logged_as, display)

    if logged_as.lower() != source_email.lower():
        logger.warning(
            "WARNING: expected %s, but authenticated as %s. "
            "Delete tokens/token_source.json and run again.",
            source_email, logged_as,
        )

    # ------------------------------------------------------------------
    # Step 2: list the latest 10 files
    # ------------------------------------------------------------------
    logger.info("Step 2/4: fetching the latest 10 files...")
    files = source_client.list_files(limit=10)

    if not files:
        logger.error("No files found. Make sure the Drive account owns at least one file.")
        sys.exit(1)

    logger.info("Files found: %d", len(files))
    logger.info("-" * 60)
    for i, f in enumerate(files, 1):
        logger.info(
            "%2d. [%s]  %s  (%s)",
            i,
            f.get("modifiedTime", "?")[:10],
            f.get("name", "—"),
            fmt_size(f.get("size")),
        )
    logger.info("-" * 60)

    # ------------------------------------------------------------------
    # Step 3: initiate ownership transfer for the newest file
    # ------------------------------------------------------------------
    newest = files[0]
    file_id = newest["id"]
    file_name = newest.get("name", file_id)

    logger.info("Step 3/4: initiating ownership transfer...")
    logger.info("File: '%s'  (id=%s)", file_name, file_id)
    logger.info("From: %s", source_email)
    logger.info("To:   %s", target_email)

    perm = source_client.initiate_ownership_transfer(file_id, target_email)
    logger.info("Done. Permission: id=%s  pendingOwner=%s", perm.get("id"), perm.get("pendingOwner"))

    # ------------------------------------------------------------------
    # Step 4: accept ownership if target credentials exist
    # ------------------------------------------------------------------
    target_secret = CREDENTIALS_DIR / "client_secret_target.json"

    if target_secret.exists():
        logger.info("Step 4/4: authenticating the target account and accepting ownership...")
        target_creds = get_credentials("target")
        target_client = DriveClient(target_creds, account_label="target")

        result = target_client.accept_ownership_transfer(file_id, source_email)
        logger.info(
            "Ownership transfer succeeded. File '%s' now belongs to %s",
            file_name, result.get("emailAddress"),
        )
    else:
        logger.info(
            "Step 4/4: skipped (credentials/client_secret_target.json not found).\n"
            "  To accept ownership manually:\n"
            "  - open the mailbox for %s\n"
            "  - find the Google Drive ownership email\n"
            "  - click 'Accept'\n"
            "  Or add credentials/client_secret_target.json to accept automatically.",
            target_email,
        )

    logger.info("=" * 60)
    logger.info("Smoke test finished.")


if __name__ == "__main__":
    main()
