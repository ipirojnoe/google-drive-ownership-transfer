import time
from typing import Any

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

from src.logger import get_logger

logger = get_logger(__name__)

WRITER_ROLE_WAIT_TIMEOUT = 20.0
WRITER_ROLE_POLL_INTERVAL = 1.0
PENDING_OWNER_RETRIES = 5
STAGING_FOLDER_NAME = ".ownership-transfer-staging"



class DriveClient:
    def __init__(self, credentials: Credentials, account_label: str = ""):
        self._credentials = credentials
        self.service = build("drive", "v3", credentials=credentials)
        self.label = account_label

    def clone(self) -> "DriveClient":
        """Create a new client with a separate service object.
        Useful when multiple threads are used because httplib2 is not thread-safe."""
        return DriveClient(self._credentials, self.label)

    # ------------------------------------------------------------------
    # Files
    # ------------------------------------------------------------------

    def list_files(self, limit: int = 10) -> list[dict[str, Any]]:
        """
        Return up to `limit` files owned by the current account,
        sorted by modified date, newest first.
        Folders are excluded.
        """
        results = (
            self.service.files()
            .list(
                pageSize=limit,
                orderBy="modifiedTime desc",
                q="'me' in owners and mimeType != 'application/vnd.google-apps.folder' and trashed = false",
                fields="files(id, name, modifiedTime, mimeType, owners, size)",
            )
            .execute()
        )
        files = results.get("files", [])
        logger.debug("[%s] Files fetched: %d", self.label, len(files))
        return files

    # ------------------------------------------------------------------
    # Permissions
    # ------------------------------------------------------------------

    def list_permissions(self, file_id: str) -> list[dict[str, Any]]:
        results = (
            self.service.permissions()
            .list(
                fileId=file_id,
                fields=(
                    "permissions("
                    "id, emailAddress, role, type, pendingOwner, "
                    "permissionDetails(permissionType, role, inherited)"
                    ")"
                ),
            )
            .execute()
        )
        return results.get("permissions", [])

    def _find_permission(
        self, file_id: str, email: str
    ) -> dict[str, Any] | None:
        perms = self.list_permissions(file_id)
        email_lower = email.lower()
        return next(
            (p for p in perms if p.get("emailAddress", "").lower() == email_lower),
            None,
        )

    def _get_permission(self, file_id: str, permission_id: str) -> dict[str, Any]:
        return (
            self.service.permissions()
            .get(
                fileId=file_id,
                permissionId=permission_id,
                fields=(
                    "id, emailAddress, role, type, pendingOwner, "
                    "permissionDetails(permissionType, role, inherited)"
                ),
            )
            .execute()
        )

    def _permission_details(self, permission: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not permission:
            return []
        return permission.get("permissionDetails", [])

    def _has_direct_permission(self, permission: dict[str, Any] | None) -> bool:
        details = self._permission_details(permission)
        if not details:
            return permission is not None
        return any(not detail.get("inherited", False) for detail in details)

    def _direct_roles(self, permission: dict[str, Any] | None) -> set[str]:
        return {
            detail.get("role", "")
            for detail in self._permission_details(permission)
            if not detail.get("inherited", False)
        }

    def get_file(self, file_id: str, fields: str) -> dict[str, Any]:
        return self.service.files().get(fileId=file_id, fields=fields).execute()

    def get_parents(self, file_id: str) -> list[str]:
        meta = self.get_file(file_id, "id, parents")
        return meta.get("parents", [])

    def _wait_until_target_not_inherited(
        self, file_id: str, target_email: str
    ) -> None:
        deadline = time.monotonic() + WRITER_ROLE_WAIT_TIMEOUT

        while time.monotonic() < deadline:
            permission = self._find_permission(file_id, target_email)
            if permission is None:
                return

            if self._has_direct_permission(permission):
                return

            logger.debug(
                "[%s] %s still has inherited-only permission on file %s, waiting...",
                self.label,
                target_email,
                file_id,
            )
            time.sleep(WRITER_ROLE_POLL_INTERVAL)

        raise TimeoutError(
            f"Target permission for {target_email} on file {file_id} "
            f"remained inherited-only for {WRITER_ROLE_WAIT_TIMEOUT:.0f}s"
        )

    def _ensure_staging_folder(self) -> str:
        query = (
            "mimeType = 'application/vnd.google-apps.folder' "
            f"and name = '{STAGING_FOLDER_NAME}' "
            "and 'root' in parents and trashed = false"
        )
        response = (
            self.service.files()
            .list(
                q=query,
                pageSize=1,
                fields="files(id, name)",
            )
            .execute()
        )
        files = response.get("files", [])
        if files:
            return files[0]["id"]

        created = (
            self.service.files()
            .create(
                body={
                    "name": STAGING_FOLDER_NAME,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": ["root"],
                },
                fields="id",
            )
            .execute()
        )
        folder_id = created["id"]
        logger.info("[%s] Created staging folder: %s", self.label, folder_id)
        return folder_id

    def move_item(
        self,
        file_id: str,
        *,
        add_parents: list[str] | None = None,
        remove_parents: list[str] | None = None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "fileId": file_id,
            "fields": "id, parents",
        }
        if add_parents:
            kwargs["addParents"] = ",".join(add_parents)
        if remove_parents:
            kwargs["removeParents"] = ",".join(remove_parents)
        return self.service.files().update(**kwargs).execute()

    def stage_item_if_needed(
        self, file_id: str, target_email: str
    ) -> dict[str, Any]:
        permission = self._find_permission(file_id, target_email)
        direct_roles = self._direct_roles(permission)

        if "owner" in direct_roles:
            return {
                "staged": False,
                "original_parents": [],
                "staging_parent": None,
            }

        if permission is None or self._has_direct_permission(permission):
            return {
                "staged": False,
                "original_parents": [],
                "staging_parent": None,
            }

        original_parents = self.get_parents(file_id)
        if not original_parents:
            raise ValueError(
                f"File {file_id} requires the staging workaround but has no parent folders"
            )

        staging_parent = self._ensure_staging_folder()
        logger.info(
            "[%s] File %s inherits access for %s; temporarily moving it to staging",
            self.label,
            file_id,
            target_email,
        )
        self.move_item(
            file_id,
            add_parents=[staging_parent],
            remove_parents=original_parents,
        )
        self._wait_until_target_not_inherited(file_id, target_email)
        return {
            "staged": True,
            "original_parents": original_parents,
            "staging_parent": staging_parent,
        }

    def restore_item_parents(
        self,
        file_id: str,
        original_parents: list[str],
        staging_parent: str | None = None,
    ) -> None:
        if not original_parents:
            return

        remove_parents = [staging_parent] if staging_parent else []
        self.move_item(
            file_id,
            add_parents=original_parents,
            remove_parents=remove_parents,
        )
        logger.info(
            "[%s] Restored parent folders for file %s",
            self.label,
            file_id,
        )

    def _wait_until_writer(
        self, file_id: str, permission_id: str, target_email: str
    ) -> dict[str, Any]:
        deadline = time.monotonic() + WRITER_ROLE_WAIT_TIMEOUT
        last_seen: dict[str, Any] | None = None

        while time.monotonic() < deadline:
            last_seen = self._get_permission(file_id, permission_id)
            role = last_seen.get("role")
            if role == "writer":
                return last_seen

            logger.debug(
                "[%s] Permission %s for %s is not writer yet (role=%s), waiting...",
                self.label, permission_id, target_email, role,
            )
            time.sleep(WRITER_ROLE_POLL_INTERVAL)

        raise TimeoutError(
            f"Permission {permission_id} for {target_email} did not become writer "
            f"within {WRITER_ROLE_WAIT_TIMEOUT:.0f}s"
        )

    def _mark_pending_owner(
        self, file_id: str, permission_id: str, target_email: str
    ) -> dict[str, Any]:
        last_error: HttpError | None = None

        for attempt in range(1, PENDING_OWNER_RETRIES + 1):
            current = self._wait_until_writer(file_id, permission_id, target_email)
            if current.get("pendingOwner"):
                return current

            try:
                return (
                    self.service.permissions()
                    .update(
                        fileId=file_id,
                        permissionId=permission_id,
                        body={"role": "writer", "pendingOwner": True},
                        fields="id, emailAddress, role, pendingOwner",
                    )
                    .execute()
                )
            except HttpError as exc:
                if "pendingOwnerWriterRequired" not in str(exc):
                    raise

                last_error = exc
                logger.warning(
                    "[%s] Drive still does not recognize %s as writer for file %s "
                    "(attempt %d/%d), retrying...",
                    self.label,
                    target_email,
                    file_id,
                    attempt,
                    PENDING_OWNER_RETRIES,
                )
                time.sleep(WRITER_ROLE_POLL_INTERVAL)

        if last_error is not None:
            raise last_error

        raise RuntimeError(
            f"Failed to set pendingOwner for {target_email} on file {file_id}"
        )

    # ------------------------------------------------------------------
    # Step 1 (source): initiate ownership transfer
    # ------------------------------------------------------------------

    def initiate_ownership_transfer(
        self, file_id: str, target_email: str
    ) -> dict[str, Any]:
        """
        Initiate ownership transfer in three steps:
          1. Find or create the target user's permission with role=writer.
          2. Wait until Drive starts returning role=writer for that permission.
          3. Set pendingOwner=true in a separate update call.

        Important: do not send role and pendingOwner as a combined "upgrade"
        assumption without checking the actual permission state first.
        Google validates the current role before applying pendingOwner and may
        return pendingOwnerWriterRequired. Also, role=writer after create/update
        can appear with delay, so polling the real permission state is required.
        """
        existing = self._find_permission(file_id, target_email)

        try:
            if existing:
                perm_id = existing["id"]
                if existing.get("pendingOwner"):
                    logger.info(
                        "[%s] Transfer ownership already initiated -> %s  "
                        "(permission id=%s)",
                        self.label,
                        target_email,
                        perm_id,
                    )
                    return existing

                if existing.get("role") != "writer":
                    logger.debug(
                        "[%s] Updating permission id=%s to writer for %s",
                        self.label,
                        perm_id,
                        target_email,
                    )
                    self.service.permissions().update(
                        fileId=file_id,
                        permissionId=perm_id,
                        body={"role": "writer"},
                        fields="id, emailAddress, role, pendingOwner",
                    ).execute()
                else:
                    logger.debug(
                        "[%s] Permission id=%s for %s is already writer",
                        self.label,
                        perm_id,
                        target_email,
                    )
            else:
                logger.debug("[%s] Creating writer permission for %s", self.label, target_email)
                created = self.service.permissions().create(
                    fileId=file_id,
                    body={
                        "type": "user",
                        "role": "writer",
                        "emailAddress": target_email,
                    },
                    fields="id",
                ).execute()
                perm_id = created["id"]

            result = self._mark_pending_owner(file_id, perm_id, target_email)

        except HttpError as exc:
            logger.error(
                "[%s] Failed to initiate transfer for file %s: %s",
                self.label, file_id, exc,
            )
            raise

        logger.info(
            "[%s] Ownership transfer initiated -> %s  (permission id=%s, pendingOwner=%s)",
            self.label, target_email, result.get("id"), result.get("pendingOwner"),
        )
        return result

    # ------------------------------------------------------------------
    # Step 2 (target): accept ownership
    # ------------------------------------------------------------------

    def accept_ownership_transfer(
        self, file_id: str, source_email: str
    ) -> dict[str, Any]:
        """
        Accept ownership from the target account.
        The method finds the target account's permission on the file and sets
        role=owner with transferOwnership=True.
        source_email is used for logging only.
        """
        # Find the permission id for the currently authenticated target account.
        about = self.service.about().get(fields="user(emailAddress)").execute()
        my_email = about["user"]["emailAddress"]

        my_perm = self._find_permission(file_id, my_email)
        if not my_perm:
            raise ValueError(
                f"No permission found for account {my_email} on file {file_id}. "
                "Make sure the source account already initiated the transfer."
            )

        try:
            result = (
                self.service.permissions()
                .update(
                    fileId=file_id,
                    permissionId=my_perm["id"],
                    body={"role": "owner"},
                    transferOwnership=True,
                    fields="id, emailAddress, role",
                )
                .execute()
            )
        except HttpError as exc:
            logger.error(
                "[%s] Failed to accept transfer for file %s: %s",
                self.label, file_id, exc,
            )
            raise

        logger.info(
            "[%s] Ownership accepted: file %s now belongs to %s  (previous owner: %s)",
            self.label, file_id, my_email, source_email,
        )
        return result

    # ------------------------------------------------------------------
    # Remove access for a given email (called as the new owner)
    # ------------------------------------------------------------------

    def remove_access(self, file_id: str, email: str) -> bool:
        """
        Remove the permission for `email` from `file_id`.
        Must be called by a client that owns the file.
        """
        perm = self._find_permission(file_id, email)
        if not perm:
            logger.debug(
                "[%s] No permission for %s on file %s - skipping delete",
                self.label, email, file_id,
            )
            return False

        if not self._has_direct_permission(perm):
            logger.warning(
                "[%s] Cannot remove access for %s on file %s: permission is inherited, "
                "the source of access lives on a parent folder",
                self.label,
                email,
                file_id,
            )
            return False

        try:
            self.service.permissions().delete(
                fileId=file_id,
                permissionId=perm["id"],
            ).execute()
        except HttpError as exc:
            logger.error(
                "[%s] Failed to remove access for %s on file %s: %s",
                self.label, email, file_id, exc,
            )
            raise

        logger.info(
            "[%s] Access removed: %s no longer has access to file %s",
            self.label, email, file_id,
        )
        return True
