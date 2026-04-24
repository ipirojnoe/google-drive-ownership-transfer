import sys
from pathlib import Path
import socket
from unittest import TestCase
from unittest.mock import Mock, patch

from googleapiclient.errors import HttpError

context_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(context_root / "src"))

project_root = context_root
if not (project_root / "transfer_all.py").exists():
    project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from drive_client import DriveClient, SharingQuotaExceededError
from retry import is_transient_error, retry_transient
from transfer_all import (
    accept_pending_ownership_transfers,
    get_next_stream_item,
    get_remaining_stream_items,
    transfer_item,
    transfer_or_stop,
)


class FakeHttpResponse:
    reason = "Forbidden"

    def __init__(self, status: int = 403):
        self.status = status


def make_http_error(reason: str, status: int = 403) -> HttpError:
    return HttpError(
        resp=FakeHttpResponse(status),
        content=(
            '{"error":{"errors":[{"reason":"%s"}],"message":"%s"}}'
            % (reason, reason)
        ).encode("utf-8"),
    )


class FakeRequest:
    def __init__(self, response=None, error: Exception | None = None):
        self.response = response
        self.error = error

    def execute(self):
        if self.error:
            raise self.error
        return self.response


class FakePermissionsApi:
    def __init__(self):
        self.create_calls = []
        self.update_calls = []
        self.get_calls = []
        self.list_calls = []
        self.permission = {
            "id": "perm-1",
            "emailAddress": "target@example.com",
            "role": "reader",
            "pendingOwner": False,
        }
        self.pending_owner_attempts = 0

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return FakeRequest(response={"permissions": [self.permission.copy()]})

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        self.permission.update(
            {
                "id": "perm-1",
                "emailAddress": kwargs["body"]["emailAddress"],
                "role": kwargs["body"]["role"],
                "pendingOwner": False,
            }
        )
        return FakeRequest(response={"id": "perm-1"})

    def update(self, **kwargs):
        self.update_calls.append(kwargs)
        body = kwargs["body"]

        if body.get("pendingOwner"):
            self.pending_owner_attempts += 1
            if self.pending_owner_attempts == 1:
                return FakeRequest(error=make_http_error("pendingOwnerWriterRequired"))

            self.permission["pendingOwner"] = True
            self.permission["role"] = body.get("role", self.permission["role"])
            return FakeRequest(response=self.permission.copy())

        self.permission["role"] = body["role"]
        return FakeRequest(response=self.permission.copy())

    def get(self, **kwargs):
        self.get_calls.append(kwargs)

        if len(self.get_calls) == 1:
            self.permission["role"] = "reader"
        else:
            self.permission["role"] = "writer"

        return FakeRequest(response=self.permission.copy())


class FakeService:
    def __init__(self, permissions_api: FakePermissionsApi):
        self._permissions_api = permissions_api

    def permissions(self):
        return self._permissions_api


class RetryTests(TestCase):
    def test_retries_transient_network_errors(self):
        calls = []

        def operation():
            calls.append("call")
            if len(calls) == 1:
                raise socket.gaierror("temporary dns failure")
            return "ok"

        with patch("retry.time.sleep", return_value=None):
            self.assertEqual(
                retry_transient("test operation", operation, attempts=2, base_delay=0),
                "ok",
            )

        self.assertEqual(len(calls), 2)

    def test_classifies_rate_limit_as_transient_but_not_sharing_quota(self):
        self.assertTrue(is_transient_error(make_http_error("rateLimitExceeded")))
        self.assertFalse(is_transient_error(make_http_error("sharingRateLimitExceeded")))

    def test_classifies_connection_reset_as_transient(self):
        self.assertTrue(is_transient_error(ConnectionResetError("connection reset")))


class DriveClientInitiateOwnershipTransferTests(TestCase):
    def test_waits_for_writer_before_marking_pending_owner(self):
        permissions_api = FakePermissionsApi()
        client = DriveClient.__new__(DriveClient)
        client._credentials = None
        client.service = FakeService(permissions_api)
        client.label = "source"

        with patch("drive_client.time.sleep", return_value=None):
            result = client.initiate_ownership_transfer("file-1", "target@example.com")

        self.assertTrue(result["pendingOwner"])
        self.assertEqual(len(permissions_api.create_calls), 0)
        self.assertGreaterEqual(len(permissions_api.get_calls), 2)
        self.assertEqual(permissions_api.pending_owner_attempts, 2)

        pending_owner_updates = [
            call for call in permissions_api.update_calls if call["body"].get("pendingOwner")
        ]
        self.assertEqual(len(pending_owner_updates), 2)
        self.assertEqual(pending_owner_updates[0]["body"]["role"], "writer")

    def test_upgrades_existing_permission_to_writer(self):
        permissions_api = FakePermissionsApi()
        permissions_api.permission.update(
            {
                "id": "perm-existing",
                "role": "reader",
                "pendingOwner": False,
            }
        )
        client = DriveClient.__new__(DriveClient)
        client._credentials = None
        client.service = FakeService(permissions_api)
        client.label = "source"

        with patch("drive_client.time.sleep", return_value=None):
            client.initiate_ownership_transfer("file-2", "target@example.com")

        self.assertEqual(len(permissions_api.create_calls), 0)
        role_updates = [
            call for call in permissions_api.update_calls if call["body"] == {"role": "writer"}
        ]
        self.assertEqual(len(role_updates), 1)

    def test_creates_direct_writer_for_inherited_only_permission(self):
        permissions_api = FakePermissionsApi()
        permissions_api.permission.update(
            {
                "id": "perm-inherited",
                "role": "writer",
                "pendingOwner": False,
                "permissionDetails": [
                    {"permissionType": "file", "role": "writer", "inherited": True}
                ],
            }
        )
        client = DriveClient.__new__(DriveClient)
        client._credentials = None
        client.service = FakeService(permissions_api)
        client.label = "source"

        with patch("drive_client.time.sleep", return_value=None):
            client.initiate_ownership_transfer("file-2", "target@example.com")

        self.assertEqual(len(permissions_api.create_calls), 1)
        self.assertEqual(
            permissions_api.create_calls[0]["body"],
            {
                "type": "user",
                "role": "writer",
                "emailAddress": "target@example.com",
            },
        )


class DriveClientStageItemTests(TestCase):
    def test_skips_staging_for_direct_permission(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "source"
        client._find_permission = Mock(
            return_value={
                "id": "perm-1",
                "emailAddress": "target@example.com",
                "role": "writer",
                "permissionDetails": [
                    {"permissionType": "file", "role": "writer", "inherited": False}
                ],
            }
        )
        client.get_parents = Mock()
        client._ensure_staging_folder = Mock()
        client.move_item = Mock()

        result = client.stage_item_if_needed("file-1", "target@example.com")

        self.assertEqual(
            result,
            {
                "staged": False,
                "original_parents": [],
                "staging_parent": None,
            },
        )
        client.get_parents.assert_not_called()
        client.move_item.assert_not_called()

    def test_moves_item_to_staging_for_inherited_permission(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "source"
        client._find_permission = Mock(
            return_value={
                "id": "perm-1",
                "emailAddress": "target@example.com",
                "role": "writer",
                "permissionDetails": [
                    {"permissionType": "file", "role": "writer", "inherited": True}
                ],
            }
        )
        client.get_parents = Mock(return_value=["parent-1"])
        client._ensure_staging_folder = Mock(return_value="staging-1")
        client.move_item = Mock()
        client._wait_until_target_not_inherited = Mock()

        result = client.stage_item_if_needed("file-2", "target@example.com")

        self.assertEqual(
            result,
            {
                "staged": True,
                "original_parents": ["parent-1"],
                "staging_parent": "staging-1",
            },
        )
        client.move_item.assert_called_once_with(
            "file-2",
            add_parents=["staging-1"],
            remove_parents=["parent-1"],
        )
        client._wait_until_target_not_inherited.assert_called_once_with(
            "file-2", "target@example.com"
        )

    def test_skips_staging_for_inherited_permission_without_parents(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "source"
        client._find_permission = Mock(
            return_value={
                "id": "perm-1",
                "emailAddress": "target@example.com",
                "role": "writer",
                "permissionDetails": [
                    {"permissionType": "file", "role": "writer", "inherited": True}
                ],
            }
        )
        client.get_parents = Mock(return_value=[])
        client._ensure_staging_folder = Mock()
        client.move_item = Mock()

        result = client.stage_item_if_needed("file-2", "target@example.com")

        self.assertEqual(
            result,
            {
                "staged": False,
                "original_parents": [],
                "staging_parent": None,
            },
        )
        client._ensure_staging_folder.assert_not_called()
        client.move_item.assert_not_called()


class DriveClientRemoveAccessTests(TestCase):
    def test_skips_inherited_permission_delete(self):
        permissions_api = FakePermissionsApi()
        client = DriveClient.__new__(DriveClient)
        client._credentials = None
        client.service = FakeService(permissions_api)
        client.label = "target"
        client._find_permission = Mock(
            return_value={
                "id": "perm-inherited",
                "emailAddress": "source@example.com",
                "role": "writer",
                "permissionDetails": [
                    {"permissionType": "file", "role": "writer", "inherited": True}
                ],
            }
        )

        removed = client.remove_access("file-3", "source@example.com")

        self.assertFalse(removed)
        self.assertEqual(permissions_api.list_calls, [])
        self.assertEqual(permissions_api.update_calls, [])
        self.assertEqual(permissions_api.create_calls, [])

    def test_cleanup_source_access_prioritizes_folders_then_files(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "target"
        client._my_email = "target@example.com"
        client._list_items_shared_with_user = Mock(
            side_effect=[
                ([{
                    "id": "folder-1",
                    "name": "Folder",
                    "mimeType": "application/vnd.google-apps.folder",
                    "owners": [{"emailAddress": "target@example.com"}],
                }], None),
                ([{
                    "id": "file-1",
                    "name": "File",
                    "mimeType": "text/plain",
                    "owners": [{"emailAddress": "target@example.com"}],
                }], None),
            ]
        )
        client.remove_access = Mock(side_effect=[True, True])

        removed = client.cleanup_source_access("source@example.com", limit=10)

        self.assertEqual(removed, 2)
        client.remove_access.assert_any_call(
            "folder-1",
            "source@example.com",
            resolve_inherited=True,
        )
        client.remove_access.assert_any_call(
            "file-1",
            "source@example.com",
            resolve_inherited=True,
        )
        self.assertEqual(client.remove_access.call_args_list[0].args[0], "folder-1")
        self.assertEqual(client.remove_access.call_args_list[1].args[0], "file-1")

    def test_cleanup_removes_all_folders_before_files_even_over_limit(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "target"
        client._my_email = "target@example.com"
        client._list_items_shared_with_user = Mock(
            side_effect=[
                ([{
                    "id": "folder-1",
                    "name": "Folder 1",
                    "mimeType": "application/vnd.google-apps.folder",
                    "owners": [{"emailAddress": "target@example.com"}],
                }], "more-folders"),
                ([{
                    "id": "folder-2",
                    "name": "Folder 2",
                    "mimeType": "application/vnd.google-apps.folder",
                    "owners": [{"emailAddress": "target@example.com"}],
                }], None),
                ([{
                    "id": "file-1",
                    "name": "File 1",
                    "mimeType": "text/plain",
                    "owners": [{"emailAddress": "target@example.com"}],
                }], None),
            ]
        )
        client.remove_access = Mock(side_effect=[True, True, True])

        removed = client.cleanup_source_access("source@example.com", limit=1)

        self.assertEqual(removed, 2)
        self.assertEqual(client.remove_access.call_count, 2)
        self.assertEqual(client.remove_access.call_args_list[0].args[0], "folder-1")
        self.assertEqual(client.remove_access.call_args_list[1].args[0], "folder-2")

    def test_remove_access_can_delete_inherited_permission_from_owned_parent(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "target"
        client._my_email = "target@example.com"
        client._find_permission = Mock(
            side_effect=[
                {
                    "id": "perm-child",
                    "emailAddress": "source@example.com",
                    "role": "writer",
                    "permissionDetails": [
                        {
                            "permissionType": "file",
                            "role": "writer",
                            "inherited": True,
                            "inheritedFrom": "parent-1",
                        }
                    ],
                },
                {
                    "id": "perm-parent",
                    "emailAddress": "source@example.com",
                    "role": "writer",
                    "permissionDetails": [
                        {
                            "permissionType": "file",
                            "role": "writer",
                            "inherited": False,
                        }
                    ],
                },
            ]
        )
        client.get_file = Mock(
            return_value={
                "id": "parent-1",
                "name": "Parent",
                "owners": [{"emailAddress": "target@example.com"}],
            }
        )
        client._delete_permission = Mock()

        removed = client.remove_access(
            "file-3",
            "source@example.com",
            resolve_inherited=True,
        )

        self.assertTrue(removed)
        client.get_file.assert_called_once_with(
            "parent-1",
            "id, name, owners(emailAddress)",
        )
        client._delete_permission.assert_called_once_with(
            "parent-1",
            "perm-parent",
            "source@example.com",
        )

    def test_remove_access_does_not_delete_inherited_permission_from_unowned_parent(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "target"
        client._my_email = "target@example.com"
        client._find_permission = Mock(
            return_value={
                "id": "perm-child",
                "emailAddress": "source@example.com",
                "role": "writer",
                "permissionDetails": [
                    {
                        "permissionType": "file",
                        "role": "writer",
                        "inherited": True,
                        "inheritedFrom": "parent-1",
                    }
                ],
            }
        )
        client.get_file = Mock(
            return_value={
                "id": "parent-1",
                "name": "Parent",
                "owners": [{"emailAddress": "other@example.com"}],
            }
        )
        client._delete_permission = Mock()

        removed = client.remove_access(
            "file-3",
            "source@example.com",
            resolve_inherited=True,
        )

        self.assertFalse(removed)
        client._delete_permission.assert_not_called()

    def test_remove_access_falls_back_to_file_parents_when_inherited_from_is_missing(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "target"
        client._my_email = "target@example.com"
        client._find_permission = Mock(
            side_effect=[
                {
                    "id": "perm-child",
                    "emailAddress": "source@example.com",
                    "role": "writer",
                    "permissionDetails": [
                        {
                            "permissionType": "file",
                            "role": "writer",
                            "inherited": True,
                        }
                    ],
                },
                {
                    "id": "perm-parent",
                    "emailAddress": "source@example.com",
                    "role": "writer",
                    "permissionDetails": [
                        {
                            "permissionType": "file",
                            "role": "writer",
                            "inherited": False,
                        }
                    ],
                },
            ]
        )
        client.get_parents = Mock(return_value=["parent-1"])
        client.get_file = Mock(
            return_value={
                "id": "parent-1",
                "name": "Parent",
                "owners": [{"emailAddress": "target@example.com"}],
            }
        )
        client._delete_permission = Mock()

        removed = client.remove_access(
            "file-3",
            "source@example.com",
            resolve_inherited=True,
        )

        self.assertTrue(removed)
        client.get_parents.assert_called_once_with("file-3")
        client._delete_permission.assert_called_once_with(
            "parent-1",
            "perm-parent",
            "source@example.com",
        )

    def test_cleanup_skips_items_not_owned_by_target(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "target"
        client._my_email = "target@example.com"
        client._list_items_shared_with_user = Mock(
            side_effect=[
                ([{
                    "id": "folder-1",
                    "name": "Folder",
                    "mimeType": "application/vnd.google-apps.folder",
                    "owners": [{"emailAddress": "other@example.com"}],
                }], None),
                ([], None),
            ]
        )
        client.remove_access = Mock()

        removed = client.cleanup_source_access("source@example.com", limit=10)

        self.assertEqual(removed, 0)
        client.remove_access.assert_not_called()


class DriveClientPendingOwnershipTests(TestCase):
    def test_list_pending_ownership_items_filters_by_source_and_pending_owner(self):
        client = DriveClient.__new__(DriveClient)
        client.label = "target"
        client._my_email = "target@example.com"
        files_api = Mock()
        files_api.list.return_value = FakeRequest(
            response={
                "files": [
                    {
                        "id": "file-1",
                        "name": "Pending",
                        "owners": [{"emailAddress": "source@example.com"}],
                    },
                    {
                        "id": "file-2",
                        "name": "Not pending",
                        "owners": [{"emailAddress": "source@example.com"}],
                    },
                    {
                        "id": "file-3",
                        "name": "Other owner",
                        "owners": [{"emailAddress": "other@example.com"}],
                    },
                ],
                "nextPageToken": "next-page",
            }
        )
        service = Mock()
        service.files.return_value = files_api
        client.service = service
        client._find_permission = Mock(
            side_effect=[
                {"id": "perm-1", "emailAddress": "target@example.com", "pendingOwner": True},
                {"id": "perm-2", "emailAddress": "target@example.com", "pendingOwner": False},
            ]
        )

        items, page_token = client.list_pending_ownership_items(
            "source@example.com",
            page_token="page-1",
        )

        self.assertEqual([item["id"] for item in items], ["file-1"])
        self.assertEqual(page_token, "next-page")
        files_api.list.assert_called_once()
        self.assertEqual(files_api.list.call_args.kwargs["pageToken"], "page-1")
        self.assertEqual(client._find_permission.call_count, 2)


class TransferItemTests(TestCase):
    def test_does_not_fail_when_source_access_is_only_inherited(self):
        source_client = Mock()
        target_client = Mock()
        source_client.get_file.return_value = {
            "id": "file-4",
            "owners": [{"emailAddress": "source@example.com"}],
        }
        source_client.stage_item_if_needed.return_value = {
            "staged": True,
            "original_parents": ["parent-1"],
            "staging_parent": "staging-1",
        }
        target_client.remove_access.return_value = False

        failed: list[dict] = []
        ok = transfer_item(
            {
                "id": "file-4",
                "name": "demo",
                "size": "100",
                "mimeType": "application/octet-stream",
            },
            1,
            "?",
            source_client,
            target_client,
            "source@example.com",
            "target@example.com",
            True,
            failed,
        )

        self.assertTrue(ok)
        self.assertEqual(failed, [])
        source_client.restore_item_parents.assert_called_once_with(
            "file-4",
            ["parent-1"],
            "staging-1",
        )

    def test_reraises_sharing_quota_error_after_rollback(self):
        source_client = Mock()
        target_client = Mock()
        source_client.get_file.return_value = {
            "id": "file-5",
            "owners": [{"emailAddress": "source@example.com"}],
        }
        source_client.stage_item_if_needed.return_value = {
            "staged": True,
            "original_parents": ["parent-1"],
            "staging_parent": "staging-1",
        }
        source_client.initiate_ownership_transfer.side_effect = SharingQuotaExceededError("quota")

        failed: list[dict] = []
        with self.assertRaises(SharingQuotaExceededError):
            transfer_item(
                {
                    "id": "file-5",
                    "name": "quota-hit",
                    "size": "100",
                    "mimeType": "application/octet-stream",
                },
                1,
                "?",
                source_client,
                target_client,
                "source@example.com",
                "target@example.com",
                True,
                failed,
            )

        source_client.restore_item_parents.assert_called_once_with(
            "file-5",
            ["parent-1"],
            "staging-1",
        )
        self.assertEqual(len(failed), 1)

    def test_transfer_or_stop_reraises_quota_error(self):
        source_client = Mock()
        target_client = Mock()
        source_client.get_file.return_value = {
            "id": "file-6",
            "owners": [{"emailAddress": "source@example.com"}],
        }
        source_client.stage_item_if_needed.return_value = {
            "staged": False,
            "original_parents": [],
            "staging_parent": None,
        }
        source_client.initiate_ownership_transfer.side_effect = SharingQuotaExceededError("quota")
        target_client.cleanup_source_access.return_value = 0

        failed: list[dict] = []
        with self.assertRaises(SharingQuotaExceededError):
            transfer_or_stop(
                {
                    "id": "file-6",
                    "name": "quota-hit",
                    "size": "100",
                    "mimeType": "application/octet-stream",
                },
                1,
                "?",
                source_client,
                target_client,
                "source@example.com",
                "target@example.com",
                True,
                failed,
            )

    def test_skips_item_when_source_no_longer_owns_it(self):
        source_client = Mock()
        target_client = Mock()
        source_client.get_file.return_value = {
            "id": "file-9",
            "owners": [{"emailAddress": "target@example.com"}],
        }

        failed: list[dict] = []
        ok = transfer_item(
            {
                "id": "file-9",
                "name": "already-transferred",
                "size": "100",
                "mimeType": "application/octet-stream",
            },
            1,
            "?",
            source_client,
            target_client,
            "source@example.com",
            "target@example.com",
            True,
            failed,
        )

        self.assertFalse(ok)
        self.assertEqual(failed, [])
        source_client.stage_item_if_needed.assert_not_called()
        source_client.initiate_ownership_transfer.assert_not_called()
        target_client.accept_ownership_transfer.assert_not_called()

    def test_transfer_or_stop_cleans_up_and_retries_after_quota_error(self):
        source_client = Mock()
        target_client = Mock()
        failed: list[dict] = []
        item = {
            "id": "file-7",
            "name": "quota-then-ok",
            "size": "100",
            "mimeType": "application/octet-stream",
        }

        with patch("transfer_all.transfer_item", side_effect=[SharingQuotaExceededError("quota"), True]) as mocked_transfer:
            target_client.cleanup_source_access.return_value = 3
            with patch("transfer_all.time.sleep", return_value=None):
                ok = transfer_or_stop(
                    item,
                    1,
                    "?",
                    source_client,
                    target_client,
                    "source@example.com",
                    "target@example.com",
                    True,
                    failed,
                )

        self.assertTrue(ok)
        self.assertEqual(mocked_transfer.call_count, 2)
        target_client.cleanup_source_access.assert_called_once_with(
            "source@example.com",
            limit=200,
        )

    def test_transfer_or_stop_stops_if_cleanup_frees_nothing(self):
        source_client = Mock()
        target_client = Mock()
        failed: list[dict] = []
        item = {
            "id": "file-8",
            "name": "quota-no-cleanup",
            "size": "100",
            "mimeType": "application/octet-stream",
        }

        with patch("transfer_all.transfer_item", side_effect=SharingQuotaExceededError("quota")):
            target_client.cleanup_source_access.return_value = 0
            with self.assertRaises(SharingQuotaExceededError):
                transfer_or_stop(
                    item,
                    1,
                    "?",
                    source_client,
                    target_client,
                    "source@example.com",
                    "target@example.com",
                    True,
                    failed,
                )


class PendingOwnershipTransferTests(TestCase):
    def test_accepts_pending_ownership_transfers_and_removes_source_access(self):
        target_client = Mock()
        target_client.list_pending_ownership_items.side_effect = [
            ([
                {
                    "id": "file-10",
                    "name": "Manual",
                    "size": "100",
                    "mimeType": "application/octet-stream",
                }
            ], None)
        ]
        target_client.remove_access.return_value = True

        failed: list[dict] = []
        with patch("transfer_all.time.sleep", return_value=None):
            accepted = accept_pending_ownership_transfers(
                target_client,
                "source@example.com",
                dry_run=False,
                remove_source_access=True,
                failed=failed,
            )

        self.assertEqual(accepted, 1)
        self.assertEqual(failed, [])
        target_client.accept_ownership_transfer.assert_called_once_with(
            "file-10",
            "source@example.com",
        )
        target_client.remove_access.assert_called_once_with(
            "file-10",
            "source@example.com",
        )

    def test_dry_run_lists_pending_ownership_transfers_without_accepting(self):
        target_client = Mock()
        target_client.list_pending_ownership_items.return_value = (
            [{
                "id": "file-10",
                "name": "Manual",
                "size": "100",
                "mimeType": "application/octet-stream",
            }],
            None,
        )

        failed: list[dict] = []
        accepted = accept_pending_ownership_transfers(
            target_client,
            "source@example.com",
            dry_run=True,
            remove_source_access=True,
            failed=failed,
        )

        self.assertEqual(accepted, 0)
        self.assertEqual(failed, [])
        target_client.accept_ownership_transfer.assert_not_called()
        target_client.remove_access.assert_not_called()


class StreamOrderingTests(TestCase):
    def test_next_stream_item_uses_global_max_of_seen_items(self):
        seen_items = [
            {"id": "a", "name": "A", "size": "100"},
            {"id": "b", "name": "B", "size": "99"},
            {"id": "c", "name": "C", "size": "50"},
        ]

        next_item = get_next_stream_item(seen_items, {"a"})

        self.assertIsNotNone(next_item)
        self.assertEqual(next_item["id"], "b")

    def test_remaining_stream_items_skip_processed_and_stay_globally_sorted(self):
        items = [
            {"id": "a", "name": "A", "size": "10"},
            {"id": "b", "name": "B", "size": "50"},
            {"id": "c", "name": "C", "size": "20"},
            {"id": "d", "name": "D", "size": None},
            {"id": "e", "name": "E", "size": "30"},
        ]

        remaining = get_remaining_stream_items(items, {"b", "e"})

        self.assertEqual([item["id"] for item in remaining], ["c", "a", "d"])
