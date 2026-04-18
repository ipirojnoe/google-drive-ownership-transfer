from unittest import TestCase
from unittest.mock import Mock, patch

from googleapiclient.errors import HttpError

from src.drive_client import DriveClient
from transfer_all import get_next_stream_item, get_remaining_stream_items, transfer_item


class FakeHttpResponse:
    status = 403
    reason = "Forbidden"


def make_http_error(reason: str) -> HttpError:
    return HttpError(
        resp=FakeHttpResponse(),
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


class DriveClientInitiateOwnershipTransferTests(TestCase):
    def test_waits_for_writer_before_marking_pending_owner(self):
        permissions_api = FakePermissionsApi()
        client = DriveClient.__new__(DriveClient)
        client._credentials = None
        client.service = FakeService(permissions_api)
        client.label = "source"

        with patch("src.drive_client.time.sleep", return_value=None):
            result = client.initiate_ownership_transfer("file-1", "target@example.com")

        self.assertEqual(result["pendingOwner"], True)
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

        with patch("src.drive_client.time.sleep", return_value=None):
            client.initiate_ownership_transfer("file-2", "target@example.com")

        self.assertEqual(len(permissions_api.create_calls), 0)
        role_updates = [
            call for call in permissions_api.update_calls if call["body"] == {"role": "writer"}
        ]
        self.assertEqual(len(role_updates), 1)


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


class TransferItemTests(TestCase):
    def test_does_not_fail_when_source_access_is_only_inherited(self):
        source_client = Mock()
        target_client = Mock()
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
