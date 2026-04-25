import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from google.auth.exceptions import RefreshError

context_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(context_root / "src"))

import auth


class AuthTests(TestCase):
    def test_falls_back_to_oauth_flow_when_refresh_token_is_revoked(self):
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            credentials_dir = tmp_path / "credentials"
            tokens_dir = tmp_path / "tokens"
            credentials_dir.mkdir()
            tokens_dir.mkdir()

            secret_path = credentials_dir / "client_secret_source.json"
            token_path = tokens_dir / "token_source.json"
            secret_path.write_text("{}", encoding="utf-8")
            token_path.write_text('{"token": "stale"}', encoding="utf-8")

            stale_creds = Mock()
            stale_creds.valid = False
            stale_creds.expired = True
            stale_creds.refresh_token = "refresh-token"
            stale_creds.refresh.side_effect = RefreshError(
                "invalid_grant: Token has been expired or revoked."
            )

            fresh_creds = Mock()
            fresh_creds.valid = True
            fresh_creds.to_json.return_value = '{"token": "fresh"}'

            flow = Mock()
            flow.run_local_server.return_value = fresh_creds

            with (
                patch.object(auth, "CREDENTIALS_DIR", credentials_dir),
                patch.object(auth, "TOKENS_DIR", tokens_dir),
                patch.object(auth.Credentials, "from_authorized_user_file", return_value=stale_creds),
                patch.object(auth.InstalledAppFlow, "from_client_secrets_file", return_value=flow) as flow_factory,
            ):
                creds = auth.get_credentials("source")

            self.assertIs(creds, fresh_creds)
            stale_creds.refresh.assert_called_once()
            flow_factory.assert_called_once_with(str(secret_path), auth.SCOPES)
            flow.run_local_server.assert_called_once_with(port=0, open_browser=False)
            self.assertEqual(token_path.read_text(encoding="utf-8"), '{"token": "fresh"}')
