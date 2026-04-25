import logging
from pathlib import Path

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from retry import retry_transient

logger = logging.getLogger(__name__)

# Full Drive access is required to transfer ownership
SCOPES = ["https://www.googleapis.com/auth/drive"]

CREDENTIALS_DIR = Path(__file__).parent.parent / "credentials"
TOKENS_DIR = Path(__file__).parent.parent / "tokens"
TOKENS_DIR.mkdir(exist_ok=True)


def _should_restart_oauth(exc: RefreshError) -> bool:
    message = str(exc).lower()
    return (
        "invalid_grant" in message
        or "expired or revoked" in message
        or "token has been expired or revoked" in message
    )


def _run_oauth_flow(secret_path: Path, account: str) -> Credentials:
    logger.info("Starting OAuth flow for account '%s'...", account)
    flow = InstalledAppFlow.from_client_secrets_file(str(secret_path), SCOPES)
    # open_browser=False is useful in WSL: the script prints a URL,
    # you open it in the Windows browser, confirm access,
    # and the localhost redirect is captured automatically by WSL2.
    return flow.run_local_server(port=0, open_browser=False)


def get_credentials(account: str) -> Credentials:
    """
    Return OAuth2 credentials for the selected account.

    account: 'source' or 'target' - controls these file names:
      credentials/client_secret_<account>.json  (OAuth client from Google Cloud Console)
      tokens/token_<account>.json               (cached token, created automatically)
    """
    token_path = TOKENS_DIR / f"token_{account}.json"
    secret_path = CREDENTIALS_DIR / f"client_secret_{account}.json"

    if not secret_path.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {secret_path}\n"
            "Download an OAuth client from Google Cloud Console "
            "(application type: Desktop App) "
            f"and save it as credentials/client_secret_{account}.json"
        )

    creds: Credentials | None = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        logger.debug("Loaded cached OAuth credentials")

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing cached OAuth credentials...")
            try:
                retry_transient(
                    f"Refresh token for account '{account}'",
                    lambda: creds.refresh(Request()),
                )
            except RefreshError as exc:
                if not _should_restart_oauth(exc):
                    raise

                logger.warning(
                    "Cached OAuth credentials for account '%s' were revoked or expired. "
                    "Starting a new OAuth flow...",
                    account,
                )
                creds = _run_oauth_flow(secret_path, account)
        else:
            creds = _run_oauth_flow(secret_path, account)

        token_path.write_text(creds.to_json(), encoding="utf-8")
        logger.info("Updated cached OAuth credentials")

    return creds
