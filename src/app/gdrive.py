import pathlib, os
from typing import Tuple, Optional
from loguru import logger

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive"]
TOKEN  = pathlib.Path(".secrets/google_token.json")

ROOT_PARENT_ID = os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip() or None  # optional parent folder

def _load_creds() -> Credentials:
    if not TOKEN.exists():
        raise RuntimeError("Google token missing. Run: python scripts/google_auth.py")
    creds = Credentials.from_authorized_user_file(str(TOKEN), SCOPES)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        TOKEN.write_text(creds.to_json())
    return creds

def _service():
    return build("drive", "v3", credentials=_load_creds())

def create_folder(name: str, parent_id: Optional[str] = None, anyone_with_link: bool = True) -> Tuple[str, str]:
    """Create a Drive folder, optionally under parent_id, return (folder_id, webViewLink).
       If anyone_with_link=True, set 'reader' permission for anyone and return a shareable link.
    """
    parent = parent_id or ROOT_PARENT_ID
    body = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent:
        body["parents"] = [parent]

    try:
        svc = _service()
        folder = svc.files().create(body=body, fields="id").execute()
        folder_id = folder["id"]

        if anyone_with_link:
            perm_body = {"role": "reader", "type": "anyone"}  # anyone can view with link
            svc.permissions().create(fileId=folder_id, body=perm_body).execute()

        meta = svc.files().get(fileId=folder_id, fields="id, webViewLink").execute()
        link = meta.get("webViewLink", f"https://drive.google.com/drive/folders/{folder_id}")
        return folder_id, link

    except HttpError as e:
        logger.exception(f"Drive API error: {e}")
        raise