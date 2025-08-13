import pathlib, json
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]  # full drive; simplest for dev
CLIENT = pathlib.Path(".secrets/google_drive_secret.json")
TOKEN  = pathlib.Path(".secrets/google_token.json")

def main():
    if not CLIENT.exists():
        raise SystemExit("Missing .secrets/google_drive_secret.json")
    TOKEN.parent.mkdir(parents=True, exist_ok=True)
    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT), SCOPES)
    # opens a browser, runs a tiny local server for the callback
    creds = flow.run_local_server(port=8765, prompt="consent")
    TOKEN.write_text(creds.to_json())
    print(f"Wrote token: {TOKEN}")

if __name__ == "__main__":
    main()