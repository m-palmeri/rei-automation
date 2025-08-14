from fastapi.testclient import TestClient
from app import main as app_main

def test_poll_enqueue(monkeypatch):
    calls = []
    # stub queue.enqueue
    monkeypatch.setattr(app_main.q, "enqueue", lambda *a, **k: calls.append((a, k)))
    # stub Notion query to return two fake pages
    def fake_query_db_since(_):
        pages = [
            {"id":"p1","last_edited_time":"2025-08-12T12:00:00.000Z","properties":{"Name":{"type":"title","title":[{"plain_text":"A"}]}}},
            {"id":"p2","last_edited_time":"2025-08-12T12:01:00.000Z","properties":{"Name":{"type":"title","title":[{"plain_text":"B"}]}}},
        ]
        return pages, "2025-08-12T12:01:00.000Z"
    monkeypatch.setenv("NOTION_DB_ID","dummy")
    import app.notion_client as nc
    monkeypatch.setattr(nc, "query_db_since", fake_query_db_since)
    c = TestClient(app_main.app)
    r = c.post("/poll?debug=true&reset=true")
    assert r.status_code == 200
    data = r.json()
    assert data["enqueued"] == 2
    assert len(calls) == 2