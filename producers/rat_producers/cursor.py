import sqlite3
from pathlib import Path


class Cursor:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(self.path)
        con.execute(
            "CREATE TABLE IF NOT EXISTS cursors (name TEXT PRIMARY KEY, value INTEGER)")
        con.commit()
        con.close()

    def get(self, name) -> int | None:
        con = sqlite3.connect(self.path)
        row = con.execute(
            "SELECT value FROM cursors WHERE name = ?", (name,)).fetchone()
        con.close()
        return row[0] if row else None

    def put(self, name, value: int):
        con = sqlite3.connect(self.path)
        con.execute(
            "INSERT INTO cursors (name, value) VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value = max(value, excluded.value)",
            (name, value))
        con.commit()
        con.close()
