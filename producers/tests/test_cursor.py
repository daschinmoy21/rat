from rat_producers.cursor import Cursor


def test_fresh_db_returns_none(tmp_path):
    assert Cursor(tmp_path / "cursors.db").get("hn") is None


def test_put_then_get_round_trips(tmp_path):
    c = Cursor(tmp_path / "cursors.db")
    c.put("hn", 1725200000000)
    assert c.get("hn") == 1725200000000


def test_put_never_moves_backwards(tmp_path):
    c = Cursor(tmp_path / "cursors.db")
    c.put("hn", 2)
    c.put("hn", 1)  # out-of-order feed items must not lower the high-water mark
    assert c.get("hn") == 2


def test_nested_missing_parent_directory_created(tmp_path):
    deep_path = tmp_path / "deep" / "nested" / "dir" / "cursors.db"
    assert not deep_path.parent.exists()
    c = Cursor(deep_path)
    c.put("hn", 100)
    assert c.get("hn") == 100

