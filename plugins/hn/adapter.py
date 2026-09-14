import json
import urllib.request

from rat_producers.extract import extract_entities


def register(app):
    app.add_source("hn", poll=poll)


def _item(item_id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def poll(since_ms=None):
    with urllib.request.urlopen(
        "https://hacker-news.firebaseio.com/v0/topstories.json"
    ) as r:
        ids = json.load(r)[:30]
    envelopes = []
    for i in ids:
        item = _item(i)
        if not item or item.get("type") != "story":
            continue
        ts_ms = item["time"] * 1000
        if since_ms is not None and ts_ms <= since_ms:
            continue
        title, url = item.get("title"), item.get("url")
        envelopes.append({
            "event_id": f"hn:{item['id']}",
            "source": "hn",
            "entities": extract_entities(" ".join(filter(None, [title, url]))),
            "ts_ms": ts_ms,
            "payload": {"title": title, "url": url, "score": item.get("score")},
        })
    return envelopes
