"""Sillo sync relay -- a dumb token-keyed mailbox between the desktop app and the phone PWA.

Pass-through only: no accounts, no token generation, nothing durable by design.
data.json beside this file is best-effort persistence of the queues only (the
disk on Render's free tier is ephemeral -- treat it as a bonus). Photos and
pairing codes live purely in memory.

Never logs tokens or content: only method + path + status.
"""

import json
import logging
import os
import threading
import time

from flask import Flask, Response, jsonify, request

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(APP_DIR, "data.json")

ALLOWED_ORIGIN = "https://paleto.gg"
DIRECTIONS = ("phone", "desktop")
MAX_QUEUE = 300                       # items per token+direction (drop oldest)
MAX_PHOTO_BYTES = 10 * 1024 * 1024    # 10 MB per photo
MAX_PENDING_PHOTOS = 5                # pending photos per token
CODE_TTL = 600                        # pairing code lifetime, seconds

MAX_JSON_BYTES = 256 * 1024           # a push body is tiny; anything else is junk
MAX_TOKENS = 500                      # bound the mailbox count (junk tokens)
PULL_TTL = 7 * 24 * 3600              # forget a token nobody has pulled in a week

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_PHOTO_BYTES + 64 * 1024

# Werkzeug's default request log includes the query string (which carries
# tokens) -- silence it and log method+path+status ourselves.
logging.getLogger("werkzeug").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("sillo-sync")

_lock = threading.Lock()
_queues = {}   # token -> {"phone": [item, ...], "desktop": [item, ...]}
_photos = {}   # (token, photo_id) -> bytes
_codes = {}    # code -> (token, expiry_epoch)
_pulls = {}    # (token, direction) -> last pull epoch (in-memory liveness only)


# ---------------------------------------------------------------- persistence

def _save():
    """Best-effort dump of the queues (never photos/codes). Call with _lock held."""
    try:
        tmp = DATA_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_queues, f)
        os.replace(tmp, DATA_PATH)
    except Exception:
        pass  # ephemeral disk -- persistence is a bonus, never an error


def _load():
    try:
        with open(DATA_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return
        for token, entry in raw.items():
            if not isinstance(token, str) or not isinstance(entry, dict):
                continue
            clean = {"phone": [], "desktop": []}
            for side in DIRECTIONS:
                items = entry.get(side)
                if isinstance(items, list):
                    clean[side] = [
                        it for it in items
                        if isinstance(it, dict)
                        and isinstance(it.get("id"), str)
                        and isinstance(it.get("kind"), str)
                    ][:MAX_QUEUE]
            _queues[token] = clean
    except Exception:
        pass


_load()


# ------------------------------------------------------------------- helpers

def _bad():
    return jsonify({"err": "bad"}), 400


def _gone():
    return jsonify({"err": "no"}), 404


def _big():
    return jsonify({"err": "big"}), 413


def _busy():
    """The mailbox is full RIGHT NOW — unlike 'big', this is temporary and the
    sender must keep the photo and retry (429, never 413)."""
    return jsonify({"err": "busy"}), 429


def _is_str(v, maxlen=200):
    return isinstance(v, str) and 0 < len(v) <= maxlen


def _evict_stale():
    """Bound the mailbox: forget tokens nobody has pulled in a week, and if
    we're still over the cap drop the quietest ones. Call with _lock held."""
    now = time.time()
    if len(_queues) <= MAX_TOKENS:
        # only tokens that HAVE been pulled and then went quiet for a week.
        # (Never-pulled tokens are brand-new pairings whose phone hasn't
        # polled yet — evicting those would delete a mailbox seconds after
        # the desktop filled it.)
        dead = [t for t in _queues
                if max(_pulls.get((t, "phone"), 0),
                       _pulls.get((t, "desktop"), 0)) > 0
                and now - max(_pulls.get((t, "phone"), 0),
                              _pulls.get((t, "desktop"), 0)) > PULL_TTL]
        for t in dead[:50]:
            _queues.pop(t, None)
        return
    ranked = sorted(_queues, key=lambda t: max(_pulls.get((t, "phone"), 0),
                                               _pulls.get((t, "desktop"), 0)))
    for t in ranked[:len(_queues) - MAX_TOKENS]:
        _queues.pop(t, None)
        for k in [k for k in _photos if k[0] == t]:
            del _photos[k]
        for d in DIRECTIONS:
            _pulls.pop((t, d), None)


def _purge_codes(now):
    """Drop expired pairing codes. Call with _lock held."""
    for code in [c for c, (_t, exp) in _codes.items() if exp <= now]:
        del _codes[code]


def _drain_body():
    """Read and discard the request body so the client sees our response."""
    try:
        while request.stream.read(1 << 20):
            pass
    except Exception:
        pass


# --------------------------------------------------------- CORS + access log

@app.before_request
def _preflight():
    if request.method == "OPTIONS" and request.path.startswith("/api/"):
        return ("", 204)
    return None


@app.after_request
def _finish(resp):
    if request.path.startswith("/api/"):
        resp.headers["Access-Control-Allow-Origin"] = ALLOWED_ORIGIN
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Max-Age"] = "86400"
    # NOTE: request.path only -- never the query string (it carries tokens).
    log.info("%s %s %s", request.method, request.path, resp.status_code)
    return resp


@app.errorhandler(400)
def _h400(_e):
    return _bad()


@app.errorhandler(404)
def _h404(_e):
    return _gone()


@app.errorhandler(405)
def _h405(_e):
    return _gone()


@app.errorhandler(413)
def _h413(_e):
    return _big()


# -------------------------------------------------------------------- routes

@app.get("/")
def health():
    return Response("sillo sync ☁️", mimetype="text/plain; charset=utf-8")


@app.get("/api/ping")
def ping():
    return jsonify({"ok": True, "v": "1"})


@app.post("/api/pair/offer")
def pair_offer():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return _bad()
    token, code = body.get("token"), body.get("code")
    if not _is_str(token) or not _is_str(code, 32):
        return _bad()
    now = time.time()
    with _lock:
        _purge_codes(now)
        _codes[code] = (token, now + CODE_TTL)
    return jsonify({"ok": True})


@app.get("/api/pair/claim")
def pair_claim():
    code = request.args.get("code", "")
    if not _is_str(code, 32):
        return _bad()
    with _lock:
        _purge_codes(time.time())
        entry = _codes.pop(code, None)  # one-shot: claiming deletes it
    if entry is None:
        return _gone()
    return jsonify({"token": entry[0]})


@app.post("/api/push")
def push():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return _bad()
    token, to, items = body.get("token"), body.get("to"), body.get("items")
    if not _is_str(token) or to not in DIRECTIONS or not isinstance(items, list):
        return _bad()
    for it in items:
        if (
            not isinstance(it, dict)
            or not _is_str(it.get("id"))
            or not _is_str(it.get("kind"))
            or not isinstance(it.get("data", {}), dict)
        ):
            return _bad()
    with _lock:
        _evict_stale()
        q = _queues.setdefault(token, {"phone": [], "desktop": []})[to]
        for it in items:
            item = {"id": it["id"], "kind": it["kind"], "data": it.get("data", {})}
            for i, old in enumerate(q):
                if old["id"] == item["id"]:
                    # a REPLACED item is a new delivery: give it a fresh
                    # version so an ack for the copy already in flight can
                    # never delete the one that just arrived
                    item["v"] = int(old.get("v", 0)) + 1
                    q[i] = item
                    break
            else:
                item["v"] = 0
                q.append(item)
        if len(q) > MAX_QUEUE:
            del q[: len(q) - MAX_QUEUE]  # drop oldest
        n = len(q)
        _save()
    return jsonify({"ok": True, "n": n})


@app.get("/api/pull")
def pull():
    token = request.args.get("token", "")
    to = request.args.get("to", "")
    if not _is_str(token) or to not in DIRECTIONS:
        return _bad()
    acks = set(a for a in request.args.get("ack", "").split(",") if a)
    with _lock:
        _pulls[(token, to)] = time.time()  # liveness: "the phone pulled just now"
        entry = _queues.get(token)
        if entry is None:
            return jsonify({"items": []})  # unknown token = empty, NOT an error
        if acks:
            # an ack may name a version ("id@3"); a bare id still works, but
            # only clears an item that has NOT been re-pushed since
            acked = {}
            for a in acks:
                aid, _, av = a.partition("@")
                acked[aid] = int(av) if av.isdigit() else None
            kept = []
            for it in entry[to]:
                want = acked.get(it["id"], "miss")
                if want == "miss":
                    kept.append(it)
                elif want is not None and want != int(it.get("v", 0)):
                    kept.append(it)          # a newer delivery — keep it
            if len(kept) != len(entry[to]):
                entry[to] = kept  # acks deleted FIRST ...
                _save()
        items = list(entry[to])  # ... then the current queue returns
    return jsonify({"items": items})


@app.get("/api/seen")
def seen():
    """When did each side last pull? Powers the desktop's 🟢/🟡/⚪ status dot.
    In-memory only — after a relay nap it just reads quiet until the next
    poll, which is the honest answer anyway."""
    token = request.args.get("token", "")
    if not _is_str(token):
        return _bad()
    with _lock:
        return jsonify({
            "phone": _pulls.get((token, "phone")),
            "desktop": _pulls.get((token, "desktop")),
        })


@app.route("/api/photo", methods=["GET", "POST"])
def photo():
    token = request.args.get("token", "")
    pid = request.args.get("id", "")
    if not _is_str(token) or not _is_str(pid):
        _drain_body()
        return _bad()

    if request.method == "GET":
        with _lock:
            data = _photos.get((token, pid))
        if data is None:
            return _gone()
        return Response(data, mimetype="image/jpeg")

    # POST: raw body bytes
    if request.content_length is not None and request.content_length > MAX_PHOTO_BYTES:
        _drain_body()
        return _big()
    data = request.get_data(cache=False)
    if len(data) > MAX_PHOTO_BYTES:
        return _big()
    with _lock:
        if (token, pid) not in _photos:
            pending = sum(1 for (t, _p) in _photos if t == token)
            if pending >= MAX_PENDING_PHOTOS:
                return _busy()      # transient: the desktop drains these
        _photos[(token, pid)] = data
    return jsonify({"ok": True})


@app.post("/api/photo/del")
def photo_del():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return _bad()
    token, pid = body.get("token"), body.get("id")
    if not _is_str(token) or not _is_str(pid):
        return _bad()
    with _lock:
        _photos.pop((token, pid), None)  # idempotent
    return jsonify({"ok": True})


@app.post("/api/clear")
def clear():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return _bad()
    token, to = body.get("token"), body.get("to")
    if not _is_str(token) or to not in DIRECTIONS:
        return _bad()
    with _lock:
        entry = _queues.get(token)
        if entry is not None:
            entry[to] = []
        if to == "desktop":
            # photos are payloads of phone->desktop items only: clearing the
            # PHONE queue must not throw away a photo the desktop hasn't
            # collected yet (that's someone's quest picture)
            for key in [k for k in _photos if k[0] == token]:
                del _photos[key]
        _save()
    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8765"))
    app.run(host="127.0.0.1", port=port, threaded=True, use_reloader=False)
