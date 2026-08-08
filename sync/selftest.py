"""Self-test for the Sillo sync relay.

Starts app.py on 127.0.0.1:8765 in a thread and exercises every endpoint,
including the caps. Prints "N/N passed"; exit code 0 if all passed, 1 otherwise.
"""

import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import app as relay  # noqa: E402

BASE = "http://127.0.0.1:8765"

# Keep the repo clean + start deterministic: park data.json in temp, empty state.
relay.DATA_PATH = os.path.join(tempfile.gettempdir(), "sillo_selftest_data.json")
try:
    os.remove(relay.DATA_PATH)
except OSError:
    pass
relay._queues.clear()
relay._photos.clear()
relay._codes.clear()

_passed = 0
_failed = 0


def check(name, cond, detail=""):
    global _passed, _failed
    if cond:
        _passed += 1
        print("PASS  " + name)
    else:
        _failed += 1
        print("FAIL  " + name + ("  -- " + str(detail) if detail else ""))


def req(method, path, body=None, raw=None, timeout=30):
    """Returns (status, bytes, headers-dict)."""
    headers = {}
    data = None
    if raw is not None:
        data = raw
        headers["Content-Type"] = "image/jpeg"
    elif body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    r = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        b = e.read()
        h = dict(e.headers)
        e.close()
        return e.code, b, h


def jbody(b):
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return None


def main():
    server = threading.Thread(
        target=relay.app.run,
        kwargs={"host": "127.0.0.1", "port": 8765, "threaded": True, "use_reloader": False},
        daemon=True,
    )
    server.start()
    for _ in range(100):
        try:
            req("GET", "/api/ping", timeout=2)
            break
        except Exception:
            time.sleep(0.1)
    else:
        print("server never came up")
        print("0/1 passed")
        sys.exit(1)

    T = "tok_selftest_A0b1C2d3E4f5G6"   # ~22 chars, desktop-style
    T2 = "tok_selftest_capqueue22"
    TX = "tok_never_seen_before_x"

    # --- ping + health + CORS -------------------------------------------------
    st, b, h = req("GET", "/api/ping")
    check("ping 200", st == 200, st)
    check("ping body", jbody(b) == {"ok": True, "v": "1"}, b)
    check("ping CORS origin", h.get("Access-Control-Allow-Origin") == "https://paleto.gg", h)

    st, b, h = req("OPTIONS", "/api/push")
    check("preflight 2xx", st in (200, 204), st)
    check("preflight CORS methods", "POST" in h.get("Access-Control-Allow-Methods", ""), h)
    check("preflight CORS headers", "Content-Type" in h.get("Access-Control-Allow-Headers", ""), h)

    st, b, _ = req("GET", "/")
    check("health page", st == 200 and "sillo sync" in b.decode("utf-8"), (st, b[:40]))

    st, _, _ = req("GET", "/api/nope")
    check("unknown route 404", st == 404, st)

    # --- pairing --------------------------------------------------------------
    st, b, _ = req("POST", "/api/pair/offer", body={"token": T, "code": "ABC234"})
    check("pair offer ok", st == 200 and jbody(b) == {"ok": True}, (st, b))

    st, b, _ = req("GET", "/api/pair/claim?code=ABC234")
    check("pair claim returns token", st == 200 and jbody(b) == {"token": T}, (st, b))

    st, b, _ = req("GET", "/api/pair/claim?code=ABC234")
    check("claim is one-shot", st == 404 and jbody(b) == {"err": "no"}, (st, b))

    st, _, _ = req("GET", "/api/pair/claim?code=ZZZZ99")
    check("claim unknown code 404", st == 404, st)

    st, _, _ = req("POST", "/api/pair/offer", body={"token": T})
    check("offer missing code 400", st == 400, st)

    # --- push / pull ----------------------------------------------------------
    items = [
        {"id": "q1", "kind": "quest", "data": {"qid": "q1", "title": "walk", "mission": "5 min", "emoji": "🚶"}},
        {"id": "e1", "kind": "exercise", "data": {"name": "heel slides", "reps": 10, "sets": 2, "hold_sec": 0, "note": "", "how": "slide"}},
    ]
    st, b, _ = req("POST", "/api/push", body={"token": T, "to": "phone", "items": items})
    check("push 2 items", st == 200 and jbody(b) == {"ok": True, "n": 2}, (st, b))

    st, b, _ = req("POST", "/api/push", body={"token": T, "to": "phone",
                                              "items": [{"id": "q1", "kind": "quest", "data": {"qid": "q1", "title": "run", "mission": "2 min", "emoji": "🏃"}}]})
    check("dedupe by id keeps n=2", st == 200 and jbody(b)["n"] == 2, (st, b))

    st, b, _ = req("GET", "/api/pull?token=" + T + "&to=phone")
    got = jbody(b)
    check("pull returns 2", st == 200 and len(got["items"]) == 2, (st, b))
    q1 = [it for it in got["items"] if it["id"] == "q1"]
    check("dedupe replaced data", q1 and q1[0]["data"]["title"] == "run", got)

    st, b, _ = req("GET", "/api/pull?token=" + TX + "&to=phone")
    check("unknown token = empty queue", st == 200 and jbody(b) == {"items": []}, (st, b))

    st, _, _ = req("GET", "/api/pull?token=" + T + "&to=fridge")
    check("bad direction 400", st == 400, st)

    st, _, _ = req("POST", "/api/push", body={"token": T, "items": items})
    check("push missing 'to' 400", st == 400, st)

    st, b, _ = req("GET", "/api/pull?token=" + T + "&to=phone&ack=q1")
    got = jbody(b)
    check("ack deletes first, then returns", st == 200 and [it["id"] for it in got["items"]] == ["e1"], (st, b))

    st, b, _ = req("GET", "/api/pull?token=" + T + "&to=phone")
    check("ack was persisted", jbody(b)["items"][0]["id"] == "e1" and len(jbody(b)["items"]) == 1, b)

    st, b, _ = req("GET", "/api/pull?token=" + T + "&to=phone&ack=q1,ghost")
    check("re-ack is idempotent", st == 200 and len(jbody(b)["items"]) == 1, (st, b))

    check("data.json written", os.path.exists(relay.DATA_PATH))

    # --- queue cap 300, drop oldest ------------------------------------------
    big = [{"id": "i%03d" % i, "kind": "exlog", "data": {"name": "x", "t": i, "action": "done"}} for i in range(305)]
    st, b, _ = req("POST", "/api/push", body={"token": T2, "to": "desktop", "items": big})
    check("cap: n=300 after 305", st == 200 and jbody(b)["n"] == 300, (st, b))
    st, b, _ = req("GET", "/api/pull?token=" + T2 + "&to=desktop")
    ids = [it["id"] for it in jbody(b)["items"]]
    check("cap: oldest dropped", len(ids) == 300 and "i004" not in ids and ids[0] == "i005" and ids[-1] == "i304", (len(ids), ids[:2], ids[-1:]))

    # --- photos ---------------------------------------------------------------
    jpg = b"\xff\xd8\xff\xe0" + b"sillo" * 100
    st, b, _ = req("POST", "/api/photo?token=" + T + "&id=p1", raw=jpg)
    check("photo post", st == 200 and jbody(b) == {"ok": True}, (st, b))

    st, b, h = req("GET", "/api/photo?token=" + T + "&id=p1")
    check("photo get bytes", st == 200 and b == jpg, (st, len(b)))
    check("photo get content-type", h.get("Content-Type", "").startswith("image/jpeg"), h)

    st, _, _ = req("GET", "/api/photo?token=" + T + "&id=nope")
    check("photo unknown 404", st == 404, st)

    st, _, _ = req("POST", "/api/photo?token=" + T + "&id=p1", raw=jpg + b"v2")
    check("photo replace same id ok", st == 200, st)

    for pid in ("p2", "p3", "p4", "p5"):
        req("POST", "/api/photo?token=" + T + "&id=" + pid, raw=jpg)
    st, b, _ = req("POST", "/api/photo?token=" + T + "&id=p6", raw=jpg)
    check("6th pending photo 413", st == 413, (st, b))

    st, _, _ = req("POST", "/api/photo/del", body={"token": T, "id": "p5"})
    check("photo del ok", st == 200, st)
    st, _, _ = req("POST", "/api/photo?token=" + T + "&id=p6", raw=jpg)
    check("slot freed after del", st == 200, st)

    oversize = b"\x00" * (10 * 1024 * 1024 + 1)
    st, b, _ = req("POST", "/api/photo?token=" + T + "&id=pbig", raw=oversize, timeout=60)
    check("oversize photo 413", st == 413, (st, b))

    # --- clear ----------------------------------------------------------------
    req("POST", "/api/push", body={"token": T, "to": "desktop",
                                   "items": [{"id": "d1", "kind": "hello", "data": {"t": 1, "ua": "test"}}]})
    st, b, _ = req("POST", "/api/clear", body={"token": T, "to": "phone"})
    check("clear ok", st == 200 and jbody(b) == {"ok": True}, (st, b))
    st, b, _ = req("GET", "/api/pull?token=" + T + "&to=phone")
    check("clear emptied phone queue", jbody(b) == {"items": []}, b)
    st, _, _ = req("GET", "/api/photo?token=" + T + "&id=p1")
    check("clear dropped photos", st == 404, st)
    st, b, _ = req("GET", "/api/pull?token=" + T + "&to=desktop")
    check("clear left desktop queue", [it["id"] for it in jbody(b)["items"]] == ["d1"], b)

    total = _passed + _failed
    print("%d/%d passed" % (_passed, total))
    try:
        os.remove(relay.DATA_PATH)
    except OSError:
        pass
    sys.exit(0 if _failed == 0 else 1)


if __name__ == "__main__":
    main()
