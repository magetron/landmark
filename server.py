"""Landmark LAN server.

Run it on one computer:    python server.py
Everyone on the same network then opens the address it prints.

It does two things: serves the game's files, and runs lobbies. A player creates a lobby and
becomes its host; others join it; the host picks the map and settings and presses Start.
During a match the server relays player actions: ten times a second it sends every device in
the match the same list of actions for the same tick. Each device runs the whole game itself,
so the server never needs to know the rules.

A device is known by a key it keeps for as long as its browser tab lives. If its connection
drops (phone asleep, browser in the background, page reloaded) and it comes back, it keeps its
place in the lobby, and if a match is running it is sent the start and every action so far,
so it replays up to now.

Standard library only. Messages to the browser travel as Server-Sent Events; messages from
the browser are small POSTs.
"""
import json
import os
import queue
import random
import socket
import sys
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ROOT = os.path.dirname(os.path.abspath(__file__))
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
TICK = 0.1                 # seconds per turn
MAX_PLAYERS = 6            # people in one lobby
AWAY_LOBBY = 30            # seconds offline before someone is dropped from a lobby with no match running
AWAY_MATCH = 300           # ... and when they hold a seat in a running match
HOST_AWAY = 20             # seconds offline before the host role passes on
ROOM_IDLE = 120            # seconds a lobby with nobody online is kept
VERSION = 3                # the page checks this, so an old server left running is noticed
GONE = object()            # put on a queue to tell its connection to stop
PERF_LOG = os.path.join(ROOT, "perf.log")     # what each device measured during matches, one line per ten seconds
perf_lock = threading.Lock()
LETTERS = "ABCDEFGHJKLMNPRSTUVWXYZ"

lock = threading.Lock()
clients = {}               # id -> {"name", "q" (None while offline), "room" (code or None), "off" (time it went offline)}
keys = {}                  # device key -> id
rooms = {}                 # code -> {"host", "members": [id], "cfg": {map, diff, bots}, "game": None or dict, "idle": time or None}
next_id = 1


def clean(name):
    return "".join(ch for ch in str(name) if ch not in "<>&\"'").strip()[:14]


def online(cid):
    c = clients.get(cid)
    return bool(c and c["q"] is not None)


def send(cid, msg):
    c = clients.get(cid)
    if c and c["q"] is not None:
        c["q"].put(msg if isinstance(msg, str) else json.dumps(msg, separators=(",", ":")))


def push_state():
    """Tell every connected device what lobbies exist and what its own lobby looks like."""
    listing = [{"code": code, "host": clients[r["host"]]["name"] if r["host"] in clients else "",
                "n": len(r["members"]), "running": r["game"] is not None}
               for code, r in rooms.items()]
    for cid, c in clients.items():
        if c["q"] is None:
            continue
        r = rooms.get(c["room"])
        mine = None
        if r:
            g = r["game"]
            mine = {"code": c["room"], "host": r["host"], "cfg": r["cfg"], "running": g is not None,
                    "playing": bool(g and cid in g["slots"]),
                    "players": [{"id": m, "name": clients[m]["name"], "on": online(m)} for m in r["members"]]}
        send(cid, {"t": "state", "rooms": listing, "room": mine})


def leave_room(cid):
    c = clients.get(cid)
    r = rooms.get(c["room"]) if c else None
    if not r:
        return
    code = c["room"]
    c["room"] = None
    if cid in r["members"]:
        r["members"].remove(cid)
    if not r["members"]:
        del rooms[code]
    elif r["host"] == cid:
        r["host"] = next((m for m in r["members"] if online(m)), r["members"][0])


def turn_loop():
    nxt = time.monotonic()
    while True:
        nxt += TICK
        time.sleep(max(0.0, nxt - time.monotonic()))
        with lock:
            for r in rooms.values():
                g = r["game"]
                if g is None:
                    continue
                g["turn"] += 1
                acts, g["pending"] = g["pending"], []
                if acts:
                    g["acts"][g["turn"]] = acts
                data = json.dumps({"t": "turn", "n": g["turn"], "in": acts}, separators=(",", ":"))
                for cid in g["slots"]:
                    send(cid, data)


def housekeeping():
    """Drop people who have been gone too long, pass on the host role, remove abandoned lobbies."""
    while True:
        time.sleep(3)
        now = time.monotonic()
        with lock:
            changed = False
            for code in list(rooms):
                r = rooms[code]
                for m in list(r["members"]):
                    c = clients[m]
                    if c["q"] is None:
                        seated = bool(r["game"] and m in r["game"]["slots"])
                        if now - c["off"] > (AWAY_MATCH if seated else AWAY_LOBBY):
                            leave_room(m)
                            changed = True
                if code not in rooms:
                    continue
                h = clients.get(r["host"])
                if h and h["q"] is None and now - h["off"] > HOST_AWAY:
                    nxt = next((m for m in r["members"] if online(m)), None)
                    if nxt is not None:
                        r["host"] = nxt
                        changed = True
                if any(online(m) for m in r["members"]):
                    r["idle"] = None
                elif r["idle"] is None:
                    r["idle"] = now
                elif now - r["idle"] > ROOM_IDLE:
                    for m in list(r["members"]):
                        clients[m]["room"] = None
                    del rooms[code]
                    changed = True
            for cid in [c for c, v in clients.items() if v["q"] is None and v["room"] is None and now - v["off"] > 600]:
                del clients[cid]
                for k in [k for k, v in keys.items() if v == cid]:
                    del keys[k]
            if changed:
                push_state()


class Handler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True      # send each small message at once; batching them made turns arrive in clumps

    def __init__(self, *a, **k):
        super().__init__(*a, directory=ROOT, **k)

    def log_message(self, *a):
        pass

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def reply(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path.endswith("/mp/ping"):
            return self.reply({"landmark": True, "v": VERSION})
        if url.path.endswith("/mp/events"):
            qs = parse_qs(url.query)
            return self.events(qs.get("name", [""])[0], qs.get("key", [""])[0][:40])
        if url.path.endswith((".py", ".log")):
            return self.send_error(404)
        return super().do_GET()

    def events(self, name, key):
        global next_id
        q = queue.Queue()
        with lock:
            cid = keys.get(key) if key else None
            if cid is None or cid not in clients:
                cid = next_id
                next_id += 1
                if key:
                    keys[key] = cid
                clients[cid] = {"name": "", "q": None, "room": None, "off": 0}
            c = clients[cid]
            if c["q"] is not None:                # the same device connecting again: retire its old connection
                c["q"].put(GONE)
            c["q"] = q
            if clean(name):
                c["name"] = clean(name)
            send(cid, {"t": "hello", "id": cid, "lan": "%s:%d" % (LAN, PORT)})
            r = rooms.get(c["room"])
            if r and r["game"] and cid in r["game"]["slots"]:
                # back in a running match: the start, then everything that has happened since
                g = r["game"]
                send(cid, g["start"])
                send(cid, {"t": "catchup", "n": g["turn"], "acts": g["acts"]})
            push_state()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        try:
            self.wfile.write(b"retry: 1000\n\n")
            while True:
                try:
                    data = q.get(timeout=2)
                except queue.Empty:
                    data = '{"t":"k"}'            # a heartbeat, so both ends notice a dead line within seconds
                if data is GONE:
                    break
                self.wfile.write(("data: " + data + "\n\n").encode())
                self.wfile.flush()
        except OSError:
            pass
        finally:
            with lock:
                c = clients.get(cid)
                if c and c["q"] is q:             # not replaced by a newer connection of the same device
                    c["q"] = None
                    c["off"] = time.monotonic()
                    push_state()
            self.close_connection = True

    def do_POST(self):
        url = urlparse(self.path)
        what = url.path.rsplit("/", 1)[-1]
        try:
            n = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(min(n, 4096)) or b"{}")
            cid = int(body.get("id", 0))
        except (ValueError, TypeError):
            return self.reply({"ok": False}, 400)
        if what == "perf":
            p = body.get("p")
            if isinstance(p, dict):
                with lock:
                    name = clients[cid]["name"] if cid in clients else ""
                line = json.dumps({"at": time.strftime("%Y-%m-%d %H:%M:%S"), "from": self.client_address[0], "name": name, **p})
                with perf_lock:
                    with open(PERF_LOG, "a", encoding="utf-8") as f:
                        f.write(line + "\n")
            return self.reply({"ok": True})
        with lock:
            c = clients.get(cid)
            if c is None:
                return self.reply({"ok": False}, 403)
            r = rooms.get(c["room"])
            host = bool(r and r["host"] == cid)
            if what == "act":
                it = body.get("it")
                g = r["game"] if r else None
                if g and isinstance(it, dict) and cid in g["slots"]:
                    if it.get("k") == "hash":
                        self.check_hash(g, cid, it)
                    else:
                        g["pending"].append([g["slots"][cid], it])
                return self.reply({"ok": True})
            if what == "name":
                c["name"] = clean(body.get("name", ""))
            elif what == "create":
                leave_room(cid)
                code = next(k for k in iter(lambda: "".join(random.choice(LETTERS) for _ in range(4)), None) if k not in rooms)
                rooms[code] = {"host": cid, "members": [cid], "game": None, "idle": None,
                               "cfg": self.cfg(body, {"map": 0, "diff": "easy", "bots": 6})}
                c["room"] = code
            elif what == "join":
                code = str(body.get("room", "")).upper()
                target = rooms.get(code)
                if target is None or (cid not in target["members"] and len(target["members"]) >= MAX_PLAYERS):
                    return self.reply({"ok": False}, 409)
                if c["room"] != code:
                    leave_room(cid)
                    target["members"].append(cid)
                    c["room"] = code
            elif what == "leave":
                leave_room(cid)
            elif what == "cfg":
                if host:
                    r["cfg"] = self.cfg(body, r["cfg"])
            elif what == "start":
                if host:
                    seats = [m for m in r["members"] if online(m)][:MAX_PLAYERS]
                    start = {"t": "start", "seed": random.getrandbits(31), **r["cfg"],
                             "players": [{"id": m, "name": clients[m]["name"]} for m in seats]}
                    r["game"] = {"turn": 0, "pending": [], "hashes": {}, "acts": {}, "start": start,
                                 "slots": {m: i + 1 for i, m in enumerate(seats)}}
                    for m in seats:
                        send(m, start)
            elif what == "stop":
                if host and r["game"]:
                    for m in r["members"]:
                        send(m, {"t": "stop"})
                    r["game"] = None
            else:
                return self.reply({"ok": False}, 404)
            push_state()
        self.reply({"ok": True})

    @staticmethod
    def cfg(body, old):
        out = dict(old)
        try:
            if "map" in body:
                out["map"] = max(0, min(63, int(body["map"])))
            if "bots" in body:
                out["bots"] = max(0, min(12, int(body["bots"])))
            if body.get("diff") in ("easy", "normal", "hard"):
                out["diff"] = body["diff"]
        except (ValueError, TypeError):
            pass
        return out

    @staticmethod
    def check_hash(g, cid, it):
        """Every 100 ticks each device reports a checksum of the game; they must all agree."""
        n = it.get("n")
        if not isinstance(n, int) or n < g["turn"] - 300:         # a device replaying old ticks: nothing to compare with
            return
        seen = g["hashes"].setdefault(n, {})
        seen[cid] = it.get("h")
        playing = [m for m in g["slots"] if online(m)]
        if all(m in seen for m in playing):
            if len({seen[m] for m in playing}) > 1:
                print("out of step at tick", n, seen, flush=True)
                for m in g["slots"]:
                    send(m, {"t": "desync", "n": n})
            del g["hashes"][n]


def lan_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


LAN = lan_address()


class Server(ThreadingHTTPServer):
    """One server per port. Windows would otherwise let a second copy listen on the same port,
    and requests would then land on either copy at random, each with its own lobbies."""
    allow_reuse_address = False
    daemon_threads = True

    def server_bind(self):
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


if __name__ == "__main__":
    try:
        httpd = Server(("0.0.0.0", PORT), Handler)
    except OSError:
        print("Port %d is already in use: another copy of this server is still running." % PORT)
        print("Close that one first (its window, or Task Manager > python.exe), then run this again.")
        sys.exit(1)
    threading.Thread(target=turn_loop, daemon=True).start()
    threading.Thread(target=housekeeping, daemon=True).start()
    print("Landmark is up. On this computer:  http://localhost:%d" % PORT, flush=True)
    print("On phones on the same network:  http://%s:%d" % (LAN, PORT), flush=True)
    print("Or one link that jumps there:   https://patrickwu.uk/landmark/?join=%s:%d" % (LAN, PORT), flush=True)
    print("One person creates a lobby, the others join it, the host presses Start. Ctrl+C stops the server.", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
