#!/usr/bin/env python3
"""
Flash Sale Stock Service (Redis, Option A) - CLI Service (No API)
================================================================
High-level / advanced Redis project service for NoSQL module.

Key ideas to impress:
- Option A stock model: stock_total never changes during reservation
  available = stock_total - stock_reserved
- Atomic reservation with Lua (prevents oversell under concurrency)
- TTL-based reservation keys (auto-expire)
- Idempotent payment (SET NX) to prevent double processing
- Audit trail + metrics counters

Usage examples:
--------------
# init stock
python app.py init-stock --product 101 --total 50

# status
python app.py status --product 101

# reserve
python app.py reserve --product 101 --user user42 --qty 2 --ttl 300

# confirm payment (idempotent)
python app.py confirm --order order789 --product 101 --user user42 --qty 2

# cancel (user cancels before ttl)
python app.py cancel --product 101 --user user42

# show metrics
python app.py metrics

# show audit logs
python app.py audit --n 10

# interactive shell
python app.py shell
"""
from __future__ import annotations

from config import load_settings
settings = load_settings()
print(settings.safe_summary())


import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional, Tuple, List

import redis


# ----------------------------
# Config
# ----------------------------
@dataclass(frozen=True)
class Config:
    host: str = os.getenv("REDIS_HOST", "127.0.0.1")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))
    username: str | None = os.getenv("REDIS_USERNAME")
    password: str | None = os.getenv("REDIS_PASSWORD")



    # keep processed payment lock for 30 days by default
    payment_lock_ttl: int = int(os.getenv("PAYMENT_LOCK_TTL", str(30 * 24 * 60 * 60)))

    # Default reservation TTL (seconds)
    default_reservation_ttl: int = int(os.getenv("RES_TTL", "300"))


# ----------------------------
# Redis Key helpers
# ----------------------------
def k_stock_total(product: int) -> str:
    return f"stock:product:{product}"


def k_stock_reserved(product: int) -> str:
    return f"stock:reserved:{product}"


def k_reservation(product: int, user: str) -> str:
    return f"reservation:{product}:{user}"


def k_payment_processed(order: str) -> str:
    return f"payment:processed:{order}"


K_AUDIT = "audit:stock"
K_METRICS_ABANDON = "metrics:abandon"
K_METRICS_SOLD = "metrics:sold"
K_METRICS_INCIDENTS = "metrics:incidents"


def rid(product: int, user: str) -> str:
    return f"{product}:{user}"


K_ACTIVE_SET = "active:reservations"      # SET of rid() = "product:user"
K_ACTIVE_QTY = "active:reservation:qty"   # HASH rid -> qty


# ----------------------------
# Lua scripts (Atomicity)
# ----------------------------
# Atomic reservation (Option A):
# - if reservation key exists => ALREADY_RESERVED
# - if available < qty => INSUFFICIENT_STOCK
# - else: set reservation with TTL, incr reserved, register in indexes, audit
RESERVE_LUA = r"""
-- KEYS:
-- 1 = stock_total_key
-- 2 = stock_reserved_key
-- 3 = reservation_key
-- 4 = active_set
-- 5 = active_qty_hash
-- 6 = audit_list
-- ARGV:
-- 1 = qty
-- 2 = ttl_seconds
-- 3 = rid ("product:user")
-- 4 = audit_msg

local qty = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local rid = ARGV[3]
local audit_msg = ARGV[4]

-- already reserved?
if redis.call("EXISTS", KEYS[3]) == 1 then
  return {0, "ALREADY_RESERVED"}
end

local total = tonumber(redis.call("GET", KEYS[1]) or "0")
local reserved = tonumber(redis.call("GET", KEYS[2]) or "0")
local available = total - reserved

if available < qty then
  return {0, "INSUFFICIENT_STOCK"}
end

-- perform reservation
redis.call("SET", KEYS[3], tostring(qty), "EX", ttl)
redis.call("INCRBY", KEYS[2], qty)
redis.call("SADD", KEYS[4], rid)
redis.call("HSET", KEYS[5], rid, tostring(qty))
redis.call("LPUSH", KEYS[6], audit_msg)

return {1, "RESERVATION_OK", tostring(available - qty)}
"""

# Safe release (Option A) when cancelling manually:
# If rid not active => IGNORE_ALREADY_HANDLED
# else decrement reserved by qty (from active_qty), remove indexes, delete reservation key, audit.
CANCEL_LUA = r"""
-- KEYS:
-- 1 = stock_reserved_key
-- 2 = reservation_key
-- 3 = active_set
-- 4 = active_qty_hash
-- 5 = audit_list
-- ARGV:
-- 1 = rid
-- 2 = audit_msg

local rid = ARGV[1]
local audit_msg = ARGV[2]

if redis.call("SISMEMBER", KEYS[3], rid) == 0 then
  return {0, "IGNORE_ALREADY_HANDLED"}
end

local qty = tonumber(redis.call("HGET", KEYS[4], rid) or "0")
if qty <= 0 then
  -- incident: missing qty
  redis.call("LPUSH", KEYS[5], "INCIDENT missing_qty rid=" .. rid)
  redis.call("INCR", "metrics:incidents")
  redis.call("SREM", KEYS[3], rid)
  redis.call("HDEL", KEYS[4], rid)
  redis.call("DEL", KEYS[2])
  return {0, "INCIDENT_MISSING_QTY"}
end

redis.call("DECRBY", KEYS[1], qty)
redis.call("SREM", KEYS[3], rid)
redis.call("HDEL", KEYS[4], rid)
redis.call("DEL", KEYS[2])
redis.call("LPUSH", KEYS[5], audit_msg)

return {1, "CANCEL_OK"}
"""


# ----------------------------
# Service class
# ----------------------------
class FlashSaleService:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.r = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    username=settings.redis_username,
    password=settings.redis_password,
    decode_responses=True
)



        # Register scripts (Redis returns SHA, faster later)
        self._reserve_sha = self.r.script_load(RESERVE_LUA)
        self._cancel_sha = self.r.script_load(CANCEL_LUA)

    def ping(self) -> bool:
        return self.r.ping()

    def init_stock(self, product: int, total: int) -> None:
        # Set total stock & ensure reserved exists
        pipe = self.r.pipeline(True)
        pipe.set(k_stock_total(product), total)
        pipe.setnx(k_stock_reserved(product), 0)
        pipe.lpush(K_AUDIT, f"INIT product={product} total={total}")
        pipe.execute()

    def status(self, product: int) -> dict:
        total = int(self.r.get(k_stock_total(product)) or 0)
        reserved = int(self.r.get(k_stock_reserved(product)) or 0)
        available = total - reserved
        return {"product": product, "total": total, "reserved": reserved, "available": available}

    def reserve(self, product: int, user: str, qty: int, ttl: Optional[int] = None) -> Tuple[str, Optional[int]]:
        """
        Atomic reservation with Lua: safe under concurrency.
        Returns (message, available_after_if_success).
        """
        ttl = ttl if ttl is not None else self.cfg.default_reservation_ttl
        reservation_key = k_reservation(product, user)
        audit_msg = f"RESERVE product={product} user={user} qty={qty} ttl={ttl} ts={int(time.time())}"

        res = self.r.evalsha(
            self._reserve_sha,
            6,
            k_stock_total(product),
            k_stock_reserved(product),
            reservation_key,
            K_ACTIVE_SET,
            K_ACTIVE_QTY,
            K_AUDIT,
            qty,
            ttl,
            rid(product, user),
            audit_msg,
        )

        ok = int(res[0]) == 1
        msg = str(res[1])
        available_after = int(res[2]) if ok and len(res) > 2 else None
        return msg, available_after

    def cancel(self, product: int, user: str) -> str:
        """
        Manual cancel before TTL expiry.
        Uses Lua to safely release reserved stock and clean indexes.
        """
        reservation_key = k_reservation(product, user)
        audit_msg = f"CANCEL product={product} user={user} ts={int(time.time())}"

        res = self.r.evalsha(
            self._cancel_sha,
            5,
            k_stock_reserved(product),
            reservation_key,
            K_ACTIVE_SET,
            K_ACTIVE_QTY,
            K_AUDIT,
            rid(product, user),
            audit_msg,
        )
        ok = int(res[0]) == 1
        msg = str(res[1])
        return msg if ok else msg

    def confirm_payment(self, order: str, product: int, user: str, qty: int) -> str:
        """
        Idempotent payment confirmation (Sprint 3 concept, but helps cas limites):
        - SET payment:processed:{order} NX EX (prevents double payment)
        - DEL reservation key (stops late TTL issues)
        - DECR reserved qty (convert reserved -> sold)
        - cleanup indexes to avoid sweeper release
        """
        lock_key = k_payment_processed(order)
        ok = self.r.set(lock_key, "1", nx=True, ex=self.cfg.payment_lock_ttl)
        if not ok:
            return "PAYMENT_ALREADY_PROCESSED"

        reservation_key = k_reservation(product, user)
        reservation_id = rid(product, user)

        pipe = self.r.pipeline(True)
        pipe.delete(reservation_key)
        pipe.decrby(k_stock_reserved(product), qty)
        pipe.incr(K_METRICS_SOLD)
        pipe.srem(K_ACTIVE_SET, reservation_id)
        pipe.hdel(K_ACTIVE_QTY, reservation_id)
        pipe.lpush(K_AUDIT, f"PAYMENT_CONFIRMED order={order} product={product} user={user} qty={qty} ts={int(time.time())}")
        pipe.execute()
        return "PAYMENT_CONFIRMED"

    def metrics(self) -> dict:
        abandon = int(self.r.get(K_METRICS_ABANDON) or 0)
        sold = int(self.r.get(K_METRICS_SOLD) or 0)
        incidents = int(self.r.get(K_METRICS_INCIDENTS) or 0)
        active = int(self.r.scard(K_ACTIVE_SET) or 0)
        return {"sold": sold, "abandon": abandon, "incidents": incidents, "active_reservations": active}

    def audit(self, n: int = 10) -> List[str]:
        return self.r.lrange(K_AUDIT, 0, max(0, n - 1))

    # -------- Solution 1: Sweeper (periodic) --------
    def sweeper_run_once(self) -> int:
        """
        Checks active reservations:
        if reservation key missing => TTL expired => release reserved (Option A)
        Here we release by:
          - DECR reserved
          - INCR metrics:abandon
          - cleanup indexes
          - audit log

        Returns number of released reservations.
        """
        released = 0
        ids = self.r.smembers(K_ACTIVE_SET)
        for reservation_id in ids:
            product, user = self._parse_rid(reservation_id)
            res_key = k_reservation(product, user)

            # expired => key missing
            if not self.r.exists(res_key):
                qty_str = self.r.hget(K_ACTIVE_QTY, reservation_id)
                if qty_str is None:
                    # incident: qty missing
                    self.r.incr(K_METRICS_INCIDENTS)
                    self.r.lpush(K_AUDIT, f"INCIDENT missing_qty rid={reservation_id} ts={int(time.time())}")
                    self.r.srem(K_ACTIVE_SET, reservation_id)
                    continue

                qty = int(qty_str)

                # Safe release: only if still active (guard)
                pipe = self.r.pipeline(True)
                pipe.decrby(k_stock_reserved(product), qty)
                pipe.incr(K_METRICS_ABANDON)
                pipe.srem(K_ACTIVE_SET, reservation_id)
                pipe.hdel(K_ACTIVE_QTY, reservation_id)
                pipe.lpush(K_AUDIT, f"EXPIRE_RELEASE product={product} user={user} qty={qty} ts={int(time.time())}")
                pipe.execute()

                released += 1
        return released

    # -------- Solution 2: Keyspace Notifications (listener) --------
    def enable_keyspace_notifications(self) -> None:
        """
        Enables expired events for Redis keyspace notifications.
        'Ex' means: E=Keyevent notifications, x=expired events.
        """
        self.r.config_set("notify-keyspace-events", "Ex")

    def listen_expirations_forever(self) -> None:
        """
        Listens to '__keyevent@db__:expired' channel.
        When a reservation key expires, releases reserved stock.
        """
        self.enable_keyspace_notifications()
        channel = f"__keyevent@{self.cfg.db}__:expired"

        pubsub = self.r.pubsub()
        pubsub.subscribe(channel)

        print(f"[listener] Listening on {channel} ... (Ctrl+C to stop)")
        try:
            for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                expired_key = msg.get("data")
                if not isinstance(expired_key, str):
                    continue
                if not expired_key.startswith("reservation:"):
                    continue

                product, user = self._parse_reservation_key(expired_key)
                reservation_id = rid(product, user)

                # qty from index
                qty_str = self.r.hget(K_ACTIVE_QTY, reservation_id)
                if qty_str is None:
                    self.r.incr(K_METRICS_INCIDENTS)
                    self.r.lpush(K_AUDIT, f"INCIDENT missing_qty expiredKey={expired_key} ts={int(time.time())}")
                    self.r.srem(K_ACTIVE_SET, reservation_id)
                    continue

                qty = int(qty_str)

                # release (guard by active set)
                if not self.r.sismember(K_ACTIVE_SET, reservation_id):
                    continue

                pipe = self.r.pipeline(True)
                pipe.decrby(k_stock_reserved(product), qty)
                pipe.incr(K_METRICS_ABANDON)
                pipe.srem(K_ACTIVE_SET, reservation_id)
                pipe.hdel(K_ACTIVE_QTY, reservation_id)
                pipe.lpush(K_AUDIT, f"EXPIRE_EVENT_RELEASE product={product} user={user} qty={qty} ts={int(time.time())}")
                pipe.execute()

        except KeyboardInterrupt:
            print("\n[listener] stopped.")
        finally:
            pubsub.close()

    @staticmethod
    def _parse_rid(reservation_id: str) -> Tuple[int, str]:
        p, u = reservation_id.split(":", 1)
        return int(p), u

    @staticmethod
    def _parse_reservation_key(key: str) -> Tuple[int, str]:
        # reservation:101:user42
        _, p, u = key.split(":", 2)
        return int(p), u


# ----------------------------
# CLI / Shell
# ----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="app.py", description="Flash Sale Redis Service (Option A) - CLI (No API)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ping", help="Ping Redis")

    p_init = sub.add_parser("init-stock", help="Initialize stock for a product")
    p_init.add_argument("--product", type=int, required=True)
    p_init.add_argument("--total", type=int, required=True)

    p_status = sub.add_parser("status", help="Show stock status")
    p_status.add_argument("--product", type=int, required=True)

    p_res = sub.add_parser("reserve", help="Reserve stock temporarily")
    p_res.add_argument("--product", type=int, required=True)
    p_res.add_argument("--user", type=str, required=True)
    p_res.add_argument("--qty", type=int, required=True)
    p_res.add_argument("--ttl", type=int, default=None, help="TTL seconds (optional)")

    p_cancel = sub.add_parser("cancel", help="Cancel reservation manually (before TTL)")
    p_cancel.add_argument("--product", type=int, required=True)
    p_cancel.add_argument("--user", type=str, required=True)

    p_conf = sub.add_parser("confirm", help="Confirm payment (idempotent) - useful for edge cases")
    p_conf.add_argument("--order", type=str, required=True)
    p_conf.add_argument("--product", type=int, required=True)
    p_conf.add_argument("--user", type=str, required=True)
    p_conf.add_argument("--qty", type=int, required=True)

    p_metrics = sub.add_parser("metrics", help="Show metrics counters")
    p_audit = sub.add_parser("audit", help="Show audit logs")
    p_audit.add_argument("--n", type=int, default=10)

    p_sweep = sub.add_parser("sweep-once", help="Run expiration sweeper once (Solution 1)")
    p_listen = sub.add_parser("listen-expired", help="Listen for expired keys (Solution 2)")

    sub.add_parser("shell", help="Interactive shell")

    return p


def interactive_shell(svc: FlashSaleService) -> None:
    print("FlashSale Redis Service Shell (type 'help' for commands, 'exit' to quit)")
    print("Examples: init 101 50 | status 101 | reserve 101 user42 2 10 | sweep | audit 5 | metrics")
    while True:
        try:
            line = input("flash> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if not line:
            continue
        if line in {"exit", "quit"}:
            return
        if line == "help":
            print("Commands:")
            print("  init <product> <total>")
            print("  status <product>")
            print("  reserve <product> <user> <qty> [ttl]")
            print("  cancel <product> <user>")
            print("  confirm <order> <product> <user> <qty>")
            print("  sweep")
            print("  metrics")
            print("  audit [n]")
            print("  listen  (Solution 2 - Ctrl+C to stop)")
            continue

        parts = line.split()
        cmd = parts[0].lower()

        try:
            if cmd == "init" and len(parts) == 3:
                svc.init_stock(int(parts[1]), int(parts[2]))
                print("OK")
            elif cmd == "status" and len(parts) == 2:
                print(svc.status(int(parts[1])))
            elif cmd == "reserve" and len(parts) in (4, 5):
                product = int(parts[1]); user = parts[2]; qty = int(parts[3])
                ttl = int(parts[4]) if len(parts) == 5 else None
                msg, avail = svc.reserve(product, user, qty, ttl)
                print({"result": msg, "available_after": avail})
            elif cmd == "cancel" and len(parts) == 3:
                print(svc.cancel(int(parts[1]), parts[2]))
            elif cmd == "confirm" and len(parts) == 5:
                print(svc.confirm_payment(parts[1], int(parts[2]), parts[3], int(parts[4])))
            elif cmd == "sweep":
                n = svc.sweeper_run_once()
                print(f"sweeper released: {n}")
            elif cmd == "metrics":
                print(svc.metrics())
            elif cmd == "audit":
                n = int(parts[1]) if len(parts) == 2 else 10
                for i, row in enumerate(svc.audit(n), 1):
                    print(f"{i:02d}. {row}")
            elif cmd == "listen":
                svc.listen_expirations_forever()
            else:
                print("Unknown command or bad args. Type 'help'.")
        except Exception as e:
            print(f"ERROR: {e}")


def main() -> int:
    cfg = Config()
    svc = FlashSaleService(cfg)

    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "ping":
        print("PONG" if svc.ping() else "NO")
        return 0

    if args.cmd == "init-stock":
        svc.init_stock(args.product, args.total)
        print({"result": "OK", "product": args.product, "total": args.total})
        return 0

    if args.cmd == "status":
        print(svc.status(args.product))
        return 0

    if args.cmd == "reserve":
        msg, avail = svc.reserve(args.product, args.user, args.qty, args.ttl)
        print({"result": msg, "available_after": avail, "status": svc.status(args.product)})
        return 0

    if args.cmd == "cancel":
        msg = svc.cancel(args.product, args.user)
        print({"result": msg, "status": svc.status(args.product)})
        return 0

    if args.cmd == "confirm":
        msg = svc.confirm_payment(args.order, args.product, args.user, args.qty)
        print({"result": msg, "status": svc.status(args.product), "metrics": svc.metrics()})
        return 0

    if args.cmd == "metrics":
        print(svc.metrics())
        return 0

    if args.cmd == "audit":
        rows = svc.audit(args.n)
        for i, row in enumerate(rows, 1):
            print(f"{i:02d}. {row}")
        return 0

    if args.cmd == "sweep-once":
        n = svc.sweeper_run_once()
        print({"released": n, "metrics": svc.metrics()})
        return 0

    if args.cmd == "listen-expired":
        svc.listen_expirations_forever()
        return 0

    if args.cmd == "shell":
        interactive_shell(svc)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
