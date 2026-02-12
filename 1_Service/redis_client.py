"""
redis_client.py
===============
Redis access layer (No API) for Flash Sale stock management (Option A).

Why this file impresses:
- Clean separation: CLI/service layer calls this repository layer.
- Option A model:
    available = stock_total - stock_reserved
- Atomic reservation + cancel using Lua scripts.
- Idempotent payment lock with SET NX + TTL.
- Consistent audit + metrics helpers.
- Designed for teaching NoSQL/Redis concepts (atomicity, TTL, idempotence, counters, audit).

This module is meant to be imported by app.py.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, List

import redis


# ----------------------------
# Config
# ----------------------------
@dataclass(frozen=True)
class RedisConfig:
    host: str = os.getenv("REDIS_HOST", "127.0.0.1")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))
    decode_responses: bool = True

    # Defaults aligned with the project
    default_reservation_ttl: int = int(os.getenv("RES_TTL", "300"))
    payment_lock_ttl: int = int(os.getenv("PAYMENT_LOCK_TTL", str(30 * 24 * 60 * 60)))  # 30 days


# ----------------------------
# Keys & constants
# ----------------------------
def k_stock_total(product_id: int) -> str:
    return f"stock:product:{product_id}"


def k_stock_reserved(product_id: int) -> str:
    return f"stock:reserved:{product_id}"


def k_reservation(product_id: int, user_id: str) -> str:
    return f"reservation:{product_id}:{user_id}"


def k_payment_processed(order_id: str) -> str:
    return f"payment:processed:{order_id}"


def rid(product_id: int, user_id: str) -> str:
    return f"{product_id}:{user_id}"


# Indexing active reservations (for sweeper / expiry listener)
K_ACTIVE_SET = "active:reservations"      # SET of "product:user"
K_ACTIVE_QTY = "active:reservation:qty"   # HASH "product:user" -> qty

# Observability
K_AUDIT = "audit:stock"
K_METRICS_ABANDON = "metrics:abandon"
K_METRICS_SOLD = "metrics:sold"
K_METRICS_INCIDENTS = "metrics:incidents"


# ----------------------------
# Lua scripts (atomicity)
# ----------------------------
RESERVE_LUA = r"""
-- Atomic reservation (Option A)
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
-- 3 = rid
-- 4 = audit_msg

local qty = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local rid = ARGV[3]
local audit_msg = ARGV[4]

-- 1) prevent double reservation
if redis.call("EXISTS", KEYS[3]) == 1 then
  return {0, "ALREADY_RESERVED"}
end

-- 2) compute available = total - reserved
local total = tonumber(redis.call("GET", KEYS[1]) or "0")
local reserved = tonumber(redis.call("GET", KEYS[2]) or "0")
local available = total - reserved

if available < qty then
  return {0, "INSUFFICIENT_STOCK"}
end

-- 3) reserve: set reservation with TTL + increment reserved + index + audit
redis.call("SET", KEYS[3], tostring(qty), "EX", ttl)
redis.call("INCRBY", KEYS[2], qty)
redis.call("SADD", KEYS[4], rid)
redis.call("HSET", KEYS[5], rid, tostring(qty))
redis.call("LPUSH", KEYS[6], audit_msg)

return {1, "RESERVATION_OK", tostring(available - qty)}
"""

CANCEL_LUA = r"""
-- Atomic cancel/release (Option A) for manual cancellation
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

-- Guard: if not active => already handled
if redis.call("SISMEMBER", KEYS[3], rid) == 0 then
  return {0, "IGNORE_ALREADY_HANDLED"}
end

local qty = tonumber(redis.call("HGET", KEYS[4], rid) or "0")
if qty <= 0 then
  redis.call("INCR", "metrics:incidents")
  redis.call("LPUSH", KEYS[5], "INCIDENT missing_qty rid=" .. rid)
  redis.call("SREM", KEYS[3], rid)
  redis.call("HDEL", KEYS[4], rid)
  redis.call("DEL", KEYS[2])
  return {0, "INCIDENT_MISSING_QTY"}
end

-- Release reserved stock
redis.call("DECRBY", KEYS[1], qty)

-- Cleanup
redis.call("SREM", KEYS[3], rid)
redis.call("HDEL", KEYS[4], rid)
redis.call("DEL", KEYS[2])

-- Audit
redis.call("LPUSH", KEYS[5], audit_msg)

return {1, "CANCEL_OK"}
"""


# ----------------------------
# Client factory
# ----------------------------
def create_redis_client(cfg: RedisConfig) -> redis.Redis:
    """
    Creates a Redis client configured for this project.
    decode_responses=True makes Redis return strings (simpler for CLI/report).
    """
    return redis.Redis(
        host=cfg.host,
        port=cfg.port,
        db=cfg.db,
        decode_responses=cfg.decode_responses,
    )


# ----------------------------
# Repository / Data access layer
# ----------------------------
class FlashSaleRedisRepository:
    """
    Redis repository that exposes high-level operations for the FlashSale project.
    Keeps all Redis logic (keys, scripts, atomic operations) in one place.
    """

    def __init__(self, cfg: RedisConfig, client: Optional[redis.Redis] = None):
        self.cfg = cfg
        self.r = client or create_redis_client(cfg)

        # Load Lua scripts once (faster, safer)
        self._reserve_sha = self.r.script_load(RESERVE_LUA)
        self._cancel_sha = self.r.script_load(CANCEL_LUA)

    # ---------- Health ----------
    def ping(self) -> bool:
        return bool(self.r.ping())

    # ---------- Stock ----------
    def init_stock(self, product_id: int, total: int) -> None:
        """
        Initializes stock total for a product.
        Ensures reserved key exists (0) and adds audit.
        """
        pipe = self.r.pipeline(transaction=True)
        pipe.set(k_stock_total(product_id), int(total))
        pipe.setnx(k_stock_reserved(product_id), 0)
        pipe.lpush(K_AUDIT, f"INIT product={product_id} total={total} ts={int(time.time())}")
        pipe.execute()

    def get_status(self, product_id: int) -> Dict[str, int]:
        """
        Returns total/reserved/available for Option A.
        """
        total = int(self.r.get(k_stock_total(product_id)) or 0)
        reserved = int(self.r.get(k_stock_reserved(product_id)) or 0)
        available = total - reserved
        return {"product": product_id, "total": total, "reserved": reserved, "available": available}

    def available_stock(self, product_id: int) -> int:
        st = self.get_status(product_id)
        return st["available"]

    # ---------- Reservation (Atomic via Lua) ----------
    def reserve(self, product_id: int, user_id: str, qty: int, ttl_seconds: Optional[int] = None) -> Tuple[str, Optional[int]]:
        """
        Atomic reservation (Option A) using Lua:
        - prevents double reservation
        - checks available
        - sets reservation with TTL
        - increments reserved counter
        - stores in active indexes
        - audit log

        Returns: (message, available_after_if_ok)
        """
        ttl = int(ttl_seconds if ttl_seconds is not None else self.cfg.default_reservation_ttl)
        qty = int(qty)

        audit_msg = f"RESERVE product={product_id} user={user_id} qty={qty} ttl={ttl} ts={int(time.time())}"

        res = self.r.evalsha(
            self._reserve_sha,
            6,
            k_stock_total(product_id),
            k_stock_reserved(product_id),
            k_reservation(product_id, user_id),
            K_ACTIVE_SET,
            K_ACTIVE_QTY,
            K_AUDIT,
            qty,
            ttl,
            rid(product_id, user_id),
            audit_msg,
        )

        ok = int(res[0]) == 1
        msg = str(res[1])
        available_after = int(res[2]) if ok and len(res) > 2 else None
        return msg, available_after

    def cancel(self, product_id: int, user_id: str) -> str:
        """
        Manual cancellation before TTL expiry.
        Uses Lua script to safely release reserved stock and cleanup indexes.
        """
        audit_msg = f"CANCEL product={product_id} user={user_id} ts={int(time.time())}"

        res = self.r.evalsha(
            self._cancel_sha,
            5,
            k_stock_reserved(product_id),
            k_reservation(product_id, user_id),
            K_ACTIVE_SET,
            K_ACTIVE_QTY,
            K_AUDIT,
            rid(product_id, user_id),
            audit_msg,
        )

        ok = int(res[0]) == 1
        msg = str(res[1])
        return msg if ok else msg

    # ---------- Expiration release support (for Solution 1/2) ----------
    def release_expired(self, product_id: int, user_id: str) -> str:
        """
        Releases a reservation that has expired (key missing), by using indexes.
        This is called by:
        - Solution 1 sweeper
        - Solution 2 keyspace listener

        Guard against double-release using active set membership.
        """
        reservation_id = rid(product_id, user_id)

        # If not active, it's already handled (payment confirmed or already released)
        if not self.r.sismember(K_ACTIVE_SET, reservation_id):
            return "IGNORE_ALREADY_HANDLED"

        qty_str = self.r.hget(K_ACTIVE_QTY, reservation_id)
        if qty_str is None:
            self.r.incr(K_METRICS_INCIDENTS)
            self.r.lpush(K_AUDIT, f"INCIDENT missing_qty rid={reservation_id} ts={int(time.time())}")
            self.r.srem(K_ACTIVE_SET, reservation_id)
            return "INCIDENT_MISSING_QTY"

        qty = int(qty_str)

        pipe = self.r.pipeline(transaction=True)
        pipe.decrby(k_stock_reserved(product_id), qty)
        pipe.incr(K_METRICS_ABANDON)
        pipe.srem(K_ACTIVE_SET, reservation_id)
        pipe.hdel(K_ACTIVE_QTY, reservation_id)
        pipe.lpush(K_AUDIT, f"EXPIRE_RELEASE product={product_id} user={user_id} qty={qty} ts={int(time.time())}")
        pipe.execute()

        return "RELEASE_OK"

    # ---------- Payment (Idempotent) ----------
    def confirm_payment(self, order_id: str, product_id: int, user_id: str, qty: int) -> str:
        """
        Idempotent payment confirmation (useful for edge cases):
        - SET NX to prevent double processing
        - DEL reservation key to avoid late TTL
        - reserved -> sold
        - cleanup active indexes
        - audit + metrics
        """
        lock_key = k_payment_processed(order_id)
        ok = self.r.set(lock_key, "1", nx=True, ex=self.cfg.payment_lock_ttl)
        if not ok:
            return "PAYMENT_ALREADY_PROCESSED"

        reservation_id = rid(product_id, user_id)
        reservation_key = k_reservation(product_id, user_id)

        pipe = self.r.pipeline(transaction=True)
        pipe.delete(reservation_key)
        pipe.decrby(k_stock_reserved(product_id), int(qty))
        pipe.incr(K_METRICS_SOLD)
        pipe.srem(K_ACTIVE_SET, reservation_id)
        pipe.hdel(K_ACTIVE_QTY, reservation_id)
        pipe.lpush(
            K_AUDIT,
            f"PAYMENT_CONFIRMED order={order_id} product={product_id} user={user_id} qty={qty} ts={int(time.time())}",
        )
        pipe.execute()

        return "PAYMENT_CONFIRMED"

    # ---------- Metrics & Audit ----------
    def get_metrics(self) -> Dict[str, int]:
        abandon = int(self.r.get(K_METRICS_ABANDON) or 0)
        sold = int(self.r.get(K_METRICS_SOLD) or 0)
        incidents = int(self.r.get(K_METRICS_INCIDENTS) or 0)
        active = int(self.r.scard(K_ACTIVE_SET) or 0)
        return {"sold": sold, "abandon": abandon, "incidents": incidents, "active_reservations": active}

    def get_audit(self, n: int = 10) -> List[str]:
        return self.r.lrange(K_AUDIT, 0, max(0, n - 1))

    # ---------- Debug helpers ----------
    def flush_all(self) -> None:
        """
        For demos/tests only.
        """
        self.r.flushdb()
