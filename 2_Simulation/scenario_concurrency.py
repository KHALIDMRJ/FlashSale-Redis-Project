#!/usr/bin/env python3
"""
scenario_concurrency.py
=======================
Demonstration script for a NoSQL/Redis course:
Compare SAFE (Lua atomic) vs UNSAFE (naive check-then-update) reservation under concurrency.

Why it impresses:
- Shows the exact concurrency bug ("race condition") causing oversell
- Proves that Lua scripts (atomicity) fix it
- Produces a clear PASS/FAIL result for anti-oversell

Run:
----
# From project root (FlashSale):
python 2_Simulation/scenario_concurrency.py --product 101 --stock 20 --clients 200 --ttl 8

Expected:
---------
Scenario SAFE: PASS
Scenario UNSAFE: may FAIL (oversell detected), especially with high clients.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import redis

# Make parent folder importable so we can import config.py
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import load_settings


# ----------------------------
# Redis Keys (match your project)
# ----------------------------
def k_stock_total(product: int) -> str:
    return f"stock:product:{product}"


def k_stock_reserved(product: int) -> str:
    return f"stock:reserved:{product}"


def k_reservation(product: int, user: str) -> str:
    return f"reservation:{product}:{user}"


def rid(product: int, user: str) -> str:
    return f"{product}:{user}"


K_ACTIVE_SET = "active:reservations"
K_ACTIVE_QTY = "active:reservation:qty"
K_AUDIT = "audit:stock"


# ----------------------------
# SAFE Atomic reservation (Lua)
# ----------------------------
RESERVE_LUA = r"""
-- KEYS:
-- 1 = stock_total_key
-- 2 = stock_reserved_key
-- 3 = reservation_key
-- 4 = active_set
-- 5 = active_qty_hash
-- ARGV:
-- 1 = qty
-- 2 = ttl_seconds
-- 3 = rid

local qty = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local rid = ARGV[3]

-- Prevent double reservation for same user
if redis.call("EXISTS", KEYS[3]) == 1 then
  return {0, "ALREADY_RESERVED"}
end

local total = tonumber(redis.call("GET", KEYS[1]) or "0")
local reserved = tonumber(redis.call("GET", KEYS[2]) or "0")
local available = total - reserved

if available < qty then
  return {0, "INSUFFICIENT_STOCK"}
end

redis.call("SET", KEYS[3], tostring(qty), "EX", ttl)
redis.call("INCRBY", KEYS[2], qty)
redis.call("SADD", KEYS[4], rid)
redis.call("HSET", KEYS[5], rid, tostring(qty))

return {1, "RESERVATION_OK"}
"""


# ----------------------------
# Scenario configuration
# ----------------------------
@dataclass(frozen=True)
class ScenarioConfig:
    product: int
    stock: int
    clients: int
    ttl: int
    qty: int = 1
    seed: int = 42

    # For UNSAFE scenario: add small delay to amplify race condition
    unsafe_delay_ms: int = 2


@dataclass
class Outcome:
    ok: int = 0
    fail: int = 0
    fail_no_stock: int = 0


# ----------------------------
# Engine
# ----------------------------
class ConcurrencyScenario:
    def __init__(self, cfg: ScenarioConfig):
        self.cfg = cfg
        self.settings = load_settings()
        print(self.settings.safe_summary())

        self.r = redis.Redis(
            host=self.settings.redis_host,
            port=self.settings.redis_port,
            db=self.settings.redis_db,
            username=self.settings.redis_username,
            password=self.settings.redis_password,
            decode_responses=True,
        )

        self.reserve_sha = self.r.script_load(RESERVE_LUA)
        self._lock = threading.Lock()

    def reset(self) -> None:
        # Clean DB for a clean demo
        self.r.flushdb()
        pipe = self.r.pipeline(True)
        pipe.set(k_stock_total(self.cfg.product), self.cfg.stock)
        pipe.set(k_stock_reserved(self.cfg.product), 0)
        pipe.delete(K_ACTIVE_SET)
        pipe.delete(K_ACTIVE_QTY)
        pipe.lpush(K_AUDIT, f"SCENARIO_INIT product={self.cfg.product} stock={self.cfg.stock} ts={int(time.time())}")
        pipe.execute()

    def status(self) -> Dict[str, int]:
        total = int(self.r.get(k_stock_total(self.cfg.product)) or 0)
        reserved = int(self.r.get(k_stock_reserved(self.cfg.product)) or 0)
        available = total - reserved
        return {"total": total, "reserved": reserved, "available": available}

    # ---------- Scenario A: SAFE ----------
    def safe_reserve(self, user: str) -> bool:
        res = self.r.evalsha(
            self.reserve_sha,
            5,
            k_stock_total(self.cfg.product),
            k_stock_reserved(self.cfg.product),
            k_reservation(self.cfg.product, user),
            K_ACTIVE_SET,
            K_ACTIVE_QTY,
            self.cfg.qty,
            self.cfg.ttl,
            rid(self.cfg.product, user),
        )
        ok = int(res[0]) == 1
        return ok

    # ---------- Scenario B: UNSAFE ----------
    def unsafe_reserve(self, user: str) -> bool:
        """
        This is intentionally WRONG under concurrency:
        1) read total/reserved
        2) compute available
        3) if enough, write reservation and increment reserved

        Between (1) and (3), another thread may change reserved,
        so many threads may all see "available enough" and oversell.
        """
        total = int(self.r.get(k_stock_total(self.cfg.product)) or 0)
        reserved = int(self.r.get(k_stock_reserved(self.cfg.product)) or 0)
        available = total - reserved

        if available < self.cfg.qty:
            return False

        # Artificial small delay to make the race condition visible
        time.sleep(self.cfg.unsafe_delay_ms / 1000.0)

        # now do writes (not atomic with the read!)
        pipe = self.r.pipeline(True)
        pipe.set(k_reservation(self.cfg.product, user), str(self.cfg.qty), ex=self.cfg.ttl)
        pipe.incrby(k_stock_reserved(self.cfg.product), self.cfg.qty)
        pipe.sadd(K_ACTIVE_SET, rid(self.cfg.product, user))
        pipe.hset(K_ACTIVE_QTY, rid(self.cfg.product, user), self.cfg.qty)
        pipe.execute()
        return True

    def _run_threads(self, mode: str) -> Tuple[Outcome, float]:
        outcome = Outcome()
        t0 = time.perf_counter()
        threads: List[threading.Thread] = []

        def worker(i: int):
            user = f"user{i:04d}"
            ok = self.safe_reserve(user) if mode == "SAFE" else self.unsafe_reserve(user)
            with self._lock:
                if ok:
                    outcome.ok += 1
                else:
                    outcome.fail += 1
                    outcome.fail_no_stock += 1

        for i in range(1, self.cfg.clients + 1):
            th = threading.Thread(target=worker, args=(i,))
            th.start()
            threads.append(th)

        for th in threads:
            th.join()

        dt = time.perf_counter() - t0
        return outcome, dt

    def oversell_detected(self) -> bool:
        st = self.status()
        # Oversell symptom: reserved > total OR available < 0
        return (st["reserved"] > st["total"]) or (st["available"] < 0)

    def run(self) -> None:
        random.seed(self.cfg.seed)

        print("\n" + "=" * 68)
        print("SCENARIO A — SAFE (Lua atomic reservation)")
        print("=" * 68)
        self.reset()
        before = self.status()
        out, dt = self._run_threads("SAFE")
        after = self.status()
        fail = self.oversell_detected()

        print(f"Before: {before}")
        print(f"After : {after}")
        print(f"Clients: {self.cfg.clients}, OK={out.ok}, FAIL={out.fail}, Time={dt:.2f}s")
        print(f"Anti-oversell check: {'FAIL (oversell detected)' if fail else 'PASS (no oversell)'}")

        print("\n" + "=" * 68)
        print("SCENARIO B — UNSAFE (naive check then update)")
        print("=" * 68)
        self.reset()
        before = self.status()
        out, dt = self._run_threads("UNSAFE")
        after = self.status()
        fail = self.oversell_detected()

        print(f"Before: {before}")
        print(f"After : {after}")
        print(f"Clients: {self.cfg.clients}, OK={out.ok}, FAIL={out.fail}, Time={dt:.2f}s")
        print(f"Anti-oversell check: {'FAIL (oversell detected)' if fail else 'PASS (no oversell)'}")

        print("\n" + "-" * 68)
        print("Short repport:")
        print("-" * 68)
        print("1) UNSAFE scenario shows race condition: multiple threads read 'available' at same time.")
        print("2) They all think stock is available, then increment reserved => reserved can exceed total.")
        print("3) SAFE scenario uses Lua => atomic check+update => prevents oversell.")
        print("-" * 68)


# ----------------------------
# CLI
# ----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Concurrency scenario: SAFE(Lua) vs UNSAFE(naive) reservations")
    p.add_argument("--product", type=int, required=True)
    p.add_argument("--stock", type=int, required=True)
    p.add_argument("--clients", type=int, required=True)
    p.add_argument("--ttl", type=int, default=8)
    p.add_argument("--qty", type=int, default=1)
    p.add_argument("--unsafe-delay-ms", type=int, default=2, help="increase to make oversell more visible")
    p.add_argument("--seed", type=int, default=42)
    return p


def main() -> int:
    args = build_parser().parse_args()
    cfg = ScenarioConfig(
        product=args.product,
        stock=args.stock,
        clients=args.clients,
        ttl=args.ttl,
        qty=args.qty,
        seed=args.seed,
        unsafe_delay_ms=args.unsafe_delay_ms,
    )
    ConcurrencyScenario(cfg).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
