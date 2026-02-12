#!/usr/bin/env python3
"""
simulate_clients.py
===================
Flash Sale simulation (concurrent clients) for Redis-only NoSQL project.

GOAL (teacher-friendly):
- Prove anti-oversell under concurrency
- Show reservation TTL + abandonment + confirmations
- Produce KPIs: sold, abandon, incidents, reserved vs sold, abandonment rate
- Works with your existing config.py and app/service logic (Option A model)

What it simulates:
- N concurrent clients (threads)
- Each client tries to reserve qty=1 (or random qty)
- Then chooses one behavior:
  - CONFIRM payment (success path)
  - ABANDON (let TTL expire, then sweeper releases)
  - CANCEL (manual cancel before TTL)
- Optional: run sweeper periodically (Solution 1) to release expired reservations

Requirements:
- redis-py installed
- Redis running (Docker ok)
- config.py available in project root

Run examples:
-------------
# From project root (FlashSale):
python 2_Simulation/simulate_clients.py --product 101 --stock 50 --clients 100 --ttl 8 --confirm-rate 0.6 --cancel-rate 0.1 --abandon-rate 0.3

# Add sweeper (recommended):
python 2_Simulation/simulate_clients.py --product 101 --stock 50 --clients 200 --ttl 8 --sweeper-interval 2

# Export CSV results:
python 2_Simulation/simulate_clients.py --product 101 --stock 50 --clients 200 --ttl 8 --csv results.csv
"""

from __future__ import annotations

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


import argparse
import csv
import random
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import redis

# Import your config (must be accessible via PYTHONPATH / same project root)
# If you run from project root, it works.
from config import load_settings


# ----------------------------
# Redis keys (must match app.py)
# ----------------------------
def k_stock_total(product: int) -> str:
    return f"stock:product:{product}"


def k_stock_reserved(product: int) -> str:
    return f"stock:reserved:{product}"


def k_reservation(product: int, user: str) -> str:
    return f"reservation:{product}:{user}"


def k_payment_processed(order: str) -> str:
    return f"payment:processed:{order}"


def rid(product: int, user: str) -> str:
    return f"{product}:{user}"


K_ACTIVE_SET = "active:reservations"
K_ACTIVE_QTY = "active:reservation:qty"

K_AUDIT = "audit:stock"
K_METRICS_ABANDON = "metrics:abandon"
K_METRICS_SOLD = "metrics:sold"
K_METRICS_INCIDENTS = "metrics:incidents"


# ----------------------------
# Lua scripts (atomic reserve) - same logic as app.py
# ----------------------------
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
-- 3 = rid
-- 4 = audit_msg

local qty = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local rid = ARGV[3]
local audit_msg = ARGV[4]

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
redis.call("LPUSH", KEYS[6], audit_msg)

return {1, "RESERVATION_OK", tostring(available - qty)}
"""


# ----------------------------
# Simulation config
# ----------------------------
@dataclass(frozen=True)
class SimConfig:
    product: int
    initial_stock: int
    clients: int
    ttl: int
    qty_min: int
    qty_max: int

    # Probabilities should sum to 1.0
    confirm_rate: float
    cancel_rate: float
    abandon_rate: float

    # Sweeper (Solution 1)
    sweeper_interval: Optional[int] = None

    # Randomness
    seed: int = 42

    # CSV output
    csv_path: Optional[str] = None


@dataclass
class ClientResult:
    user: str
    action: str  # "CONFIRM" | "CANCEL" | "ABANDON" | "FAIL_NO_STOCK" | "FAIL_ALREADY_RESERVED"
    qty: int
    reserve_status: str
    confirm_status: Optional[str] = None
    latency_ms: int = 0


# ----------------------------
# Redis simulation engine
# ----------------------------
class FlashSaleSim:
    def __init__(self, sim: SimConfig):
        self.sim = sim
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

        # Load Lua script SHA
        self.reserve_sha = self.r.script_load(RESERVE_LUA)

        self._lock = threading.Lock()
        self.results: List[ClientResult] = []

        # Stop flag for sweeper thread
        self._stop_sweeper = threading.Event()

    # ---------- helpers ----------
    def status(self) -> Dict[str, int]:
        total = int(self.r.get(k_stock_total(self.sim.product)) or 0)
        reserved = int(self.r.get(k_stock_reserved(self.sim.product)) or 0)
        return {"total": total, "reserved": reserved, "available": total - reserved}

    def reset_db_for_demo(self) -> None:
        """
        Optional: wipe DB for clean demo.
        Comment out if you don't want to clear existing data.
        """
        self.r.flushdb()

    def init_stock(self) -> None:
        pipe = self.r.pipeline(True)
        pipe.set(k_stock_total(self.sim.product), self.sim.initial_stock)
        pipe.set(k_stock_reserved(self.sim.product), 0)
        pipe.set(K_METRICS_ABANDON, 0)
        pipe.set(K_METRICS_SOLD, 0)
        pipe.set(K_METRICS_INCIDENTS, 0)
        pipe.delete(K_ACTIVE_SET)
        pipe.delete(K_ACTIVE_QTY)
        pipe.lpush(K_AUDIT, f"SIM_INIT product={self.sim.product} total={self.sim.initial_stock} ts={int(time.time())}")
        pipe.execute()

    # ---------- atomic reserve ----------
    def atomic_reserve(self, product: int, user: str, qty: int, ttl: int) -> Tuple[str, bool]:
        """
        Returns (reserve_status, ok)
        """
        audit_msg = f"SIM_RESERVE product={product} user={user} qty={qty} ttl={ttl} ts={int(time.time())}"
        res = self.r.evalsha(
            self.reserve_sha,
            6,
            k_stock_total(product),
            k_stock_reserved(product),
            k_reservation(product, user),
            K_ACTIVE_SET,
            K_ACTIVE_QTY,
            K_AUDIT,
            qty,
            ttl,
            rid(product, user),
            audit_msg,
        )
        ok = int(res[0]) == 1
        return str(res[1]), ok

    # ---------- confirm payment (idempotent) ----------
    def confirm_payment(self, order: str, product: int, user: str, qty: int) -> str:
        """
        Idempotent confirm:
          - SET payment lock NX
          - delete reservation key
          - DECR reserved qty
          - cleanup indexes
          - metrics:sold++
        """
        lock_key = k_payment_processed(order)
        ok = self.r.set(lock_key, "1", nx=True, ex=self.settings.payment_lock_ttl)
        if not ok:
            return "PAYMENT_ALREADY_PROCESSED"

        reservation_id = rid(product, user)
        pipe = self.r.pipeline(True)
        pipe.delete(k_reservation(product, user))
        pipe.decrby(k_stock_reserved(product), qty)
        pipe.incr(K_METRICS_SOLD)
        pipe.srem(K_ACTIVE_SET, reservation_id)
        pipe.hdel(K_ACTIVE_QTY, reservation_id)
        pipe.lpush(K_AUDIT, f"SIM_CONFIRM order={order} product={product} user={user} qty={qty} ts={int(time.time())}")
        pipe.execute()
        return "PAYMENT_CONFIRMED"

    # ---------- cancel ----------
    def cancel(self, product: int, user: str) -> str:
        """
        Manual cancel:
          - get qty from index
          - release reserved
          - cleanup indexes
          - delete reservation key
        """
        reservation_id = rid(product, user)
        if not self.r.sismember(K_ACTIVE_SET, reservation_id):
            return "IGNORE_ALREADY_HANDLED"

        qty_str = self.r.hget(K_ACTIVE_QTY, reservation_id)
        if qty_str is None:
            self.r.incr(K_METRICS_INCIDENTS)
            self.r.lpush(K_AUDIT, f"SIM_INCIDENT missing_qty rid={reservation_id}")
            self.r.srem(K_ACTIVE_SET, reservation_id)
            return "INCIDENT_MISSING_QTY"

        qty = int(qty_str)

        pipe = self.r.pipeline(True)
        pipe.decrby(k_stock_reserved(product), qty)
        pipe.srem(K_ACTIVE_SET, reservation_id)
        pipe.hdel(K_ACTIVE_QTY, reservation_id)
        pipe.delete(k_reservation(product, user))
        pipe.lpush(K_AUDIT, f"SIM_CANCEL product={product} user={user} qty={qty} ts={int(time.time())}")
        pipe.execute()
        return "CANCEL_OK"

    # ---------- sweeper (Solution 1) ----------
    def sweeper_once(self) -> int:
        """
        Releases expired reservations:
        - if reservation key doesn't exist -> TTL expired
        - release reserved qty
        - metrics:abandon++
        """
        released = 0
        ids = self.r.smembers(K_ACTIVE_SET)
        for reservation_id in ids:
            product, user = reservation_id.split(":", 1)
            product = int(product)
            res_key = k_reservation(product, user)

            # expired
            if not self.r.exists(res_key):
                qty_str = self.r.hget(K_ACTIVE_QTY, reservation_id)
                if qty_str is None:
                    self.r.incr(K_METRICS_INCIDENTS)
                    self.r.lpush(K_AUDIT, f"SIM_INCIDENT missing_qty rid={reservation_id}")
                    self.r.srem(K_ACTIVE_SET, reservation_id)
                    continue

                qty = int(qty_str)

                pipe = self.r.pipeline(True)
                pipe.decrby(k_stock_reserved(product), qty)
                pipe.incr(K_METRICS_ABANDON)
                pipe.srem(K_ACTIVE_SET, reservation_id)
                pipe.hdel(K_ACTIVE_QTY, reservation_id)
                pipe.lpush(K_AUDIT, f"SIM_EXPIRE_RELEASE product={product} user={user} qty={qty} ts={int(time.time())}")
                pipe.execute()
                released += 1
        return released

    def _sweeper_loop(self) -> None:
        assert self.sim.sweeper_interval is not None
        while not self._stop_sweeper.is_set():
            self.sweeper_once()
            self._stop_sweeper.wait(self.sim.sweeper_interval)

    # ---------- client worker ----------
    def _client_thread(self, idx: int) -> None:
        user = f"user{idx:04d}"
        qty = random.randint(self.sim.qty_min, self.sim.qty_max)

        start = time.perf_counter()
        reserve_status, ok = self.atomic_reserve(self.sim.product, user, qty, self.sim.ttl)

        if not ok:
            action = "FAIL_NO_STOCK" if reserve_status == "INSUFFICIENT_STOCK" else "FAIL_ALREADY_RESERVED"
            latency_ms = int((time.perf_counter() - start) * 1000)
            self._record(ClientResult(user=user, action=action, qty=qty, reserve_status=reserve_status, latency_ms=latency_ms))
            return

        # Decide behavior after reservation
        r = random.random()
        if r < self.sim.confirm_rate:
            # confirm quickly (simulate user paying)
            order_id = f"order-{user}-{int(time.time() * 1000)}"
            confirm_status = self.confirm_payment(order_id, self.sim.product, user, qty)
            action = "CONFIRM"
        elif r < self.sim.confirm_rate + self.sim.cancel_rate:
            # cancel manually before TTL
            confirm_status = self.cancel(self.sim.product, user)
            action = "CANCEL"
        else:
            # abandon: do nothing, let TTL expire
            confirm_status = None
            action = "ABANDON"

        latency_ms = int((time.perf_counter() - start) * 1000)
        self._record(ClientResult(user=user, action=action, qty=qty, reserve_status=reserve_status, confirm_status=confirm_status, latency_ms=latency_ms))

    def _record(self, result: ClientResult) -> None:
        with self._lock:
            self.results.append(result)

    # ---------- main run ----------
    def run(self) -> None:
        random.seed(self.sim.seed)

        # For clean demo, you can reset DB
        # If you don't want, comment out:
        self.reset_db_for_demo()
        self.init_stock()

        print("\n[SIM] Initial status:", self.status())

        # Start sweeper thread if requested
        sweeper_thread: Optional[threading.Thread] = None
        if self.sim.sweeper_interval is not None:
            sweeper_thread = threading.Thread(target=self._sweeper_loop, daemon=True)
            sweeper_thread.start()
            print(f"[SIM] Sweeper enabled: interval={self.sim.sweeper_interval}s")

        # Launch client threads
        threads: List[threading.Thread] = []
        t0 = time.perf_counter()

        for i in range(1, self.sim.clients + 1):
            th = threading.Thread(target=self._client_thread, args=(i,))
            th.start()
            threads.append(th)

        for th in threads:
            th.join()

        # Wait for TTL expirations to happen, then final sweeper pass
        # (only needed to count ABANDON releases if sweeper not running continuously)
        print("[SIM] All clients finished. Waiting for TTL expirations...")
        time.sleep(self.sim.ttl + 1)

        # Run sweeper once at the end to release abandoned reservations
        released = self.sweeper_once()
        print(f"[SIM] Final sweeper release count: {released}")

        # Stop sweeper loop
        if self.sim.sweeper_interval is not None:
            self._stop_sweeper.set()
            if sweeper_thread is not None:
                sweeper_thread.join(timeout=1)

        total_time = time.perf_counter() - t0

        # Print final report
        self.report(total_time)

        # CSV export (optional)
        if self.sim.csv_path:
            self.export_csv(self.sim.csv_path)
            print(f"[SIM] CSV exported to: {self.sim.csv_path}")

    def report(self, total_time_seconds: float) -> None:
        st = self.status()
        sold = int(self.r.get(K_METRICS_SOLD) or 0)
        abandon = int(self.r.get(K_METRICS_ABANDON) or 0)
        incidents = int(self.r.get(K_METRICS_INCIDENTS) or 0)

        # Summarize client actions
        counts: Dict[str, int] = {}
        for r in self.results:
            counts[r.action] = counts.get(r.action, 0) + 1

        total_clients = len(self.results)
        confirms = counts.get("CONFIRM", 0)
        cancels = counts.get("CANCEL", 0)
        abandons = counts.get("ABANDON", 0)
        fails_no_stock = counts.get("FAIL_NO_STOCK", 0)
        fails_reserved = counts.get("FAIL_ALREADY_RESERVED", 0)

        # Oversell check: reserved should not be negative; available should not be negative.
        oversell_detected = st["available"] < 0 or st["reserved"] < 0

        # Latency stats
        latencies = [r.latency_ms for r in self.results]
        p50 = percentile(latencies, 50)
        p95 = percentile(latencies, 95)
        p99 = percentile(latencies, 99)

        # Abandon rate among successful reservations (confirm/cancel/abandon)
        successful_paths = confirms + cancels + abandons
        abandon_rate = (abandons / successful_paths) if successful_paths > 0 else 0.0

        print("\n" + "=" * 60)
        print("[SIM] FINAL REPORT")
        print("=" * 60)
        print(f"Product: {self.sim.product}")
        print(f"Initial stock_total: {self.sim.initial_stock}")
        print(f"Final status: total={st['total']} reserved={st['reserved']} available={st['available']}")
        print(f"Time: {total_time_seconds:.2f}s for {total_clients} clients")
        print(f"Latency(ms): p50={p50}  p95={p95}  p99={p99}")
        print("-" * 60)
        print("Client outcomes:")
        print(f"  CONFIRM:            {confirms}")
        print(f"  CANCEL:             {cancels}")
        print(f"  ABANDON:            {abandons}")
        print(f"  FAIL_NO_STOCK:      {fails_no_stock}")
        print(f"  FAIL_ALREADY_RES:   {fails_reserved}")
        print("-" * 60)
        print("Redis metrics:")
        print(f"  metrics:sold:       {sold}")
        print(f"  metrics:abandon:    {abandon}")
        print(f"  metrics:incidents:  {incidents}")
        print("-" * 60)
        print(f"Abandon rate (among successful reservations): {abandon_rate:.2%}")
        print(f"Anti-oversell check: {'FAIL (oversell detected)' if oversell_detected else 'PASS (no oversell)'}")
        print("=" * 60)

        if oversell_detected:
            print("[WARNING] Oversell detected (available/reserved < 0). Investigate atomicity/logic.")
        else:
            print("[OK] Concurrency safety demonstrated (stock never negative).")

    def export_csv(self, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["user", "action", "qty", "reserve_status", "confirm_status", "latency_ms"])
            for r in self.results:
                w.writerow([r.user, r.action, r.qty, r.reserve_status, r.confirm_status or "", r.latency_ms])


# ----------------------------
# Stats helper
# ----------------------------
def percentile(values: List[int], p: int) -> int:
    if not values:
        return 0
    values_sorted = sorted(values)
    k = (len(values_sorted) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(values_sorted) - 1)
    if f == c:
        return values_sorted[f]
    d0 = values_sorted[f] * (c - k)
    d1 = values_sorted[c] * (k - f)
    return int(d0 + d1)


# ----------------------------
# CLI
# ----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Flash Sale Redis Simulation (concurrent clients)")
    p.add_argument("--product", type=int, required=True, help="product id")
    p.add_argument("--stock", type=int, required=True, help="initial stock_total")
    p.add_argument("--clients", type=int, required=True, help="number of concurrent clients")
    p.add_argument("--ttl", type=int, default=8, help="reservation TTL seconds (demo-friendly)")
    p.add_argument("--qty-min", type=int, default=1, help="min qty per reservation")
    p.add_argument("--qty-max", type=int, default=1, help="max qty per reservation")

    p.add_argument("--confirm-rate", type=float, default=0.6, help="probability of confirming after reserve")
    p.add_argument("--cancel-rate", type=float, default=0.1, help="probability of manual cancel after reserve")
    p.add_argument("--abandon-rate", type=float, default=0.3, help="probability of abandon after reserve")

    p.add_argument("--sweeper-interval", type=int, default=None, help="enable sweeper loop with interval seconds")
    p.add_argument("--seed", type=int, default=42, help="random seed")
    p.add_argument("--csv", type=str, default=None, help="export results to CSV path")
    return p


def validate_rates(confirm_rate: float, cancel_rate: float, abandon_rate: float) -> None:
    s = confirm_rate + cancel_rate + abandon_rate
    # Allow small float error
    if abs(s - 1.0) > 1e-6:
        raise SystemExit(f"ERROR: rates must sum to 1.0, got {s:.4f}")


def main() -> int:
    args = build_parser().parse_args()
    validate_rates(args.confirm_rate, args.cancel_rate, args.abandon_rate)

    sim_cfg = SimConfig(
        product=args.product,
        initial_stock=args.stock,
        clients=args.clients,
        ttl=args.ttl,
        qty_min=args.qty_min,
        qty_max=args.qty_max,
        confirm_rate=args.confirm_rate,
        cancel_rate=args.cancel_rate,
        abandon_rate=args.abandon_rate,
        sweeper_interval=args.sweeper_interval,
        seed=args.seed,
        csv_path=args.csv,
    )

    engine = FlashSaleSim(sim_cfg)
    engine.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
