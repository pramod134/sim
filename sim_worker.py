"""
One-time Simulation Worker (Railway-friendly)

Behavior:
1) Connect to Postgres via DATABASE_URL (asyncpg pool)
2) Claim ONE sim job from public.sim_ticker where start_sim='y'
3) Build CandleEngine + IndicatorBot
4) Load seed candles (as-of seed_date 16:00 ET), bootstrap IndicatorBot
5) Run simulation for sim_period trading days (RTH only), step-by-step (no sleeps)
6) Mark sim_ticker as done/error and exit

Notes:
- No DB writes for indicators/events/trades during simulation (cache-only).
- sim_ticker is the only table we update (status tracking).
"""

import os
import asyncio
import datetime as dt
from typing import Optional, Dict, Any

import asyncpg

from candle_engine import CandleEngine
from indicator_bot import IndicatorBot


CLAIM_ONE_JOB_SQL = """
WITH picked AS (
  SELECT symbol
  FROM public.sim_ticker
  WHERE start_sim = 'y'
    AND status IN ('idle','done','error')
  ORDER BY symbol
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE public.sim_ticker t
SET
  start_sim     = 'n',
  status        = 'running',
  run_id        = gen_random_uuid(),
  started_at    = now(),
  finished_at   = NULL,
  error_message = NULL
FROM picked
WHERE t.symbol = picked.symbol
RETURNING
  t.symbol,
  t.seed_date,
  t.sim_period,
  t.run_id,
  t.status,
  t.started_at;
"""

MARK_DONE_SQL = """
UPDATE public.sim_ticker
SET
  status = 'done',
  finished_at = now()
WHERE symbol = $1
  AND run_id = $2;
"""

MARK_ERROR_SQL = """
UPDATE public.sim_ticker
SET
  status = 'error',
  finished_at = now(),
  error_message = $3
WHERE symbol = $1
  AND run_id = $2;
"""


def _parse_iso_dt(s: str) -> dt.datetime:
    # Expect ISO string with timezone (UTC).
    # Example: "2026-02-24T20:59:00+00:00"
    return dt.datetime.fromisoformat(s)


async def _claim_one_job(pool: asyncpg.Pool) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(CLAIM_ONE_JOB_SQL)
            return row


async def _mark_done(pool: asyncpg.Pool, symbol: str, run_id: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(MARK_DONE_SQL, symbol, run_id)


async def _mark_error(pool: asyncpg.Pool, symbol: str, run_id: str, message: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(MARK_ERROR_SQL, symbol, run_id, message[:2000])


async def run_one_job(pool: asyncpg.Pool, job: asyncpg.Record) -> None:
    symbol: str = (job["symbol"] or "").upper()
    seed_date: dt.date = job["seed_date"]
    sim_period: int = int(job["sim_period"])
    run_id: str = str(job["run_id"])

    print(
        f"[SIM_WORKER] Claimed job: symbol={symbol}, seed_date={seed_date}, sim_period={sim_period}, run_id={run_id}"
    )

    # Build engine + bot (cache-only simulation)
    engine = CandleEngine(db_pool=pool, symbols=[symbol])
    bot = IndicatorBot(engine=engine, timeframes=None, enable_zone_finder=False, enable_liquidity_pool=False)

    # Seed
    seed_payload: Dict[str, Any] = await engine.load_seed(symbol, seed_date)
    await bot.bootstrap(seed_payload)

    # Simulation loop: run for sim_period *trading days*.
    # CandleEngine.step() will load next RTH day buffer automatically from DB.
    days_completed = 0
    current_day: Optional[dt.date] = None

    # The engine maintains per-symbol day buffer and current day (ET).
    # We count a "sim day" once we begin processing candles for a new ET date.
    while True:
        try:
            # Stop exactly after finishing the last minute of the last requested day:
            # If we've completed sim_period days AND the current day's buffer is consumed, stop BEFORE loading next day.
            if current_day is not None and days_completed >= sim_period:
                buf = engine._day_buffer_1m.get(symbol, [])
                idx = engine._day_buffer_idx.get(symbol, 0)
                if idx >= len(buf):
                    print(f"[SIM_WORKER] Completed {days_completed} trading days for {symbol}. Stopping.")
                    break

            events = await engine.step(symbol)

            # Determine current ET day from engine state (set when day buffer is loaded)
            day_et = engine._current_day_et.get(symbol)

            if day_et is not None and day_et != current_day:
                current_day = day_et
                days_completed += 1
                print(f"[SIM_WORKER] Day {days_completed}/{sim_period} started: {symbol} {current_day} (ET)")

            # Only process events if we are within sim_period days
            # (This prevents processing first candle of day sim_period+1 if edge occurs)
            if days_completed <= sim_period:
                await bot.process(events)

        except StopAsyncIteration:
            print(f"[SIM_WORKER] No more candles available for {symbol}. Stopping early.")
            break
        except Exception as e:
            # Bubble up for outer handler to mark error
            raise RuntimeError(f"Simulation failed for {symbol}: {e}") from e

    # Print a tiny summary from caches
    try:
        # Last known 1m snapshot
        k = (symbol, "1m")
        if k in bot.spot_tf_cache:
            last_asof = bot.spot_tf_cache[k].get("asof")
            print(f"[SIM_WORKER] Finished {symbol}. Last 1m asof={last_asof}")
    except Exception:
        pass

    # Mark done
    await _mark_done(pool, symbol, run_id)
    print(f"[SIM_WORKER] Marked DONE: {symbol} run_id={run_id}")


async def main() -> int:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        print("[SIM_WORKER] ERROR: DATABASE_URL is not set.")
        return 2

    # Small pool is enough for a one-time worker
    pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5)

    try:
        job = await _claim_one_job(pool)
        if not job:
            print("[SIM_WORKER] No sim_ticker rows with start_sim='y'. Exiting.")
            return 0

        symbol = (job["symbol"] or "").upper()
        run_id = str(job["run_id"])

        try:
            await run_one_job(pool, job)
            return 0
        except Exception as e:
            msg = str(e)
            print(f"[SIM_WORKER] ERROR during run: {msg}")
            try:
                await _mark_error(pool, symbol, run_id, msg)
            except Exception as e2:
                print(f"[SIM_WORKER] Failed to mark error in sim_ticker: {e2}")
            return 1

    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
