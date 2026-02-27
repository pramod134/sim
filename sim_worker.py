import os
import asyncio
import datetime as dt
import uuid
from typing import Any, Dict, Optional

import httpx

from candle_engine import CandleEngine
from indicator_bot import IndicatorBot


# ----------------------------- Supabase REST -----------------------------

def _sb_env() -> tuple[str, str]:
    url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
        or ""
    )
    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)."
        )
    return url, key


def _sb_headers(key: str) -> Dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


async def _sb_select_one(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    table: str,
    params: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    endpoint = f"{base_url}/rest/v1/{table}"
    hdrs = _sb_headers(key)
    # Prefer PostgREST "single object" semantics
    hdrs["Accept"] = "application/vnd.pgrst.object+json"
    r = await client.get(endpoint, headers=hdrs, params=params, timeout=30.0)
    if r.status_code == 406:
        # No rows matched (PostgREST returns 406 for object+json when empty)
        return None
    r.raise_for_status()
    return r.json()


async def _sb_patch(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    table: str,
    params: Dict[str, str],
    payload: Dict[str, Any],
    *,
    returning: str = "representation",
) -> Any:
    endpoint = f"{base_url}/rest/v1/{table}"
    hdrs = _sb_headers(key)
    hdrs["Prefer"] = f"return={returning}"
    r = await client.patch(endpoint, headers=hdrs, params=params, json=payload, timeout=30.0)
    r.raise_for_status()
    # representation returns json array (or object), minimal returns empty
    return r.json() if r.text else None


async def _claim_one_job(client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    """
    Claim exactly one sim_ticker row where start_sim='y'.

    We do this in 2 steps (read -> patch) because we’re on REST.
    It’s not perfectly atomic like SQL, but it’s good enough for a single-run worker.
    """
    base_url, key = _sb_env()

    # 1) Find one candidate job
    job = await _sb_select_one(
        client,
        base_url,
        key,
        "sim_ticker",
        params={
            "select": "*",
            "start_sim": "eq.y",
            "limit": "1",
        },
    )
    if not job:
        return None

    symbol = (job.get("symbol") or "").upper()
    run_id = str(uuid.uuid4())
    now = dt.datetime.now(dt.timezone.utc).isoformat()

    # 2) Patch it to running + flip start_sim to 'n'
    updated = await _sb_patch(
        client,
        base_url,
        key,
        "sim_ticker",
        params={"symbol": f"eq.{symbol}"},
        payload={
            "start_sim": "n",
            "status": "running",
            "run_id": run_id,
            "started_at": now,
            "finished_at": None,
            "error_message": None,
        },
        returning="representation",
    )

    # PostgREST returns a list for PATCH with representation
    if isinstance(updated, list) and updated:
        return updated[0]
    if isinstance(updated, dict):
        return updated
    return job


async def _mark_done(client: httpx.AsyncClient, symbol: str) -> None:
    base_url, key = _sb_env()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    await _sb_patch(
        client,
        base_url,
        key,
        "sim_ticker",
        params={"symbol": f"eq.{symbol}"},
        payload={"status": "done", "finished_at": now},
        returning="minimal",
    )


async def _mark_error(client: httpx.AsyncClient, symbol: str, msg: str) -> None:
    base_url, key = _sb_env()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    await _sb_patch(
        client,
        base_url,
        key,
        "sim_ticker",
        params={"symbol": f"eq.{symbol}"},
        payload={"status": "error", "error_message": msg[:2000], "finished_at": now},
        returning="minimal",
    )


# ----------------------------- Worker Main -----------------------------

async def main() -> int:
    # quick env validation early
    _sb_env()

    async with httpx.AsyncClient() as client:
        job = await _claim_one_job(client)
        if not job:
            print("[SIM_WORKER] No sim_ticker rows with start_sim='y'. Exiting.")
            return 0

        symbol = (job.get("symbol") or "").upper()
        seed_date = job.get("seed_date")  # expected 'YYYY-MM-DD' in ET
        sim_period = int(job.get("sim_period") or 0)  # days

        if not symbol or not seed_date or sim_period <= 0:
            msg = f"Invalid job fields: symbol={symbol!r} seed_date={seed_date!r} sim_period={sim_period!r}"
            print(f"[SIM_WORKER] {msg}")
            await _mark_error(client, symbol or "UNKNOWN", msg)
            return 1

        print(f"[SIM_WORKER] Claimed job: symbol={symbol} seed_date={seed_date} sim_period={sim_period}")

        try:
            # Candle engine reads candles from DB via Supabase REST (we’ll patch candle_engine next)
            engine = CandleEngine(symbols=[symbol])

            # Indicator bot in simulation mode (no DB writes)
            bot = IndicatorBot(engine=engine, sim_mode=True)

            # Seed counts per your spec
            seed_counts = {
                "1m": 5000,
                "3m": 2500,
                "5m": 1500,
                "15m": 600,
                "1h": 400,
                "1d": 200,
                "1w": 50,
            }

            seed = await engine.load_seed_from_db(symbol=symbol, seed_date_et=seed_date, counts=seed_counts)
            await bot.bootstrap(symbol, seed)

            # ---------------- SEED LOGS ----------------
            def _ts_str(x: Any) -> str:
                try:
                    if isinstance(x, dt.datetime):
                        t = x
                    else:
                        t = dt.datetime.fromisoformat(str(x))
                    if t.tzinfo is None:
                        t = t.replace(tzinfo=dt.timezone.utc)
                    return t.astimezone(dt.timezone.utc).isoformat()
                except Exception:
                    return str(x)

            print("[SIM][SEED] Seed candle stats (UTC):")
            for tf in sorted(seed_counts.keys(), key=lambda s: (len(s), s)):
                arr = (seed or {}).get(tf) or []
                n = len(arr)
                if n == 0:
                    print(f"[SIM][SEED] {symbol} {tf}: n=0")
                    continue
                first_ts = _ts_str(arr[0].get("ts"))
                last_ts = _ts_str(arr[-1].get("ts"))
                print(f"[SIM][SEED] {symbol} {tf}: n={n} first_ts={first_ts} last_ts={last_ts}")

            # Run sim day-by-day starting next trading day 09:30 ET
            sim_days = await engine.get_sim_days(symbol=symbol, start_after_seed_date_et=seed_date, num_days=sim_period)
            print(f"[SIM_WORKER] Sim days: {sim_days[:3]}{'...' if len(sim_days) > 3 else ''}")

            # ---------------- LIVE SIM LOGS ----------------
            first_live_ts: Dict[str, str] = {}
            last_live_ts: Dict[str, str] = {}

            for d in sim_days:
                async for event in engine.stream_day(symbol=symbol, date_et=d):
                    # event = {"tf": "1m"/"3m"/..., "candle": {...}}
                    try:
                        tf = str(event.get("tf") or "")
                        c = event.get("candle") or {}
                        ts = _ts_str(c.get("ts"))
                        if tf and tf not in first_live_ts:
                            first_live_ts[tf] = ts
                        if tf:
                            last_live_ts[tf] = ts
                    except Exception:
                        pass
                    await bot.on_candle(symbol=symbol, timeframe=event["tf"], candle=event["candle"])

            print("[SIM][LIVE] Live sim candle range (UTC):")
            for tf in sorted(last_live_ts.keys(), key=lambda s: (len(s), s)):
                print(f"[SIM][LIVE] {symbol} {tf}: first_live_ts={first_live_ts.get(tf)} last_live_ts={last_live_ts.get(tf)}")

            # Print final event summary (totals + per timeframe + per day)
            try:
                bot.print_event_summary()
            except Exception as e:
                print(f"[SIM_WORKER] Failed to print event summary: {e}")

            await _mark_done(client, symbol)
            print(f"[SIM_WORKER] DONE: {symbol}")
            return 0

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"[SIM_WORKER] ERROR: {msg}")
            try:
                await _mark_error(client, symbol, msg)
            except Exception as e2:
                print(f"[SIM_WORKER] Failed to mark error in DB: {e2}")
            return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
