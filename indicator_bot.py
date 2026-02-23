import os
import math
import asyncio
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import httpx

from zone_finder import build_symbol_zone_map, upsert_symbol_zone_map

from candle_engine import CandleEngine, SUPPORTED_TFS

from indicator_calc1 import compute_all_indicators

from indicator_calc2 import compute_advanced_extras



from strategies import evaluate_strategies

from liquidity_pool_builder import build_liquidity_pool

# Events engine (NEW)
from spot_event import SpotEventContext, compute_spot_events




EASTERN = dt.timezone(dt.timedelta(hours=-5))  # not critical, mainly for consistency


# ---------------------------------------------------------------------------
# Supabase helper: update indicators columns in spot_tf
# ---------------------------------------------------------------------------


async def update_indicators_in_spot_tf(
    symbol: str,
    timeframe: str,
    trend: Dict[str, Any],
    pivots: Dict[str, Any],
    swings: Dict[str, Any],
    structural: Dict[str, Any],
    fvgs: List[Dict[str, Any]],
    liquidity: Dict[str, Any],
    volume_profile: Dict[str, Any],
    extras: Dict[str, Any],
    extras_advanced: Optional[Dict[str, Any]],
    structure_state: Optional[str],
    strategies: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Patch public.spot_tf for the given symbol+timeframe with the computed indicators.

    Uses Supabase REST. Requires:
      - SUPABASE_URL
      - SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SERVICE_KEY or SUPABASE_KEY
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        print("[INDICATORS][DB] Skipping update: Supabase env vars missing")
        return

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_tf"
    params = {
        "symbol": f"eq.{symbol}",
        "timeframe": f"eq.{timeframe}",
    }
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "return=minimal",
    }

    payload: Dict[str, Any] = {
        "trend": trend,
        "pivots": pivots,
        "swings": swings,
        "structural": structural,
        "fvgs": fvgs,
        "liquidity": liquidity,
        "volume_profile": volume_profile,
        "extras": extras,
        "extras_advanced": extras_advanced,
        "last_updated": dt.datetime.now(dt.timezone.utc).isoformat(),
    }

    if structure_state is not None:
        payload["structure_state"] = structure_state
    if strategies is not None:
        payload["strategies"] = strategies

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.patch(
                endpoint,
                params=params,
                json=payload,
                headers=headers,
            )
            if resp.status_code >= 400:
                print(
                    f"[INDICATORS][DB] Failed to update indicators for "
                    f"{symbol} {timeframe} (status={resp.status_code})"
                )
                print(f"[INDICATORS][DB] Response text: {resp.text}")
                resp.raise_for_status()
        except httpx.HTTPError as e:
            print(f"[INDICATORS][DB] HTTP error for {symbol} {timeframe}: {e}")
        except Exception as e:
            print(f"[INDICATORS][DB] Unexpected error for {symbol} {timeframe}: {e}")


async def fetch_spot_tf_rows_for_symbol(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch all spot_tf rows for a symbol across all timeframes.
    Used by ZoneFinder to build a unified (Option A) zone map.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_* key env vars")

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_tf"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    params = {
        "symbol": f"eq.{symbol.upper()}",
        "select": "*",
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(endpoint, headers=headers, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"spot_tf fetch failed ({r.status_code}): {r.text}")
        return r.json()



async def upsert_spot_liquidity_pool(
    symbol: str,
    asof: str,
    pool_version: str,
    levels: List[Dict[str, Any]],
    stats: Dict[str, Any],
    tol_price: Optional[float] = None,
) -> None:
    """Upsert current liquidity pool snapshot into public.spot_liquidity_pool."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        print("[LIQ_POOL][DB] Skipping upsert: Supabase env vars missing")
        return

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_liquidity_pool"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    payload: Dict[str, Any] = {
        "symbol": symbol.upper(),
        "asof": asof,
        "pool_version": pool_version,
        "levels": levels,
        "stats": stats,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if tol_price is not None:
        payload["tol_price"] = tol_price

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(endpoint, json=payload, headers=headers)
        if resp.status_code >= 400:
            print(f"[LIQ_POOL][DB] Upsert failed for {symbol} (status={resp.status_code})")
            print(f"[LIQ_POOL][DB] Response text: {resp.text}")
            resp.raise_for_status()


# ---------------------------------------------------------------------------
# Supabase helper: fetch liquidity pool row (for sweep detection)
# ---------------------------------------------------------------------------
async def fetch_spot_liquidity_pool_row(symbol: str) -> Optional[Dict[str, Any]]:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )
    if not supabase_url or not supabase_key:
        return None

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_liquidity_pool"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    params = {
        "symbol": f"eq.{symbol.upper()}",
        "select": "*",
        "order": "updated_at.desc",
        "limit": "1",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.get(endpoint, headers=headers, params=params)
            if r.status_code != 200:
                return None
            rows = r.json() or []
            return rows[0] if rows else None
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Supabase helper: upsert spot_events row (verification/audit)
# ---------------------------------------------------------------------------
async def upsert_spot_events_row(
    symbol: str,
    timeframe: str,
    events_latest: Dict[str, Any],
    events_active: Dict[str, Any],
    events_recent: List[Dict[str, Any]],
) -> None:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        print("[EVENTS][DB] Skipping upsert: Supabase env vars missing")
        return

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_events"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    payload: Dict[str, Any] = {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "last_updated": dt.datetime.now(dt.timezone.utc).isoformat(),
        "events_latest": events_latest,
        "events_active": events_active,
        "events_recent": events_recent,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(endpoint, json=payload, headers=headers)
        if resp.status_code >= 400:
            print(f"[EVENTS][DB] Upsert failed for {symbol} {timeframe} (status={resp.status_code})")
            print(f"[EVENTS][DB] Response text: {resp.text}")
            resp.raise_for_status()







# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
async def needs_backfill_structure_state(symbol: str, timeframe: str) -> bool:
    """
    Return True if spot_tf.structure_state is NULL/empty for this symbol+timeframe.
    Used to force a one-time indicator backfill even when the last candle ts
    has not changed (e.g. after deleting/recreating spot_tf rows).
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        # Can't check; don't force backfill in this case.
        return False

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_tf"
    params = {
        "symbol": f"eq.{symbol}",
        "timeframe": f"eq.{timeframe}",
        "select": "structure_state",
    }
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(endpoint, params=params, headers=headers)
            resp.raise_for_status()
            rows = resp.json() or []
        except Exception as e:
            print(
                f"[INDICATORS][DB] Failed to check structure_state for "
                f"{symbol} {timeframe}: {e}"
            )
            return False

    if not rows:
        # No row → don't force backfill here (engine should create rows first).
        return False

    ss = rows[0].get("structure_state")

    # Treat NULL, empty string, or purely whitespace as "missing"
    if ss is None:
        return True
    if isinstance(ss, str) and not ss.strip():
        return True

    return False



def _ensure_dt(ts_raw: Any) -> dt.datetime:
    """
    Try to convert whatever 'ts' is into an aware UTC datetime.
    CandleEngine usually stores ISO strings or datetime already.
    """
    if isinstance(ts_raw, dt.datetime):
        ts = ts_raw
    elif isinstance(ts_raw, str):
        try:
            ts = dt.datetime.fromisoformat(ts_raw)
        except Exception:
            ts = dt.datetime.now(dt.timezone.utc)
    else:
        ts = dt.datetime.now(dt.timezone.utc)

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts = ts.astimezone(dt.timezone.utc)
    return ts


def _tf_to_timedelta(tf: str) -> Optional[dt.timedelta]:
    """
    Convert a timeframe string like '1m', '5m', '15m', '1h', '1d' or plain
    digits like '5', '15', '60' into a timedelta. If we can't parse it,
    return None and the caller can fall back to existing behavior.
    """
    if not tf:
        return None

    s = str(tf).strip().lower()

    # Explicit suffixes
    if s.endswith("m"):  # minutes
        try:
            mins = int(s[:-1])
            return dt.timedelta(minutes=mins)
        except ValueError:
            return None

    if s.endswith("h"):  # hours
        try:
            hours = int(s[:-1])
            return dt.timedelta(hours=hours)
        except ValueError:
            return None

    if s.endswith("d"):  # days
        try:
            days = int(s[:-1])
            return dt.timedelta(days=days)
        except ValueError:
            return None

    # Plain digits → treat as minutes (e.g. "5", "15", "60")
    if s.isdigit():
        try:
            mins = int(s)
            return dt.timedelta(minutes=mins)
        except ValueError:
            return None

    return None



# ---------------------------------------------------------------------------
# VP enrichment (prop-desk style context ranker)
# ---------------------------------------------------------------------------

TF_ORDER = ["1m", "3m", "5m", "15m", "1h", "1d", "1w"]
TF_WEIGHT = {"1m": 0.20, "3m": 0.25, "5m": 0.30, "15m": 0.35, "1h": 0.45, "1d": 0.60, "1w": 0.75}

def _tf_higher_or_equal(current_tf: str) -> List[str]:
    try:
        i = TF_ORDER.index(current_tf)
    except ValueError:
        return []
    return TF_ORDER[i:]

def _tf_higher(current_tf: str) -> List[str]:
    try:
        i = TF_ORDER.index(current_tf)
    except ValueError:
        return []
    return TF_ORDER[i+1:]

def _range_overlaps(a_low: float, a_high: float, b_low: float, b_high: float) -> bool:
    lo1, hi1 = (a_low, a_high) if a_low <= a_high else (a_high, a_low)
    lo2, hi2 = (b_low, b_high) if b_low <= b_high else (b_high, b_low)
    return not (hi1 < lo2 or hi2 < lo1)

def _level_in_range(level: float, low: float, high: float, pad: float = 0.0) -> bool:
    lo, hi = (low, high) if low <= high else (high, low)
    return (lo - pad) <= level <= (hi + pad)

def _trend_state(snapshot: Optional[Dict[str, Any]]) -> str:
    try:
        st = (snapshot or {}).get("trend", {}).get("state") or "unknown"
        return str(st)
    except Exception:
        return "unknown"

def _regime_state(snapshot: Optional[Dict[str, Any]]) -> str:
    # Simple proxy: if trend is range -> balance; else expansion.
    st = _trend_state(snapshot)
    if st == "range":
        return "balance"
    if st in ("bull", "bear"):
        return "expansion"
    return "unknown"

def _iter_active_fvgs(snapshot: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fvgs = (snapshot or {}).get("fvgs") or []
    out = []
    for f in fvgs:
        try:
            if f.get("filled") is True:
                continue
            # If trade_score exists and is 0, skip (fully mitigated or invalidated)
            if "trade_score" in f and float(f.get("trade_score") or 0.0) <= 0.0:
                continue
            out.append(f)
        except Exception:
            continue
    return out

def _iter_liq_levels(snapshot: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    liq = (snapshot or {}).get("liquidity") or {}
    levels = liq.get("levels") or []
    return [lv for lv in levels if isinstance(lv, dict) and "price" in lv]

def _enrich_vp_for_tf(
    *,
    current_tf: str,
    current_snapshot: Dict[str, Any],
    snapshots_by_tf: Dict[str, Dict[str, Any]],
) -> None:
    vp = (current_snapshot or {}).get("volume_profile") or {}
    profiles = vp.get("profiles") or {}
    if not isinstance(profiles, dict):
        return

    cur_trend = _trend_state(current_snapshot)
    cur_regime = _regime_state(current_snapshot)

    # Small helper to compute context multipliers + tags
    def _context_for_node(node: Dict[str, Any], *, node_kind: str) -> Tuple[float, List[str]]:
        low = float(node.get("low", 0.0))
        high = float(node.get("high", 0.0))
        tags: List[str] = []
        mult = 1.0

        # Regime preference (cheap but useful)
        if cur_regime == "balance":
            if node_kind == "hvn":
                mult *= 1.08
                tags.append("regime_balance_hvn")
            else:
                mult *= 0.92
                tags.append("regime_balance_lvn_penalty")
        elif cur_regime == "expansion":
            if node_kind == "lvn":
                mult *= 1.08
                tags.append("regime_expansion_lvn")
            else:
                mult *= 0.93
                tags.append("regime_expansion_hvn_penalty")

        # Multi-TF confluence: trend + FVG + liquidity
        for htf in _tf_higher_or_equal(current_tf):
            snap = snapshots_by_tf.get(htf)
            if not snap:
                continue

            w = TF_WEIGHT.get(htf, 0.35)

            # Trend alignment
            htf_trend = _trend_state(snap)
            if htf_trend in ("bull", "bear") and cur_trend in ("bull", "bear"):
                if htf_trend == cur_trend:
                    mult *= (1.0 + 0.10 * w)   # up to ~+7.5% for 1w
                    tags.append(f"trend_aligned_{htf}")
                else:
                    mult *= (1.0 - 0.18 * w)   # up to ~-13.5% for 1w
                    tags.append(f"trend_counter_{htf}")
            elif htf_trend == "range":
                # weekly range especially: penalize LVN breakouts (chop risk)
                if htf == "1w" and node_kind == "lvn":
                    mult *= 0.90
                    tags.append("weekly_balance_lvn_penalty")

            # FVG overlap
            fvgs = _iter_active_fvgs(snap)
            for f in fvgs:
                try:
                    fl = float(f.get("low"))
                    fh = float(f.get("high"))
                    if _range_overlaps(low, high, fl, fh):
                        # HTF overlap is more meaningful; weight it gently
                        mult *= (1.0 + 0.12 * w)
                        tags.append(f"fvg_overlap_{htf}")
                        break
                except Exception:
                    continue

            # Liquidity overlap (price level inside node)
            for lv in _iter_liq_levels(snap):
                try:
                    p = float(lv.get("price"))
                    if _level_in_range(p, low, high, pad=0.0):
                        # intact levels are better than broken levels
                        st = str(lv.get("state") or "")
                        bump = 0.08 if st == "intact" else 0.04
                        mult *= (1.0 + bump * w)
                        tags.append(f"liq_overlap_{htf}")
                        break
                except Exception:
                    continue

        # Clamp to keep it sane
        mult = max(0.60, min(1.80, mult))
        return mult, tags

    # Apply enrichment per profile, and re-rank by final_score
    for profile_name, p in profiles.items():
        if not isinstance(p, dict):
            continue

        for node_kind, key in (("hvn", "hvn_ranges"), ("lvn", "lvn_ranges")):
            nodes = p.get(key) or []
            if not isinstance(nodes, list) or not nodes:
                continue

            for nd in nodes:
                if not isinstance(nd, dict):
                    continue
                base = float(nd.get("base_score") or 0.0)
                cm, tags = _context_for_node(nd, node_kind=node_kind)
                nd["context_mult"] = float(cm)
                nd["final_score"] = float(base * cm)
                nd["tags"] = tags

            # re-rank by final_score
            nodes.sort(key=lambda x: float((x or {}).get("final_score") or 0.0), reverse=True)
            for r, nd in enumerate(nodes, start=1):
                if isinstance(nd, dict):
                    nd["rank"] = r
            p[key] = nodes

        # profile-level meta
        p.setdefault("meta", {})
        if isinstance(p["meta"], dict):
            p["meta"]["rank_basis"] = "final_score"
            p["meta"]["context_algo"] = "multi_tf_confluence_v1"

    current_snapshot["volume_profile"] = vp




# ---------------------------------------------------------------------------
# IndicatorBot
# ---------------------------------------------------------------------------

class IndicatorBot:
    """
    Reads enriched candles from CandleEngine and keeps spot_tf indicators fresh
    for all SUPPORTED_TFS.

    v1 responsibilities:
      - For each (symbol, timeframe):
          * detect if there's a new last candle (ts changed)
          * compute:
              - trend (EMA50/EMA200 + distances + state)
              - swings (HH/HL/LH/LL pivots)
              - fvgs (simple 3-candle FVGs)
              - liquidity (levels from swings)
              - volume_profile (simple price/volume bins + POC)
              - extras (ATR, vol regime)
              - structure_state (trend + swings summary)
          * patch those into spot_tf via Supabase REST.
    """

    def __init__(
        self,
        engine: CandleEngine,
        timeframes: Optional[List[str]] = None,
    ):
        self.engine = engine
        self.timeframes = timeframes or list(SUPPORTED_TFS)
        # last_processed_ts[symbol][tf] -> datetime of last processed candle
        self.last_processed_ts: Dict[str, Dict[str, dt.datetime]] = {}
        # last_snapshots[symbol][tf] -> most recent snapshot dict (for multi-TF VP context)
        self.last_snapshots: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # Events cache (NEW): previous spot_events state per symbol/tf
        self.last_events_state: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # FVG previous snapshot cache for transition detection (NEW)
        self.last_fvgs_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

        # Liquidity pool cache per symbol (NEW)
        self.liq_pool_cache: Dict[str, Dict[str, Any]] = {}



    

    # ------------------------------------------------------------------ #
    # Public loop
    # ------------------------------------------------------------------ #

    async def run_loop(self, interval_seconds: int = 60) -> None:
        """
        Periodically scan all symbols/timeframes and update indicators
        whenever a new candle has appeared.
        """
        print(f"[INDICATORS] Starting indicator bot (interval={interval_seconds}s)")

        while True:
            try:
                await self._update_all_symbols()
            except Exception as e:
                print(f"[INDICATORS] Exception in run_loop: {e}")
            await asyncio.sleep(interval_seconds)

    # ------------------------------------------------------------------ #
    # Core update logic
    # ------------------------------------------------------------------ #

    
    async def _update_all_symbols(self) -> None:
        symbols = getattr(self.engine, "symbols", [])
        if not symbols:
            print("[INDICATORS] No symbols in engine yet, skipping cycle")
            return
    
        for sym in symbols:
            sym_upper = sym.upper()
            symbol_candles = self.engine.candles.get(sym_upper, {})

            # Ensure we have liquidity pool row cached for sweep detection (NEW)
            if sym_upper not in self.liq_pool_cache:
                row = await fetch_spot_liquidity_pool_row(sym_upper)
                if row:
                    self.liq_pool_cache[sym_upper] = row
    
            # Collect all TF updates first so we can do multi-TF VP enrichment
            pending: Dict[str, Dict[str, Any]] = {}
    
            for tf in self.timeframes:
                candles = symbol_candles.get(tf)
                if not candles or len(candles) < 10:
                    continue
    
                # --------------------------------------------------
                # CLOSED-CANDLE FILTER
                # --------------------------------------------------
                tf_delta = _tf_to_timedelta(tf)
                now_utc = dt.datetime.now(dt.timezone.utc)
    
                closed_candles = candles
                if tf_delta is not None:
                    raw_last = candles[-1]
                    raw_last_ts = _ensure_dt(raw_last.get("ts"))
                    if now_utc < (raw_last_ts + tf_delta):
                        closed_candles = candles[:-1]
    
                if not closed_candles or len(closed_candles) < 10:
                    continue
    
                last_candle = closed_candles[-1]
                ts_dt = _ensure_dt(last_candle.get("ts"))
    
                prev_ts = self.last_processed_ts.get(sym_upper, {}).get(tf)
                needs_backfill = await needs_backfill_structure_state(sym_upper, tf)
    
                if (not needs_backfill) and (prev_ts is not None) and (ts_dt <= prev_ts):
                    continue
    
                # Session candles (for advanced extras)
                session_date = last_candle.get("date_et")
                if session_date:
                    session_candles = [c for c in closed_candles if c.get("date_et") == session_date]
                else:
                    session_candles = closed_candles
    
                # Base indicators (single pass)
                snapshot = compute_all_indicators(closed_candles)
    
                # Advanced extras (needs base_snapshot)
                advanced = compute_advanced_extras(
                    candles=closed_candles,
                    base_snapshot=snapshot,
                    htf_snapshot=None,
                    session_candles=session_candles,
                )
    
                pending[tf] = {
                    "closed_candles": closed_candles,
                    "session_candles": session_candles,
                    "snapshot": snapshot,
                    "extras_advanced": advanced,
                    "last_candle": last_candle,
                    "ts_dt": ts_dt,
                }
    
            if not pending:
                continue
    
            # Build multi-TF snapshot map (use cached snapshots for TFs that didn't update this cycle)
            snap_map: Dict[str, Dict[str, Any]] = {}
            snap_map.update(self.last_snapshots.get(sym_upper, {}))
            for tf, pack in pending.items():
                snap_map[tf] = pack["snapshot"]
    
            # Enrich VP for each pending TF with multi-TF context
            for tf, pack in pending.items():
                try:
                    _enrich_vp_for_tf(
                        current_tf=tf,
                        current_snapshot=pack["snapshot"],
                        snapshots_by_tf=snap_map,
                    )
                except Exception as e:
                    print(f"[VP][ENRICH] Failed for {sym_upper} {tf}: {e}")
    
            # Now write to DB (and update caches)
            for tf, pack in pending.items():
                snapshot = pack["snapshot"]
                last_candle = pack["last_candle"]
                ts_dt = pack["ts_dt"]
    
                trend = snapshot["trend"]
                pivots = snapshot["pivots"]
                swings = snapshot["swings"]
                structural = snapshot["structural"]
                fvgs = snapshot["fvgs"]
                liquidity = snapshot["liquidity"]
                volume_profile = snapshot["volume_profile"]
                extras = snapshot["extras"]
                structure_state = snapshot["structure_state"]
                extras_advanced = pack["extras_advanced"]
    
                cluster = last_candle.get("cluster") or {}
    
                strategies = evaluate_strategies(
                    symbol=sym_upper,
                    timeframe=tf,
                    candles=pack["closed_candles"],
                    swings=swings,
                    fvgs=fvgs,
                    liquidity=liquidity,
                    trend=trend,
                    cluster=cluster,
                    volume_profile=volume_profile,
                    htf_swings=None,
                )

                # ---------------- EVENTS (NEW) ----------------
                prev_state = self.last_events_state.get(sym_upper, {}).get(tf) or {}
                prev_latest = prev_state.get("events_latest")
                prev_active = prev_state.get("events_active")
                prev_recent = prev_state.get("events_recent")

                fvgs_prev = self.last_fvgs_cache.get(sym_upper, {}).get(tf)
                liq_row = self.liq_pool_cache.get(sym_upper)

                # Build reference lists from YOUR stored shapes:
                # swings: {"asof":..., "swings":[{ts,type,price,state,...}, ...]}
                swing_items = (swings or {}).get("swings") or []
                swing_highs = [
                    {"ts": s.get("ts"), "price": s.get("price")}
                    for s in swing_items
                    if s.get("type") == "swing_high" and s.get("state") == "active"
                ]
                swing_lows = [
                    {"ts": s.get("ts"), "price": s.get("price")}
                    for s in swing_items
                    if s.get("type") == "swing_low" and s.get("state") == "active"
                ]

                # structural: {"asof":..., "points":[{ts,type,label,price,...}, ...]}
                structural_points = (structural or {}).get("points") or []
                structural_highs = [
                    {"ts": p.get("ts"), "price": p.get("price")}
                    for p in structural_points
                    if p.get("type") == "swing_high" and p.get("label") in ("HH", "LH")
                ]
                structural_lows = [
                    {"ts": p.get("ts"), "price": p.get("price")}
                    for p in structural_points
                    if p.get("type") == "swing_low" and p.get("label") in ("LL", "HL")
                ]

                ev_ctx = SpotEventContext(
                    symbol=sym_upper,
                    timeframe=tf,
                    last_candle=last_candle,
                    liquidity_pool_row=liq_row,
                    structural_highs=structural_highs,
                    structural_lows=structural_lows,
                    swing_highs=swing_highs,
                    swing_lows=swing_lows,
                    structure_state=structure_state or "",
                    fvgs_now=fvgs,
                    fvgs_prev=fvgs_prev,
                    prev_events_latest=prev_latest,
                    prev_events_active=prev_active,
                    prev_events_recent=prev_recent,
                )

                ev_out = compute_spot_events(ev_ctx)
    
                await update_indicators_in_spot_tf(
                    symbol=sym_upper,
                    timeframe=tf,
                    trend=trend,
                    pivots=pivots,
                    swings=swings,
                    structural=structural,
                    fvgs=fvgs,
                    liquidity=liquidity,
                    volume_profile=volume_profile,
                    extras=extras,
                    extras_advanced=extras_advanced,
                    structure_state=structure_state,
                    strategies=strategies,
                )

                # Write spot_events only if changed (NEW)
                if ev_out.get("changed"):
                    await upsert_spot_events_row(
                        symbol=sym_upper,
                        timeframe=tf,
                        events_latest=ev_out["events_latest"],
                        events_active=ev_out["events_active"],
                        events_recent=ev_out["events_recent"],
                    )

                # Update caches (NEW)
                self.last_events_state.setdefault(sym_upper, {})[tf] = {
                    "events_latest": ev_out["events_latest"],
                    "events_active": ev_out["events_active"],
                    "events_recent": ev_out["events_recent"],
                }
                self.last_fvgs_cache.setdefault(sym_upper, {})[tf] = list(fvgs or [])
    
                # Cache for next cycle's multi-TF enrichment
                self.last_snapshots.setdefault(sym_upper, {})[tf] = snapshot
                self.last_processed_ts.setdefault(sym_upper, {})[tf] = ts_dt
    
                print(
                    f"[INDICATORS] Updated {sym_upper} {tf} "
                    f"(ts={ts_dt.isoformat()}, trend={trend.get('state')})"
                )
    
            # ---------------- ZONE FINDER (Option A: all TFs -> one symbol map) ----------------
            try:
                spot_rows = await fetch_spot_tf_rows_for_symbol(sym_upper)

                pool = build_liquidity_pool(
                    symbol=sym_upper,
                    spot_tf_rows=spot_rows,
                    candle_engine=self.engine,
                )
                await upsert_spot_liquidity_pool(
                    symbol=sym_upper,
                    asof=pool["asof"],
                    pool_version=pool.get("pool_version") or "liq_pool_v1",
                    levels=pool.get("levels") or [],
                    stats=pool.get("stats") or {},
                    tol_price=pool.get("tol_price"),
                )
                print(f"[LIQ_POOL] Updated spot_liquidity_pool for {sym_upper} (levels={len(pool.get('levels') or [])})")

                # Refresh liquidity pool cache from computed pool (NEW)
                self.liq_pool_cache[sym_upper] = {
                    "symbol": sym_upper,
                    "asof": pool.get("asof"),
                    "pool_version": pool.get("pool_version") or "liq_pool_v1",
                    "levels": pool.get("levels") or [],
                    "stats": pool.get("stats") or {},
                    "tol_price": pool.get("tol_price"),
                    "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                }

    
                zone_map = build_symbol_zone_map(
                    symbol=sym_upper,
                    spot_tf_rows=spot_rows,
                    candle_engine=self.engine,
                )
    
                await upsert_symbol_zone_map(
                    zone_map=zone_map,
                    table="zone_finder",
                )
    
                print(f"[ZONE_FINDER] Updated zone_finder row for {sym_upper}")
    
            except Exception as e:
                print(f"[ZONE_FINDER] Failed for {sym_upper}: {e}")
