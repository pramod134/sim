import os
import asyncio
import datetime as dt
from typing import Dict, List, Any, Optional

import httpx
from zoneinfo import ZoneInfo

# Import your existing Alpaca market data module
import market_data  # make sure this is on PYTHONPATH

EASTERN = ZoneInfo("America/New_York")

# Timeframes we manage live with Twelve Data
LIVE_TFS = ["1m", "3m", "5m", "15m", "1h"]

# All supported timeframes for API / storage
SUPPORTED_TFS = ["1m", "3m", "5m", "15m", "1h", "1d", "1w"]

# ---- Candle classification config -----------------------------------------

ATR_PERIOD = 14          # for mom_atr
VOL_LOOKBACK = 20        # for vol_rel

BODY_TINY = 0.10         # 10% of range
BODY_SMALL = 0.25        # 25% of range
BODY_LARGE = 0.60        # 60% of range

WICK_LONG = 0.60         # 60% of range
WICK_SHORT = 0.20        # 20% of range
WICK_DOMINANT = 2.0      # wick >= 2x body

VOL_HIGH = 2.0           # 2x avg volume
VOL_LOW = 0.5            # 0.5x avg volume


# ---- RTH Helpers -----------------------------------------------------------

def is_rth_timestamp(ts_utc: dt.datetime) -> bool:
    """
    Return True if timestamp is within US RTH (Mon–Fri, 09:30–16:00 ET).
    """
    ts_et = ts_utc.astimezone(EASTERN)
    if ts_et.weekday() > 4:  # Sat/Sun
        return False
    t = ts_et.time()
    # 09:30 <= t < 16:00
    if t.hour < 9 or t.hour > 16:
        return False
    if t.hour == 9 and t.minute < 30:
        return False
    if t.hour == 16:
        # any time at/after 16:00:00 is not RTH
        if t.minute > 0 or t.second > 0:
            return False
        return False
    return True


def is_rth_now() -> bool:
    """
    Decide whether the live update loop should run right now.
    We widen the window to:
      - Start:  09:25 ET
      - End:    16:05 ET

    IMPORTANT:
    We do NOT change is_rth_timestamp(ts), so individual candles
    are still filtered to strict RTH (09:30–16:00). This lets the
    engine run slightly early/late but still ignore pre/post-market data.
    """
    now_utc = dt.datetime.now(dt.timezone.utc)
    now_et = now_utc.astimezone(EASTERN)

    weekday = now_et.weekday()
    hour = now_et.hour
    minute = now_et.minute

    # Weekends → never run the loop
    if weekday >= 5:
        return False

    # Start running at 09:25 ET
    if hour < 9 or (hour == 9 and minute < 25):
        return False

    # Stop running at 16:05 ET
    if hour > 16 or (hour == 16 and minute > 5):
        return False

    return True



# ---- Bucket helpers for aggregation ---------------------------------------

def _bucket_start_3m(ts_et: dt.datetime) -> dt.datetime:
    minute = (ts_et.minute // 3) * 3
    return ts_et.replace(minute=minute, second=0, microsecond=0)


def _bucket_start_5m(ts_et: dt.datetime) -> dt.datetime:
    minute = (ts_et.minute // 5) * 5
    return ts_et.replace(minute=minute, second=0, microsecond=0)


def _bucket_start_15m(ts_et: dt.datetime) -> dt.datetime:
    minute = (ts_et.minute // 15) * 15
    return ts_et.replace(minute=minute, second=0, microsecond=0)


def _bucket_start_1h(ts_et: dt.datetime) -> dt.datetime:
    """
    Align hourly bars to 09:30, 10:30, 11:30, ...
    Using minutes since 09:30 as anchor.
    """
    open_minutes = 9 * 60 + 30
    total_minutes = ts_et.hour * 60 + ts_et.minute
    delta = max(0, total_minutes - open_minutes)
    bucket_index = delta // 60
    bucket_start_total = open_minutes + bucket_index * 60
    hour = bucket_start_total // 60
    minute = bucket_start_total % 60
    return ts_et.replace(hour=hour, minute=minute, second=0, microsecond=0)


def bucket_start(ts_utc: dt.datetime, tf: str) -> dt.datetime:
    """
    Given a UTC timestamp and timeframe key ('3m','5m','15m','1h'),
    return the bucket start as a UTC datetime.
    """
    ts_et = ts_utc.astimezone(EASTERN)
    if tf == "3m":
        start_et = _bucket_start_3m(ts_et)
    elif tf == "5m":
        start_et = _bucket_start_5m(ts_et)
    elif tf == "15m":
        start_et = _bucket_start_15m(ts_et)
    elif tf == "1h":
        start_et = _bucket_start_1h(ts_et)
    else:
        raise ValueError(f"Unsupported bucket timeframe: {tf}")
    return start_et.astimezone(dt.timezone.utc)


def tf_to_timedelta(tf: str) -> Optional[dt.timedelta]:
    """
    Convert a timeframe string like '1m', '3m', '5m', '15m', '1h', '1d'
    into a datetime.timedelta. If we can't parse it, return None.
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




# ---- Supabase helpers ------------------------------------------------------

async def update_last_candle_in_spot_tf(symbol: str, timeframe: str, candle: Dict[str, Any]) -> None:
    """
    Update public.spot_tf.last_candle for the given symbol+timeframe.

    This uses Supabase REST API. Expect env vars:
      - SUPABASE_URL
      - SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SERVICE_KEY or SUPABASE_KEY

    If these are not set, this function is a no-op.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        # No DB config; skip updating silently.
        print("[DB] Skipping update_last_candle_in_spot_tf: Supabase env vars missing")
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
        # not required, but cleaner: don't ask Supabase to return row data
        "Prefer": "return=minimal",
    }

    payload = {"last_candle": candle}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.patch(endpoint, params=params, json=payload, headers=headers)
            if resp.status_code >= 400:
                # Print everything we need to debug
                print(
                    f"[DB] Failed to update last_candle for {symbol} {timeframe} "
                    f"(status={resp.status_code})"
                )
                print(f"[DB] Response text: {resp.text}")
                resp.raise_for_status()
        except httpx.HTTPError as e:
            print(f"[DB] HTTP error updating last_candle for {symbol} {timeframe}: {e}")
        except Exception as e:
            print(f"[DB] Unexpected error updating last_candle for {symbol} {timeframe}: {e}")


async def load_symbols_from_spot_tf() -> List[str]:
    """
    Fetch distinct symbols from public.spot_tf using Supabase REST API.
    Requires:
        SUPABASE_URL
        SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SERVICE_KEY or SUPABASE_KEY
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        print("[WARN] Supabase env vars missing — fallback to no symbols.")
        return []

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_tf"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
    }

    params = {
        "select": "symbol",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(endpoint, headers=headers, params=params)
            resp.raise_for_status()
            rows = resp.json()

            # Extract symbols & remove empties / duplicates
            symbols = sorted(
                {row.get("symbol", "").upper() for row in rows if row.get("symbol")}
            )
            print(f"[SYMBOLS] Loaded from spot_tf: {symbols}")
            return symbols

        except Exception as e:
            print(f"[ERROR] Failed to load symbols from spot_tf: {e}")
            return []


async def load_symbols_with_missing_last_candle() -> List[str]:
    """
    Fetch symbols from public.spot_tf where last_candle is NULL.
    We treat these as needing a baseline backfill from Alpaca.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        print("[WARN] Supabase env vars missing — cannot check missing last_candle.")
        return []

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_tf"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
    }
    params = {
        "select": "symbol,last_candle",
        "last_candle": "is.null",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(endpoint, headers=headers, params=params)
            resp.raise_for_status()
            rows = resp.json()

            symbols = sorted(
                {row.get("symbol", "").upper() for row in rows if row.get("symbol")}
            )
            if symbols:
                print(f"[SYMBOLS] Missing last_candle (needs backfill): {symbols}")
            return symbols

        except Exception as e:
            print(f"[ERROR] Failed to load symbols with missing last_candle: {e}")
            return []



class CandleEngine:
    """
    Maintains RTH-only candles for multiple symbols and timeframes, using:
      - Alpaca (via market_data) for baseline history
      - Twelve Data 1min for live updates every few minutes
    """

    def __init__(self, twelve_data_api_key: str, symbols: List[str]):
        self.td_api_key = twelve_data_api_key
        self.symbols = sorted({s.upper() for s in symbols if s.strip()})

        # candles[symbol][tf] -> list of enriched candle dicts
        self.candles: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        # latest_ts[symbol][tf] -> dt.datetime of last candle
        self.latest_ts: Dict[str, Dict[str, Optional[dt.datetime]]] = {}



        # ---- Micro-cluster (last few candles behavior) ------------------------

    def _compute_cluster(
        self,
        window_candles: List[Dict[str, Any]],
        target_lookback: int = 5,
    ) -> Dict[str, Any]:
        """
        Compute a 'cluster' summary describing how the last few candles behaved.
        Returns fields:
            bull_count, bear_count, net_change_pct,
            avg_mom_atr, avg_vol_rel, state
        """
        if not window_candles:
            return {
                "lookback": 0,
                "bull_count": 0,
                "bear_count": 0,
                "net_change_pct": None,
                "avg_mom_atr": None,
                "avg_vol_rel": None,
                "state": "neutral",
            }

        lookback = min(target_lookback, len(window_candles))
        candles = window_candles[-lookback:]

        bull_count = 0
        bear_count = 0
        closes = []
        mom_vals = []
        vol_vals = []

        for c in candles:
            o = float(c["open"])
            cl = float(c["close"])
            closes.append(cl)

            if cl > o:
                bull_count += 1
            elif cl < o:
                bear_count += 1

            mr = c.get("mom_atr")
            if mr is not None:
                try:
                    mom_vals.append(float(mr))
                except:
                    pass

            vr = c.get("vol_rel")
            if vr is not None:
                try:
                    vol_vals.append(float(vr))
                except:
                    pass

        net_change_pct = None
        if len(closes) >= 2 and closes[0] != 0:
            net_change_pct = (closes[-1] - closes[0]) / closes[0]

        avg_mom_atr = sum(mom_vals) / len(mom_vals) if mom_vals else None
        avg_vol_rel = sum(vol_vals) / len(vol_vals) if vol_vals else None

        # ---- Classification thresholds ----
        thr_momo = 0.003
        thr_chop = 0.002
        thr_range = 0.0015
        trend_ratio = 0.6

        bull_ratio = bull_count / lookback
        bear_ratio = bear_count / lookback

        state = "neutral"
        pos = lambda x: x is not None and x > 0
        neg = lambda x: x is not None and x < 0

        if (
            net_change_pct is not None
            and net_change_pct > thr_momo
            and bull_ratio >= trend_ratio
            and pos(avg_mom_atr)
        ):
            state = "bull_momo"

        elif (
            net_change_pct is not None
            and net_change_pct < -thr_momo
            and bear_ratio >= trend_ratio
            and neg(avg_mom_atr)
        ):
            state = "bear_momo"

        elif net_change_pct is not None:
            low_vol = avg_vol_rel is not None and avg_vol_rel < 1.0
            very_low_vol = avg_vol_rel is not None and avg_vol_rel < 0.7

            if abs(net_change_pct) < thr_range and very_low_vol:
                state = "range_low_vol"
            elif abs(net_change_pct) < thr_chop:
                state = "chop"
            elif net_change_pct > 0 and low_vol:
                state = "bull_drift"
            elif net_change_pct < 0 and low_vol:
                state = "bear_drift"
            else:
                state = "neutral"

        return {
            "lookback": lookback,
            "bull_count": bull_count,
            "bear_count": bear_count,
            "net_change_pct": net_change_pct,
            "avg_mom_atr": avg_mom_atr,
            "avg_vol_rel": avg_vol_rel,
            "state": state,
        }



    def _get_last_closed_candle(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Return the last *closed* candle for (symbol, timeframe).

        Logic:
          - If no candles → None
          - For 1d / 1w, we assume all loaded candles are closed → return last
          - For intraday TFs:
              * Use tf_to_timedelta to get the bar duration
              * If now >= ts + duration → last candle is closed
              * Else, fall back to previous candle if it exists
        """
        sym = symbol.upper()
        tf_candles = self.candles.get(sym, {}).get(timeframe, [])
        if not tf_candles:
            return None

        # Daily / weekly: all bars from Alpaca / history are closed
        if timeframe in ("1d", "1w"):
            return tf_candles[-1]

        delta = tf_to_timedelta(timeframe)
        if delta is None:
            # Unknown format; just return the last one as-is
            return tf_candles[-1]

        now_utc = dt.datetime.now(dt.timezone.utc)

        last = tf_candles[-1]
        try:
            ts = dt.datetime.fromisoformat(last["ts"])
        except Exception:
            # If ts is weird, just treat it as closed
            return last

        # If current time has passed the end of this bar, it's closed.
        if now_utc >= ts + delta:
            return last

        # Otherwise, this bar is still forming → use previous if available
        if len(tf_candles) >= 2:
            return tf_candles[-2]

        # No previous bar: nothing closed yet
        return None




    

    # ---- Candle enrichment / builder --------------------------------------
    def _enrich_candle(
        self,
        symbol: str,
        timeframe: str,
        raw: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Take a raw OHLCV candle dict (ts, open, high, low, close, volume),
        compute all derived fields (geometry, shape, volume context, momentum,
        micro-structure) and return the enriched candle.
        """

        # --- timestamp handling: keep UTC as primary, add ET helpers ----
        ts_raw = raw.get("ts")
        ts: dt.datetime

        if isinstance(ts_raw, dt.datetime):
            ts = ts_raw
        elif isinstance(ts_raw, str):
            try:
                ts = dt.datetime.fromisoformat(ts_raw)
            except Exception:
                ts = dt.datetime.now(dt.timezone.utc)
        else:
            ts = dt.datetime.now(dt.timezone.utc)

        # Make sure it's timezone-aware (assume UTC if naive)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)

        # View in US/Eastern for readability
        ts_et = ts.astimezone(EASTERN)

        # --- core OHLCV fields ----------------------------------------------
        o = float(raw["open"])
        h = float(raw["high"])
        l = float(raw["low"])
        c = float(raw["close"])
        v = float(raw.get("volume", 0.0))

        # Base list of existing candles for context
        symbol_candles = self.candles.get(symbol, {})
        tf_candles: List[Dict[str, Any]] = symbol_candles.get(timeframe, [])

        # --- basic geometry ---
        range_ = max(h - l, 0.0)
        body = abs(c - o)
        upper_wick = max(h - max(o, c), 0.0)
        lower_wick = max(min(o, c) - l, 0.0)
        spread_strength = (body / range_) if range_ > 0 else 0.0

        # direction
        eps = 1e-8
        if c > o + eps:
            direction = "bull"
        elif c < o - eps:
            direction = "bear"
        else:
            direction = "neutral"

        # --- previous candle context ---
        prev_candle: Optional[Dict[str, Any]] = tf_candles[-1] if tf_candles else None
        prev_close = float(prev_candle["close"]) if prev_candle else c
        prev_high = float(prev_candle["high"]) if prev_candle else h
        prev_low = float(prev_candle["low"]) if prev_candle else l

        # --- swing flags ---
        is_higher_high = h > prev_high if prev_candle else False
        is_lower_high = h < prev_high if prev_candle else False
        is_higher_low = l > prev_low if prev_candle else False
        is_lower_low = l < prev_low if prev_candle else False

        if prev_candle:
            if is_higher_high and is_higher_low:
                swing_type: Optional[str] = "HH"
            elif is_lower_high and is_lower_low:
                swing_type = "LL"
            elif is_higher_low and not is_higher_high and not is_lower_high:
                swing_type = "HL"
            elif is_lower_high and not is_lower_low and not is_higher_low:
                swing_type = "LH"
            else:
                swing_type = None
        else:
            swing_type = None

        # --- volume context ---
        vol_rel = 1.0
        is_high_vol = False
        is_low_vol = False
        if tf_candles:
            vols = [float(cand.get("volume", 0.0)) for cand in tf_candles[-VOL_LOOKBACK:]]
            avg_vol = sum(vols) / len(vols) if vols else 0.0
            if avg_vol > 0:
                vol_rel = v / avg_vol
                is_high_vol = vol_rel >= VOL_HIGH
                is_low_vol = vol_rel <= VOL_LOW

        # --- ATR-based momentum ---
        mom_raw = c - prev_close
        atr = 0.0
        if len(tf_candles) >= 2:
            start_idx = max(1, len(tf_candles) - ATR_PERIOD)
            trs: List[float] = []
            for i in range(start_idx, len(tf_candles)):
                cur = tf_candles[i]
                prev = tf_candles[i - 1]
                ch = float(cur["high"])
                cl = float(cur["low"])
                pc = float(prev["close"])
                tr = max(ch - cl, abs(ch - pc), abs(cl - pc))
                trs.append(tr)
            atr = (sum(trs) / len(trs)) if trs else 0.0

        mom_atr = (mom_raw / atr) if atr > 0 else 0.0

        # --- shape classification ---

        def classify_shape() -> str:
            if range_ <= 0:
                return "normal"

            body_ratio = (body / range_) if range_ > 0 else 0.0
            upper_ratio = (upper_wick / range_) if range_ > 0 else 0.0
            lower_ratio = (lower_wick / range_) if range_ > 0 else 0.0

            # 1) marubozu
            if body_ratio >= BODY_LARGE and upper_ratio <= WICK_SHORT and lower_ratio <= WICK_SHORT:
                if direction == "bull":
                    return "marubozu_bull"
                if direction == "bear":
                    return "marubozu_bear"

            # 2) hammer / inverted / shooting star
            if lower_ratio >= WICK_LONG and upper_ratio <= WICK_SHORT and body_ratio <= BODY_SMALL:
                # body near top half
                if min(o, c) >= l + 0.5 * range_:
                    return "hammer"

            if upper_ratio >= WICK_LONG and lower_ratio <= WICK_SHORT and body_ratio <= BODY_SMALL:
                if direction == "bear":
                    return "shooting_star"
                else:
                    return "inverted_hammer"

            # 3) doji (small body)
            if body_ratio <= BODY_TINY:
                return "doji"

            # 4) long wicks
            if upper_ratio >= WICK_LONG and upper_wick >= WICK_DOMINANT * body:
                return "long_upper_wick"
            if lower_ratio >= WICK_LONG and lower_wick >= WICK_DOMINANT * body:
                return "long_lower_wick"

            # 5) small body
            if body_ratio <= BODY_SMALL:
                return "small_body"

            return "normal"

        shape = classify_shape()


        enriched = dict(raw)
        enriched.update({
            "ts": ts.isoformat(),
            "ts_et": ts_et.isoformat(),
            "date_et": ts_et.date().isoformat(),
            "time_et": ts_et.strftime("%H:%M:%S"),
        
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
            "body": body,
            "range": range_,
            "upper_wick": upper_wick,
            "lower_wick": lower_wick,
            "direction": direction,
            "spread_strength": spread_strength,
            "shape": shape,
            "vol_rel": vol_rel,
            "is_high_vol": is_high_vol,
            "is_low_vol": is_low_vol,
            "mom_raw": mom_raw,
            "mom_atr": mom_atr,
            "is_higher_high": is_higher_high,
            "is_lower_high": is_lower_high,
            "is_higher_low": is_higher_low,
            "is_lower_low": is_lower_low,
            "swing_type": swing_type,
        })
        
        # ---------- NEW BLOCK: cluster summary ----------
        try:
            # previous TF candles + this one
            window = (tf_candles[-4:] if len(tf_candles) >= 4 else tf_candles[:]) + [enriched]
            cluster = self._compute_cluster(window)
            enriched["cluster"] = cluster
        except Exception as e:
            enriched["cluster"] = {
                "lookback": 0,
                "bull_count": 0,
                "bear_count": 0,
                "net_change_pct": None,
                "avg_mom_atr": None,
                "avg_vol_rel": None,
                "state": "neutral",
            }
        # --------------------------------------------------
        
        return enriched



    # ---- Initialization from Alpaca --------------------------------------

    async def initialize_from_alpaca(self) -> None:
        """
        For all symbols, fetch baseline candles from Alpaca via market_data.
        Populates candles and latest_ts for all managed timeframes.
        """
        if not self.symbols:
            print("CandleEngine.initialize_from_alpaca: No symbols provided.")
            return

        async with httpx.AsyncClient(timeout=30.0) as client:
            data = await market_data.fetch_multi_tf_candles(client, self.symbols)

            for sym in self.symbols:
                try:
                    per_sym = data.get(sym, {})
                except Exception:
                    per_sym = {}

                self.candles.setdefault(sym, {})
                self.latest_ts.setdefault(sym, {})

                for tf, c_list in per_sym.items():
                    if tf not in SUPPORTED_TFS:
                        continue

                    enriched_list: List[Dict[str, Any]] = []
                    self.candles[sym][tf] = enriched_list

                    for raw_candle in c_list:
                        enriched = self._enrich_candle(sym, tf, raw_candle)
                        enriched_list.append(enriched)

                    if enriched_list:
                        last_ts = dt.datetime.fromisoformat(enriched_list[-1]["ts"])
                        await update_last_candle_in_spot_tf(sym, tf, enriched_list[-1])
                    else:
                        last_ts = None

                    self.latest_ts[sym][tf] = last_ts

                print(
                    f"[INIT] {sym}: "
                    + ", ".join(
                        f"{tf}={len(self.candles[sym].get(tf, []))}"
                        for tf in sorted(self.candles[sym].keys())
                    )
                )

    # ---- Dynamic symbol reload from spot_tf --------------------------------

    async def reload_symbols_if_changed(self) -> None:
        """
        Re-fetch symbols from spot_tf. If there are new or removed symbols,
        update self.symbols accordingly:

          - For added symbols:
              * fetch baseline Alpaca candles
              * populate candles/latest_ts
              * update last_candle in spot_tf
          - For removed symbols:
              * drop them from candles/latest_ts
        """
        new_symbols = await load_symbols_from_spot_tf()
        new_set = {s.upper() for s in new_symbols if s.strip()}
        old_set = set(self.symbols)


        if not new_set:
            # don't blow away existing universe if DB is temporarily empty / error
            return

        # Base added/removed sets from symbol universe change
        added = sorted(new_set - old_set)
        removed = sorted(old_set - new_set)

        # NEW: detect symbols whose spot_tf rows have NULL last_candle
        # and treat them as needing a baseline backfill, even if they
        # are already in the engine's symbol set.
        missing_last_candle = await load_symbols_with_missing_last_candle()
        missing_set = {s.upper() for s in missing_last_candle if s.strip()} & new_set

        # Union of truly new and needing-backfill symbols
        added_all = sorted(set(added) | missing_set)

        if not added_all and not removed:
            # Symbol universe is the same and no symbol needs backfill
            return

        if added_all:
            print(f"[SYMBOLS] Added / needs-backfill: {added_all}")
        if removed:
            print(f"[SYMBOLS] Removed: {removed}")

        # Remove unwanted
        for sym in removed:
            self.candles.pop(sym, None)
            self.latest_ts.pop(sym, None)

        # Initialize new-or-needing-backfill symbols from Alpaca
        if added_all:
            async with httpx.AsyncClient(timeout=30.0) as client:
                try:
                    data = await market_data.fetch_multi_tf_candles(client, added_all)
                except Exception as e:
                    print(f"[INIT] Error fetching Alpaca candles for new symbols {added_all}: {e}")
                    data = {}

                for sym in added_all:
                    per_sym = data.get(sym, {})
                    self.candles.setdefault(sym, {})
                    self.latest_ts.setdefault(sym, {})

                    for tf, c_list in per_sym.items():
                        if tf not in SUPPORTED_TFS:
                            continue

                        enriched_list: List[Dict[str, Any]] = []
                        self.candles[sym][tf] = enriched_list

                        for c in c_list:
                            enriched = self._enrich_candle(sym, tf, c)
                            enriched_list.append(enriched)

                        if enriched_list:
                            last_ts = dt.datetime.fromisoformat(enriched_list[-1]["ts"])
                            await update_last_candle_in_spot_tf(sym, tf, enriched_list[-1])
                        else:
                            last_ts = None

                        self.latest_ts[sym][tf] = last_ts

                    print(
                        f"[INIT-NEW] {sym}: "
                        + ", ".join(
                            f"{tf}={len(self.candles[sym].get(tf, []))}"
                            for tf in sorted(self.candles[sym].keys())
                        )
                    )

        # Finally, update the in-memory symbol list to match DB
        self.symbols = sorted(new_set)
        print(f"[SYMBOLS] New universe: {self.symbols}")

    
    # ---- Twelve Data integration -----------------------------------------

    async def _fetch_td_1m(self, client: httpx.AsyncClient, symbol: str) -> List[Dict[str, Any]]:
        """
        Fetch last 20 1m candles from Twelve Data for a given symbol.
        Returns normalized candle dicts:
          {ts, open, high, low, close, volume}
        RTH-only (we filter by timestamp).

        NOTE: Twelve Data 'datetime' is in exchange_timezone (America/New_York),
        so we must parse as ET and then convert to UTC before:
          - RTH filtering (is_rth_timestamp expects UTC)
          - downstream comparisons / aggregation.
        """
        params = {
            "symbol": symbol,
            "interval": "1min",
            "outputsize": "20",
            "apikey": self.td_api_key,
        }
        resp = await client.get("https://api.twelvedata.com/time_series", params=params)
        resp.raise_for_status()
        data = resp.json()
        # DEBUG: Show full raw TD response before any filtering
        print(f"\n[TD][RAW_RESPONSE][{symbol}] {data}\n")


        if "values" not in data:
            print(f"[TD] No 'values' in response for {symbol}: {data}")
            return []

        raw_values = data["values"]
        out: List[Dict[str, Any]] = []

        for v in raw_values:
            ts_str = v.get("datetime")
            if not ts_str:
                continue

            try:
                # TD gives 'datetime' in ET (exchange_timezone = America/New_York).
                # Parse as naive ET, then localize and convert to UTC.
                ts_et_naive = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

            ts_et = ts_et_naive.replace(tzinfo=EASTERN)
            ts_utc = ts_et.astimezone(dt.timezone.utc)
            # DEBUG: Show timestamp comparison
            print(f"[TD][TRACE] Compare TD={ts_utc}  vs  last_1m={self.latest_ts.get(symbol,{}).get('1m')}")

            

            # is_rth_timestamp expects a UTC datetime
            if not is_rth_timestamp(ts_utc):
                continue

            try:
                out.append(
                    {
                        "ts": ts_utc.isoformat(),   # store canonical UTC timestamp
                        "open": float(v["open"]),
                        "high": float(v["high"]),
                        "low": float(v["low"]),
                        "close": float(v["close"]),
                        "volume": float(v.get("volume", 0.0)),
                    }
                )
            except (KeyError, ValueError):
                continue

        # Twelve Data returns newest first; sort ascending by UTC ts
        out.sort(key=lambda c: c["ts"])
        print(f"[TD][TRACE] Final accepted TD candles for {symbol}: {[c['ts'] for c in out]}")
        return out

    def _aggregate_from_1m(
        self,
        symbol: str,
        target_tf: str,
        new_1m: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Aggregate newly added 1m candles into higher timeframe candles (3m,5m,15m,1h).

        IMPORTANT:
        - We only build a higher-TF candle if the underlying 1m bucket is
          COMPLETE, i.e. we have all required 1m bars for that bucket.
        - For example, a 3m bucket starting at 10:51 requires 1m bars at
          10:51, 10:52, 10:53 (in UTC minute steps).
        - This prevents partially built 3m/5m candles when Twelve Data is
          delayed in delivering some 1m bars.
        """
        if target_tf not in ("3m", "5m", "15m", "1h"):
            return []

        # Ensure containers exist
        self.candles.setdefault(symbol, {})
        self.candles[symbol].setdefault(target_tf, [])
        self.latest_ts.setdefault(symbol, {})
        self.latest_ts[symbol].setdefault(target_tf, None)

        existing = self.candles[symbol][target_tf]
        last_ts = self.latest_ts[symbol][target_tf]

        # All known 1m candles for this symbol
        all_1m = self.candles[symbol].get("1m", [])
        if not all_1m:
            return []

        # Determine earliest ts to consider:
        #   - if we already have higher-TF candles, start from their last ts
        #   - otherwise, start from the first 1m ts
        if last_ts is not None:
            start_ts = last_ts
        else:
            start_ts = dt.datetime.fromisoformat(all_1m[0]["ts"])

        # Collect 1m candles with ts >= start_ts
        relevant_1m = [
            c for c in all_1m if dt.datetime.fromisoformat(c["ts"]) >= start_ts
        ]
        if not relevant_1m:
            return []

        # Group relevant 1m candles into buckets by target_tf
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for c in relevant_1m:
            ts = dt.datetime.fromisoformat(c["ts"])
            b_start = bucket_start(ts, target_tf)  # UTC bucket start
            key = b_start.isoformat()
            buckets.setdefault(key, []).append(c)

        new_htf: List[Dict[str, Any]] = []

        # Determine how many 1m candles are required per bar for this timeframe
        delta = tf_to_timedelta(target_tf)
        # Fallback: if we can't parse the timeframe, behave like old logic
        if delta is None or delta.total_seconds() <= 0:
            required_1m_count = None
        else:
            # e.g. "3m" -> 3, "5m" -> 5, "15m" -> 15, "1h" -> 60
            required_1m_count = int(delta.total_seconds() // 60) or 1

        for b_ts_str, group in sorted(buckets.items(), key=lambda kv: kv[0]):
            b_ts = dt.datetime.fromisoformat(b_ts_str)

            # Never rebuild or go backwards: only build buckets AFTER last_ts
            if last_ts is not None and b_ts <= last_ts:
                continue

            # If we don't know how many 1m bars are required (shouldn't happen
            # for '3m','5m','15m','1h'), fall back to old behavior.
            if required_1m_count is None:
                opens = [g["open"] for g in group]
                highs = [g["high"] for g in group]
                lows = [g["low"] for g in group]
                closes = [g["close"] for g in group]
                vols = [g.get("volume", 0.0) for g in group]

                raw_candle = {
                    "ts": b_ts_str,
                    "open": float(opens[0]),
                    "high": float(max(highs)),
                    "low": float(min(lows)),
                    "close": float(closes[-1]),
                    "volume": float(sum(vols)),
                }
                enriched = self._enrich_candle(symbol, target_tf, raw_candle)
                existing.append(enriched)
                self.latest_ts[symbol][target_tf] = b_ts
                new_htf.append(enriched)
                continue

            # ---------- COMPLETENESS CHECK FOR THE BUCKET ----------
            #
            # A bucket is considered COMPLETE only if we have all expected 1m
            # timestamps:
            #   expected_ts[i] = b_ts + i * 1 minute, for i in [0, required_1m_count-1]
            #
            # This guarantees, for example, that a 3m bar at 10:51 has 1m bars
            # for 10:51, 10:52, 10:53 before we aggregate it.
            group_ts_set = {
                dt.datetime.fromisoformat(g["ts"]) for g in group
            }

            complete = True
            for i in range(required_1m_count):
                expected_ts = b_ts + dt.timedelta(minutes=i)
                if expected_ts not in group_ts_set:
                    complete = False
                    break

            if not complete:
                # Underlying 1m bars for this bucket are not all present yet.
                # We skip building this bar now; a later Twelve Data poll can
                # fill in missing minutes and then we'll aggregate it.
                continue

            # ---------- AGGREGATE COMPLETE BUCKET ----------
            # Now we know we have all required 1m candles for this bar.
            # We can safely build an OHLCV bar aligned with b_ts.

            # Sort group by ts to get correct open/close ordering
            group_sorted = sorted(
                group,
                key=lambda g: dt.datetime.fromisoformat(g["ts"]),
            )

            opens = [g["open"] for g in group_sorted]
            highs = [g["high"] for g in group_sorted]
            lows = [g["low"] for g in group_sorted]
            closes = [g["close"] for g in group_sorted]
            vols = [g.get("volume", 0.0) for g in group_sorted]

            raw_candle = {
                "ts": b_ts_str,
                "open": float(opens[0]),
                "high": float(max(highs)),
                "low": float(min(lows)),
                "close": float(closes[-1]),
                "volume": float(sum(vols)),
            }

            enriched = self._enrich_candle(symbol, target_tf, raw_candle)
            existing.append(enriched)
            self.latest_ts[symbol][target_tf] = b_ts
            new_htf.append(enriched)

        return new_htf


    def _aggregate_new_1m(self, symbol: str, new_1m: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Helper to aggregate newly added 1m candles to 3m,5m,15m,1h.
        Returns dict[tf] = list of new higher-TF candles.
        """
        result: Dict[str, List[Dict[str, Any]]] = {}
        for tf in ("3m", "5m", "15m", "1h"):
            new_htf = self._aggregate_from_1m(symbol, tf, new_1m)
            if new_htf:
                result[tf] = new_htf
        return result

    async def update_from_twelvedata_once(self) -> None:
        """
        Single update cycle:
          - Reload symbols from spot_tf if changed
          - Only runs if now is RTH (ET)
          - For each symbol:
             * fetch last 20×1m from TD
             * filter out already-known candles
             * append new 1m
             * aggregate to 3m,5m,15m,1h
             * update spot_tf.last_candle for affected timeframes
        """

        print("\n==============================")
        print("[TD][TRACE] Starting update cycle")
        now_et = dt.datetime.now(dt.timezone.utc).astimezone(EASTERN)
        print(f"[TD][TRACE] Current ET time = {now_et.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[TD][TRACE] is_rth_now() = {is_rth_now()}")
        print("==============================")

        # Auto-reload symbols from DB first
        try:
            await self.reload_symbols_if_changed()
        except Exception as e:
            print(f"[SYMBOLS] Error reloading symbols: {e}")

        if not is_rth_now():
            print("[TD] Outside RTH; skipping Twelve Data update.")
            return

        if not self.symbols:
            print("[TD] No symbols to update.")
            return

        async with httpx.AsyncClient(timeout=15.0) as client:
            for sym in self.symbols:
                print(f"[TD][TRACE] Processing symbol {sym}")
                try:
                    td_candles = await self._fetch_td_1m(client, sym)
                    print(f"[TD][TRACE] Raw TD candles fetched: {len(td_candles)}")
                except Exception as e:
                    print(f"[TD] Error fetching Twelve Data candles for {sym}: {e}")
                    continue

                if not td_candles:
                    continue

                self.candles.setdefault(sym, {})
                self.latest_ts.setdefault(sym, {})
                for tf in LIVE_TFS:
                    self.candles[sym].setdefault(tf, [])
                    self.latest_ts[sym].setdefault(tf, None)

                last_1m_ts = self.latest_ts[sym]["1m"]
                new_1m: List[Dict[str, Any]] = []
                for c in td_candles:
                    ts = dt.datetime.fromisoformat(c["ts"])
                    if last_1m_ts is None or ts > last_1m_ts:
                        enriched = self._enrich_candle(sym, "1m", c)
                        self.candles[sym]["1m"].append(enriched)
                        self.latest_ts[sym]["1m"] = ts
                        new_1m.append(enriched)

                print(f"[TD][TRACE] New 1m candles after filtering: {len(new_1m)}")


                if not new_1m:
                    continue

                # Aggregate new 1m to higher TFs
                print(f"[TD][TRACE] Aggregating {len(new_1m)} new 1m candles for {sym}")
                new_by_tf = self._aggregate_new_1m(sym, new_1m)
                print(f"[TD][TRACE] Aggregation result: { {tf: len(lst) for tf, lst in new_by_tf.items()} }")


                print(f"[TD] {sym}: +{len(new_1m)} new 1m candles")

                # Update last_candle in DB for affected TFs
                # 1m



                # ---------------------------------------------
                # Update last_candle in DB for affected TFs,
                # using ONLY *closed* candles.
                # ---------------------------------------------

                # 1m: typically Twelve Data gives closed 1m bars,
                # but we still run through the same helper for consistency.
                last_closed_1m = self._get_last_closed_candle(sym, "1m")
                if last_closed_1m is not None:
                    await update_last_candle_in_spot_tf(sym, "1m", last_closed_1m)

                # Higher timeframes: only write the last *closed* bar.
                for tf in new_by_tf.keys():
                    last_closed = self._get_last_closed_candle(sym, tf)
                    if last_closed is not None:
                        await update_last_candle_in_spot_tf(sym, tf, last_closed)

                print(f"[TD][TRACE] DB updated for {sym} (1m + higher TFs, closed only)")


    # ---- Public accessors / loop ------------------------------------------

    def get_candles(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        """
        Return list of enriched candles for the given symbol+timeframe.
        """
        sym = symbol.upper()
        return self.candles.get(sym, {}).get(timeframe, [])

    async def run_loop(self, interval_seconds: int = 180):
        print(f"[ENGINE] Starting live update loop (interval={interval_seconds}s).")
        while True:
            try:
                await self.update_from_twelvedata_once()
            except Exception as e:
                print(f"[ENGINE] Exception inside run_loop: {e}")
            await asyncio.sleep(interval_seconds)



# ---- Shared init helper ---------------------------------------------------

async def init_engine_from_env() -> "CandleEngine":
    """
    Shared initializer: load symbols from spot_tf, create engine,
    bootstrap from Alpaca.
    """
    td_key = os.getenv("TWELVEDATA_API_KEY")
    if not td_key:
        raise RuntimeError("TWELVEDATA_API_KEY env var is required")

    symbols = await load_symbols_from_spot_tf()
    if not symbols:
        print("No symbols loaded from spot_tf; exiting.")
        raise RuntimeError("No symbols loaded from spot_tf")

    engine = CandleEngine(td_key, symbols)
    await engine.initialize_from_alpaca()
    return engine
