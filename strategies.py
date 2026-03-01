from typing import Dict, Any, List, Optional

def _compute_sl_tp(
    direction: str,
    swings: Dict[str, Any],
    fvgs: List[Dict[str, Any]],
    liquidity: Dict[str, Any],
    trend: Dict[str, Any],
    last_candle: Dict[str, Any],
    volume_profile: Dict[str, Any],
    htf_swings: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Compute SL/TP zones for a bullish or bearish strategy signal.
    Implements your approved spec exactly.
    """

    is_bull = (direction == "bull")
    buffer_pct = 0.0010  # 0.10% SL buffer

    # ------------------------------
    # 1️⃣ Collect candidate SL levels
    # ------------------------------

    # 1. Current FVG (use nearest by time)
    fvg_candidates = [
        f for f in fvgs if f.get("direction") == direction
    ]
    fvg_candidates = sorted(
        fvg_candidates, key=lambda x: x.get("created_ts", ""), reverse=True
    )
    fvg = fvg_candidates[0] if fvg_candidates else None

    if is_bull:
        fvg_low = fvg["low"] if fvg else None
    else:
        fvg_high = fvg["high"] if fvg else None

    # 2. Retest candle wick (last candle low/high)
    retest_low = float(last_candle["low"])
    retest_high = float(last_candle["high"])

    # 3. Recent swing low/high (pivot)
    pivots = swings.get("pivots", [])
    recent_swing = pivots[-1] if pivots else None
    swing_price = recent_swing.get("price") if recent_swing else None

    # -------------------------
    # 2️⃣ Choose SL (invalidation)
    # -------------------------
    sl_raw_candidates = []

    if is_bull:
        if fvg and fvg_low:
            sl_raw_candidates.append(fvg_low)
        sl_raw_candidates.append(retest_low)
        if swing_price:
            sl_raw_candidates.append(swing_price)

        sl_raw = max(sl_raw_candidates)
        sl = sl_raw * (1 - buffer_pct)

    else:  # bearish
        if fvg and fvg_high:
            sl_raw_candidates.append(fvg_high)
        sl_raw_candidates.append(retest_high)
        if swing_price:
            sl_raw_candidates.append(swing_price)

        sl_raw = min(sl_raw_candidates)
        sl = sl_raw * (1 + buffer_pct)

    # SL zone is small band around SL (optional)
    sl_zone = {
        "low": sl if is_bull else sl_raw,
        "high": sl_raw if is_bull else sl,
        "reason": "invalidation"
    }

    # ------------------------------
    # 3️⃣ TP1 = nearest liquidity
    # ------------------------------
    liq_levels = liquidity.get("levels", [])

    def upward_levels():
        return [l for l in liq_levels if float(l["price"]) > float(last_candle["close"])]

    def downward_levels():
        return [l for l in liq_levels if float(l["price"]) < float(last_candle["close"])]

    if is_bull:
        liqs = upward_levels()
    else:
        liqs = downward_levels()

    if liqs:
        # nearest liquidity in direction
        liqs_sorted = sorted(liqs, key=lambda x: abs(x["price"] - last_candle["close"]))
        tp1 = {
            "target": float(liqs_sorted[0]["price"]),
            "confidence": 0.8,
            "reason": "liquidity"
        }
    else:
        # fallback: EMA200 magnet
        ema200 = trend.get("ema200")
        tp1 = {
            "target": ema200,
            "confidence": 0.6,
            "reason": "ema200_magnet"
        }

    # ------------------------------
    # 4️⃣ TP2 = opposing FVG or midpoint
    # ------------------------------
    # Opposing direction FVGs
    opposite_fvg = [
        f for f in fvgs if f.get("direction") != direction
    ]
    opposite_fvg = sorted(opposite_fvg, key=lambda x: x.get("created_ts", ""), reverse=True)
    opp = opposite_fvg[0] if opposite_fvg else None

    if opp:
        mid = (opp["high"] + opp["low"]) / 2.0
        tp2 = {
            "target": float(opp["high"] if is_bull else opp["low"]),
            "confidence": 0.6,
            "reason": "fvg_far"
        }
    else:
        # fallback to Volume Profile VAH/VAL
        vah = volume_profile.get("vah")
        val = volume_profile.get("val")
        tp2 = {
            "target": vah if is_bull else val,
            "confidence": 0.5,
            "reason": "volume_profile"
        }

    # ------------------------------
    # 5️⃣ TP3 = HTF swing extension
    # ------------------------------

    if htf_swings:
        # choose first HTF pivot in direction
        if is_bull:
            htf_targets = [p["price"] for p in htf_swings if p["price"] > last_candle["close"]]
        else:
            htf_targets = [p["price"] for p in htf_swings if p["price"] < last_candle["close"]]

        if htf_targets:
            htf_targets = sorted(htf_targets)
            tp3 = {
                "target": float(htf_targets[0]),
                "confidence": 0.4,
                "reason": "htf_swing"
            }
        else:
            tp3 = None
    else:
        tp3 = None

    # combine TP levels
    tp_list = [tp1, tp2]
    if tp3:
        tp_list.append(tp3)

    return {
        "sl_zone": sl_zone,
        "tp_zones": tp_list
        }


def _detect_break_retest_fvg(
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    swings: Dict[str, Any],
    fvgs: List[Dict[str, Any]],
    liquidity: Dict[str, Any],
    trend: Dict[str, Any],
    cluster: Dict[str, Any],
    volume_profile: Dict[str, Any],
    htf_swings: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Detect Break → FVG → Retest → Continuation pattern (v1).

    Returns a strategy object:
        { id, direction, step, status, sl_tp, best_trade_types, ... }

    Or returns None if the setup is not active.
    """

    # ------------------------------------------------------------
    # Preconditions: We need at least 10 candles to evaluate pattern
    # ------------------------------------------------------------
    if len(candles) < 10:
        return None

    last = candles[-1]
    close = float(last["close"])

    # ------------------------------------------------------------
    # Step 1 — Detect BOS (Break of Structure)
    # ------------------------------------------------------------
    close_breaks = liquidity.get("close_breaks") or []
    if not close_breaks:
        return None  # No BOS = no setup

    last_break = close_breaks[-1]
    bos_side = last_break.get("side")  # "up" or "down"
    broken_level = float(last_break.get("broken_price") or 0)

    if bos_side not in ("up", "down"):
        return None

    # BOS direction
    direction = "bull" if bos_side == "up" else "bear"

    # Trend must agree
    if trend.get("state") != direction:
        return None

    # ------------------------------------------------------------
    # Step 2 — Find FVG in BOS direction
    # ------------------------------------------------------------
    same_dir_fvgs = [f for f in fvgs if f.get("direction") == direction]
    if not same_dir_fvgs:
        return None

    # Use the most recent FVG
    fvg = sorted(same_dir_fvgs, key=lambda x: x.get("created_ts", ""))[-1]

    fvg_low = float(fvg["low"])
    fvg_high = float(fvg["high"])

    # ------------------------------------------------------------
    # Step 3 — Detect Retest
    # ------------------------------------------------------------

    # tolerate 0.15% distance inside FVG
    def inside_fvg_bullish():
        return fvg_low <= close <= fvg_high

    def inside_fvg_bearish():
        return fvg_low <= close <= fvg_high

    def is_retest():
        # For now: price must be inside the FVG boundaries
        return inside_fvg_bullish() if direction == "bull" else inside_fvg_bearish()

    # If we have BOS but no retest yet → step 1 (setup)
    if not is_retest():
        return {
            "id": "break_retest_fvg",
            "direction": direction,
            "timeframe": timeframe,
            "step": 1,
            "status": "setup",
            "broken_level": broken_level,
            "fvg_zone": {"low": fvg_low, "high": fvg_high},
            "best_trade_types": ["scalp", "day"] if timeframe in ("1m","5m") else ["day", "swing"]
        }

    # Retest is happening → Step 2 (armed), but require cluster state pullback
    pullback_states = {"chop", "bear_drift", "bull_drift", "range_low_vol"}

    if cluster.get("state") in pullback_states:
        # Retest detected & pullback valid → Armed
        # Wait for continuation candle (step 3)
        retest_valid = True
    else:
        # If retest but not pullback-state, still treat as setup
        retest_valid = False

    if not retest_valid:
        return {
            "id": "break_retest_fvg",
            "direction": direction,
            "timeframe": timeframe,
            "step": 1,
            "status": "setup",
            "broken_level": broken_level,
            "fvg_zone": {"low": fvg_low, "high": fvg_high},
            "best_trade_types": ["scalp", "day"] if timeframe in ("1m","5m") else ["day", "swing"]
        }

    # Step 2: retest confirmed & pullback valid
    # Now check continuation trigger
    # ------------------------------------------------------------
    # Step 4 — Continuation (Trigger Candle)
    # ------------------------------------------------------------

    # Bullish continuation = bullish bar with close > mid-FVG
    # Bearish continuation = bearish bar with close < mid-FVG
    fvg_mid = (fvg_low + fvg_high) / 2.0

    continuation = False

    if direction == "bull":
        if last.get("direction") == "bull" and close > fvg_mid:
            continuation = True
    else:
        if last.get("direction") == "bear" and close < fvg_mid:
            continuation = True

    # If continuation not confirmed → ARMED, waiting
    if not continuation:
        return {
            "id": "break_retest_fvg",
            "direction": direction,
            "timeframe": timeframe,
            "step": 2,
            "status": "armed",
            "broken_level": broken_level,
            "fvg_zone": {"low": fvg_low, "high": fvg_high},
            "best_trade_types": ["scalp", "day"] if timeframe in ("1m","5m") else ["day", "swing"]
        }

    # ------------------------------------------------------------
    # Step 5 — TRIGGERED (pattern complete)
    # ------------------------------------------------------------
    # Compute SL/TP using your function
    sl_tp = _compute_sl_tp(
        direction=direction,
        swings=swings,
        fvgs=fvgs,
        liquidity=liquidity,
        trend=trend,
        last_candle=last,
        volume_profile=volume_profile,
        htf_swings=htf_swings,
    )

    return {
        "id": "break_retest_fvg",
        "direction": direction,
        "timeframe": timeframe,
        "step": 3,
        "status": "triggered",
        "broken_level": broken_level,
        "fvg_zone": {"low": fvg_low, "high": fvg_high},
        "sl_tp": sl_tp,
        "best_trade_types": ["scalp", "day"] if timeframe in ("1m","5m") else ["day", "swing"],
        "trigger_ts": last.get("ts"),
    }

  

def evaluate_strategies(
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    swings: Dict[str, Any],
    fvgs: List[Dict[str, Any]],
    liquidity: Dict[str, Any],
    trend: Dict[str, Any],
    cluster: Dict[str, Any],
    volume_profile: Dict[str, Any],
    htf_swings: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Evaluate all strategies for this symbol/timeframe.
    Returns a list of strategy dicts.
    """

    strategies = []

    # --- STRATEGY 1: Break → FVG → Retest → Continuation ---
    try:
        brt = _detect_break_retest_fvg(
            symbol=symbol,
            timeframe=timeframe,
            candles=candles,
            swings=swings,
            fvgs=fvgs,
            liquidity=liquidity,
            trend=trend,
            cluster=cluster,
            volume_profile=volume_profile,
            htf_swings=htf_swings,
        )
        if brt:
            strategies.append(brt)
    except Exception:
        pass

    return strategies
