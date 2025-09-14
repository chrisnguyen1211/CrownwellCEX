import os
import time
from datetime import datetime, timezone, timedelta, date
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
import warnings

import streamlit as st
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException

# Suppress Streamlit secrets warning in production
warnings.filterwarnings("ignore", message=".*secrets.*")

# Reuse helpers from script if available
try:
    from binance_spot_history import (
        parse_time_to_ms,
        fetch_symbols_from_orders,
        fetch_trades_for_symbol,
    )
except Exception:
    # Fallback minimal implementations if script not importable
    from datetime import datetime, timezone

    def parse_time_to_ms(raw: Optional[str]) -> Optional[int]:
        if not raw:
            return None
        if "T" in raw:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    def fetch_symbols_from_orders(client: Client, start_ms: Optional[int], end_ms: Optional[int]) -> List[str]:
        found = set()
        info = client.get_exchange_info()
        for s in info["symbols"]:
            if s.get("status") != "TRADING":
                continue
            symbol = s["symbol"]
            try:
                orders = client.get_all_orders(symbol=symbol, startTime=start_ms, endTime=end_ms, limit=5)
                if orders:
                    found.add(symbol)
            except Exception:
                pass
            time.sleep(0.02)
        return sorted(found)

    def fetch_trades_for_symbol(client: Client, symbol: str, start_ms: Optional[int], end_ms: Optional[int], limit: int = 1000):
        results = []
        from_id = None
        per_page = max(1, min(limit, 1000))
        while True:
            trades = client.get_my_trades(symbol=symbol, startTime=start_ms, endTime=end_ms, fromId=from_id, limit=per_page)
            if not trades:
                break
            results.extend(trades)
            if len(trades) < per_page:
                break
            from_id = trades[-1]["id"] + 1
            time.sleep(0.1)
        return results


st.set_page_config(page_title="Crownwell CEX Tracking", layout="wide", page_icon="/Users/nguyenhuycuong/Downloads/crownwell.jpg")

st.markdown("""
<style>
:root { --cw-gold: #d4af37; --cw-bg: #0b0b0b; --cw-panel: #121212; --cw-text: #e9e9e9; --cw-muted: #9aa0a6; }

/* Global */
html, body, .stApp { background-color: var(--cw-bg) !important; color: var(--cw-text) !important; }
.stButton>button, .stDownloadButton>button { background: var(--cw-gold) !important; color: #101010 !important; border: none !important; }
.stButton>button:hover, .stDownloadButton>button:hover { filter: brightness(1.05); }

/* Inputs */
.stTextInput>div>div>input, .stNumberInput input, .stSelectbox>div>div>div, .stDateInput input { 
  background: var(--cw-panel) !important; color: var(--cw-text) !important; border: 1px solid #2c2c2c !important; 
}

/* Dataframes */
.stDataFrame, .stTable { background: var(--cw-panel) !important; }

/* Metrics */
.metric { padding: 12px 16px; background: var(--cw-panel); border: 1px solid #2c2c2c; border-radius: 10px }
.muted { color: var(--cw-muted) }

/* Sidebar */
section[data-testid="stSidebar"] { background: #0e0e0e }

/* Headers accent */
h1, h2, h3, h4 { color: var(--cw-gold) !important; }
/* Top nav radio compact */
.top-nav .stRadio > label { font-size: 0.95rem; color: var(--cw-muted); }
.top-nav .stRadio div[role="radiogroup"] > label { padding: 2px 8px; }
/* Smaller app title */
.cw-title { font-size: 34px; line-height: 1.1; margin: 8px 0 0 0; color: var(--cw-gold); }
</style>
""", unsafe_allow_html=True)

col_logo, col_title, col_nav = st.columns([1,5,4])
with col_logo:
    try:
        st.image("/Users/nguyenhuycuong/Downloads/crownwell.jpg", width=56)
    except Exception:
        pass
with col_title:
    st.markdown("<h1 class='cw-title'>Crownwell CEX Tracking</h1>", unsafe_allow_html=True)
with col_nav:
    st.markdown("<div class='top-nav'>", unsafe_allow_html=True)
    page = st.radio(" ", options=["Trades", "Balances"], index=0, horizontal=True, label_visibility="collapsed")
    st.markdown("</div>", unsafe_allow_html=True)

def _maybe_pause_on_rate_limit(client: Client, near_threshold: int = 1100, sleep_seconds: float = 20.0) -> None:
    """Pause when Binance used weight in last minute approaches the limit.

    Binance sends headers like 'x-mbx-used-weight-1m'. If present and above
    near_threshold (default ~92% of 1200), pause briefly to let the window
    recover before continuing.

    This function is best-effort; if headers are unavailable it does nothing.
    """
    try:
        resp = getattr(client, "response", None)
        headers = getattr(resp, "headers", {}) or {}
        used = int(headers.get("x-mbx-used-weight-1m") or headers.get("X-MBX-USED-WEIGHT-1M") or 0)
        if used >= near_threshold:
            time.sleep(sleep_seconds)
    except Exception:
        # Header not available or unparsable; skip pausing
        pass

# Centralized rate-safe call wrapper
_last_call_ts = 0.0
def _binance_call(callable_fn, *args, **kwargs):
    """Call Binance API with spacing, header-based throttling and robust retries.

    - Enforces a minimal spacing between requests
    - After each successful call, inspects x-mbx-used-weight-1m and sleeps if near cap
    - On -1003 / -1015, backs off exponentially (and up to 60s)
    """
    global _last_call_ts
    min_spacing = 0.25  # seconds between calls
    reserve = 120  # keep ~10% headroom of 1200
    weight_cap = 1200
    backoff = 1.0
    while True:
        now = time.time()
        delta = now - _last_call_ts
        if delta < min_spacing:
            time.sleep(min_spacing - delta)
        try:
            result = callable_fn(*args, **kwargs)
            _last_call_ts = time.time()
            # Inspect headers and pause if necessary
            try:
                resp = getattr(callable_fn.__self__, "response", None) if hasattr(callable_fn, "__self__") else None
                headers = getattr(resp, "headers", {}) or {}
                used = int(headers.get("x-mbx-used-weight-1m") or headers.get("X-MBX-USED-WEIGHT-1M") or 0)
                if used and used >= (weight_cap - reserve):
                    # Sleep close to the remaining seconds in the minute window
                    time.sleep(60)
            except Exception:
                pass
            return result
        except BinanceAPIException as e:
            code = getattr(e, "code", None)
            if code in (-1003, -1015):
                # Too many requests / Banned temporarily, exponential backoff up to 60s
                time.sleep(min(backoff, 60))
                backoff = min(backoff * 2, 60)
                continue
            raise

# Simple navigator

# Dedicated Balances page (filters, includes Earn/Staking)
if page == "Balances":
    st.subheader("Asset Holdings")
    try:
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")
        if not api_key or not api_secret:
            st.error("Missing BINANCE_API_KEY/BINANCE_API_SECRET in environment. Set them before running the UI.")
            st.stop()
        client_bal = Client(api_key, api_secret)
        rows = []

        # Spot
        try:
            acct = client_bal.get_account()
            for b in acct.get("balances", []):
                free = float(b.get("free", 0) or 0)
                locked = float(b.get("locked", 0) or 0)
                total = free + locked
                if total > 0:
                    rows.append({"asset": b.get("asset"), "free": free, "locked": locked, "total": total, "source": "SPOT"})
        except Exception:
            pass

        # Futures USDT-M
        try:
            fut = client_bal.futures_account_balance()
            for it in fut:
                asset = it.get("asset")
                total = float(it.get("balance", 0) or 0)
                if total > 0:
                    rows.append({"asset": asset, "free": total, "locked": 0.0, "total": total, "source": "FUTURES_USDTM"})
        except Exception:
            pass

        # Earn (Flexible Savings)
        try:
            earn = client_bal.get_lending_account()
            for it in earn.get("positionAmountVos", []):
                asset = it.get("asset")
                total = float(it.get("amount", 0) or 0)
                if total > 0:
                    rows.append({"asset": asset, "free": 0.0, "locked": total, "total": total, "source": "EARN"})
        except Exception:
            pass

        # Staking (Locked)
        try:
            staking = client_bal.get_staking_product_position(product="STAKING")
            for it in staking or []:
                asset = it.get("asset")
                total = float(it.get("amount", 0) or 0)
                if total > 0:
                    rows.append({"asset": asset, "free": 0.0, "locked": total, "total": total, "source": "STAKING"})
        except Exception:
            pass

        # Normalize LD* tokens (Earn) regardless of original source labels
        normalized_rows = []
        for r in rows:
            asset_name = str(r.get("asset")) if r.get("asset") is not None else ""
            if asset_name.startswith("LD") and len(asset_name) > 2:
                r = {**r}
                r["source"] = "EARN"
            normalized_rows.append(r)

        hdf = pd.DataFrame(normalized_rows)
        if not hdf.empty:
            # Group and filter by threshold
            hdf = hdf.groupby(["asset","source"], as_index=False).agg({"free":"sum","locked":"sum","total":"sum"})
            hdf = hdf.sort_values(["asset","source"]).reset_index(drop=True)

            threshold = st.number_input("Ẩn các tài sản có total <", min_value=0.0, max_value=1000.0, value=1.0, step=0.5)
            hdf = hdf[hdf["total"] >= float(threshold)]

            # USD estimate
            try:
                info = client_bal.get_exchange_info()
                # Map base -> USDT symbol
                usdt_symbols = {s["baseAsset"]: s["symbol"] for s in info.get("symbols", []) if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}
                # Stablecoins worth $1
                prices = {"USDT": 1.0, "USDC": 1.0, "FDUSD": 1.0, "TUSD": 1.0, "BUSD": 1.0}
                assets = set(hdf["asset"].tolist())
                # Handle LD-prefixed assets like LDUSDC, LDBTC (map to underlying)
                def underlying_symbol(a: str) -> str:
                    if not isinstance(a, str):
                        return a
                    if a.startswith("LD") and len(a) > 2:
                        return a[2:]
                    return a
                for asset in assets:
                    base = underlying_symbol(asset)
                    if base in prices:
                        prices[asset] = prices[base]
                        continue
                    sym = usdt_symbols.get(base)
                    if not sym:
                        continue
                    try:
                        t = _binance_call(client_bal.get_symbol_ticker, symbol=sym)
                        prices[asset] = float(t.get("price"))
                        _maybe_pause_on_rate_limit(client_bal)
                        time.sleep(0.02)
                    except Exception:
                        pass
                hdf["usd_est"] = hdf.apply(lambda r: round(r["total"] * float(prices.get(r["asset"], prices.get(underlying_symbol(r["asset"]), 0))), 2), axis=1)
            except Exception:
                pass

            disp_cols = [c for c in ["asset","source","free","locked","total","usd_est"] if c in hdf.columns]
            st.dataframe(hdf[disp_cols], use_container_width=True)

            try:
                g = hdf.groupby("source")["usd_est"].sum().reset_index().sort_values("usd_est", ascending=False)
                st.caption("USD estimate by source:")
                st.dataframe(g, use_container_width=True)
            except Exception:
                pass
        else:
            st.caption("No holdings found.")

        st.info("Earn bao gồm Savings/Staking. LD token thường là các token đại diện cho tài sản bị khoá/liquidity (ví dụ Locked Savings/Launchpool). Bạn có thể kiểm tra thêm trong nguồn EARN/STAKING để đối chiếu.")
    except Exception as e:
        st.caption(f"Holdings unavailable: {e}")

    st.stop()

with st.expander("Security note", expanded=True):
    st.markdown(
        "- Prefer using environment variables or Streamlit Secrets to avoid exposing your API Secret.\n"
        "- Use a read-only API key. Revoke any leaked keys immediately."
    )

def _get_api_keys() -> (Optional[str], Optional[str]):
    # 1) Environment variables (production priority)
    ek = os.getenv("BINANCE_API_KEY")
    es = os.getenv("BINANCE_API_SECRET")
    if ek and es:
        # Clean whitespace and validate format
        ek = ek.strip()
        es = es.strip()
        # Binance API keys are typically 64 characters
        if len(ek) >= 60 and len(es) >= 60 and ek.isalnum() and es.isalnum():
            return ek, es
        else:
            st.error(f"❌ Invalid API key format. Key length: {len(ek)}, Secret length: {len(es)}")
            st.error(f"Key preview: {ek[:10]}...{ek[-10:] if len(ek) > 20 else ek}")
            st.error(f"Secret preview: {es[:10]}...{es[-10:] if len(es) > 20 else es}")
    
    # 2) Streamlit secrets (local development)
    try:
        sk = st.secrets.get("BINANCE_API_KEY", None)
        ss = st.secrets.get("BINANCE_API_SECRET", None)
        if sk and ss:
            sk = str(sk).strip()
            ss = str(ss).strip()
            if len(sk) > 10 and len(ss) > 10:
                return sk, ss
    except Exception:
        pass
    
    # 3) Session inputs (fallback)
    k = st.session_state.get("__api_key__")
    s = st.session_state.get("__api_secret__")
    if k and s:
        k = str(k).strip()
        s = str(s).strip()
    return k, s

api_key, api_secret = _get_api_keys()

# Debug info for production
if os.getenv("BINANCE_API_KEY"):
    st.sidebar.success("✅ Environment variables detected")
    st.sidebar.caption(f"API Key: {api_key[:8]}...{api_key[-4:] if api_key else 'None'}")
else:
    st.sidebar.warning("⚠️ Using fallback credentials")

if not api_key or not api_secret:
    with st.sidebar:
        st.warning("Enter API credentials to continue")
        st.text_input("BINANCE_API_KEY", key="__api_key__")
        st.text_input("BINANCE_API_SECRET", type="password", key="__api_secret__")
        st.info("You can also set them via environment variables or .streamlit/secrets.toml")
    st.stop()

# Live stream removed

cols = st.columns([1,1,1,1])
with cols[0]:
    default_start = date.today() - timedelta(days=90)
    d = st.date_input("Start date", value=default_start)
    start_date = d.isoformat() if d else ""
with cols[1]:
    d2 = st.date_input("End date (optional)", value=None)
    end_date = d2.isoformat() if d2 else ""
with cols[2]:
    per_page = st.slider("Page size", min_value=50, max_value=1000, value=500, step=50, help="Items per request to Binance")
with cols[3]:
    sort_desc = st.selectbox("Sort", options=["Newest first", "Oldest first"], index=0)

# Live tab removede

# Defaults: always use cache; if cache missing, auto-infer once and cache
use_cache = True
infer_symbols = False
flt = st.container()
with flt:
    token_base = st.text_input("Token (auto expand stable pairs)", value="", placeholder="e.g., ETH")
    symbols_text = st.text_input("Specific pair", value="", placeholder="e.g., BTCUSDT ETHUSDT")

run = st.button("Fetch Trades")
# Auto-load on first visit with default (last 1 year, infer=true)
if "__auto_loaded__" not in st.session_state:
    st.session_state["__auto_loaded__"] = True
    run = True

if run:
    if not api_key or not api_secret:
        st.error("API key/secret required.")
        st.stop()

    try:
        client = Client(api_key, api_secret)
    except Exception as e:
        st.error(f"Failed to create Binance client: {e}")
        st.stop()

    try:
        start_ms = parse_time_to_ms(start_date) if start_date else None
        end_ms = parse_time_to_ms(end_date) if end_date else None
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Helper: cached symbol list
    def load_cached_symbols() -> List[str]:
        try:
            p = Path.home() / ".tradehis_symbols.json"
            if p.exists():
                obj = json.loads(p.read_text())
                return list(sorted(set(obj.get("symbols", []))))
        except Exception:
            pass
        return []

    def save_cached_symbols(symbols_list: List[str]) -> None:
        try:
            p = Path.home() / ".tradehis_symbols.json"
            p.write_text(json.dumps({"symbols": sorted(list(set(symbols_list)))}, ensure_ascii=False))
        except Exception:
            pass

    def load_cache_state() -> Dict[str, Any]:
        try:
            p = Path.home() / ".tradehis_symbols.json"
            if p.exists():
                return json.loads(p.read_text())
        except Exception:
            pass
        return {"symbols": [], "probe_index": 0}

    def save_cache_state(state: Dict[str, Any]) -> None:
        try:
            p = Path.home() / ".tradehis_symbols.json"
            p.write_text(json.dumps(state, ensure_ascii=False))
        except Exception:
            pass

    def infer_symbols_safely(local_client: Client, start_ms: Optional[int], end_ms: Optional[int], max_probe: int = 10, start_index: int = 0) -> (List[str], int):
        # 1) Build candidate bases from account balances (>0)
        try:
            account = local_client.get_account()
            base_assets = [b["asset"] for b in account.get("balances", []) if float(b.get("free", 0)) + float(b.get("locked", 0)) > 0]
        except Exception:
            base_assets = []
        # Always include common bases to avoid missing
        for common in ["BTC", "ETH", "BNB", "ARB", "SOL"]:
            if common not in base_assets:
                base_assets.append(common)

        # 2) Map to stable-quote symbols via exchangeInfo
        try:
            info = local_client.get_exchange_info()
            stable_quotes = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD"}
            all_syms = []
            for s in info.get("symbols", []):
                if s.get("status") != "TRADING":
                    continue
                if s.get("baseAsset") in base_assets and s.get("quoteAsset") in stable_quotes:
                    all_syms.append(s.get("symbol"))
        except Exception:
            all_syms = []

        # 3) Probe một phần nhỏ có giới hạn để tránh rate limit
        total = len(all_syms)
        if total == 0 or max_probe <= 0:
            return all_syms[:max_probe] if max_probe else all_syms, start_index
        end_index = min(start_index + max_probe, total)
        subset = all_syms[start_index:end_index]
        confirmed: List[str] = []
        backoff = 0.5
        for sym in subset:
            try:
                od = _binance_call(local_client.get_all_orders, symbol=sym, startTime=start_ms, endTime=end_ms, limit=1)
                _maybe_pause_on_rate_limit(local_client)
                if od:
                    confirmed.append(sym)
            except BinanceAPIException as e:
                if getattr(e, "code", None) == -1003:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 4)
                    continue
            except Exception:
                pass
            time.sleep(0.08)
        next_index = end_index if end_index < total else 0
        return (confirmed if confirmed else subset), next_index

    # Backoff-safe trade fetcher (mitigate -1003)
    def fetch_trades_for_symbol_safe(local_client: Client, symbol: str, start_ms: Optional[int], end_ms: Optional[int], limit: int = 1000):
        collected = []
        from_id = None
        per_page = max(1, min(limit, 1000))
        backoff = 0.5
        while True:
            try:
                params = {"symbol": symbol, "limit": per_page}
                if from_id is None:
                    if start_ms is not None:
                        params["startTime"] = start_ms
                    if end_ms is not None:
                        params["endTime"] = end_ms
                else:
                    params["fromId"] = from_id
                trades = _binance_call(local_client.get_my_trades, **params)
                _maybe_pause_on_rate_limit(local_client)
            except BinanceAPIException as e:
                if getattr(e, "code", None) == -1003:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 8)
                    continue
                if getattr(e, "code", None) == -1121:
                    break
                raise
            if not trades:
                break
            collected.extend(trades)
            if len(trades) < per_page:
                break
            from_id = trades[-1]["id"] + 1
            time.sleep(0.05)
        return collected

    if token_base.strip():
        try:
            info = client.get_exchange_info()
            stables = {"USDT","USDC","FDUSD","TUSD","BUSD"}
            base_u = token_base.strip().upper()
            symbols = [s.get("symbol") for s in info.get("symbols", []) if s.get("status")=="TRADING" and s.get("baseAsset")==base_u and s.get("quoteAsset") in stables]
            if not symbols:
                st.warning(f"No stable-quote symbols found for token {base_u}")
            else:
                st.info(f"Expanded {base_u} to: {' '.join(symbols)}")
        except Exception as e:
            st.error(f"Failed to expand token: {e}")
            st.stop()
    elif symbols_text.strip():
        symbols = [s.strip().upper() for s in symbols_text.strip().split()]
    else:
        # Try cache first for speed
        symbols = load_cached_symbols() if use_cache else []
        if not symbols:
            with st.spinner("Preparing symbols safely (rate-limit aware)..."):
                try:
                    state = load_cache_state()
                    current = state.get("symbols", [])
                    start_idx = int(state.get("probe_index", 0))
                    newly, next_idx = infer_symbols_safely(client, start_ms, end_ms, max_probe=10, start_index=start_idx)
                    symbols = sorted(list(set(current + newly)))
                    save_cache_state({"symbols": symbols, "probe_index": next_idx})
                except BinanceAPIException as e:
                    st.error(f"Binance error while inferring symbols: {e}")
                    st.stop()
                except Exception as e:
                    st.error(f"Failed to infer symbols: {e}")
                    st.stop()
        if not symbols:
            st.info("No symbols found. Provide symbols explicitly.")
            st.stop()

    # Default: restrict to USDT/USDC quote pairs
    symbols = [s for s in symbols if s.endswith("USDT") or s.endswith("USDC")]
    if not symbols:
        st.info("No USDT/USDC symbols found from cache/inference. Please enter symbols or a base.")
        st.stop()

    all_rows: List[Dict[str, Any]] = []
    progress = st.progress(0.0, text="Fetching trades...")
    for i, sym in enumerate(symbols):
        progress.progress((i) / max(1, len(symbols)), text=f"{sym}")
        try:
            trades = fetch_trades_for_symbol(client, sym, start_ms, end_ms, limit=int(per_page))
            all_rows.extend(trades)
        except BinanceAPIException as e:
            st.warning(f"Skipping {sym} due to Binance error: {e}")
        except Exception as e:
            st.warning(f"Skipping {sym} due to error: {e}")

    progress.progress(1.0, text="Done")

    if not all_rows:
        st.info("No trades found for the selected range/symbols.")
        st.stop()

    # Normalize to DataFrame
    df = pd.DataFrame(all_rows)
    if "time" in df.columns:
        df["time_iso"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")
    # Order columns
    preferred_cols = [
        "symbol","time_iso","price","qty","quoteQty","commission","commissionAsset","isBuyer","isMaker","orderId","id"
    ]
    ordered = [c for c in preferred_cols if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    df = df[ordered + remaining]

    # Add simple Buy/Sell label
    if "isBuyer" in df.columns:
        df["side_simple"] = df["isBuyer"].apply(lambda x: "BUY" if bool(x) else "SELL")

    # Optional enrichment: fetch order type/side from orders (limit/market)
    try:
        unique_pairs = df[["symbol", "orderId"]].dropna().drop_duplicates()
        order_info: Dict[str, Dict[str, str]] = {}
        client = Client(api_key, api_secret)
        fetched = 0
        for _, row in unique_pairs.iterrows():
            sym = str(row["symbol"]).upper()
            oid = int(row["orderId"])
            key = f"{sym}:{oid}"
            if key in order_info:
                continue
            try:
                od = _binance_call(client.get_order, symbol=sym, orderId=oid)
                order_info[key] = {"orderType": od.get("type"), "orderSide": od.get("side")}
                _maybe_pause_on_rate_limit(client)
            except Exception:
                order_info[key] = {"orderType": None, "orderSide": None}
            fetched += 1
            if fetched % 10 == 0:
                time.sleep(0.1)
        def _map_type(sym: str, oid: Any, field: str) -> Optional[str]:
            try:
                return order_info.get(f"{str(sym).upper()}:{int(oid)}", {}).get(field)
            except Exception:
                return None
        df["orderType"] = df.apply(lambda r: _map_type(r.get("symbol"), r.get("orderId"), "orderType"), axis=1)
        df["orderSide_raw"] = df.apply(lambda r: _map_type(r.get("symbol"), r.get("orderId"), "orderSide"), axis=1)
        # Prefer explicit order side, fallback to simple side
        df["side"] = df["orderSide_raw"].fillna(df.get("side_simple"))
    except Exception:
        df["side"] = df.get("side_simple")

    # Sidebar filters
    st.sidebar.header("Filters")
    symbols_available = sorted(df["symbol"].dropna().unique().tolist()) if "symbol" in df.columns else []
    selected_symbols = st.sidebar.multiselect("Symbol", options=symbols_available, default=symbols_available)
    if selected_symbols:
        df = df[df["symbol"].isin(selected_symbols)]
    # Date filter
    try:
        df["_ts"] = pd.to_datetime(df["time_iso"], utc=True)
        start_f = st.sidebar.date_input("Filter start", value=None)
        end_f = st.sidebar.date_input("Filter end", value=None)
        if start_f:
            df = df[df["_ts"] >= pd.to_datetime(start_f)]
        if end_f:
            df = df[df["_ts"] <= pd.to_datetime(end_f) + pd.Timedelta(days=1)]
    except Exception:
        pass

    # Sorting
    try:
        df = df.sort_values("time", ascending=(sort_desc == "Oldest first"))
    except Exception:
        pass

    # Summary metrics
    try:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
        df["quoteQty"] = pd.to_numeric(df.get("quoteQty"), errors="coerce") if "quoteQty" in df.columns else 0
    except Exception:
        pass

    total_trades = len(df)
    total_base = float(df["qty"].sum()) if "qty" in df else 0.0
    total_quote = float(df["quoteQty"].sum()) if "quoteQty" in df else 0.0

    m1, m2, m3 = st.columns(3)
    with m1:
        st.markdown(f"<div class='metric'><b>Total trades</b><br>{total_trades}</div>", unsafe_allow_html=True)
    with m2:
        st.markdown(f"<div class='metric'><b>Total base qty</b><br>{total_base:.6f}</div>", unsafe_allow_html=True)
    with m3:
        st.markdown(f"<div class='metric'><b>Total quote qty</b><br>{total_quote:.2f}</div>", unsafe_allow_html=True)

    st.subheader("Trades")
    display_cols = [c for c in ["symbol","time_iso","side","orderType","price","qty","quoteQty","commission","commissionAsset","orderId","id"] if c in df.columns]
    st.dataframe(df[display_cols], use_container_width=True)

    st.subheader("Open Orders (unfilled)")
    try:
        # Fetch open orders for current symbol set in the same session
        current_symbols = sorted(df["symbol"].dropna().unique().tolist()) if "symbol" in df.columns else []
        if current_symbols:
            client2 = Client(api_key, api_secret)
            open_orders = []
            for s in current_symbols:
                try:
                    open_orders.extend(_binance_call(client2.get_open_orders, symbol=str(s).upper()))
                    _maybe_pause_on_rate_limit(client2)
                except Exception:
                    pass
                time.sleep(0.05)
            if open_orders:
                odf = pd.DataFrame(open_orders)
                cols = [c for c in ["symbol","orderId","type","side","price","origQty","executedQty","status","time"] if c in odf.columns]
                if "time" in odf.columns:
                    odf["time_iso"] = pd.to_datetime(odf["time"], unit="ms", utc=True).dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")
                    cols = ["symbol","time_iso","type","side","price","origQty","executedQty","status","orderId"]
                st.dataframe(odf[cols], use_container_width=True)
            else:
                st.caption("No open orders for the current symbols.")
        else:
            st.caption("No symbols to query open orders.")
    except Exception as e:
        st.caption(f"Open orders unavailable: {e}")

    # Asset holdings (Spot/Futures/Earn) with source labels
    st.subheader("Asset Holdings")
    try:
        client_bal = Client(api_key, api_secret)
        rows = []

        # Spot
        try:
            acct = client_bal.get_account()
            for b in acct.get("balances", []):
                free = float(b.get("free", 0) or 0)
                locked = float(b.get("locked", 0) or 0)
                total = free + locked
                if total > 0:
                    rows.append({"asset": b.get("asset"), "free": free, "locked": locked, "total": total, "source": "SPOT"})
        except Exception:
            pass

        # Futures USDT-M
        try:
            fut = client_bal.futures_account_balance()
            for it in fut:
                asset = it.get("asset")
                total = float(it.get("balance", 0) or 0)
                if total > 0:
                    rows.append({"asset": asset, "free": total, "locked": 0.0, "total": total, "source": "FUTURES_USDTM"})
        except Exception:
            pass

        # Earn (Savings)
        try:
            earn = client_bal.get_lending_account()
            for it in earn.get("positionAmountVos", []):
                asset = it.get("asset")
                total = float(it.get("amount", 0) or 0)
                if total > 0:
                    rows.append({"asset": asset, "free": 0.0, "locked": total, "total": total, "source": "EARN"})
        except Exception:
            pass

        # Staking (best-effort)
        try:
            staking = client_bal.get_staking_product_position(product="STAKING")
            for it in staking or []:
                asset = it.get("asset")
                total = float(it.get("amount", 0) or 0)
                if total > 0:
                    rows.append({"asset": asset, "free": 0.0, "locked": total, "total": total, "source": "STAKING"})
        except Exception:
            pass

        hdf = pd.DataFrame(rows)
        if not hdf.empty:
            hdf = hdf.groupby(["asset","source"], as_index=False).agg({"free":"sum","locked":"sum","total":"sum"})
            hdf = hdf.sort_values(["asset","source"]).reset_index(drop=True)

            # USD estimate
            try:
                info = client_bal.get_exchange_info()
                usdt_symbols = {s["baseAsset"]: s["symbol"] for s in info.get("symbols", []) if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}
                prices = {"USDT": 1.0}
                for asset in set(hdf["asset"].tolist()):
                    if asset in prices:
                        continue
                    sym = usdt_symbols.get(asset)
                    if not sym:
                        continue
                    try:
                        t = _binance_call(client_bal.get_symbol_ticker, symbol=sym)
                        prices[asset] = float(t.get("price"))
                        _maybe_pause_on_rate_limit(client_bal)
                        time.sleep(0.02)
                    except Exception:
                        pass
                hdf["usd_est"] = hdf.apply(lambda r: round(r["total"] * float(prices.get(r["asset"], 0)), 2), axis=1)
            except Exception:
                pass

            disp_cols = [c for c in ["asset","source","free","locked","total","usd_est"] if c in hdf.columns]
            st.dataframe(hdf[disp_cols], use_container_width=True)
            # Totals by source
            try:
                g = hdf.groupby("source")["usd_est"].sum().reset_index().sort_values("usd_est", ascending=False)
                st.caption("USD estimate by source:")
                st.dataframe(g, use_container_width=True)
            except Exception:
                pass
        else:
            st.caption("No holdings found.")
    except Exception as e:
        st.caption(f"Holdings unavailable: {e}")

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", data=csv_bytes, file_name="spot_trades.csv", mime="text/csv")


