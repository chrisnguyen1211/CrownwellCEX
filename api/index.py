import os
import time
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Set

from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from binance.client import Client
from binance.exceptions import BinanceAPIException
import requests


app = FastAPI(title="Binance Spot Trades API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _maybe_pause_on_rate_limit(client: Client, near_threshold: int = 1100, sleep_seconds: float = 20.0) -> None:
    """Pause when Binance used weight in last minute approaches the limit.

    Checks 'x-mbx-used-weight-1m' from the last response if available and
    sleeps briefly when near the limit. Best-effort; no-op if headers missing.
    """
    try:
        resp = getattr(client, "response", None)
        headers = getattr(resp, "headers", {}) or {}
        used = int(headers.get("x-mbx-used-weight-1m") or headers.get("X-MBX-USED-WEIGHT-1M") or 0)
        if used >= near_threshold:
            time.sleep(sleep_seconds)
    except Exception:
        pass

# Centralized rate-safe call wrapper
_last_call_ts = 0.0
def _binance_call(callable_fn, *args, **kwargs):
    global _last_call_ts
    min_spacing = 0.25
    reserve = 120
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
            try:
                resp = getattr(callable_fn.__self__, "response", None) if hasattr(callable_fn, "__self__") else None
                headers = getattr(resp, "headers", {}) or {}
                used = int(headers.get("x-mbx-used-weight-1m") or headers.get("X-MBX-USED-WEIGHT-1M") or 0)
                if used and used >= (weight_cap - reserve):
                    time.sleep(60)
            except Exception:
                pass
            return result
        except BinanceAPIException as e:
            code = getattr(e, "code", None)
            if code in (-1003, -1015):
                time.sleep(min(backoff, 60))
                backoff = min(backoff * 2, 60)
                continue
            raise

def parse_time_to_ms(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        if "T" in raw:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid time format. Use ISO8601 or yyyy-mm-dd") from exc


def fetch_symbols_from_orders(client: Client, start_ms: Optional[int], end_ms: Optional[int]) -> List[str]:
    candidate_quotes = ["USDT", "BUSD", "USDC", "FDUSD", "TUSD", "BTC", "BNB"]
    found: Set[str] = set()

    try:
        info = client.get_exchange_info()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to get exchange info: {exc}") from exc

    for quote in candidate_quotes:
        quote_symbols = [s["symbol"] for s in info["symbols"] if s.get("quoteAsset") == quote and s.get("status") == "TRADING"]
        for symbol in quote_symbols:
            try:
                orders = _binance_call(client.get_all_orders, symbol=symbol, startTime=start_ms, endTime=end_ms, limit=10)
                _maybe_pause_on_rate_limit(client)
            except BinanceAPIException as e:
                if e.code in (-1121, -2013, -2011):
                    continue
                raise HTTPException(status_code=502, detail=f"Binance error: {e}") from e
            except Exception:
                continue
            if orders:
                found.add(symbol)
            time.sleep(0.01)
    return sorted(found)


def fetch_trades_for_symbol(client: Client, symbol: str, start_ms: Optional[int], end_ms: Optional[int], limit: int = 1000) -> List[Dict[str, Any]]:
    all_trades: List[Dict[str, Any]] = []
    from_id: Optional[int] = None
    per_page = max(1, min(limit, 1000))

    while True:
        try:
            params: Dict[str, Any] = {
                "symbol": symbol,
                "limit": per_page,
            }
            # Binance does not allow combining fromId with start/end
            if from_id is None:
                if start_ms is not None:
                    params["startTime"] = start_ms
                if end_ms is not None:
                    params["endTime"] = end_ms
            else:
                params["fromId"] = from_id

            trades = _binance_call(client.get_my_trades, **params)
            _maybe_pause_on_rate_limit(client)
        except BinanceAPIException as e:
            if e.code == -1121:
                break
            raise HTTPException(status_code=502, detail=f"Binance error: {e}") from e
        except requests.exceptions.Timeout:
            break

        if not trades:
            break
        all_trades.extend(trades)
        if len(trades) < per_page:
            break
        from_id = trades[-1]["id"] + 1
        time.sleep(0.05)

    return all_trades


def ensure_client() -> Client:
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        raise HTTPException(status_code=500, detail="Server missing BINANCE_API_KEY/BINANCE_API_SECRET")
    # Short timeouts keep serverless within Vercel execution limits
    requests_params = {"timeout": 10}
    try:
        return Client(api_key, api_secret, requests_params=requests_params)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to init Binance client: {exc}") from exc

STABLE_QUOTES = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD"}

def symbols_for_base_with_stables(client: Client, base: str) -> List[str]:
    try:
        info = client.get_exchange_info()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to get exchange info: {exc}") from exc
    base = base.upper()
    symbols: List[str] = []
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("baseAsset") == base and s.get("quoteAsset") in STABLE_QUOTES:
            symbols.append(s.get("symbol"))
    return sorted(symbols)


def _require_token(request: Request) -> None:
    token = os.getenv("API_PROTECT_TOKEN")
    if not token:
        return  # protection disabled
    # Read from header or query param
    header_token = request.headers.get("x-access-token")
    query_token = request.query_params.get("access_token")
    if header_token == token or query_token == token:
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/health")
def health(_: None = Depends(_require_token)) -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/trades")
def trades(
    symbols: Optional[str] = Query(default=None, description="Comma-separated symbols, e.g., BTCUSDT,ETHUSDT"),
    base: Optional[str] = Query(default=None, description="Base asset to expand into stable quotes, e.g., ARB => ARBUSDT, ARBUSDC, ..."),
    infer: bool = Query(default=False, description="Infer traded symbols from orders if symbols not provided"),
    start: Optional[str] = Query(default=None, description="Start time ISO8601 or yyyy-mm-dd"),
    end: Optional[str] = Query(default=None, description="End time ISO8601 or yyyy-mm-dd"),
    limit: int = Query(default=1000, ge=1, le=1000, description="Page size"),
    _: None = Depends(_require_token),
):
    client = ensure_client()
    start_ms = parse_time_to_ms(start)
    end_ms = parse_time_to_ms(end)

    symbol_list: List[str] = []
    if base:
        symbol_list = symbols_for_base_with_stables(client, base)
        if not symbol_list:
            raise HTTPException(status_code=400, detail=f"No stable-quote symbols found for base {base}")
    elif symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    elif infer:
        symbol_list = fetch_symbols_from_orders(client, start_ms, end_ms)
        if not symbol_list:
            return JSONResponse({"trades": [], "symbols": [], "count": 0})
    else:
        raise HTTPException(status_code=400, detail="Provide symbols or set infer=true")

    all_rows: List[Dict[str, Any]] = []
    for sym in symbol_list:
        rows = fetch_trades_for_symbol(client, sym, start_ms, end_ms, limit)
        all_rows.extend(rows)

    return {"trades": all_rows, "symbols": symbol_list, "count": len(all_rows)}


# Duplicate routes under "/api" to be compatible with Vercel's path forwarding
@app.get("/api/health")
def health_api(_: None = Depends(_require_token)) -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/trades")
def trades_api(
    symbols: Optional[str] = Query(default=None, description="Comma-separated symbols, e.g., BTCUSDT,ETHUSDT"),
    base: Optional[str] = Query(default=None, description="Base asset to expand into stable quotes, e.g., ARB => ARBUSDT, ARBUSDC, ..."),
    infer: bool = Query(default=False, description="Infer traded symbols from orders if symbols not provided"),
    start: Optional[str] = Query(default=None, description="Start time ISO8601 or yyyy-mm-dd"),
    end: Optional[str] = Query(default=None, description="End time ISO8601 or yyyy-mm-dd"),
    limit: int = Query(default=1000, ge=1, le=1000, description="Page size"),
    _: None = Depends(_require_token),
):
    return trades(symbols=symbols, base=base, infer=infer, start=start, end=end, limit=limit)


# --- Open orders and recent helpers ---
def fetch_open_orders_for_symbols(client: Client, symbols: List[str]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for sym in symbols:
        try:
            orders = client.get_open_orders(symbol=sym)
        except BinanceAPIException:
            orders = []
        for od in orders:
            results.append(od)
        time.sleep(0.02)
    return results


@app.get("/open_orders")
def open_orders(
    symbols: Optional[str] = Query(default=None),
    base: Optional[str] = Query(default=None),
    infer: bool = Query(default=False),
    _: None = Depends(_require_token),
):
    client = ensure_client()
    symbol_list: List[str] = []
    if base:
        symbol_list = symbols_for_base_with_stables(client, base)
    elif symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    elif infer:
        symbol_list = fetch_symbols_from_orders(client, None, None)
    else:
        raise HTTPException(status_code=400, detail="Provide symbols or set infer=true")

    data = fetch_open_orders_for_symbols(client, symbol_list)
    return {"openOrders": data, "symbols": symbol_list, "count": len(data)}


@app.get("/recent_trades")
def recent_trades(
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    perSymbol: int = Query(default=200, ge=1, le=1000),
    infer: bool = Query(default=True),
    base: Optional[str] = Query(default=None),
    _: None = Depends(_require_token),
):
    client = ensure_client()
    start_ms = parse_time_to_ms(start)
    end_ms = parse_time_to_ms(end)
    if base:
        symbols = symbols_for_base_with_stables(client, base)
    else:
        symbols = fetch_symbols_from_orders(client, start_ms, end_ms) if infer else []
    all_rows: List[Dict[str, Any]] = []
    for sym in symbols:
        rows = fetch_trades_for_symbol(client, sym, start_ms, end_ms, perSymbol)
        all_rows.extend(rows)
    return {"trades": all_rows, "symbols": symbols, "count": len(all_rows)}


