from __future__ import annotations

from typing import Iterable

import pandas as pd


def dedupe_keep_order(values: Iterable[int]) -> list[int]:
    seen = set()
    out: list[int] = []
    for v in values:
        n = int(v)
        if n > 0 and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def filter_business_days(
    df: pd.DataFrame,
    date_col: str = "dt",
    business_days: set | None = None,
) -> pd.DataFrame:
    """
    Keep weekday rows first, then (optionally) keep only given business-day set.
    - business_days: set of python date objects
    """
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col])
    out["dt_date"] = out[date_col].dt.date
    out = out[out["dt_date"].apply(lambda d: d.weekday() < 5)].copy()
    if business_days is not None:
        out = out[out["dt_date"].isin(business_days)].copy()
    return out


def add_moving_averages(
    df: pd.DataFrame,
    price_col: str = "price",
    windows: list[int] | tuple[int, ...] = (20, 60, 120, 200),
) -> pd.DataFrame:
    """
    Add simple moving averages (SMA) on price column.
    """
    out = df.copy()
    out[price_col] = pd.to_numeric(out[price_col], errors="coerce")
    out = out.dropna(subset=[price_col])
    clean_windows = dedupe_keep_order(windows)
    for w in clean_windows:
        out[f"MA_{w}"] = out[price_col].rolling(window=w, min_periods=w).mean()
    return out
from __future__ import annotations
from datetime import date, datetime
from typing import Iterable, Optional, Union
import matplotlib.pyplot as plt
import pandas as pd
from call import get_constituents_for_date, get_price_factset

DateLike = Union[str, date, datetime, pd.Timestamp]


def _to_date_str(value: DateLike) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def _dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen = set()
    result = []
    for v in values:
        if v and v not in seen:
            seen.add(v)
            result.append(v)
    return result


def _find_ticker_column(df: pd.DataFrame) -> str:
    candidates = ["bb_ticker", "ticker", "factset_ticker", "id"]
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"Ticker column not found. Available columns: {list(df.columns)}")


def get_index_tickers(index_name: str, ref_date: DateLike) -> list[str]:
    """
    Get constituent tickers for an index on a reference date.
    Uses existing call.py function: get_constituents_for_date.
    """
    rows = get_constituents_for_date(index_name, _to_date_str(ref_date))
    cons_df = pd.DataFrame(rows) if not isinstance(rows, pd.DataFrame) else rows.copy()
    if cons_df.empty:
        return []
    ticker_col = _find_ticker_column(cons_df)
    tickers = cons_df[ticker_col].astype(str).str.strip().tolist()
    return _dedupe_keep_order(tickers)


def fetch_price_wide(
    tickers: list[str],
    start_date: DateLike,
    end_date: DateLike,
) -> pd.DataFrame:
    """
    Fetch price_factset data and return wide DataFrame:
    index = Date, columns = tickers, values = Close.
    """
    clean_tickers = _dedupe_keep_order([str(t).strip() for t in tickers if str(t).strip()])
    if not clean_tickers:
        return pd.DataFrame()

    price_df = get_price_factset(
        clean_tickers,
        _to_date_str(start_date),
        _to_date_str(end_date),
    )
    if price_df is None or len(price_df) == 0:
        return pd.DataFrame()

    price_df = pd.DataFrame(price_df).copy()
    required = {"dt", "bb_ticker", "price"}
    if not required.issubset(set(price_df.columns)):
        raise KeyError(f"Price data must include {required}, got {list(price_df.columns)}")

    price_df["dt"] = pd.to_datetime(price_df["dt"], errors="coerce")
    price_df["price"] = pd.to_numeric(price_df["price"], errors="coerce")
    price_df = price_df.dropna(subset=["dt", "bb_ticker", "price"])
    if price_df.empty:
        return pd.DataFrame()

    wide = (
        price_df.sort_values(["dt", "bb_ticker"])
        .pivot_table(index="dt", columns="bb_ticker", values="price", aggfunc="last")
        .sort_index()
    )
    wide.index.name = "Date"
    return wide


def moving_average(
    data: pd.DataFrame,
    windows: list[int],
    price_column: str = "Close",
) -> pd.DataFrame:
    """
    Add multiple moving averages to DataFrame.
    """
    if price_column not in data.columns:
        raise KeyError(f"Column '{price_column}' not found. Available: {list(data.columns)}")

    out = data.copy()
    for w in windows:
        if int(w) <= 0:
            raise ValueError(f"Window must be positive: {w}")
        out[f"MA_{int(w)}"] = out[price_column].rolling(window=int(w), min_periods=int(w)).mean()
    return out


def build_ticker_ma(
    price_wide: pd.DataFrame,
    ticker: str,
    windows: list[int],
) -> pd.DataFrame:
    """
    Build Close + MA frame for one ticker from wide price frame.
    """
    if ticker not in price_wide.columns:
        raise KeyError(f"Ticker '{ticker}' not found in price data")
    df = price_wide[[ticker]].copy().rename(columns={ticker: "Close"})
    return moving_average(df, windows=windows, price_column="Close")


def plot_price_with_ma(
    df: pd.DataFrame,
    ticker: str,
    windows: list[int],
    plot_start: Optional[DateLike] = None,
) -> None:
    """
    Plot Close and multiple MA lines.
    """
    chart_df = df.copy()
    if plot_start is not None:
        chart_df = chart_df[chart_df.index >= pd.to_datetime(plot_start)]
    if chart_df.empty:
        return

    plt.figure(figsize=(14, 7))
    plt.plot(chart_df.index, chart_df["Close"], label="Close", color="black", linewidth=2, alpha=0.85)

    for w in windows:
        col = f"MA_{int(w)}"
        if col in chart_df.columns:
            plt.plot(chart_df.index, chart_df[col], label=col, alpha=0.9, linewidth=1.2 if int(w) < 100 else 2.0)

    plt.title(f"{ticker} Multi-MA")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.show()


def price_with_moving_average(
    price_data: pd.DataFrame,
    ticker: str,
    windows: list[int] = [20, 50, 120, 200],
    tail: Optional[int] = 10,
    plot: bool = True,
    plot_start: Optional[DateLike] = None,
) -> pd.DataFrame:
    """
    Use preloaded wide price data.
    """
    df = build_ticker_ma(price_data, ticker=ticker, windows=windows)
    if tail:
        print(f"\n[ {ticker} recent data + MA ]")
        print(df.tail(tail))
    if plot:
        plot_price_with_ma(df, ticker=ticker, windows=windows, plot_start=plot_start)
    return df


def price_with_moving_average_from_db(
    ticker: str,
    start_date: DateLike,
    end_date: DateLike,
    windows: list[int] = [20, 50, 120, 200],
    tail: Optional[int] = 10,
    plot: bool = True,
    plot_start: Optional[DateLike] = None,
) -> pd.DataFrame:
    """
    Fetch one ticker from DB then compute MA.
    """
    price_wide = fetch_price_wide([ticker], start_date=start_date, end_date=end_date)
    if price_wide.empty:
        raise ValueError(f"No price data: {ticker}, {start_date} ~ {end_date}")
    return price_with_moving_average(
        price_data=price_wide,
        ticker=ticker,
        windows=windows,
        tail=tail,
        plot=plot,
        plot_start=plot_start,
    )


def index_moving_average_map(
    index_name: str,
    ref_date: DateLike,
    start_date: DateLike,
    end_date: DateLike,
    windows: list[int] = [20, 50, 120, 200],
) -> dict[str, pd.DataFrame]:
    """
    Fetch index constituents + prices and return MA frame per ticker.
    """
    tickers = get_index_tickers(index_name=index_name, ref_date=ref_date)
    if not tickers:
        return {}

    price_wide = fetch_price_wide(tickers=tickers, start_date=start_date, end_date=end_date)
    if price_wide.empty:
        return {}

    result: dict[str, pd.DataFrame] = {}
    for t in tickers:
        if t in price_wide.columns:
            result[t] = build_ticker_ma(price_wide, ticker=t, windows=windows)
    return result