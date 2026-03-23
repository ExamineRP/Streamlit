from __future__ import annotations

import html
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from call import (
    get_constituents_for_date,
    get_op_factset_by_ticker,
    get_price_factset,
    get_sales_factset_by_ticker,
)
from utils import get_business_day_by_country

INDEX_OPTIONS = ["SPX Index", "NDX Index"]
RS_WEIGHTS = {"3M": 0.5, "6M": 0.3, "12M": 0.2}
RS_CALC_VERSION = "2026-03-20-relative-gross-v1"
SECTOR_CARD_ROWS = 6


def _ref_str(ref_date: date) -> str:
    d = ref_date.date() if hasattr(ref_date, "date") else ref_date
    return d.strftime("%Y-%m-%d")


def _format_mkt_cap(val):
    x = pd.to_numeric(val, errors="coerce")
    if pd.isna(x):
        return "-"
    x = float(x)
    if x >= 1_000_000_000_000:
        return f"{x / 1_000_000_000_000:.3f} T"
    if x >= 1_000_000_000:
        return f"{x / 1_000_000_000:.2f} B"
    if x >= 1_000_000:
        return f"{x / 1_000_000:.2f} M"
    return f"{x:,.0f}"


@st.cache_data(ttl=300)
def _cached_constituents(ref_str: str) -> pd.DataFrame:
    ref_d = datetime.strptime(ref_str, "%Y-%m-%d").date()
    frames = []
    for idx in INDEX_OPTIONS:
        data = get_constituents_for_date(idx, ref_d)
        if data is None or len(data) == 0:
            continue
        d = pd.DataFrame(data).copy()
        d["source_index"] = idx
        frames.append(d)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["bb_ticker"] = out.get("bb_ticker", "").astype(str).str.strip()
    out["name"] = out.get("name", out["bb_ticker"]).astype(str).str.strip()
    out["gics_name"] = out.get("gics_name", "-").fillna("-").astype(str).replace({"Information Technology": "IT"})
    out["index_weight"] = pd.to_numeric(out.get("index_weight"), errors="coerce")
    out["index_market_cap"] = pd.to_numeric(out.get("index_market_cap"), errors="coerce")
    out = out[out["bb_ticker"] != ""].copy()
    return out


@st.cache_data(ttl=300)
def _cached_prices(ref_str: str, start_str: str, end_str: str) -> pd.DataFrame:
    const_df = _cached_constituents(ref_str)
    if const_df.empty:
        return pd.DataFrame()
    tickers = const_df["bb_ticker"].dropna().astype(str).str.strip().unique().tolist()
    if not tickers:
        return pd.DataFrame()
    px = get_price_factset(tickers, start_str, end_str)
    px = pd.DataFrame(px) if px is not None else pd.DataFrame()
    if px.empty:
        return px
    px["dt"] = pd.to_datetime(px["dt"], errors="coerce")
    px["price"] = pd.to_numeric(px["price"], errors="coerce")
    px["bb_ticker"] = px["bb_ticker"].astype(str).str.strip()
    px["dt_date"] = px["dt"].dt.date
    return px.dropna(subset=["dt", "dt_date", "price", "bb_ticker"])


@st.cache_data(ttl=300)
def _cached_stock_detail(bb_ticker: str, ref_str: str):
    ref_d = datetime.strptime(ref_str, "%Y-%m-%d").date()
    start_str = (ref_d - timedelta(days=420)).strftime("%Y-%m-%d")
    px = get_price_factset([bb_ticker], start_str, ref_str)
    px_df = pd.DataFrame(px) if px is not None else pd.DataFrame()
    if not px_df.empty:
        px_df["dt"] = pd.to_datetime(px_df["dt"], errors="coerce")
        px_df["price"] = pd.to_numeric(px_df["price"], errors="coerce")
        px_df = px_df.dropna(subset=["dt", "price"]).sort_values("dt")

    factset_ticker = str(bb_ticker).split(" ")[0].strip()
    op_df = get_op_factset_by_ticker(factset_ticker)
    op_df = pd.DataFrame(op_df) if op_df is not None else pd.DataFrame()
    if not op_df.empty and "dt" in op_df.columns:
        op_df["dt"] = pd.to_datetime(op_df["dt"], errors="coerce")
        op_df = op_df.sort_values("dt", ascending=False)

    sales_df = get_sales_factset_by_ticker(factset_ticker)
    sales_df = pd.DataFrame(sales_df) if sales_df is not None else pd.DataFrame()
    if not sales_df.empty and "dt" in sales_df.columns:
        sales_df["dt"] = pd.to_datetime(sales_df["dt"], errors="coerce")
        sales_df = sales_df.sort_values("dt", ascending=False)
    return px_df, op_df, sales_df


def _latest_val(df: pd.DataFrame, candidates: list[str]):
    if df.empty:
        return None
    for col in candidates:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            if s.notna().any():
                return float(s.dropna().iloc[0])
    return None


def _fmt_num(v, as_pct: bool = False):
    if v is None or pd.isna(v):
        return "-"
    x = float(v)
    if as_pct:
        return f"{x:+.2f}%"
    if abs(x) >= 1_000_000_000_000:
        return f"{x/1_000_000_000_000:.2f}T"
    if abs(x) >= 1_000_000_000:
        return f"{x/1_000_000_000:.2f}B"
    if abs(x) >= 1_000_000:
        return f"{x/1_000_000:.2f}M"
    return f"{x:,.2f}"


def _render_stock_detail_panel(selected_row: pd.Series, ref_str: str):
    bb_ticker = str(selected_row.get("bb_ticker", "")).strip()
    if not bb_ticker:
        return
    px_df, op_df, sales_df = _cached_stock_detail(bb_ticker, ref_str)

    st.markdown("---")
    title_left, title_right = st.columns([7, 1.8])
    with title_left:
        st.subheader(f"{selected_row.get('Name', bb_ticker)} ({bb_ticker})")
    with title_right:
        if st.button("????", key=f"rs_detail_back_{bb_ticker}", use_container_width=True, type="primary"):
            st.session_state.pop("rs_selected_bb_ticker", None)
            st.rerun()

    left, right = st.columns([2.2, 1.0], gap="large")
    with left:
        if px_df.empty:
            st.info("?? ???? ????.")
        else:
            chart_df = px_df.copy()
            chart_df["MA20"] = chart_df["price"].rolling(20, min_periods=20).mean()
            chart_df["MA60"] = chart_df["price"].rolling(60, min_periods=60).mean()
            chart_df["MA120"] = chart_df["price"].rolling(120, min_periods=120).mean()
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(x=chart_df["dt"], y=chart_df["price"], mode="lines", name="Close", line=dict(color="#1f2937", width=2.0))
            )
            for ma_col, color in [("MA20", "#f59e0b"), ("MA60", "#8b5cf6"), ("MA120", "#06b6d4")]:
                if chart_df[ma_col].notna().any():
                    fig.add_trace(
                        go.Scatter(x=chart_df["dt"], y=chart_df[ma_col], mode="lines", name=ma_col, line=dict(color=color, width=1.6))
                    )
            fig.update_layout(
                height=460,
                template="plotly_white",
                margin=dict(l=8, r=8, t=8, b=8),
                xaxis_title="Date",
                yaxis_title="Price",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            )
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("**?? ??**")
        c1, c2 = st.columns(2)
        with c1:
            st.metric("????", str(selected_row.get("Market Cap($)", "-")))
            st.metric("??", str(selected_row.get("Sector", "-")))
            st.metric("RS", str(selected_row.get("RS", "-")))
        with c2:
            st.metric("RS(1M)", str(selected_row.get("RS(1M)", "-")))
            st.metric("RS(3M)", str(selected_row.get("RS(3M)", "-")))
            st.metric("RS(6M)", str(selected_row.get("RS(6M)", "-")))

        st.markdown("**?? ?? (??)**")
        revenue = _latest_val(sales_df, ["sales", "revenue", "???", "amount"])
        op_income = _latest_val(op_df, ["operating_income", "op", "????"])
        net_income = _latest_val(op_df, ["net_income", "?????", "netincome"])
        eps = _latest_val(op_df, ["eps", "diluted_eps", "basic_eps"])
        roe = _latest_val(op_df, ["roe", "ROE"])

        fin_df = pd.DataFrame(
            {
                "??": ["??", "????", "???", "EPS", "ROE"],
                "?": [
                    _fmt_num(revenue),
                    _fmt_num(op_income),
                    _fmt_num(net_income),
                    _fmt_num(eps),
                    _fmt_num(roe, as_pct=True),
                ],
            }
        )
        st.dataframe(fin_df, use_container_width=True, hide_index=True)


@st.cache_data(ttl=300)
def _cached_rs_board(ref_str: str, start_str: str, end_str: str, calc_ver: str) -> pd.DataFrame:
    _ = calc_ver
    const_df = _cached_constituents(ref_str)
    px_df = _cached_prices(ref_str, start_str, end_str)
    if const_df.empty or px_df.empty:
        return pd.DataFrame()

    asof = datetime.strptime(ref_str, "%Y-%m-%d").date()
    ytd_start = date(asof.year, 1, 1)
    d1, m1, m3, m6, y1 = 1, 21, 63, 126, 252

    rows = []
    for bb, g in px_df.groupby("bb_ticker"):
        s = g.sort_values("dt")[["dt_date", "price"]].dropna().reset_index(drop=True)
        if len(s) <= y1:
            continue

        p0 = float(s.iloc[-1]["price"])
        p_1d = float(s.iloc[-(d1 + 1)]["price"]) if len(s) > d1 else None
        p_1m = float(s.iloc[-(m1 + 1)]["price"]) if len(s) > m1 else None
        p_3m = float(s.iloc[-(m3 + 1)]["price"]) if len(s) > m3 else None
        p_6m = float(s.iloc[-(m6 + 1)]["price"]) if len(s) > m6 else None
        p_1y = float(s.iloc[-(y1 + 1)]["price"]) if len(s) > y1 else None

        ytd_candidates = s[s["dt_date"] >= ytd_start]
        p_ytd = float(ytd_candidates.iloc[0]["price"]) if not ytd_candidates.empty else None
        if not (p_1m and p_3m and p_6m and p_1y):
            continue

        ret_1d = ((p0 - p_1d) / p_1d * 100.0) if p_1d and p_1d > 0 else None
        ret_1m = ((p0 - p_1m) / p_1m * 100.0) if p_1m and p_1m > 0 else None
        ret_3m = ((p0 - p_3m) / p_3m * 100.0) if p_3m and p_3m > 0 else None
        ret_6m = ((p0 - p_6m) / p_6m * 100.0) if p_6m and p_6m > 0 else None
        ret_1y = ((p0 - p_1y) / p_1y * 100.0) if p_1y and p_1y > 0 else None
        ret_ytd = ((p0 - p_ytd) / p_ytd * 100.0) if p_ytd and p_ytd > 0 else None
        if ret_1m is None or ret_3m is None or ret_6m is None:
            continue

        rows.append(
            {
                "bb_ticker": bb,
                "ret_1d_pct": ret_1d,
                "ret_1m_pct": ret_1m,
                "ret_3m_pct": ret_3m,
                "ret_6m_pct": ret_6m,
                "ret_ytd_pct": ret_ytd,
                "ret_1y_pct": ret_1y,
            }
        )

    rs_df = pd.DataFrame(rows)
    if rs_df.empty:
        return rs_df

    spx_tickers = set(
        const_df.loc[const_df["source_index"] == "SPX Index", "bb_ticker"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )
    spx_weight_map = (
        const_df.loc[const_df["source_index"] == "SPX Index", ["bb_ticker", "index_weight"]]
        .drop_duplicates(subset=["bb_ticker"], keep="first")
        .set_index("bb_ticker")["index_weight"]
    )
    rs_df["spx_weight"] = pd.to_numeric(rs_df["bb_ticker"].map(spx_weight_map), errors="coerce")

    def _weighted_benchmark_return(col: str):
        mask = rs_df["bb_ticker"].isin(spx_tickers)
        vals = pd.to_numeric(rs_df.loc[mask, col], errors="coerce")
        ws = pd.to_numeric(rs_df.loc[mask, "spx_weight"], errors="coerce")
        valid = vals.notna()
        if not valid.any():
            return None
        vals = vals[valid]
        ws = ws[valid]
        if ws.notna().any() and float(ws.fillna(0).sum()) > 0:
            w = ws.fillna(0)
            return float((vals * w).sum() / w.sum())
        return float(vals.mean())

    bm_1m = _weighted_benchmark_return("ret_1m_pct")
    bm_3m = _weighted_benchmark_return("ret_3m_pct")
    bm_6m = _weighted_benchmark_return("ret_6m_pct")
    bm_12m = _weighted_benchmark_return("ret_1y_pct")
    rs_df = rs_df.drop(columns=["spx_weight"], errors="ignore")

    def _relative_return(stock_ret: pd.Series, bm_ret):
        s = pd.to_numeric(stock_ret, errors="coerce")
        if bm_ret is None or pd.isna(bm_ret):
            return pd.Series(pd.NA, index=s.index, dtype="Float64")
        # ?????? ??? ?? ???? ??? ??? ??? ????
        # ?? ???? ??? ? ?? ??(?? ???)? ??? ? ??.
        bm_gross = 1.0 + float(bm_ret) / 100.0
        if bm_gross <= 0:
            return pd.Series(pd.NA, index=s.index, dtype="Float64")
        stock_gross = 1.0 + (s / 100.0)
        out = stock_gross / bm_gross
        return pd.to_numeric(out, errors="coerce").astype("Float64")

    rs_df["rel_1m_ratio"] = _relative_return(rs_df["ret_1m_pct"], bm_1m)
    rs_df["rel_3m_ratio"] = _relative_return(rs_df["ret_3m_pct"], bm_3m)
    rs_df["rel_6m_ratio"] = _relative_return(rs_df["ret_6m_pct"], bm_6m)
    rs_df["rel_12m_ratio"] = _relative_return(rs_df["ret_1y_pct"], bm_12m)
    rs_df["weighted_score"] = (
        rs_df["rel_3m_ratio"] * RS_WEIGHTS["3M"]
        + rs_df["rel_6m_ratio"] * RS_WEIGHTS["6M"]
        + rs_df["rel_12m_ratio"] * RS_WEIGHTS["12M"]
    )

    def _to_rs_1_99_by_spx(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce")
        base = s[rs_df["bb_ticker"].isin(spx_tickers)].dropna().sort_values().to_numpy()
        if len(base) == 0:
            pct = s.rank(pct=True)
            rs_val = (pct * 98).apply(lambda v: int(v) if pd.notna(v) else pd.NA).astype("Int64") + 1
            return rs_val.clip(lower=1, upper=99).astype("Int64")

        def _map_one(v):
            if pd.isna(v):
                return pd.NA
            pos = int(np.searchsorted(base, float(v), side="right"))
            pct = pos / len(base)
            return int(pct * 98) + 1

        rs_val = s.apply(_map_one).astype("Int64")
        return rs_val.clip(lower=1, upper=99).astype("Int64")

    rs_df["RS"] = _to_rs_1_99_by_spx(rs_df["weighted_score"])
    rs_df["RS(1M)"] = _to_rs_1_99_by_spx(rs_df["rel_1m_ratio"])
    rs_df["RS(3M)"] = _to_rs_1_99_by_spx(rs_df["rel_3m_ratio"])
    rs_df["RS(6M)"] = _to_rs_1_99_by_spx(rs_df["rel_6m_ratio"])
    rs_df["RS(12M)"] = _to_rs_1_99_by_spx(rs_df["rel_12m_ratio"])

    out = const_df.merge(rs_df, on="bb_ticker", how="inner")
    out["Sector"] = out["gics_name"].fillna("-").astype(str)
    out["Name"] = out["name"].fillna("").astype(str)
    out["market_cap_num"] = pd.to_numeric(out["index_market_cap"], errors="coerce")
    out["Market Cap($)"] = out["market_cap_num"].apply(_format_mkt_cap)
    out["sector_rank"] = out.groupby("Sector")["RS"].rank(ascending=False, method="first").astype("Int64")

    for col in ["RS", "RS(1M)", "RS(3M)", "RS(6M)", "RS(12M)"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").clip(lower=1, upper=99).astype("Int64")
    return out.sort_values(["RS", "Name"], ascending=[False, True]).reset_index(drop=True)


def render():
    st.subheader("RS")
    try:
        ref_date = get_business_day_by_country(datetime.now().date(), 1, "KR")
        ref_d = ref_date.date() if hasattr(ref_date, "date") else ref_date
        ref_str = _ref_str(ref_d)
        start_str = (ref_d - timedelta(days=460)).strftime("%Y-%m-%d")
        end_str = ref_d.strftime("%Y-%m-%d")

        st.caption(
            f"Reference date: {ref_d} / Universe: SPX + NDX / "
            f"RS weights: 3M {RS_WEIGHTS['3M']:.0%}, 6M {RS_WEIGHTS['6M']:.0%}, 12M {RS_WEIGHTS['12M']:.0%}"
        )

        with st.spinner("Calculating RS data..."):
            board = _cached_rs_board(ref_str, start_str, end_str, RS_CALC_VERSION)
        if board.empty:
            st.warning("No RS data available.")
            return

        # 1) ?? ??: RS ???? ?? ??/??? 1?? ??
        dedup_board = board.copy()
        dedup_board = dedup_board.sort_values(["RS", "Name"], ascending=[False, True])
        # ?? ??/?? ??? ?? ??
        dedup_board = dedup_board.drop_duplicates(subset=["bb_ticker"], keep="first")
        dedup_board = dedup_board.drop_duplicates(subset=["Name"], keep="first").drop(
            columns=["index_weight"], errors="ignore"
        )
        # SPX/NDX ?? ??? RS ????? ??
        dedup_board = dedup_board.sort_values(["RS", "Name"], ascending=[False, True]).reset_index(drop=True)
        dedup_board["sector_rank"] = (
            dedup_board.groupby("Sector")["RS"].rank(ascending=False, method="first").astype("Int64")
        )

        # ?? ???? ????: ?? ??? ??? ?? ?? ?? ??? ??
        selected_bb = str(st.session_state.get("rs_selected_bb_ticker", "")).strip()
        if selected_bb:
            selected_match = dedup_board[dedup_board["bb_ticker"].astype(str).str.strip() == selected_bb].head(1)
            if not selected_match.empty:
                _render_stock_detail_panel(selected_match.iloc[0], ref_str)
                return
            st.session_state.pop("rs_selected_bb_ticker", None)

        # 2) SPX/NDX ??? ? ??(?? ? ? ??)
        def _build_benchmark_rows(src_df: pd.DataFrame) -> pd.DataFrame:
            rows = []
            rs_cols = ["RS", "RS(1M)", "RS(3M)", "RS(6M)", "RS(12M)"]
            for idx_name in INDEX_OPTIONS:
                g = src_df[src_df["source_index"] == idx_name].copy()
                if g.empty:
                    continue

                weights = pd.to_numeric(g.get("index_weight"), errors="coerce")

                def _agg(col: str):
                    vals = pd.to_numeric(g.get(col), errors="coerce")
                    valid = vals.notna()
                    if not valid.any():
                        return pd.NA
                    if weights.notna().any() and float(weights.fillna(0).sum()) > 0:
                        w = weights.fillna(0)
                        return float((vals.fillna(0) * w).sum() / w.sum())
                    return float(vals.mean())

                row = {
                    "Sector": "",
                    "Name": "SPX" if idx_name.startswith("SPX") else "NDX",
                    "Market Cap($)": _format_mkt_cap(pd.to_numeric(g.get("market_cap_num"), errors="coerce").sum()),
                }
                for col in rs_cols:
                    v = _agg(col)
                    row[col] = int(round(v)) if pd.notna(v) else pd.NA
                rows.append(row)
            out = pd.DataFrame(rows)
            if out.empty:
                return out
            for col in rs_cols:
                out[col] = pd.to_numeric(out[col], errors="coerce").clip(lower=1, upper=99).astype("Int64")
            return out

        benchmark_rows = _build_benchmark_rows(board)

        page_key = "rs_board_page"
        size_key = "rs_board_page_size"
        if size_key not in st.session_state:
            st.session_state[size_key] = 15
        if page_key not in st.session_state:
            st.session_state[page_key] = 1

        toolbar_left, toolbar_right = st.columns([6.3, 1.7], gap="small")
        with toolbar_right:
            st.select_slider(
                "Rows per page",
                options=[10, 15, 20, 30, 50],
                key=size_key,
                label_visibility="collapsed",
            )
        page_size = int(st.session_state[size_key])

        total = len(dedup_board)
        total_pages = max(1, (total + page_size - 1) // page_size)
        current = max(1, min(int(st.session_state[page_key]), total_pages))
        st.session_state[page_key] = current
        start_i = (current - 1) * page_size
        end_i = min(start_i + page_size, total)
        display_cols = ["Sector", "Name", "RS", "RS(1M)", "RS(3M)", "RS(6M)", "Market Cap($)"]
        show_stock = dedup_board.iloc[start_i:end_i][display_cols].copy()
        if benchmark_rows.empty:
            show = show_stock
        else:
            show = pd.concat([benchmark_rows, show_stock], ignore_index=True)
        show = show[display_cols]
        ticker_map = (
            dedup_board[["Name", "bb_ticker"]]
            .dropna(subset=["Name", "bb_ticker"])
            .drop_duplicates(subset=["Name"], keep="first")
            .set_index("Name")["bb_ticker"]
            .to_dict()
        )
        show_meta = show.copy()
        show_meta["bb_ticker"] = show_meta["Name"].map(ticker_map)

        rs_cols = ["RS", "RS(1M)", "RS(3M)", "RS(6M)"]
        spx_row = show[show["Name"].astype(str).str.upper() == "SPX"]
        spx_ref = {}
        if not spx_row.empty:
            for c in rs_cols:
                spx_ref[c] = pd.to_numeric(spx_row.iloc[0][c], errors="coerce")

        def _style_rs_vs_spx_row(row: pd.Series):
            name_val = str(show.loc[row.name, "Name"]).upper()
            if name_val in {"SPX", "NDX"}:
                return [""] * len(row)

            out = []
            for col_name, v in row.items():
                ref = spx_ref.get(col_name, pd.NA)
                x = pd.to_numeric(v, errors="coerce")
                if pd.isna(x) or pd.isna(ref):
                    out.append("")
                elif x > ref:
                    out.append("color:#2e7d32; font-weight:700;")
                elif x < ref:
                    out.append("color:#c62828; font-weight:700;")
                else:
                    out.append("")
            return out

        def _style_index_name(v):
            s = str(v).upper()
            if s == "SPX":
                return "background-color:#e8f1ff; color:#1565c0; font-weight:700;"
            if s == "NDX":
                return "background-color:#f4e8ff; color:#7b1fa2; font-weight:700;"
            return ""

        styled = (
            show.style
            .applymap(_style_index_name, subset=["Name"])
            .apply(_style_rs_vs_spx_row, axis=1, subset=rs_cols)
            .set_properties(**{"text-align": "center"})
            .set_properties(subset=rs_cols, **{"font-size": "24px", "font-weight": "700"})
            .set_table_styles(
                [
                    {"selector": "th", "props": [("font-size", "18px"), ("text-align", "center"), ("padding", "6px 10px")]},
                    {"selector": "td", "props": [("font-size", "22px"), ("text-align", "center"), ("padding", "5px 10px")]},
                ]
            )
            .format({"RS": "{:d}", "RS(1M)": "{:d}", "RS(3M)": "{:d}", "RS(6M)": "{:d}"}, na_rep="")
        )
        table_event = st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            height=min(980, 28 + int(len(show) * 35)),
            key="rs_board_table",
            on_select="rerun",
            selection_mode="single-row",
        )
        selected_rows = []
        if table_event is not None:
            try:
                selected_rows = list(table_event.selection.rows)
            except Exception:
                selected_rows = table_event.get("selection", {}).get("rows", []) if isinstance(table_event, dict) else []
        if selected_rows:
            sel_idx = int(selected_rows[0])
            if 0 <= sel_idx < len(show_meta):
                sel_bb = str(show_meta.iloc[sel_idx].get("bb_ticker", "")).strip()
                sel_name = str(show_meta.iloc[sel_idx].get("Name", "")).upper().strip()
                if sel_bb and sel_name not in {"SPX", "NDX"}:
                    st.session_state["rs_selected_bb_ticker"] = sel_bb
                    st.rerun()

        nav = [{"label": "First", "target": 1}, {"label": "Prev", "target": max(1, current - 1)}]
        for p in range(max(1, current - 1), min(total_pages, current + 1) + 1):
            nav.append({"label": str(p), "target": p})
        nav.extend([{"label": "Next", "target": min(total_pages, current + 1)}, {"label": "Last", "target": total_pages}])

        footer_left, nav_center, footer_right = st.columns([3.6, 2.2, 2.6], gap="small")
        with footer_left:
            st.caption(f"Stocks {start_i + 1 if total else 0}-{end_i} / {total}")

        with nav_center:
            nav_widths = [0.95 if item["label"].isdigit() else 1.25 for item in nav]
            cols = st.columns(nav_widths, gap="small")
            for idx, (c, item) in enumerate(zip(cols, nav)):
                with c:
                    if st.button(
                        item["label"],
                        key=f"rs_nav_{current}_{idx}_{item['target']}",
                        use_container_width=True,
                    ):
                        st.session_state[page_key] = int(item["target"])
                        st.rerun()
        with footer_right:
            st.markdown(
                f"<div style='text-align:right; color:#6b7280; font-size:0.8rem;'>Page {current} / {total_pages}</div>",
                unsafe_allow_html=True,
            )

        st.markdown("---")
        sec = dedup_board[
            ["Sector", "sector_rank", "Name", "RS", "Market Cap($)", "ret_1d_pct", "ret_1m_pct", "ret_ytd_pct", "ret_1y_pct"]
        ].copy()
        sec = sec.rename(columns={"Market Cap($)": "MarketCapTxt"})
        if sec.empty:
            return
        order = sec.groupby("Sector", as_index=False)["RS"].max().sort_values("RS", ascending=False)["Sector"].tolist()
        sec_palette = ["#b58af7", "#4f86f7", "#26a69a", "#66bb6a", "#f2a516", "#ef5350", "#8d6e63", "#78909c", "#ec407a"]
        sec_colors = {s: c for s, c in zip(order, sec_palette * 2)}
        st.markdown(
            """
            <style>
            .rs-card{border:1px solid #e6e8ee;border-radius:10px;padding:8px 12px;margin-bottom:10px;background:#fff}
            .rs-title{font-weight:700;font-size:26px;line-height:1.15;margin-bottom:8px;color:#1f2937}
            .rs-dot{
                display:inline-block;
                width:10px;
                height:10px;
                border-radius:50%;
                margin-right:6px;
                vertical-align:middle;
            }
            .rs-item{
                border:none;
                border-top:1px solid #edf0f6;
                border-radius:0;
                padding:8px 0;
                margin-bottom:0;
                background:#fff;
            }
            .rs-card .rs-item:first-of-type{border-top:none}
            .rs-row{
                display:grid;
                grid-template-columns: 2.6fr 1.1fr 3fr;
                align-items:center;
                column-gap:10px;
            }
            .rs-name{font-size:22px;font-weight:700;line-height:1.2;color:#25324a}
            .rs-rs{font-size:26px;font-weight:700;text-align:center;line-height:1.2;color:#25324a}
            .rs-cap{font-size:16px;color:#6b7280;line-height:1.2;margin-top:3px}
            .rs-metrics{
                display:grid;
                grid-template-columns:repeat(2,minmax(0,1fr));
                gap:6px 12px;
                font-size:20px;
                justify-items:start;
            }
            .rs-metric{white-space:nowrap; line-height:1.2}
            .rs-label{color:#334155;margin-right:4px;font-weight:700}
            .pos{color:#43a047;font-weight:700}
            .neg{color:#e24b4b;font-weight:700}
            </style>
            """,
            unsafe_allow_html=True,
        )

        def _pct(v):
            x = pd.to_numeric(v, errors="coerce")
            if pd.isna(x):
                return "-", ""
            return f"{float(x):+.1f}%", ("pos" if float(x) >= 0 else "neg")

        cards = st.columns(2)
        for i, sector in enumerate(order):
            d = sec[sec["Sector"] == sector].sort_values(["sector_rank", "Name"], ascending=[True, True]).head(SECTOR_CARD_ROWS).copy()
            if d.empty:
                continue
            html_items = ""
            for row in d.itertuples(index=False):
                d1, d1c = _pct(row.ret_1d_pct)
                m1, m1c = _pct(row.ret_1m_pct)
                ytd, ytdc = _pct(row.ret_ytd_pct)
                y1, y1c = _pct(row.ret_1y_pct)
                html_items += (
                    "<div class='rs-item'>"
                    "<div class='rs-row'>"
                    f"<div class='rs-name'>{row.sector_rank}.&nbsp; {html.escape(str(row.Name))}</div>"
                    f"<div class='rs-rs'>RS {int(row.RS) if pd.notna(row.RS) else '-'}</div>"
                    "<div class='rs-metrics'>"
                    f"<div class='rs-metric'><span class='rs-label'>1D</span><span class='{d1c}'>{d1}</span></div>"
                    f"<div class='rs-metric'><span class='rs-label'>1M</span><span class='{m1c}'>{m1}</span></div>"
                    f"<div class='rs-metric'><span class='rs-label'>YTD</span><span class='{ytdc}'>{ytd}</span></div>"
                    f"<div class='rs-metric'><span class='rs-label'>1Y</span><span class='{y1c}'>{y1}</span></div>"
                    "</div>"
                    "</div>"
                    "<div class='rs-row'>"
                    f"<div class='rs-cap'>{html.escape(str(row.MarketCapTxt))}</div>"
                    "<div></div><div></div>"
                    "</div>"
                    "</div>"
                )
            block = (
                f"<div class='rs-card'><div class='rs-title'><span class='rs-dot' style='background:{sec_colors.get(sector, '#b58af7')}'></span>{html.escape(str(sector))}</div>"
                f"{html_items}</div>"
            )
            with cards[i % 2]:
                st.markdown(block, unsafe_allow_html=True)
    except Exception as exc:
        st.error(f"RS tab error: {exc}")