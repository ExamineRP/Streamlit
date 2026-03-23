"""
종목 분석 탭 - index_constituents + price_factset 기반, 지수별 구성종목·가격
기준일: 오늘 KR 1영업일 전. SPX Index / NDX Index만 지원.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from call import (
    get_constituents_for_date,
    get_price_factset,
    get_52w_high_stocks_from_factset,
    get_all_constituents_52w_summary,
    get_earnings_calendar_closest_dates,
    get_earnings_calendar_by_date_range,
    execute_custom_query,
)
from utils import get_business_day_by_country
from performance_RS import render as render_RS

INDEX_OPTIONS = ["SPX Index", "NDX Index"]

# 섹터별 아이콘 미사용
SECTOR_EMOJI = {}
# 섹터별 배경색 통일 (실적 캘린더 타일용, 연한 톤)
SECTOR_COLOR = {
    "Energy": "#fef3e6",
    "Materials": "#f5f0e6", "Basic Materials": "#f5f0e6",
    "Industrials": "#e8f4f8",
    "Consumer Discretionary": "#fce8ec",
    "Consumer Staples": "#f0f7ee", "Consumer Defensive": "#f0f7ee",
    "Health Care": "#e8f5e9", "Healthcare": "#e8f5e9",
    "Financials": "#e3f2fd",
    "Information Technology": "#e8eaf6",
    "Communication Services": "#f3e5f5",
    "Utilities": "#fff8e1",
    "Real Estate": "#efebe9",
}

PERIOD_OPTIONS = [ # 수익률 기간: (라벨, US 영업일 수; None이면 YTD)
    ("Daily", 1),
    ("1W", 5),
    ("1M", 21),
    ("3M", 63),
    ("1Y", 252),
    ("YTD", None),
]

# 무거운 DB 조회 캐시 (5분) — 다중 사용자·리런 시 동일 키로 캐시 공유, 중복 조회 방지
# 캐시 키는 ref_str(YYYY-MM-DD)로 통일해 date/datetime 혼용 시 캐시 분리 방지

def _ref_str(ref_date):
    """캐시 키·API 호출용 기준일 문자열"""
    d = ref_date.date() if hasattr(ref_date, "date") else ref_date
    return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)


@st.cache_data(ttl=300)
def _cached_constituents_impl(index_name: str, ref_str: str) -> pd.DataFrame:
    from datetime import datetime as dt
    ref = dt.strptime(ref_str, "%Y-%m-%d").date()
    return get_constituents_for_date(index_name, ref)


def _cached_constituents(index_name: str, ref_date) -> pd.DataFrame:
    return _cached_constituents_impl(index_name, _ref_str(ref_date))


@st.cache_data(ttl=300)
def _cached_52w_high_impl(index_name: str, ref_str: str) -> pd.DataFrame:
    from datetime import datetime as dt
    ref = dt.strptime(ref_str, "%Y-%m-%d").date()
    return get_52w_high_stocks_from_factset(index_name, ref)


def _cached_52w_high(index_name: str, ref_date) -> pd.DataFrame:
    return _cached_52w_high_impl(index_name, _ref_str(ref_date))


@st.cache_data(ttl=300)
def _cached_52w_summary_impl(index_name: str, ref_str: str) -> pd.DataFrame:
    from datetime import datetime as dt
    ref = dt.strptime(ref_str, "%Y-%m-%d").date()
    return get_all_constituents_52w_summary(index_name, ref)


def _cached_52w_summary(index_name: str, ref_date) -> pd.DataFrame:
    return _cached_52w_summary_impl(index_name, _ref_str(ref_date))


@st.cache_data(ttl=300)
def _cached_price_df_impl(index_name: str, ref_str: str, start_str: str, end_str: str) -> pd.DataFrame:
    const = _cached_constituents_impl(index_name, ref_str)
    if const.empty:
        return pd.DataFrame()
    bb_tickers = const["bb_ticker"].dropna().astype(str).str.strip().unique().tolist()
    return get_price_factset(bb_tickers, start_str, end_str)


def _cached_price_df(index_name: str, ref_date, start_str: str, ref_str: str) -> pd.DataFrame:
    return _cached_price_df_impl(index_name, _ref_str(ref_date), start_str, ref_str)


@st.cache_data(ttl=300)
def _cached_op_factset_ticker_list() -> pd.DataFrame:
    """재무 탭 종목 목록 캐시"""
    from call import get_op_factset_ticker_list
    return get_op_factset_ticker_list()


@st.cache_data(ttl=300)
def _cached_op_factset_by_ticker(factset_ticker: str) -> pd.DataFrame:
    """재무 탭 개별 종목 데이터 캐시"""
    from call import get_op_factset_by_ticker
    return get_op_factset_by_ticker(factset_ticker)


@st.cache_data(ttl=300)
def _cached_sales_factset_ticker_list() -> pd.DataFrame:
    """재무 탭 Sales 종목 목록 캐시"""
    from call import get_sales_factset_ticker_list
    return get_sales_factset_ticker_list()


@st.cache_data(ttl=300)
def _cached_sales_factset_by_ticker(factset_ticker: str) -> pd.DataFrame:
    """재무 탭 Sales 개별 종목 데이터 캐시"""
    from call import get_sales_factset_by_ticker
    return get_sales_factset_by_ticker(factset_ticker)


@st.cache_data(ttl=600)
def _cached_index_constituents_name_map() -> pd.DataFrame:
    """index_constituents의 최신 factset_ticker -> name 매핑 캐시"""
    query = """
        SELECT DISTINCT ON (SPLIT_PART(bb_ticker, ' ', 1))
            SPLIT_PART(bb_ticker, ' ', 1) AS factset_ticker,
            name
        FROM index_constituents
        WHERE bb_ticker IS NOT NULL
          AND TRIM(bb_ticker) <> ''
          AND TRIM(SPLIT_PART(bb_ticker, ' ', 1)) <> ''
          AND name IS NOT NULL
          AND TRIM(name) <> ''
        ORDER BY SPLIT_PART(bb_ticker, ' ', 1), dt DESC
    """
    rows = execute_custom_query(query)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["factset_ticker"] = df["factset_ticker"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[(df["factset_ticker"] != "") & (df["name"] != "")]
    return df.drop_duplicates(subset=["factset_ticker"], keep="first")


def _get_op_factset_by_ticker_fast(factset_ticker: str, max_keep: int = 12) -> pd.DataFrame:
    """
    전역 캐시 + 세션 최근조회 캐시를 함께 사용해 종목 전환 속도 최적화.
    - st.cache_data: 앱 전역 재사용
    - session_state: 현재 사용자가 최근 본 종목은 즉시 반환
    """
    data_key = "_재무_op_session_cache"
    order_key = "_재무_op_session_cache_order"
    if data_key not in st.session_state:
        st.session_state[data_key] = {}
    if order_key not in st.session_state:
        st.session_state[order_key] = []

    cache_map = st.session_state[data_key]
    cache_order = st.session_state[order_key]

    if factset_ticker in cache_map:
        return cache_map[factset_ticker].copy()

    df = _cached_op_factset_by_ticker(factset_ticker)
    cache_map[factset_ticker] = df.copy()
    if factset_ticker in cache_order:
        cache_order.remove(factset_ticker)
    cache_order.append(factset_ticker)

    while len(cache_order) > max_keep:
        old = cache_order.pop(0)
        cache_map.pop(old, None)

    st.session_state[data_key] = cache_map
    st.session_state[order_key] = cache_order
    return df.copy()


def _get_sales_factset_by_ticker_fast(factset_ticker: str, max_keep: int = 12) -> pd.DataFrame:
    """
    Sales 데이터: 전역 캐시 + 세션 최근조회 캐시를 함께 사용.
    """
    data_key = "_재무_sales_session_cache"
    order_key = "_재무_sales_session_cache_order"
    if data_key not in st.session_state:
        st.session_state[data_key] = {}
    if order_key not in st.session_state:
        st.session_state[order_key] = []

    cache_map = st.session_state[data_key]
    cache_order = st.session_state[order_key]

    if factset_ticker in cache_map:
        return cache_map[factset_ticker].copy()

    df = _cached_sales_factset_by_ticker(factset_ticker)
    cache_map[factset_ticker] = df.copy()
    if factset_ticker in cache_order:
        cache_order.remove(factset_ticker)
    cache_order.append(factset_ticker)

    while len(cache_order) > max_keep:
        old = cache_order.pop(0)
        cache_map.pop(old, None)

    st.session_state[data_key] = cache_map
    st.session_state[order_key] = cache_order
    return df.copy()


def render():
    """종목 분석 탭 렌더링"""
    today = datetime.now().date()
    try:
        ref_date = get_business_day_by_country(today, 1, "KR")
    except Exception:
        ref_date = today - timedelta(days=1)

    st.header("종목 분석")

    stock_tab_labels = ["종합", "실적 캘린더", "재무", "RS"]
    if "stock_sub_tab" not in st.session_state:
        st.session_state["stock_sub_tab"] = "종합"
    selected_stock_tab = st.session_state.get("stock_sub_tab", "종합")

    st.markdown(
        """
        <style>
        .st-key-kbam_stock_sub_tabs {
            position: sticky;
            top: 6.8rem;
            z-index: 995;
            background: #ffffff;
            border-bottom: 1px solid #eceff4;
            padding: 6px 0 4px 0;
        }
        .st-key-kbam_stock_fin_tabs {
            position: sticky;
            top: 10.4rem;
            z-index: 994;
            background: #ffffff;
            border-bottom: 1px solid #eceff4;
            padding: 4px 0 4px 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with st.container(key="kbam_stock_sub_tabs"):
        selected_stock_tab = st.radio(
            "종목 분석 하위 탭",
            options=stock_tab_labels,
            index=stock_tab_labels.index(selected_stock_tab) if selected_stock_tab in stock_tab_labels else 0,
            horizontal=True,
            label_visibility="collapsed",
            key="stock_sub_tab_nav",
        )
    st.session_state["stock_sub_tab"] = selected_stock_tab

    if selected_stock_tab == "종합":
        _render_종합(ref_date)
    elif selected_stock_tab == "실적 캘린더":
        _render_실적캘린더(ref_date)
    elif selected_stock_tab == "재무":
        _render_재무(ref_date)
    else:
        render_RS()


def _render_종합(ref_date):
    """종합: 수익률 TOP/WORST, 상승/하락, 52주, 차트 등"""
    st.caption(f"**기준일** (KR 1영업일 전): **{ref_date}**")
    selected_index = st.selectbox("Index 선택", INDEX_OPTIONS, key="종목분석_index")
    try:
        with st.spinner("구성종목 및 가격 조회 중..."):
            const_df = _cached_constituents(selected_index, ref_date)
        if const_df.empty:
            st.warning(f"기준일({ref_date}) 해당 Index({selected_index}) 구성종목이 없습니다.")
            return

        # DB에서 Decimal 등 올 수 있으므로 숫자 컬럼은 float로 통일
        for col in ["index_weight", "local_price"]:
            if col in const_df.columns:
                const_df[col] = pd.to_numeric(const_df[col], errors="coerce")

        # 수익률 기간 선택 (US 영업일 기준)
        period_choice = st.selectbox(
            "수익률 기간",
            options=[p[0] for p in PERIOD_OPTIONS],
            index=0,
            key="종목분석_period",
        )
        period_days = next(p[1] for p in PERIOD_OPTIONS if p[0] == period_choice)

        # 가격 조회 구간: 1Y·YTD 대비하기 위해 넉넉히 (US 영업일 기준)
        ref_d = ref_date.date() if hasattr(ref_date, "date") else ref_date
        try:
            us_1y_back = get_business_day_by_country(ref_d, 252, "US")
        except Exception:
            us_1y_back = ref_d - timedelta(days=365)
        ytd_start = date(ref_d.year, 1, 1)
        fetch_start = min(ytd_start, us_1y_back)
        ref_str = ref_d.strftime("%Y-%m-%d")
        start_str = fetch_start.strftime("%Y-%m-%d")
        # 가격·52주: Index/기준일 바뀔 때만 재조회, 기준 지표(1M/3M 등)만 바꿀 땐 세션 캐시 재사용
        _cache_key = ("종목분석_종합", selected_index, ref_str)
        if _cache_key == st.session_state.get("_종목분석_data_key"):
            price_df = st.session_state["_종목분석_price_df"].copy()
            high52_df = st.session_state["_종목분석_high52_df"].copy()
            summary52_df = st.session_state["_종목분석_summary52_df"].copy()
        else:
            with st.spinner("가격·52주 데이터 조회 중..."):
                with ThreadPoolExecutor(max_workers=3) as ex:
                    f_price = ex.submit(_cached_price_df, selected_index, ref_date, start_str, ref_str)
                    f_52w_high = ex.submit(_cached_52w_high, selected_index, ref_date)
                    f_52w_summary = ex.submit(_cached_52w_summary, selected_index, ref_date)
                    price_df = f_price.result()
                    high52_df = f_52w_high.result()
                    summary52_df = f_52w_summary.result()
            if not price_df.empty:
                price_df = price_df.copy()
                price_df["price"] = pd.to_numeric(price_df["price"], errors="coerce")
                price_df["dt_date"] = price_df["dt"].dt.date
                price_df = price_df.sort_values("dt")
            st.session_state["_종목분석_data_key"] = _cache_key
            st.session_state["_종목분석_price_df"] = price_df.copy()
            st.session_state["_종목분석_high52_df"] = high52_df.copy()
            st.session_state["_종목분석_summary52_df"] = summary52_df.copy()

        # 선택 기간별 수익률 (US 영업일 기준)
        price_on_ref = pd.DataFrame(columns=["bb_ticker", "price_factset", "daily_ret_pct"])
        if not price_df.empty:
            price_df["price"] = pd.to_numeric(price_df["price"], errors="coerce")
            price_df["dt_date"] = price_df["dt"].dt.date
            price_df = price_df.sort_values("dt")
            ref_prices = price_df[price_df["dt_date"] <= ref_d].groupby("bb_ticker").last().reset_index()[["bb_ticker", "price"]].rename(columns={"price": "price_ref"})

            if period_days is not None:
                # Daily / 1W / 1M / 3M / 1Y: start = ref_d 기준 US 영업일 N일 전
                try:
                    start_date = get_business_day_by_country(ref_d, period_days, "US")
                except Exception:
                    start_date = ref_d - timedelta(days=max(period_days * 2, 30))
                start_prices = price_df[price_df["dt_date"] <= start_date].groupby("bb_ticker").last().reset_index()[["bb_ticker", "price"]].rename(columns={"price": "price_start"})
            else:
                # YTD: 해당 연도 첫 거래일 종가 (ref_d 기준 연도 1/1 이후 첫 관측일)
                first_in_year = price_df[price_df["dt_date"] >= ytd_start].groupby("bb_ticker")["dt_date"].min().reset_index().rename(columns={"dt_date": "first_dt"})
                start_prices = price_df.merge(first_in_year, left_on=["bb_ticker", "dt_date"], right_on=["bb_ticker", "first_dt"], how="inner")[["bb_ticker", "price"]].rename(columns={"price": "price_start"}).drop_duplicates(subset=["bb_ticker"], keep="first")

            both = ref_prices.merge(start_prices, on="bb_ticker", how="inner")
            both["price_start"] = pd.to_numeric(both["price_start"], errors="coerce")
            both["price_ref"] = pd.to_numeric(both["price_ref"], errors="coerce")
            both = both[both["price_start"] > 0]
            both["daily_ret_pct"] = (both["price_ref"].astype(float) - both["price_start"].astype(float)) / both["price_start"].astype(float) * 100.0
            price_on_ref = both[["bb_ticker", "price_ref", "daily_ret_pct"]].rename(columns={"price_ref": "price_factset"})
        else:
            price_on_ref["daily_ret_pct"] = None

        merged = const_df.merge(price_on_ref, on="bb_ticker", how="left")
        merged["index_weight"] = pd.to_numeric(merged["index_weight"], errors="coerce")
        merged = merged.sort_values(["gics_name", "index_weight"], ascending=[True, False])

        def _style_return(val):
            if pd.isna(val):
                return ""
            try:
                v = float(val)
                if v > 0:
                    return "color: #2e7d32; font-weight: bold;"
                if v < 0:
                    return "color: #c62828; font-weight: bold;"
            except (TypeError, ValueError):
                pass
            return ""

        def _style_52w_return(val):
            """52주 수익률/이격률: + 초록, - 빨강, 0/NA 색 없음"""
            if pd.isna(val):
                return ""
            try:
                v = float(val)
                if v > 0:
                    return "color: #2e7d32; font-weight: bold;"
                if v < 0:
                    return "color: #c62828; font-weight: bold;"
            except (TypeError, ValueError):
                pass
            return ""

        def _calc_period_return_table(price_source_df: pd.DataFrame, base_date) -> pd.DataFrame:
            """
            구성종목별 기간 수익률 테이블 생성.
            반환 컬럼: bb_ticker, price_ref, 1D, 1M, 3M, YTD, 1Y
            """
            out_cols = ["bb_ticker", "price_ref", "1D", "1M", "3M", "YTD", "1Y"]
            if price_source_df.empty:
                return pd.DataFrame(columns=out_cols)

            px = price_source_df.copy()
            if "dt_date" not in px.columns:
                px["dt_date"] = px["dt"].dt.date
            px["price"] = pd.to_numeric(px["price"], errors="coerce")
            px = px.dropna(subset=["bb_ticker", "dt_date", "price"])
            if px.empty:
                return pd.DataFrame(columns=out_cols)

            ref_prices = (
                px[px["dt_date"] <= base_date]
                .groupby("bb_ticker")
                .last()
                .reset_index()[["bb_ticker", "price"]]
                .rename(columns={"price": "price_ref"})
            )
            if ref_prices.empty:
                return pd.DataFrame(columns=out_cols)

            result = ref_prices.copy()

            def _merge_period_return(label: str, start_prices_df: pd.DataFrame):
                nonlocal result
                if start_prices_df.empty:
                    result[label] = pd.NA
                    return
                both = ref_prices.merge(start_prices_df, on="bb_ticker", how="inner")
                both["price_start"] = pd.to_numeric(both["price_start"], errors="coerce")
                both["price_ref"] = pd.to_numeric(both["price_ref"], errors="coerce")
                both = both[both["price_start"] > 0]
                both[label] = (both["price_ref"].astype(float) - both["price_start"].astype(float)) / both["price_start"].astype(float) * 100.0
                result = result.merge(both[["bb_ticker", label]], on="bb_ticker", how="left")

            for label, days in [("1D", 1), ("1M", 21), ("3M", 63), ("1Y", 252)]:
                try:
                    start_d = get_business_day_by_country(base_date, days, "US")
                except Exception:
                    start_d = base_date - timedelta(days=max(days * 2, 30))
                start_prices = (
                    px[px["dt_date"] <= start_d]
                    .groupby("bb_ticker")
                    .last()
                    .reset_index()[["bb_ticker", "price"]]
                    .rename(columns={"price": "price_start"})
                )
                _merge_period_return(label, start_prices)

            ytd_start_local = date(base_date.year, 1, 1)
            first_in_year = (
                px[px["dt_date"] >= ytd_start_local]
                .groupby("bb_ticker")["dt_date"]
                .min()
                .reset_index()
                .rename(columns={"dt_date": "first_dt"})
            )
            ytd_start_prices = (
                px.merge(
                    first_in_year,
                    left_on=["bb_ticker", "dt_date"],
                    right_on=["bb_ticker", "first_dt"],
                    how="inner",
                )[["bb_ticker", "price"]]
                .rename(columns={"price": "price_start"})
                .drop_duplicates(subset=["bb_ticker"], keep="first")
            )
            _merge_period_return("YTD", ytd_start_prices)

            for c in out_cols:
                if c not in result.columns:
                    result[c] = pd.NA
            return result[out_cols]

        # bb_ticker 중복 시 한 행만 유지 (name/gics_name이 있는 행 우선)
        merged["_has_name"] = merged["name"].notna() & (merged["name"].astype(str).str.strip() != "")
        merged_sorted = merged.sort_values(["_has_name", "gics_name", "index_weight"], ascending=[False, True, False])
        merged_dedup = merged_sorted.drop_duplicates(subset=["bb_ticker"], keep="first").drop(columns=["_has_name"], errors="ignore")
        period_ret_df = _calc_period_return_table(price_df, ref_d)

        # ----- 1) 기간별 수익률 TOP N / WORST N -----
        st.subheader(f"{period_choice} 수익률 Top & Worst 종목")
        top_n = st.selectbox("표시 개수", [10, 20, 30], index=0, key="종목분석_topn")
        with_ret = merged_dedup[merged_dedup["daily_ret_pct"].notna()].copy()
        display_cols = ["ticker", "name", "gics_name", "price_factset", "daily_ret_pct", "index_weight"]
        with_ret = with_ret.dropna(subset=[c for c in display_cols if c in with_ret.columns])
        with_ret = with_ret[with_ret["ticker"].astype(str).str.strip() != ""]
        with_ret = with_ret[with_ret["name"].astype(str).str.strip() != ""]
        ret_col_label = "일수익률(%)" if period_choice == "Daily" else "수익률(%)"
        if not with_ret.empty:
            with_ret = with_ret.sort_values("daily_ret_pct", ascending=False)
            top_df = with_ret.head(top_n)[["ticker", "name", "gics_name", "price_factset", "daily_ret_pct", "index_weight"]].copy()
            top_df["비중(%)"] = (pd.to_numeric(top_df["index_weight"], errors="coerce").astype(float) * 100).round(2)
            worst_df = with_ret.tail(top_n)[["ticker", "name", "gics_name", "price_factset", "daily_ret_pct", "index_weight"]].copy()
            worst_df["비중(%)"] = (pd.to_numeric(worst_df["index_weight"], errors="coerce").astype(float) * 100).round(2)
            worst_df = worst_df.sort_values("daily_ret_pct", ascending=True).reset_index(drop=True)

            def _fmt_ret(df, ret_label):
                d = df.copy()
                d = d.rename(columns={"ticker": "티커", "name": "종목명", "gics_name": "섹터명", "price_factset": "가격", "daily_ret_pct": ret_label})
                d[ret_label] = d[ret_label].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "")
                d["가격"] = d["가격"].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "")
                d["비중(%)"] = d["비중(%)"].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "")
                d = d.drop(columns=["index_weight"], errors="ignore")
                return d[["티커", "종목명", "섹터명", "가격", ret_label, "비중(%)"]]

            _table_font = [{"selector": "th, td", "props": [("font-size", "15px")]}]
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f"**TOP {top_n}**")
                t = _fmt_ret(top_df, ret_col_label)
                st.dataframe(
                    t.style.applymap(_style_return, subset=[ret_col_label]).set_table_styles(_table_font),
                    use_container_width=True, hide_index=True,
                )
            with c2:
                st.markdown(f"**WORST {top_n}**")
                w = _fmt_ret(worst_df, ret_col_label)
                st.dataframe(
                    w.style.applymap(_style_return, subset=[ret_col_label]).set_table_styles(_table_font),
                    use_container_width=True, hide_index=True,
                )
        else:
            st.info(f"{period_choice} 수익률을 계산할 수 있는 가격 데이터가 없습니다. (price_factset 확인)")

        # ----- 2) 전체·섹터별 상승/하락 종목수 (기간 별도 선택, 기본 Daily) -----
        adv_dec_period_choice = st.selectbox(
            "상승/하락 기간",
            options=[p[0] for p in PERIOD_OPTIONS],
            index=0,
            key="종목분석_adv_dec_period",
        )
        period_days_adv = next(p[1] for p in PERIOD_OPTIONS if p[0] == adv_dec_period_choice)
        # 상승/하락용 수익률 계산 (선택 기간만)
        with_ret_adv = pd.DataFrame()
        if not price_df.empty:
            ref_prices_adv = price_df[price_df["dt_date"] <= ref_d].groupby("bb_ticker").last().reset_index()[["bb_ticker", "price"]].rename(columns={"price": "price_ref"})
            if period_days_adv is not None:
                try:
                    start_date_adv = get_business_day_by_country(ref_d, period_days_adv, "US")
                except Exception:
                    start_date_adv = ref_d - timedelta(days=max(period_days_adv * 2, 30))
                start_prices_adv = price_df[price_df["dt_date"] <= start_date_adv].groupby("bb_ticker").last().reset_index()[["bb_ticker", "price"]].rename(columns={"price": "price_start"})
            else:
                first_in_year = price_df[price_df["dt_date"] >= ytd_start].groupby("bb_ticker")["dt_date"].min().reset_index().rename(columns={"dt_date": "first_dt"})
                start_prices_adv = price_df.merge(first_in_year, left_on=["bb_ticker", "dt_date"], right_on=["bb_ticker", "first_dt"], how="inner")[["bb_ticker", "price"]].rename(columns={"price": "price_start"}).drop_duplicates(subset=["bb_ticker"], keep="first")
            both_adv = ref_prices_adv.merge(start_prices_adv, on="bb_ticker", how="inner")
            both_adv["price_start"] = pd.to_numeric(both_adv["price_start"], errors="coerce")
            both_adv["price_ref"] = pd.to_numeric(both_adv["price_ref"], errors="coerce")
            both_adv = both_adv[both_adv["price_start"] > 0]
            both_adv["adv_dec_ret_pct"] = (both_adv["price_ref"].astype(float) - both_adv["price_start"].astype(float)) / both_adv["price_start"].astype(float) * 100.0
            merged_adv = merged_dedup.merge(both_adv[["bb_ticker", "adv_dec_ret_pct"]], on="bb_ticker", how="left")
            with_ret_adv = merged_adv[merged_adv["adv_dec_ret_pct"].notna()].copy()

        st.subheader(f"{adv_dec_period_choice} 상승/하락 종목수")
        if not with_ret_adv.empty:
            up = (with_ret_adv["adv_dec_ret_pct"] > 0).sum()
            down = (with_ret_adv["adv_dec_ret_pct"] < 0).sum()
            st.markdown(f"**전체** — **상승: {up}** / **하락: {down}** (수익률 산출 가능 종목 {len(with_ret_adv)}개)")
            with_ret_dir = with_ret_adv.assign(
                direction=with_ret_adv["adv_dec_ret_pct"].apply(lambda x: "상승" if x > 0 else ("하락" if x < 0 else "보합"))
            )
            sector_counts = with_ret_dir.groupby("gics_name")["direction"].value_counts().unstack(fill_value=0).reset_index()
            sector_counts = sector_counts.rename(columns={"gics_name": "섹터명"})
            for c in ["상승", "하락"]:
                if c not in sector_counts.columns:
                    sector_counts[c] = 0
            sector_counts = sector_counts[["섹터명", "상승", "하락"]]
            up_vals = sector_counts["상승"].copy()
            down_vals = sector_counts["하락"].copy()
            total = up_vals + down_vals
            sector_counts["_상승비율"] = up_vals / total.replace(0, 1)
            sector_counts = sector_counts.sort_values("_상승비율", ascending=False).reset_index(drop=True)
            up_vals = sector_counts["상승"].astype(int)
            down_vals = sector_counts["하락"].astype(int)
            total = up_vals + down_vals
            pct_up = (up_vals / total.replace(0, 1) * 100).round(1)
            pct_down = (down_vals / total.replace(0, 1) * 100).round(1)
            # 섹터별 카드 (예전 형식): 상승/하락 종목 수·비율 + 진행바, 종목 보기 클릭 시 해당 섹터 종목 테이블이 바로 아래에 표시
            st.caption("섹터별 상승/하락 종목 수와 비율")
            COLS_PER_ROW = 3
            sectors = sector_counts["섹터명"].tolist()
            for start in range(0, len(sectors), COLS_PER_ROW):
                cols = st.columns(COLS_PER_ROW)
                for ci, col in enumerate(cols):
                    i = start + ci
                    if i >= len(sectors):
                        break
                    sec = sectors[i]
                    emoji_adv = SECTOR_EMOJI.get(sec, "")
                    u, d = int(up_vals.iloc[i]), int(down_vals.iloc[i])
                    t = total.iloc[i]
                    pu = pct_up.iloc[i] if t > 0 else 0
                    pct_d = pct_down.iloc[i] if t > 0 else 0
                    card_html = (
                        f'<div style="background:#37474f; border-radius:8px; padding:12px 14px; margin-bottom:8px;">'
                        f'<div style="font-weight:600; color:#fff; margin-bottom:8px; font-size:18px;">{sec}</div>'
                        f'<div style="display:flex; justify-content:space-between; margin-bottom:4px;">'
                        f'<span style="color:#43a047; font-size:13px;">상승</span><span style="color:#e53935; font-size:13px;">하락</span>'
                        f'</div>'
                        f'<div style="display:flex; justify-content:space-between; margin-bottom:10px;">'
                        f'<span style="color:#43a047; font-weight:600;">{pu:.1f}% ({u})</span>'
                        f'<span style="color:#e53935; font-weight:600;">({d}) {pct_d:.1f}%</span>'
                        f'</div>'
                        f'<div style="display:flex; height:10px; border-radius:4px; overflow:hidden;">'
                        f'<span style="width:{pu}%; background:#43a047;"></span>'
                        f'<span style="width:{pct_d}%; background:#e53935;"></span>'
                        f'</div>'
                        f'</div>'
                    )
                    with col:
                        st.markdown(card_html, unsafe_allow_html=True)
                        if st.button("종목 보기", key=f"adv_dec_btn_{i}", use_container_width=True):
                            st.session_state["adv_dec_sector"] = sec
                            st.rerun()
            # 선택한 섹터가 있으면 해당 섹터 종목 테이블 (이미 로드한 summary52_df 재사용)
            if st.session_state.get("adv_dec_sector"):
                sec_sel = st.session_state["adv_dec_sector"]
                if not summary52_df.empty and "업종" in summary52_df.columns:
                    sector_df = summary52_df[summary52_df["업종"] == sec_sel]
                    if not sector_df.empty:
                        st.markdown("---")
                        st.subheader(f"해당 섹터 종목 — {sec_sel}")
                        st.caption("티커/종목명/업종/현재가/1M·3M·1Y 수익률/이격률(52주 고가 대비)/12M-1M.")
                        disp = sector_df[["종목코드", "종목명", "업종", "현재종가", "1개월수익률(%)", "3개월수익률(%)", "1년수익률(%)", "이격률(%)", "12M-1M"]].copy()
                        disp = disp.rename(columns={
                            "종목코드": "티커", "현재종가": "현재가",
                            "1개월수익률(%)": "1M", "3개월수익률(%)": "3M", "1년수익률(%)": "1Y", "이격률(%)": "이격률", "12M-1M": "12M-1M"
                        })
                        _ret_cols_52w = ["1M", "3M", "1Y", "이격률", "12M-1M"]
                        _right_cols = ["현재가"] + [c for c in _ret_cols_52w if c in disp.columns]
                        _num_fmt = {c: "{:.2f}" for c in _ret_cols_52w if c in disp.columns}
                        _num_fmt["현재가"] = "{:,.2f}"
                        _sector_font = [{"selector": "th, td", "props": [("font-size", "15px")]}]
                        styled_sec_disp = disp.style.format(_num_fmt, na_rep="").applymap(_style_52w_return, subset=_ret_cols_52w)
                        styled_sec_disp = styled_sec_disp.applymap(lambda _: "text-align: right;", subset=_right_cols).set_table_styles(_sector_font)
                        st.dataframe(styled_sec_disp, use_container_width=True, hide_index=True)
                        if st.button("목록 닫기", key="adv_dec_close"):
                            st.session_state["adv_dec_sector"] = None
                            st.rerun()
                    else:
                        st.info(f"해당 섹터({sec_sel})에 종목 데이터가 없습니다.")
                        if st.button("목록 닫기", key="adv_dec_close"):
                            st.session_state["adv_dec_sector"] = None
                            st.rerun()
        else:
            st.info(f"{adv_dec_period_choice} 수익률 데이터가 없어 상승/하락 종목수를 집계할 수 없습니다.")

        # ----- 1) 52주 신고가 주요종목 (이미 로드한 high52_df 재사용) -----
        st.markdown("---")
        st.subheader("52주 신고가 주요종목")
        st.caption("52주 신고가는 강한 상승 모멘텀의 신호입니다. 최근 7일 중 52주 최고가를 돌파한 종목만 표시합니다. 섹터별 확률(해당 섹터 대비 비율) 기준 정렬.")
        if not high52_df.empty:
            high52_df = high52_df.dropna(subset=["종목코드", "종목명"])
            high52_disp_df = high52_df.copy()

            # 52주 신고가 표에 1D/YTD 포함 기간 수익률 병합
            if "bb_ticker" not in high52_disp_df.columns and {"ticker", "bb_ticker"}.issubset(const_df.columns):
                ticker_map = (
                    const_df[["ticker", "bb_ticker"]]
                    .dropna()
                    .copy()
                    .rename(columns={"ticker": "종목코드"})
                )
                high52_disp_df = high52_disp_df.merge(
                    ticker_map.drop_duplicates(subset=["종목코드"], keep="first"),
                    on="종목코드",
                    how="left",
                )
            if not period_ret_df.empty and "bb_ticker" in high52_disp_df.columns:
                high52_disp_df = high52_disp_df.merge(period_ret_df, on="bb_ticker", how="left")

            # 기존 1M/3M/1Y, 이격률 컬럼을 표준 이름으로 보정
            if "1M" not in high52_disp_df.columns and "1개월수익률(%)" in high52_disp_df.columns:
                high52_disp_df["1M"] = pd.to_numeric(high52_disp_df["1개월수익률(%)"], errors="coerce")
            if "3M" not in high52_disp_df.columns and "3개월수익률(%)" in high52_disp_df.columns:
                high52_disp_df["3M"] = pd.to_numeric(high52_disp_df["3개월수익률(%)"], errors="coerce")
            if "1Y" not in high52_disp_df.columns and "1년수익률(%)" in high52_disp_df.columns:
                high52_disp_df["1Y"] = pd.to_numeric(high52_disp_df["1년수익률(%)"], errors="coerce")
            if "이격률" not in high52_disp_df.columns and "이격률(%)" in high52_disp_df.columns:
                high52_disp_df["이격률"] = pd.to_numeric(high52_disp_df["이격률(%)"], errors="coerce")
            if "현재종가" not in high52_disp_df.columns and "price_ref" in high52_disp_df.columns:
                high52_disp_df["현재종가"] = pd.to_numeric(high52_disp_df["price_ref"], errors="coerce")

            sector_totals = const_df["gics_name"].value_counts()
            # 섹터별 52주 신고가 종목 수 및 확률(섹터 구성종목 대비 비율)
            sector_52w = high52_disp_df.groupby("업종").size()
            sector_order = []
            for sec in sector_52w.index:
                cnt = int(sector_52w[sec])
                tot = int(sector_totals.get(sec, 0) or 1)
                prob = cnt / tot * 100
                sector_order.append((sec, cnt, tot, prob))
            sector_order.sort(key=lambda x: x[3], reverse=True)
            display_cols = ["종목코드", "종목명", "업종", "현재종가", "1D", "1M", "3M", "YTD", "1Y", "이격률"]
            for sec_name, cnt, tot, prob in sector_order:
                sec_df = high52_disp_df[high52_disp_df["업종"] == sec_name].copy()
                sec_df = sec_df[[c for c in display_cols if c in sec_df.columns]]
                sec_df = sec_df.rename(columns={
                    "종목코드": "티커",
                    "현재종가": "현재가",
                })
                _ret_cols_52w = ["1D", "1M", "3M", "YTD", "1Y", "이격률"]
                _right_cols = ["현재가"] + _ret_cols_52w
                _num_fmt = {c: "{:.2f}" for c in _ret_cols_52w if c in sec_df.columns}
                _num_fmt["현재가"] = "{:,.2f}"
                _expander_font = [{"selector": "th, td", "props": [("font-size", "15px")]}]
                styled_sec = sec_df.style.format(_num_fmt, na_rep="").applymap(_style_52w_return, subset=_ret_cols_52w)
                styled_sec = styled_sec.applymap(lambda _: "text-align: right;", subset=[c for c in _right_cols if c in sec_df.columns]).set_table_styles(_expander_font)
                emoji_52 = SECTOR_EMOJI.get(sec_name, "")
                # 섹터 추세 한눈에: 진행바 + 숫자 강조
                prob_ratio = min(1.0, prob / 100.0)
                st.markdown(f'<div style="font-size:1.05rem; font-weight:700; margin-bottom:4px;">{sec_name}</div>', unsafe_allow_html=True)
                st.markdown(
                    f'<div style="background:#e0e0e0; border-radius:8px; height:24px; overflow:hidden; margin-bottom:6px;">'
                    f'<div style="background:linear-gradient(90deg,#43a047,#81c784); width:{prob_ratio*100:.1f}%; height:100%; border-radius:8px;"></div></div>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div style="display:flex; gap:16px; margin-bottom:8px; font-size:0.95rem;">'
                    f'<span><strong>전체</strong> <span style="color:#1565c0; font-weight:700;">{tot}개</span></span>'
                    f'<span><strong>신고가</strong> <span style="color:#2e7d32; font-weight:700;">{cnt}개</span></span>'
                    f'<span><strong>비율</strong> <span style="color:#1b5e20; font-weight:700;">{prob:.1f}%</span></span></div>',
                    unsafe_allow_html=True,
                )
                with st.expander(f"해당 섹터 종목 목록 ({cnt}종목)", expanded=False):
                    st.dataframe(styled_sec, use_container_width=True, hide_index=True)
        else:
            st.info("52주 신고가 종목이 없거나 index_constituents·price_factset 데이터를 조회할 수 없습니다.")

        # 차트만 fragment로 격리 → 차트 선택 시 이 블록만 리런, 52주 요약 테이블은 그대로 유지
        CHART_COLORS = ["#e53935", "#1e88e5", "#43a047", "#fb8c00", "#8e24aa"]  # 최대 5종목 색상

        @st.fragment
        def _chart_block():
            st.markdown('<div style="margin-top: 24px;"></div>', unsafe_allow_html=True)
            st.caption("아래에서 종목을 선택하면 해당 종목의 최근 1년 차트를 볼 수 있습니다. (최대 5종목)")
            chart_candidates = merged_dedup.dropna(subset=["ticker", "name", "bb_ticker"])
            chart_candidates = chart_candidates[chart_candidates["ticker"].astype(str).str.strip() != ""]
            chart_candidates = chart_candidates[chart_candidates["name"].astype(str).str.strip() != ""]
            chart_options = sorted(chart_candidates.drop_duplicates(subset=["bb_ticker"]).apply(lambda r: f"{r['ticker']} | {r['name']}", axis=1).tolist())
            try:
                selected_labels = st.multiselect("차트로 볼 종목 선택 (최대 5종목)", chart_options, default=[], key="52w_chart_select", max_selections=5)
            except TypeError:
                selected_labels = st.multiselect("차트로 볼 종목 선택 (최대 5종목)", chart_options, default=[], key="52w_chart_select")
                selected_labels = selected_labels[:5]
            if selected_labels:
                selected_labels = selected_labels[:5]
                ref_d = ref_date.date() if hasattr(ref_date, "date") else ref_date
                end_str = ref_d.strftime("%Y-%m-%d")
                start_d = ref_d - timedelta(days=365)
                start_str = start_d.strftime("%Y-%m-%d")
                bb_list = []
                label_by_bb = {}
                for lab in selected_labels:
                    chosen_code = lab.split(" | ")[0].strip()
                    row = merged_dedup[merged_dedup["ticker"].astype(str).str.strip() == chosen_code]
                    if not row.empty and "bb_ticker" in row.columns:
                        bb = row["bb_ticker"].iloc[0]
                        bb_list.append(bb)
                        label_by_bb[bb] = lab
                if not bb_list:
                    st.warning("선택 종목 정보를 찾을 수 없습니다.")
                else:
                    series_df = get_price_factset(bb_list, start_str, end_str)
                    if series_df.empty:
                        st.warning("선택 종목의 기간 내 price_factset 데이터가 없습니다.")
                    else:
                        series_df = series_df.sort_values("dt")
                        # 수익률 계산 (기간 내 첫가/마지막가 기준)
                        returns_list = []
                        for bb in bb_list:
                            sub = series_df[series_df["bb_ticker"] == bb].sort_values("dt")
                            if len(sub) >= 2:
                                p0, p1 = sub["price"].iloc[0], sub["price"].iloc[-1]
                                ret = (p1 / p0 - 1) * 100 if p0 and p0 != 0 else None
                            else:
                                ret = None
                            returns_list.append((label_by_bb.get(bb, bb), ret))
                        # 종목 선택 영역과 차트 사이 여백
                        st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
                        # 다중 라인 차트: 1Y 수익률 기준 (시점=100 지수)
                        fig = go.Figure()
                        for i, bb in enumerate(bb_list):
                            sub = series_df[series_df["bb_ticker"] == bb].sort_values("dt")
                            if sub.empty:
                                continue
                            p0 = sub["price"].iloc[0]
                            if not p0 or p0 == 0:
                                continue
                            # 수익률 지수: (가격/시점가격)*100 → 100 기준 상대 수익률 비교
                            return_index = (sub["price"].values / p0) * 100
                            color = CHART_COLORS[i] if i < len(CHART_COLORS) else CHART_COLORS[0]
                            fig.add_trace(go.Scatter(
                                x=sub["dt"], y=return_index, mode="lines",
                                name=label_by_bb.get(bb, bb), line=dict(color=color, width=2),
                            ))
                        fig.update_layout(
                            title="최근 1년 수익률 비교 (시점=100)" if len(bb_list) > 1 else f"{label_by_bb.get(bb_list[0], bb_list[0])} 최근 1년 수익률",
                            xaxis_title="날짜",
                            yaxis_title="지수 (시점=100)",
                            height=400,
                            margin=dict(l=20, r=20, t=40, b=20),
                            template="plotly_white",
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        # 기간 수익률 표 (차트 아래) — 연노란색 영역, 숫자 강조
                        rows_html = ""
                        for i, (label, ret) in enumerate(returns_list):
                            c = CHART_COLORS[i] if i < len(CHART_COLORS) else CHART_COLORS[0]
                            ret_str = f"{ret:.2f}%" if ret is not None else "—"
                            ret_color = "#2e7d32" if ret is not None and ret >= 0 else "#c62828" if ret is not None else "#333"
                            rows_html += (
                                f'<div style="display:flex; align-items:center; gap:12px; margin:10px 0;">'
                                f'<span style="color:{c}; font-size:16px;">●</span>'
                                f'<span style="flex:1; font-size:15px; color:#37474f;">{label}</span>'
                                f'<span style="color:{ret_color}; font-size:20px; font-weight:700;">{ret_str}</span></div>'
                            )
                        st.markdown(
                            f'<div style="background-color:#fffde7; padding:16px 20px; margin-top:28px; margin-bottom:24px; border-radius:8px;">'
                            f'<div style="font-weight:bold; font-size:16px; margin-bottom:10px;">기간 수익률 (최근 1년)</div>{rows_html}</div>',
                            unsafe_allow_html=True,
                        )
        _chart_block()

        # ----- 2) 52주 최고가·현재가 요약 (이미 로드한 summary52_df 재사용) -----
        st.subheader("52주 최고가·현재가 요약")
        st.caption("티커/종목명/업종/현재가/1D·1M·3M·YTD·1Y 수익률/이격률(52주 고가 대비).")
        if not summary52_df.empty:
            summary_disp_df = summary52_df.copy()
            if "bb_ticker" not in summary_disp_df.columns and {"ticker", "bb_ticker"}.issubset(const_df.columns):
                ticker_map = (
                    const_df[["ticker", "bb_ticker"]]
                    .dropna()
                    .copy()
                    .rename(columns={"ticker": "종목코드"})
                )
                summary_disp_df = summary_disp_df.merge(
                    ticker_map.drop_duplicates(subset=["종목코드"], keep="first"),
                    on="종목코드",
                    how="left",
                )
            if not period_ret_df.empty and "bb_ticker" in summary_disp_df.columns:
                summary_disp_df = summary_disp_df.merge(period_ret_df, on="bb_ticker", how="left")

            if "1M" not in summary_disp_df.columns and "1개월수익률(%)" in summary_disp_df.columns:
                summary_disp_df["1M"] = pd.to_numeric(summary_disp_df["1개월수익률(%)"], errors="coerce")
            if "3M" not in summary_disp_df.columns and "3개월수익률(%)" in summary_disp_df.columns:
                summary_disp_df["3M"] = pd.to_numeric(summary_disp_df["3개월수익률(%)"], errors="coerce")
            if "1Y" not in summary_disp_df.columns and "1년수익률(%)" in summary_disp_df.columns:
                summary_disp_df["1Y"] = pd.to_numeric(summary_disp_df["1년수익률(%)"], errors="coerce")
            if "이격률" not in summary_disp_df.columns and "이격률(%)" in summary_disp_df.columns:
                summary_disp_df["이격률"] = pd.to_numeric(summary_disp_df["이격률(%)"], errors="coerce")
            if "현재종가" not in summary_disp_df.columns and "price_ref" in summary_disp_df.columns:
                summary_disp_df["현재종가"] = pd.to_numeric(summary_disp_df["price_ref"], errors="coerce")

            display_cols = ["종목코드", "종목명", "업종", "현재종가", "1D", "1M", "3M", "YTD", "1Y", "이격률"]
            summary_display = summary_disp_df[[c for c in display_cols if c in summary_disp_df.columns]].copy()
            if "이격률" in summary_display.columns:
                summary_display["이격률"] = pd.to_numeric(summary_display["이격률"], errors="coerce")
            if "YTD" in summary_display.columns:
                summary_display["YTD"] = pd.to_numeric(summary_display["YTD"], errors="coerce")
            # 1차: 이격률 내림차순, 2차: YTD 내림차순
            _sort_cols = [c for c in ["이격률", "YTD"] if c in summary_display.columns]
            if _sort_cols:
                summary_display = summary_display.sort_values(
                    _sort_cols,
                    ascending=[False] * len(_sort_cols),
                    na_position="last",
                ).reset_index(drop=True)
            summary_display = summary_display.rename(columns={"종목코드": "티커", "현재종가": "현재가"})

            _ret_cols_52w = ["1D", "1M", "3M", "YTD", "1Y", "이격률"]
            _right_cols = ["현재가"] + [c for c in _ret_cols_52w if c in summary_display.columns]
            _num_fmt = {c: "{:.2f}" for c in _ret_cols_52w if c in summary_display.columns}
            _num_fmt["현재가"] = "{:,.2f}"
            _summary_font = [{"selector": "th, td", "props": [("font-size", "15px")]}]
            styled_summary = summary_display.style.format(_num_fmt, na_rep="").applymap(_style_52w_return, subset=_ret_cols_52w)
            styled_summary = styled_summary.applymap(lambda _: "text-align: right;", subset=_right_cols).set_table_styles(_summary_font)
            st.dataframe(styled_summary, use_container_width=True, hide_index=True)

            # ----- 업종별 Top 5 (지표 선택) — 섹터별 카드 5장(종목 정보 + 미니 차트) -----
            st.markdown("---")
            st.subheader("업종별 Top 5")
            st.caption("기준 지표를 선택하면 업종별로 해당 지표 상위 5종목을 카드와 미니 차트로 볼 수 있습니다.")
            metric_options = [
                ("1M (1개월 수익률)", "1개월수익률(%)"),
                ("3M (3개월 수익률)", "3개월수익률(%)"),
                ("1Y (1년 수익률)", "1년수익률(%)"),
                ("12M-1M (모멘텀)", "12M-1M"),
            ]
            metric_label = st.radio("기준 지표", options=[m[0] for m in metric_options], key="52w_top5_metric", horizontal=True)
            col_key = next(m[1] for m in metric_options if m[0] == metric_label)
            col_short = {"1개월수익률(%)": "1M", "3개월수익률(%)": "3M", "1년수익률(%)": "1Y", "12M-1M": "12M-1M"}.get(col_key, col_key)
            # 섹터별 1색 (그라데이션), 동일 섹터 내 카드는 같은 색
            SECTOR_GRADIENT = ["#263238", "#37474f", "#455a64", "#546e7a", "#607d8b", "#78909c", "#5c6bc0", "#7e57c2", "#512da8", "#311b92", "#1a237e", "#0d47a1"]
            if col_key in summary52_df.columns and "업종" in summary52_df.columns:
                df_top5 = summary52_df.copy()
                df_top5[col_key] = pd.to_numeric(df_top5[col_key], errors="coerce")
                df_top5 = df_top5.dropna(subset=[col_key])
                has_bb = "bb_ticker" in df_top5.columns
                # 52주 신고가 주요종목과 동일한 섹터 순서 (신고가 확률 기준 내림차순)
                all_industries_set = set(df_top5["업종"].dropna().unique().tolist())
                sector_order_names = []
                if not high52_df.empty and "업종" in high52_df.columns and "gics_name" in const_df.columns:
                    sector_totals = const_df["gics_name"].value_counts()
                    sector_52w = high52_df.groupby("업종").size()
                    _order = []
                    for sec in sector_52w.index:
                        cnt = int(sector_52w[sec])
                        tot = int(sector_totals.get(sec, 0) or 1)
                        prob = cnt / tot * 100
                        _order.append((sec, cnt, tot, prob))
                    _order.sort(key=lambda x: x[3], reverse=True)
                    sector_order_names = [sec for sec, _c, _t, _p in _order]
                industries_ordered = [s for s in sector_order_names if s in all_industries_set] + sorted([s for s in all_industries_set if s not in sector_order_names])
                # Top 5 차트용 가격: 필요한 bb만 한 번 필터·기간 필터·일별 집계 후 dict로 재사용 (중복 제거)
                all_needed_bb = set()
                for _ind in industries_ordered:
                    _sub = df_top5[df_top5["업종"] == _ind].nlargest(5, col_key)
                    for _, _row in _sub.iterrows():
                        _bb = _row.get("bb_ticker")
                        if _bb:
                            all_needed_bb.add(str(_bb).strip())
                chart_by_bb = {}
                if has_bb and not price_df.empty and all_needed_bb:
                    sub = price_df[price_df["bb_ticker"].astype(str).str.strip().isin(all_needed_bb)].copy()
                    if not sub.empty:
                        end_d = sub["dt_date"].max()
                        if col_key == "1개월수익률(%)":
                            start_d = end_d - pd.Timedelta(days=31)
                        elif col_key == "3개월수익률(%)":
                            start_d = end_d - pd.Timedelta(days=91)
                        else:
                            start_d = end_d - pd.Timedelta(days=365)
                        sub = sub[sub["dt_date"] >= start_d]
                        for _bb, grp in sub.groupby("bb_ticker"):
                            grp = grp.sort_values("dt_date").groupby("dt_date", as_index=False).last()
                            chart_by_bb[str(_bb).strip()] = grp
                for idx, ind in enumerate(industries_ordered):
                    sub = df_top5[df_top5["업종"] == ind].nlargest(5, col_key)
                    if sub.empty:
                        continue
                    sector_bg = SECTOR_GRADIENT[idx % len(SECTOR_GRADIENT)]
                    emoji = SECTOR_EMOJI.get(ind, "")
                    top_margin = "28px" if idx > 0 else "12px"
                    st.markdown(
                        f'<div style="margin-top:{top_margin}; margin-bottom:14px; padding-bottom:10px; border-bottom:2px solid #546e7a;">'
                        f'<span style="font-size:1.35rem; font-weight:700;">{emoji} {ind}</span></div>',
                        unsafe_allow_html=True,
                    )
                    cols = st.columns(5)
                    for ci, (_, row) in enumerate(sub.iterrows()):
                        if ci >= 5:
                            break
                        ticker = str(row.get("종목코드", ""))
                        name = str(row.get("종목명", "")).strip()
                        price = row.get("현재종가")
                        try:
                            price_f = float(price) if price is not None else 0
                        except (TypeError, ValueError):
                            price_f = 0
                        val = row.get(col_key)
                        try:
                            val_f = float(val) if val is not None else 0
                        except (TypeError, ValueError):
                            val_f = 0
                        val_str = f"{val_f:+.2f}%" if val_f != 0 else "0.00%"
                        card_html = (
                            f'<div style="background:{sector_bg}; border-radius:8px; padding:12px; margin-bottom:8px;">'
                            f'<div style="font-weight:700; color:#fff; font-size:16px;">{ticker}</div>'
                            f'<div style="color:#e0e0e0; font-size:13px; margin-bottom:6px; word-wrap:break-word; line-height:1.3;">{name}</div>'
                            f'<div style="font-size:22px; font-weight:700; color:#ffffff; margin-bottom:4px;">{col_short} {val_str}</div>'
                            f'<div style="font-size:15px; color:#b0bec5;">{price_f:,.2f}</div>'
                            f'</div>'
                        )
                        with cols[ci]:
                            st.markdown(card_html, unsafe_allow_html=True)
                            if has_bb and chart_by_bb:
                                bb = row.get("bb_ticker")
                                bb_str = str(bb).strip() if bb else ""
                                if bb_str:
                                    series = chart_by_bb.get(bb_str)
                                    if series is None or series.empty:
                                        continue
                                    series = series.copy()
                                    series["price"] = pd.to_numeric(series["price"], errors="coerce")
                                    series = series.dropna(subset=["price"])
                                    if series.empty:
                                        continue
                                    prices = series["price"]
                                    p0 = float(prices.iloc[0]) if len(prices) else None
                                    if p0 and p0 != 0:
                                        norm = (prices / p0) * 100
                                    else:
                                        norm = prices
                                    # 종목별 Y축: 윗대가리·빨간점 절대 안 짤리게 — 상단 여유 크게, 상한 높게
                                    n_min = float(norm.min()) if norm.notna().any() else 98
                                    n_max = float(norm.max()) if norm.notna().any() else 102
                                    rng = max(1, n_max - n_min)
                                    pad_bottom = max(2, rng * 0.06)
                                    pad_top = max(8, rng * 0.15)
                                    y_min = max(50, n_min - pad_bottom)
                                    y_max = n_max + pad_top
                                    x_min = series["dt_date"].min()
                                    x_max = series["dt_date"].max()
                                    # 해당 기간 최고가(MAX) 지점에 빨간점
                                    max_idx = norm.idxmax()
                                    max_date = series.loc[max_idx, "dt_date"]
                                    max_norm = float(norm.loc[max_idx])
                                    fig = go.Figure()
                                    fig.add_trace(go.Scatter(
                                        x=series["dt_date"], y=norm,
                                        mode="lines", line=dict(color="#81c784", width=2), fill="tozeroy",
                                        connectgaps=True,
                                    ))
                                    fig.add_trace(go.Scatter(
                                        x=[max_date], y=[max_norm],
                                        mode="markers",
                                        marker=dict(size=8, color="#e53935", line=dict(width=1, color="white"), symbol="circle"),
                                    ))
                                    fig.update_layout(
                                        margin=dict(l=0, r=0, t=0, b=0),
                                            height=170,
                                            autosize=True,
                                        xaxis=dict(
                                            visible=False, domain=[0, 1], zeroline=False,
                                            range=[x_min, x_max], autorange=False,
                                        ),
                                        yaxis=dict(visible=False, range=[y_min, y_max], fixedrange=True, domain=[0, 1], zeroline=False),
                                        paper_bgcolor="rgba(0,0,0,0)",
                                        plot_bgcolor="rgba(0,0,0,0)",
                                        showlegend=False,
                                    )
                                    fig.update_xaxes(automargin=False)
                                    fig.update_yaxes(automargin=False)
                                    st.plotly_chart(
                                        fig, use_container_width=True, key=f"top5_{ind}_{ci}_{ticker}",
                                        config=dict(displayModeBar=False, displaylogo=False),
                                    )
                    st.markdown("")  # 간격
            else:
                st.info("선택한 지표 데이터가 없습니다.")

    except Exception as e:
        st.error(f"오류 발생: {e}")
        import traceback
        with st.expander("상세 오류 정보"):
            st.code(traceback.format_exc())


# 요일 라벨 (캘린더 헤더용)
_WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def _render_실적캘린더(ref_date):
    """실적 캘린더: 기준일 전후 기간 내 실적 발표일을 날짜별 칸·타일 형식으로 표시. 페이지 이동(< >), 종목/티커 검색."""
    sel_ticker = st.session_state.get("실적캘린더_선택")
    in_detail = sel_ticker and isinstance(sel_ticker, str)
    quick_search_ticker = None
    calendar_window_days = 365

    if not in_detail:
        st.caption(f"**기준일** (KR 1영업일 전): **{ref_date}**")
        selected_index = st.selectbox("Index 선택", INDEX_OPTIONS, key="종목분석_index_실적캘린더")
        # 상단: 제목
        head_col1, head_col2 = st.columns([1.2, 2.6])
        with head_col1:
            st.subheader("실적 캘린더")
        search_query = ""
    else:
        selected_index = st.session_state.get("종목분석_index_실적캘린더", INDEX_OPTIONS[0])
        search_query = ""

    try:
        with st.spinner("구성종목·실적 일정 조회 중..." if not in_detail else "상세 로딩 중..."):
            const = _cached_constituents(selected_index, ref_date)
        if const.empty:
            st.warning("해당 지수·기준일 구성종목이 없습니다.")
            return
        const = const.copy()
        const["factset_ticker"] = const["bb_ticker"].astype(str).str.strip().str.split().str[0].replace("", pd.NA)
        const = const.dropna(subset=["factset_ticker"])
        factset_list = const["factset_ticker"].unique().tolist()
        if not factset_list:
            st.warning("구성종목에서 factset_ticker를 추출할 수 없습니다.")
            return

        if not in_detail:
            # 재무 탭과 유사한 빠른 검색: 티커|종목명 searchable selectbox
            quick_df = const.drop_duplicates("factset_ticker")[["factset_ticker", "name"]].copy()
            quick_df["factset_ticker"] = quick_df["factset_ticker"].astype(str).str.strip()
            if "name" in quick_df.columns:
                quick_df["name"] = quick_df["name"].astype(str).str.strip()
            else:
                quick_df["name"] = ""
            quick_df["label"] = quick_df.apply(
                lambda r: f"{r['factset_ticker']} | {r['name']}" if r["name"] else r["factset_ticker"],
                axis=1,
            )
            quick_options = ["— 선택 —"] + sorted(quick_df["label"].dropna().unique().tolist())
            selected_quick = st.selectbox(
                "종목·티커 빠른 검색",
                options=quick_options,
                key="실적캘린더_빠른검색_select",
                placeholder="종목명 또는 티커 입력 후 선택",
                label_visibility="collapsed",
            )
            if selected_quick and selected_quick != "— 선택 —":
                quick_search_ticker = selected_quick.split(" | ")[0].strip()

            search_active = bool(quick_search_ticker) or bool(search_query and search_query.strip())
            search_target_list = factset_list
            if search_active:
                if quick_search_ticker:
                    search_target_list = [quick_search_ticker]
                else:
                    q = search_query.strip().lower()
                    _const_search = const.copy()
                    _const_search["factset_ticker"] = _const_search["factset_ticker"].astype(str).str.strip()
                    if "name" in _const_search.columns:
                        _const_search["name"] = _const_search["name"].astype(str).str.strip()
                    else:
                        _const_search["name"] = ""
                    matched = _const_search[
                        _const_search["factset_ticker"].str.lower().str.contains(q, na=False)
                        | _const_search["name"].str.lower().str.contains(q, na=False)
                    ]
                    search_target_list = matched["factset_ticker"].dropna().astype(str).str.strip().unique().tolist()
                if not search_target_list:
                    st.info("검색 조건에 맞는 종목이 없습니다.")
                    return
                # 검색 시에는 기간 밖 일정도 확인할 수 있도록 범위를 넓혀 조회
                earnings = get_earnings_calendar_by_date_range(
                    ref_date,
                    search_target_list,
                    days_before=calendar_window_days,
                    days_after=calendar_window_days,
                )
                st.caption("검색 결과는 기준일 전후 최대 1년 일정까지 함께 표시됩니다.")
            else:
                earnings = get_earnings_calendar_by_date_range(
                    ref_date,
                    factset_list,
                    days_before=calendar_window_days,
                    days_after=calendar_window_days,
                )
            earnings = earnings.dropna(subset=["dt", "factset_ticker"])
            if earnings.empty:
                st.info(
                    "선택 기간 내 실적 발표 일정이 없습니다."
                    if not search_active
                    else "검색한 종목의 실적 일정이 없습니다."
                )
                return
            earnings["dt_date"] = pd.to_datetime(earnings["dt"], errors="coerce").dt.date
            earnings = earnings[earnings["dt_date"].notna()].drop_duplicates(subset=["dt_date", "factset_ticker"])
            name_map = const.drop_duplicates("factset_ticker").set_index("factset_ticker")["name"].astype(str).to_dict()
            sector_map = {}
            if "gics_name" in const.columns:
                sec_series = const.drop_duplicates("factset_ticker").set_index("factset_ticker")["gics_name"]
                sector_map = {k: (str(v).strip() if pd.notna(v) else "") for k, v in sec_series.items()}
            if "index_market_cap" in const.columns:
                cap_series = const.drop_duplicates("factset_ticker").set_index("factset_ticker")["index_market_cap"]
                cap_map = {k: (float(v) if pd.notna(v) else 0.0) for k, v in cap_series.items()}
            else:
                cap_map = {}
            by_date = {}
            for d, g in earnings.groupby("dt_date"):
                rows = []
                for _, r in g.iterrows():
                    ticker = r.get("factset_ticker")
                    if pd.isna(ticker) or ticker == "":
                        continue
                    name = name_map.get(ticker) or ticker
                    if pd.isna(name):
                        name = str(ticker)
                    sector = sector_map.get(ticker) or ""
                    ticker_s = str(ticker).strip()
                    name_s = str(name).strip()
                    rows.append((ticker_s, name_s, sector))
                if rows:
                    rows.sort(key=lambda x: -(cap_map.get(x[0], 0.0)))
                    by_date[d] = rows
            if search_active:
                search_target_set = set(search_target_list)
                by_date = {
                    d: [(t, n, s) for t, n, s in lst if t in search_target_set]
                    for d, lst in by_date.items()
                }
                by_date = {d: lst for d, lst in by_date.items() if lst}
            if not by_date:
                has_search_input = bool(quick_search_ticker) or bool(search_query and search_query.strip())
                st.info("표시할 실적 일정이 없습니다." + (" (검색 조건에 맞는 일정이 없습니다.)" if has_search_input else ""))
                return

        # 선택된 종목 → 상세 화면 전환 (캘린더 숨김, 상세만 표시)
        sel_ticker = st.session_state.get("실적캘린더_선택")
        if sel_ticker and isinstance(sel_ticker, str):
            _const_sel = const[const["factset_ticker"].astype(str).str.strip() == sel_ticker.strip()]
            if not _const_sel.empty:
                _bb = _const_sel["bb_ticker"].iloc[0]
                _bb = str(_bb).strip() if pd.notna(_bb) else None
                _name_sel = _const_sel["name"].iloc[0]
                _name_sel = str(_name_sel).strip() if pd.notna(_name_sel) else sel_ticker
                _sector_sel = _const_sel["gics_name"].iloc[0]
                _sector_sel = str(_sector_sel).strip() if pd.notna(_sector_sel) else ""
                # 캐시로 상세 로딩 최적화: 52주 요약·1년 가격 한 번만
                _cache_key = f"실적상세_{selected_index}_{ref_date}_{sel_ticker}"
                if _cache_key not in st.session_state:
                    with st.spinner("상세 로딩 중..."):
                        _summary52 = _cached_52w_summary(selected_index, ref_date)
                        _price_1y = None
                        if _bb:
                            ref_str = ref_date.strftime("%Y-%m-%d") if hasattr(ref_date, "strftime") else str(ref_date)[:10]
                            start_1y = (ref_date - timedelta(days=365)) if hasattr(ref_date, "__sub__") else ref_date
                            start_str = start_1y.strftime("%Y-%m-%d") if hasattr(start_1y, "strftime") else str(start_1y)[:10]
                            _price_1y = get_price_factset([_bb], start_str, ref_str)
                        _past = get_earnings_calendar_by_date_range(ref_date, [sel_ticker], days_before=365, days_after=0)
                        _closest_dates = get_earnings_calendar_closest_dates(ref_date, [sel_ticker])
                        st.session_state[_cache_key] = {"summary52": _summary52, "price_1y": _price_1y, "past": _past, "closest_dates": _closest_dates}
                _cache = st.session_state.get(_cache_key, {})
                _summary52 = _cache.get("summary52", pd.DataFrame())
                _price_1y = _cache.get("price_1y")
                _past = _cache.get("past", pd.DataFrame())

                st.markdown(
                    '<div style="background:#263238;border-radius:12px;padding:20px 24px;margin-bottom:20px;border:1px solid #37474f;">',
                    unsafe_allow_html=True,
                )
                # 상단: D-day만 우측 배치 (돌아가기 버튼은 하단 좌측으로 이동)
                col_dday = st.columns([1, 4])[1]
                with col_dday:
                    _closest = _cache.get("closest_dates")
                    if _closest is not None and not _closest.empty:
                        _row_c = _closest[_closest["factset_ticker"].astype(str).str.strip() == sel_ticker.strip()]
                        if not _row_c.empty:
                            _next_dt = _row_c.iloc[0].get("closest_future_dt")
                            if pd.notna(_next_dt):
                                _next_d = _next_dt.date() if hasattr(_next_dt, "date") and callable(getattr(_next_dt, "date")) else _next_dt
                                ref_d = ref_date.date() if hasattr(ref_date, "date") and callable(getattr(ref_date, "date")) else ref_date
                                _days = (_next_d - ref_d).days
                                if _days == 0:
                                    _d_label = "D-DAY"
                                elif _days > 0:
                                    _d_label = f"D-{_days}"
                                else:
                                    _d_label = None
                                if _d_label:
                                    st.markdown(
                                        f'<div style="text-align:right;margin-top:4px;">'
                                        f'<span style="display:inline-block;background:#ff9800;color:#fff;font-size:1.25rem;font-weight:800;'
                                        f'padding:10px 20px;border-radius:8px;letter-spacing:0.05em;">실적 발표 {_d_label}</span></div>',
                                        unsafe_allow_html=True,
                                    )
                st.markdown("<div style='margin-top:16px; color:#cfd8dc;'>", unsafe_allow_html=True)

                # ----- 1) 가장 위: 최근 1년 수익률 차트 + 기간 수익률 박스 -----
                _label_1y = f"{_bb or sel_ticker} US | {_name_sel} 최근 1년 수익률"
                if _price_1y is not None and not _price_1y.empty:
                    _price_1y = _price_1y.copy()
                    _price_1y["price"] = pd.to_numeric(_price_1y["price"], errors="coerce")
                    _price_1y = _price_1y.dropna(subset=["price"]).sort_values("dt")
                    _price_1y["dt_date"] = _price_1y["dt"].dt.date
                    _price_1y = _price_1y.drop_duplicates(subset=["dt_date"], keep="last")
                    if len(_price_1y) >= 2:
                        _p0 = float(_price_1y["price"].iloc[0])
                        _p1 = float(_price_1y["price"].iloc[-1])
                        _ret_1y = (_p1 - _p0) / _p0 * 100.0 if _p0 else 0.0
                        _idx = (_price_1y["price"].astype(float) / _p0 * 100.0)
                        _fig = go.Figure()
                        _fig.add_trace(go.Scatter(
                            x=_price_1y["dt_date"], y=_idx,
                            mode="lines", line=dict(color="#c62828", width=2), connectgaps=True,
                        ))
                        # 차트 기간 내 실적 발표일 세로선 표시
                        _chart_start = _price_1y["dt_date"].min()
                        _chart_end = _price_1y["dt_date"].max()
                        ref_d = ref_date.date() if hasattr(ref_date, "date") and callable(getattr(ref_date, "date")) else ref_date
                        _earnings_in_range = []
                        if not _past.empty:
                            _past_dates = _past.copy()
                            _past_dates["dt_date"] = pd.to_datetime(_past_dates["dt"], errors="coerce").dt.date
                            _earnings_in_range = _past_dates[_past_dates["dt_date"].notna() & (_past_dates["dt_date"] <= ref_d) & (_past_dates["dt_date"] >= _chart_start) & (_past_dates["dt_date"] <= _chart_end)]
                            _earnings_in_range = _earnings_in_range.drop_duplicates(subset=["dt_date"])["dt_date"].tolist()
                            for _ed in _earnings_in_range:
                                _fig.add_vline(x=_ed, line_dash="dot", line_color="rgba(0,100,0,0.6)", line_width=1.5)
                                # 점선 위에 날짜 표시 (25.10.23 형식)
                                _date_str = _ed.strftime("%y.%m.%d") if hasattr(_ed, "strftime") else str(_ed)[2:10].replace("-", ".")
                                _fig.add_annotation(x=_ed, y=1, yref="paper", text=_date_str, showarrow=False, font=dict(size=13, color="rgba(0,80,0,0.9)"), yanchor="bottom")
                        _fig.update_layout(
                            title=dict(text=_label_1y, font=dict(size=20)),
                            height=480,
                            xaxis_title="날짜",
                            yaxis_title="지수(시점=100)",
                            margin=dict(l=56, r=36, t=56, b=56),
                            paper_bgcolor="#ffffff",
                            plot_bgcolor="#ffffff",
                            xaxis=dict(showgrid=False, title_font=dict(size=15), tickfont=dict(size=14)),
                            yaxis=dict(showgrid=False, zeroline=False, title_font=dict(size=15), tickfont=dict(size=14)),
                            font=dict(size=14),
                        )
                        st.plotly_chart(_fig, use_container_width=True, config=dict(displayModeBar=False, displaylogo=False))
                        if len(_earnings_in_range) > 0:
                            st.markdown('<p style="font-size:15px; color:#666;">점선: 해당 기간 내 실적 발표일</p>', unsafe_allow_html=True)
                        st.markdown(
                            f'<div style="background:#fffde7;padding:22px 26px;margin:20px 0;border-radius:10px;">'
                            f'<div style="font-weight:bold;font-size:19px;margin-bottom:10px;">기간 수익률 (최근 1년)</div>'
                            f'<div style="display:flex;align-items:center;gap:12px;"><span style="color:#c62828;font-size:20px;">●</span>'
                            f'<span style="flex:1;font-size:17px;">{_bb or sel_ticker} US | {_name_sel}</span>'
                            f'<span style="color:#c62828;font-weight:700;font-size:22px;">{_ret_1y:+.2f}%</span></div></div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.caption("1년 가격 데이터가 부족합니다.")
                else:
                    st.caption("1년 가격 데이터를 불러올 수 없습니다.")

                # ----- 2) 52주 최고가·현재가 요약 표 (크기 키움) -----
                st.markdown("<div style='color:#cfd8dc; font-weight:700; font-size:1.45rem; margin:24px 0 14px 0;'>52주 최고가·현재가 요약</div>", unsafe_allow_html=True)
                if not _summary52.empty and _bb and "bb_ticker" in _summary52.columns:
                    _row = _summary52[_summary52["bb_ticker"].astype(str).str.strip() == _bb]
                    if not _row.empty:
                        _row = _row.iloc[0]
                        disp_52 = pd.DataFrame([{
                            "티커": _row.get("종목코드", sel_ticker),
                            "종목명": _row.get("종목명", _name_sel),
                            "업종": _row.get("업종", _sector_sel),
                            "현재가": _row.get("현재종가"),
                            "1M": _row.get("1개월수익률(%)"),
                            "3M": _row.get("3개월수익률(%)"),
                            "1Y": _row.get("1년수익률(%)"),
                            "이격률": _row.get("이격률(%)"),
                            "12M-1M": _row.get("12M-1M"),
                        }])
                        def _style_ret(v):
                            if pd.isna(v): return ""
                            try:
                                f = float(v)
                                if f > 0: return "color: #2e7d32; font-weight: bold;"
                                if f < 0: return "color: #c62828; font-weight: bold;"
                            except (TypeError, ValueError): pass
                            return ""
                        _table_big = [{"selector": "th, td", "props": [("font-size", "21px"), ("padding", "18px 22px")]}, {"selector": "th", "props": [("font-size", "22px")]}]
                        styled = disp_52.style.format({
                            "현재가": "{:,.2f}", "1M": "{:.2f}%", "3M": "{:.2f}%", "1Y": "{:.2f}%",
                            "이격률": "{:.2f}%", "12M-1M": "{:.2f}",
                        }, na_rep="—").applymap(_style_ret, subset=["1M", "3M", "1Y", "이격률", "12M-1M"]).set_table_styles(_table_big)
                        st.dataframe(styled, use_container_width=True, hide_index=True)
                    else:
                        st.caption("52주 요약에 해당 종목이 없습니다.")
                else:
                    st.caption("52주 요약 데이터를 불러올 수 없습니다.")

                # 좌측 하단: 돌아가기 버튼 (가장 최근 실적 발표일은 차트 점선·날짜로 표시되므로 별도 블록 제거)
                st.markdown("<div style='margin-top:24px;'>", unsafe_allow_html=True)
                if st.button("돌아가기", key="실적캘린더_닫기", type="primary"):
                    del st.session_state["실적캘린더_선택"]
                    if _cache_key in st.session_state:
                        del st.session_state[_cache_key]
                    st.rerun()
                st.markdown("</div></div>", unsafe_allow_html=True)
                return

        ref_d = ref_date.date() if hasattr(ref_date, "date") and callable(getattr(ref_date, "date")) else ref_date
        # 기준일 당일은 Past로 분류 (Upcoming은 기준일 다음 날짜부터)
        upcoming_by_date = {d: rows for d, rows in by_date.items() if d > ref_d}
        past_by_date = {d: rows for d, rows in by_date.items() if d <= ref_d}

        def _render_calendar_section(section_title: str, section_data: dict, section_key: str, sort_desc: bool = False):
            if section_title:
                st.markdown(f"#### {section_title}")
            if not section_data:
                st.info("표시할 일정이 없습니다.")
                return

            dates_sorted = sorted(section_data.keys(), reverse=sort_desc)
            max_cols = 5
            page_key = f"실적캘린더_page_{section_key}"
            if page_key not in st.session_state:
                st.session_state[page_key] = 0

            total_pages = max(1, (len(dates_sorted) + max_cols - 1) // max_cols)
            nav_cols = st.columns([6, 0.3, 0.3])
            with nav_cols[1]:
                if st.button("◀", key=f"{section_key}_prev", help="이전 페이지"):
                    st.session_state[page_key] = max(0, st.session_state[page_key] - 1)
            with nav_cols[2]:
                if st.button("▶", key=f"{section_key}_next", help="다음 페이지"):
                    st.session_state[page_key] = min(total_pages - 1, st.session_state[page_key] + 1)

            page = max(0, min(st.session_state[page_key], total_pages - 1))
            st.session_state[page_key] = page

            start = page * max_cols
            chunk = dates_sorted[start : start + max_cols]
            key_sector_list = [
                (f"{section_key}_sel_{d}_{ticker}", sector)
                for d in chunk
                for ticker, _n, sector in section_data[d]
            ]
            btn_css = "\n".join(
                f'  button[id="{k}"] {{ background: {SECTOR_COLOR.get(s, "#f8f9fa")} !important; }}'
                for k, s in key_sector_list
            )
            st.markdown(
                f"""
                <style>
                button[id^="{section_key}_sel_"] {{
                    width: 100% !important; margin: 0 !important; padding: 4px 8px !important;
                    border-radius: 0 0 8px 8px !important; font-size: 0.85rem !important;
                    border: none !important; border-top: 1px solid rgba(0,0,0,0.08) !important;
                }}
                {btn_css}
                </style>
                """,
                unsafe_allow_html=True,
            )

            cols = st.columns(max_cols)
            for col_idx in range(max_cols):
                with cols[col_idx]:
                    if col_idx < len(chunk):
                        d = chunk[col_idx]
                        wday = d.weekday()
                        wday_str = _WEEKDAY_KR[wday]
                        st.markdown(f"**{d.strftime('%Y/%m/%d')} ({wday_str})**")
                        for ticker, name, sector in section_data[d]:
                            emoji = SECTOR_EMOJI.get(sector, "")
                            bg = SECTOR_COLOR.get(sector, "#f8f9fa")
                            border = bg if sector else "#eee"
                            name_esc = name.replace("<", "&lt;").replace(">", "&gt;")
                            sector_esc = (sector or "").replace("<", "&lt;").replace(">", "&gt;")
                            st.markdown(
                                f'<div style="background:{bg};border-radius:8px 8px 0 0;padding:10px 12px;margin:6px 0 0 0;'
                                f'border:1px solid {border};border-bottom:none;'
                                f'display:flex;justify-content:space-between;align-items:flex-start;gap:8px;'
                                f'word-wrap:break-word;overflow-wrap:break-word;">'
                                f'<div style="flex:1;min-width:0;"><span style="font-weight:700;font-size:1.15rem;">{ticker}</span><br/>'
                                f'<span style="color:#444;font-size:1rem;">{name_esc}</span></div>'
                                f'<div style="flex-shrink:0;text-align:right;font-size:1rem;font-weight:600;color:#333;">{emoji} {sector_esc}</div></div>',
                                unsafe_allow_html=True,
                            )
                            if st.button("상세", key=f"{section_key}_sel_{d}_{ticker}", type="secondary"):
                                st.session_state["실적캘린더_선택"] = ticker
                                st.rerun()
                    else:
                        st.markdown("—")
            st.caption(f"페이지 {page + 1} / {total_pages}")

        with st.expander("1) Upcoming", expanded=True):
            upcoming_years = sorted({d.year for d in upcoming_by_date.keys()}, reverse=True)
            if upcoming_years:
                up_year_options = ["전체"] + [f"{y}년" for y in upcoming_years]
                up_selected_year_label = st.selectbox(
                    "연도 필터",
                    options=up_year_options,
                    index=0,
                    key="실적캘린더_upcoming_year_filter",
                )
                if up_selected_year_label == "전체":
                    filtered_upcoming = upcoming_by_date
                else:
                    up_selected_year = int(up_selected_year_label.replace("년", ""))
                    filtered_upcoming = {d: rows for d, rows in upcoming_by_date.items() if d.year == up_selected_year}
            else:
                filtered_upcoming = upcoming_by_date
            _render_calendar_section("", filtered_upcoming, "upcoming", sort_desc=False)
        with st.expander("2) Past", expanded=bool(search_active)):
            past_years = sorted({d.year for d in past_by_date.keys()}, reverse=True)
            if past_years:
                year_options = ["전체"] + [f"{y}년" for y in past_years]
                selected_year_label = st.selectbox(
                    "연도 필터",
                    options=year_options,
                    index=0,
                    key="실적캘린더_past_year_filter",
                )
                if selected_year_label == "전체":
                    filtered_past = past_by_date
                else:
                    selected_year = int(selected_year_label.replace("년", ""))
                    filtered_past = {d: rows for d, rows in past_by_date.items() if d.year == selected_year}
            else:
                filtered_past = past_by_date
            _render_calendar_section("", filtered_past, "past", sort_desc=True)
    except Exception as e:
        st.error(f"오류 발생: {e}")
        import traceback
        with st.expander("상세 오류 정보"):
            st.code(traceback.format_exc())


def _render_재무_단일(ref_date):
    """재무 서브탭: OP/Sales 기반, 티커/종목명 검색 후 재무 정보 표·차트"""
    metric_option = st.radio(
        "지표 선택",
        options=["Sales", "Operating Profit"],
        horizontal=True,
        key="재무_metric_select",
    )
    st.subheader(metric_option)

    is_sales = metric_option == "Sales"
    metric_short = "Sales" if is_sales else "OP"
    metric_table_name = "sales_factset" if is_sales else "op_factset"

    ticker_list_df = _cached_sales_factset_ticker_list() if is_sales else _cached_op_factset_ticker_list()
    if ticker_list_df.empty:
        st.warning(f"market.{metric_table_name}에서 종목 목록을 불러올 수 없습니다.")
        return

    # 한 줄 검색(Selectbox)에서 티커/종목명 모두 검색되도록 라벨 구성
    has_name = "name" in ticker_list_df.columns or "종목명" in ticker_list_df.columns
    name_col = "name" if "name" in ticker_list_df.columns else ("종목명" if "종목명" in ticker_list_df.columns else None)
    ticker_col = "factset_ticker" if "factset_ticker" in ticker_list_df.columns else "ticker"

    base_df = ticker_list_df.copy()
    base_df[ticker_col] = base_df[ticker_col].astype(str).str.strip()
    base_df = base_df[base_df[ticker_col] != ""].drop_duplicates(subset=[ticker_col], keep="first")

    if has_name and name_col:
        base_df[name_col] = base_df[name_col].astype(str).str.strip()
    else:
        base_df["__name_fallback__"] = ""
        name_col = "__name_fallback__"

    if base_df.empty:
        st.info("종목 목록이 없습니다.")
        return

    name_map_df = _cached_index_constituents_name_map()
    if not name_map_df.empty and "factset_ticker" in name_map_df.columns and "name" in name_map_df.columns:
        idx_name_map = (
            name_map_df.drop_duplicates(subset=["factset_ticker"], keep="first")
            .set_index("factset_ticker")["name"]
            .to_dict()
        )
    else:
        idx_name_map = {}

    base_df["__name_from_ic__"] = base_df[ticker_col].map(idx_name_map)
    base_df["__display_name__"] = base_df["__name_from_ic__"].where(
        base_df["__name_from_ic__"].notna() & (base_df["__name_from_ic__"].astype(str).str.strip() != ""),
        base_df[name_col],
    )
    base_df["__display_name__"] = (
        base_df["__display_name__"].astype(str).str.strip().replace({"": pd.NA}).fillna(base_df[ticker_col])
    )
    base_df["__label__"] = base_df.apply(
        lambda r: f"{r[ticker_col]} | {r['__display_name__']}",
        axis=1,
    )
    op_options = sorted(base_df["__label__"].drop_duplicates().tolist())

    if not op_options:
        st.info("종목 목록이 없습니다.")
        return

    op_options_with_prompt = ["— 선택 —"] + op_options
    selected = st.selectbox(
        "종목",
        op_options_with_prompt,
        key="재무_종목선택",
        placeholder="티커 또는 종목명 입력 후 선택",
        label_visibility="collapsed",
    )
    if not selected or selected == "— 선택 —":
        return

    sel_ticker = selected.split(" | ")[0].strip()
    df = _get_sales_factset_by_ticker_fast(sel_ticker) if is_sales else _get_op_factset_by_ticker_fast(sel_ticker)
    if df.empty:
        st.warning(f"'{sel_ticker}'에 대한 {metric_option} 데이터가 없습니다.")
        return

    df = df.copy()
    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    period_type_col = None
    for c in ["period_type", "periodtype", "period"]:
        if c in df.columns:
            period_type_col = c
            break
    if not period_type_col:
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    # period_type Y / Q 분리
    df_y = df[df[period_type_col].astype(str).str.upper().str.strip() == "Y"].copy()
    df_q = df[df[period_type_col].astype(str).str.upper().str.strip() == "Q"].copy()
    skip_cols = {"dt", "factset_ticker", "ticker", period_type_col}
    value_cols = [c for c in df.columns if c not in skip_cols]
    value_col = next((c for c in value_cols if c.lower() == "value" or "rev" in c.lower() or "sale" in c.lower() or "revenue" in c.lower()), value_cols[0] if value_cols else None)

    def _fmt_val(v):
        if pd.isna(v) or v is None: return "—"
        n = pd.to_numeric(v, errors="coerce")
        if pd.notna(n): return f"{n:,.0f}"
        return str(v).strip() or "—"

    def _fmt_period_row(r):
        if "dt" not in r or pd.isna(r.get("dt")): return "", ""
        d = r["dt"]
        if hasattr(d, "year"):
            y, m, day = d.year, getattr(d, "month", 1), getattr(d, "day", 1)
            return str(y), f"{y}-{m:02d}-{day:02d}"
        return str(d)[:4], str(d)[:10]

    def _period_range_str(r, fallback_end_dt=None):
        """연도/분기 행의 기간 문자열 반환 (예: 1 Nov 2024 - 31 Oct 2025). fallback_end_dt는 end가 없을 때 사용."""
        start_cols = ["start_date", "period_start", "report_period_start", "period_begin", "begin_date"]
        end_cols = ["end_date", "period_end", "report_period_end", "end_date", "period_end_date"]
        start_d, end_d = None, None
        for c in start_cols:
            if c in r and pd.notna(r.get(c)):
                start_d = pd.to_datetime(r[c], errors="coerce")
                if pd.notna(start_d): break
        for c in end_cols:
            if c in r and pd.notna(r.get(c)):
                end_d = pd.to_datetime(r[c], errors="coerce")
                if pd.notna(end_d): break
        if end_d is None or (hasattr(end_d, "year") and pd.isna(end_d)):
            end_d = fallback_end_dt or r.get("dt")
            if end_d is not None:
                end_d = pd.to_datetime(end_d, errors="coerce")
        if start_d is None or pd.isna(start_d):
            if end_d is not None and hasattr(end_d, "year"):
                start_d = end_d - pd.DateOffset(months=11)
        if start_d is None or end_d is None or (pd.isna(start_d) or pd.isna(end_d)):
            return ""
        def _fmt_d(d):
            if hasattr(d, "strftime") and hasattr(d, "day"):
                return f"{d.day} {d.strftime('%b %Y')}"
            if hasattr(d, "strftime"):
                return str(d)[:10]
            return str(d)[:10]
        try:
            return f"{_fmt_d(start_d)} - {_fmt_d(end_d)}"
        except Exception:
            return f"{start_d} - {end_d}" if start_d and end_d else ""

    if not value_col:
        st.caption(f"{metric_short}(Value) 컬럼을 찾을 수 없습니다. 원본 데이터만 표시합니다.")
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    df_y_sorted = df_y.sort_values("dt", ascending=False).reset_index(drop=True) if not df_y.empty else pd.DataFrame()
    df_q_sorted = df_q.sort_values("dt", ascending=False).reset_index(drop=True) if not df_q.empty else pd.DataFrame()
    vy = pd.to_numeric(df_y_sorted[value_col], errors="coerce") if not df_y_sorted.empty and value_col in df_y_sorted.columns else pd.Series(dtype=float)
    vq = pd.to_numeric(df_q_sorted[value_col], errors="coerce") if not df_q_sorted.empty and value_col in df_q_sorted.columns else pd.Series(dtype=float)
    latest_y = float(vy.iloc[0]) if len(vy) and pd.notna(vy.iloc[0]) else None
    latest_q = float(vq.iloc[0]) if len(vq) and pd.notna(vq.iloc[0]) else None
    yoy_pct = (float(vy.iloc[0]) - float(vy.iloc[1])) / float(vy.iloc[1]) * 100 if len(vy) >= 2 and pd.notna(vy.iloc[0]) and pd.notna(vy.iloc[1]) and vy.iloc[1] != 0 else None
    qoq_pct = (float(vq.iloc[0]) - float(vq.iloc[1])) / float(vq.iloc[1]) * 100 if len(vq) >= 2 and pd.notna(vq.iloc[0]) and pd.notna(vq.iloc[1]) and vq.iloc[1] != 0 else None
    # 동분기 YoY (최근 분기 vs 1년 전 같은 분기)
    df_qa = df_q.sort_values("dt", ascending=True).reset_index(drop=True) if not df_q.empty else pd.DataFrame()
    q_map = {}
    if not df_qa.empty and value_col in df_qa.columns:
        for _, r in df_qa.iterrows():
            d = r.get("dt")
            if pd.notna(d) and hasattr(d, "year"):
                q_map[(d.year, (d.month - 1) // 3 + 1)] = pd.to_numeric(r[value_col], errors="coerce")
    same_q_yoy = None
    prior_year_same_q_op = None  # 최근 분기의 전년 동분기 OP (예: 2025Q4 기준 2024Q4)
    if df_q_sorted.empty == False and len(df_q_sorted) and "dt" in df_q_sorted.columns:
        d0 = df_q_sorted["dt"].iloc[0]
        if hasattr(d0, "year"):
            y0, q0 = d0.year, (d0.month - 1) // 3 + 1
            v0 = pd.to_numeric(df_q_sorted[value_col].iloc[0], errors="coerce")
            v_ly = q_map.get((y0 - 1, q0))
            if pd.notna(v0) and pd.notna(v_ly) and v_ly != 0:
                same_q_yoy = (float(v0) - float(v_ly)) / float(v_ly) * 100
                prior_year_same_q_op = float(v_ly)

    # ----- 1) KPI (펀드매니저 핵심 지표) -----
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        if latest_y is not None:
            delta_txt = f"{yoy_pct:+.1f}%" if yoy_pct is not None else None
            st.metric(
                f"최근 연도 {metric_short}",
                f"{latest_y:,.0f}",
                delta=None,
            )
            if delta_txt is not None:
                delta_color_hex = "#2e7d32" if float(yoy_pct) >= 0 else "#c62828"
                arrow = "▲" if float(yoy_pct) >= 0 else "▼"
                st.markdown(
                    f"<div style='margin-top:4px; font-size:13px; font-weight:700; color:{delta_color_hex};'>{arrow} {delta_txt}</div>",
                    unsafe_allow_html=True,
                )
    with k2:
        if latest_q is not None:
            delta_txt = f"{qoq_pct:+.1f}%" if qoq_pct is not None else None
            st.metric(
                f"최근 분기 {metric_short}",
                f"{latest_q:,.0f}",
                delta=None,
            )
            if delta_txt is not None:
                delta_color_hex = "#2e7d32" if float(qoq_pct) >= 0 else "#c62828"
                arrow = "▲" if float(qoq_pct) >= 0 else "▼"
                st.markdown(
                    f"<div style='margin-top:4px; font-size:13px; font-weight:700; color:{delta_color_hex};'>{arrow} {delta_txt}</div>",
                    unsafe_allow_html=True,
                )
    with k3:
        if prior_year_same_q_op is not None and same_q_yoy is not None:
            delta_txt = f"{same_q_yoy:+.1f}%"
            st.metric(
                f"전년 동분기 {metric_short}",
                f"{prior_year_same_q_op:,.0f}",
                delta=None,
            )
            delta_color_hex = "#2e7d32" if float(same_q_yoy) >= 0 else "#c62828"
            arrow = "▲" if float(same_q_yoy) >= 0 else "▼"
            st.markdown(
                f"<div style='margin-top:4px; font-size:13px; font-weight:700; color:{delta_color_hex};'>{arrow} {delta_txt}</div>",
                unsafe_allow_html=True,
            )
    with k4:
        if len(vy) >= 3 and all(pd.notna(vy.iloc[:3])):
            y3 = [float(vy.iloc[i]) for i in range(3)]
            yoy3 = [(y3[i] - y3[i+1]) / y3[i+1] * 100 if y3[i+1] else None for i in range(2)]
            if yoy3[0] is not None and yoy3[1] is not None:
                # 감속 = YoY 감소(음수) → 빨강/아래화살표, 가속 = YoY 증가(양수) → 초록/위화살표
                delta_pp = yoy3[0] - yoy3[1]  # 양수면 가속, 음수면 감속 (숫자 delta로 방향/색상 표시)
                st.metric(
                    "연도 성장 추이 (최근 2년 YoY)",
                    f"{yoy3[1]:+.1f}% → {yoy3[0]:+.1f}%",
                    delta=None,
                )
                delta_color_hex = "#2e7d32" if float(delta_pp) >= 0 else "#c62828"
                arrow = "▲" if float(delta_pp) >= 0 else "▼"
                st.markdown(
                    f"<div style='margin-top:4px; font-size:13px; font-weight:700; color:{delta_color_hex};'>{arrow} {delta_pp:+.1f}%p</div>",
                    unsafe_allow_html=True,
                )

    # ----- 2) 연도별 차트 + YoY 막대에 표시 -----
    st.markdown("---")
    st.markdown(f"#### 연도별 {metric_option}")
    if not df_y.empty and value_col in df_y.columns:
        # 차트는 과거 -> 최근(왼쪽 -> 오른쪽) 순서로 고정
        df_ya_asc = df_y.sort_values("dt", ascending=True).reset_index(drop=True)
        y_labels_asc = df_ya_asc["dt"].dt.year.astype(str) if hasattr(df_ya_asc["dt"].iloc[0], "year") else df_ya_asc["dt"].astype(str).str[:4]
        v_vals_asc = pd.to_numeric(df_ya_asc[value_col], errors="coerce")
        growth_y_asc = v_vals_asc.pct_change() * 100
        # 차트 색상도 YoY 추이 차트와 톤을 맞춰 연한 색상으로 표시
        colors_y = ["#a5d6a7" if g > 0 else "#ef9a9a" if pd.notna(g) and g < 0 else "#cfd8dc" for g in growth_y_asc]
        if v_vals_asc.notna().any():
            text_y = [f"{v:,.0f}" for v in v_vals_asc]
            y_tickvals = y_labels_asc.tolist()
            fig_y = go.Figure(go.Bar(x=y_labels_asc, y=v_vals_asc, text=text_y, textposition="outside", marker_color=colors_y, textfont=dict(size=18)))
            fig_y.update_traces(hovertemplate=f"%{{x}}<br>{metric_short}: %{{y:,.0f}}<extra></extra>")
            fig_y.update_layout(
                height=620,
                xaxis_title="",
                yaxis_title=metric_option,
                margin=dict(t=100, b=56, l=56, r=80),
                showlegend=False,
                font=dict(size=13),
                yaxis=dict(rangemode="tozero", tickfont=dict(size=18), title_font=dict(size=18)),
                xaxis=dict(
                    tickfont=dict(size=16),
                    type="category",
                    categoryorder="array",
                    categoryarray=y_tickvals,
                    tickmode="array",
                    tickvals=y_tickvals,
                    ticktext=y_tickvals,
                ),
            )
            fig_y.add_annotation(x=1, y=1.14, xref="paper", yref="paper", text="Unit: $ million", showarrow=False, xanchor="right", yanchor="top", font=dict(size=14, color="#333"))
            st.plotly_chart(fig_y, use_container_width=True, config=dict(displayModeBar=False))
        # 연도별 YoY(%) 추세 차트 (과거 -> 최근)
        yoy_plot_df = pd.DataFrame({"연도": y_labels_asc, "YoY(%)": growth_y_asc}).dropna(subset=["YoY(%)"]).copy()
        if not yoy_plot_df.empty:
            yoy_plot_df["연도"] = yoy_plot_df["연도"].astype(str)
            yoy_plot_df["색상"] = yoy_plot_df["YoY(%)"].apply(
                lambda v: "#a5d6a7" if v > 0 else "#ef9a9a" if v < 0 else "#cfd8dc"
            )

            st.markdown("##### 연도별 YoY(%) 추이")
            fig_yoy = go.Figure()
            fig_yoy.add_trace(
                go.Bar(
                    x=yoy_plot_df["연도"],
                    y=yoy_plot_df["YoY(%)"],
                    marker_color=yoy_plot_df["색상"],
                    opacity=0.35,
                    name="YoY(%)",
                    hovertemplate="%{x}<br>YoY: %{y:+.1f}%<extra></extra>",
                )
            )
            fig_yoy.add_trace(
                go.Scatter(
                    x=yoy_plot_df["연도"],
                    y=yoy_plot_df["YoY(%)"],
                    mode="lines+markers+text",
                    line=dict(color="#607d8b", width=2),
                    marker=dict(size=8, color=yoy_plot_df["색상"]),
                    text=[f"{v:+.1f}%" for v in yoy_plot_df["YoY(%)"]],
                    textposition="top center",
                        textfont=dict(size=18),
                    name="YoY 추세",
                    hovertemplate="%{x}<br>YoY: %{y:+.1f}%<extra></extra>",
                )
            )
            fig_yoy.update_layout(
                height=620,
                xaxis_title="",
                yaxis_title="YoY (%)",
                template="plotly_white",
                hovermode="x unified",
                showlegend=False,
                margin=dict(t=30, b=40, l=40, r=130),
                xaxis=dict(
                    tickfont=dict(size=14),
                    type="category",
                    categoryorder="array",
                    categoryarray=yoy_plot_df["연도"].tolist(),
                    tickmode="array",
                    tickvals=yoy_plot_df["연도"].tolist(),
                    ticktext=yoy_plot_df["연도"].tolist(),
                ),
                yaxis=dict(
                    tickfont=dict(size=14),
                    zeroline=True,
                    zerolinecolor="#b0bec5",
                    zerolinewidth=1,
                ),
            )
            st.plotly_chart(fig_yoy, use_container_width=True, config=dict(displayModeBar=False))

        # 연도별 요약 표는 제거하고 차트 중심으로 표시
    else:
        st.caption("연간 데이터 없음")

    # ----- 3) 분기별 차트 + QoQ -----
    st.markdown("---")
    st.markdown(f"#### 분기별 {metric_option}")
    if not df_q.empty and value_col in df_q.columns:
        # 차트는 과거 -> 최근(왼쪽 -> 오른쪽) 순서로 고정
        df_qa_asc = df_q.sort_values("dt", ascending=True).reset_index(drop=True)
        df_qa_asc = df_qa_asc[df_qa_asc["dt"].notna()].copy()
        if df_qa_asc.empty:
            st.caption("분기 데이터 없음")
        else:
            q_labels_asc = df_qa_asc["dt"].apply(lambda x: f"{x.year}-Q{(x.month-1)//3+1}" if pd.notna(x) and hasattr(x, "year") else str(x)[:10])
            v_vals_asc = pd.to_numeric(df_qa_asc[value_col], errors="coerce")
            if v_vals_asc.notna().any():
                # 전체 분기 데이터 기준 직전분기 대비(QoQ)
                qoq_asc = v_vals_asc.pct_change() * 100
                colors_chart = ["#a5d6a7" if g and g > 0 else "#ef9a9a" if g and g < 0 else "#cfd8dc" for g in qoq_asc]
                fig_q = go.Figure(go.Bar(x=q_labels_asc, y=v_vals_asc, text=[f"{v:,.0f}" for v in v_vals_asc], textposition="outside", marker_color=colors_chart, textfont=dict(size=18)))
                year_ticks_df = (
                    pd.DataFrame({"라벨": q_labels_asc, "연도": df_qa_asc["dt"].dt.year.astype(str)})
                    .groupby("연도", as_index=False)
                    .tail(1)
                )
                year_ticks_df["표시라벨"] = year_ticks_df["라벨"].astype(str).str.replace("-Q", ".Q", regex=False)
                fig_q.update_layout(
                    height=620,
                    xaxis_title="",
                    yaxis_title=metric_option,
                    margin=dict(t=100, b=56, l=56, r=80),
                    xaxis_tickangle=-40,
                    showlegend=False,
                    font=dict(size=13),
                    yaxis=dict(rangemode="tozero", tickfont=dict(size=18), title_font=dict(size=18)),
                    xaxis=dict(
                        tickfont=dict(size=16),
                        type="category",
                        categoryorder="array",
                        categoryarray=q_labels_asc.tolist(),
                        tickmode="array",
                        tickvals=year_ticks_df["라벨"].tolist(),
                        ticktext=year_ticks_df["표시라벨"].tolist(),
                    ),
                )
                fig_q.add_annotation(x=1, y=1.14, xref="paper", yref="paper", text="Unit: $ million", showarrow=False, xanchor="right", yanchor="top", font=dict(size=14, color="#333"))
                st.plotly_chart(fig_q, use_container_width=True, config=dict(displayModeBar=False))

                # 연도별 YoY 추이와 동일 양식으로 분기 QoQ 추이 표시
                qoq_plot_df = pd.DataFrame({"분기": q_labels_asc, "QoQ(%)": qoq_asc}).dropna(subset=["QoQ(%)"]).copy()
                if not qoq_plot_df.empty:
                    qoq_plot_df["분기"] = qoq_plot_df["분기"].astype(str)
                    qoq_plot_df["색상"] = qoq_plot_df["QoQ(%)"].apply(
                        lambda v: "#a5d6a7" if v > 0 else "#ef9a9a" if v < 0 else "#cfd8dc"
                    )
                    qoq_plot_df["연도"] = qoq_plot_df["분기"].str.slice(0, 4)
                    yoy_year_ticks_df = qoq_plot_df.groupby("연도", as_index=False).tail(1)
                    yoy_year_ticks_df["표시라벨"] = yoy_year_ticks_df["분기"].astype(str).str.replace("-Q", ".Q", regex=False)

                    st.markdown("##### 분기별 QoQ(%) 추이")
                    fig_qoq = go.Figure()
                    fig_qoq.add_trace(
                        go.Bar(
                            x=qoq_plot_df["분기"],
                            y=qoq_plot_df["QoQ(%)"],
                            marker_color=qoq_plot_df["색상"],
                            opacity=0.35,
                            name="QoQ(%)",
                            hovertemplate="%{x}<br>QoQ: %{y:+.1f}%<extra></extra>",
                        )
                    )
                    fig_qoq.add_trace(
                        go.Scatter(
                            x=qoq_plot_df["분기"],
                            y=qoq_plot_df["QoQ(%)"],
                            mode="lines+markers+text",
                            line=dict(color="#607d8b", width=2),
                            marker=dict(size=8, color=qoq_plot_df["색상"]),
                            text=[f"{v:+.1f}%" for v in qoq_plot_df["QoQ(%)"]],
                            textposition="top center",
                            textfont=dict(size=18),
                            name="QoQ 추세",
                            hovertemplate="%{x}<br>QoQ: %{y:+.1f}%<extra></extra>",
                        )
                    )
                    fig_qoq.update_layout(
                        height=620,
                        xaxis_title="",
                        yaxis_title="QoQ (%)",
                        template="plotly_white",
                        hovermode="x unified",
                        showlegend=False,
                        margin=dict(t=30, b=40, l=40, r=130),
                        xaxis=dict(
                            tickfont=dict(size=16),
                            tickangle=-35,
                            type="category",
                            categoryorder="array",
                            categoryarray=qoq_plot_df["분기"].tolist(),
                            tickmode="array",
                            tickvals=yoy_year_ticks_df["분기"].tolist(),
                            ticktext=yoy_year_ticks_df["표시라벨"].tolist(),
                        ),
                        yaxis=dict(
                            tickfont=dict(size=18),
                            title_font=dict(size=18),
                            zeroline=True,
                            zerolinecolor="#b0bec5",
                            zerolinewidth=1,
                        ),
                    )
                    st.plotly_chart(fig_qoq, use_container_width=True, config=dict(displayModeBar=False))
    else:
        st.caption("분기 데이터 없음")

    # ----- 4) 상세 원본 테이블 (접이식) -----
    with st.expander("상세 원본 데이터 (연도/기준일·분기)", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            if not df_y.empty:
                df_ys = df_y.sort_values("dt", ascending=False).reset_index(drop=True)
                rows = []
                for _, r in df_ys.iterrows():
                    y_str, d_str = _fmt_period_row(r)
                    row = {"연도": y_str, "기준일": d_str}
                    for c in value_cols:
                        if c in r:
                            row[c] = _fmt_val(r[c])
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        with col2:
            if not df_q.empty:
                df_qs = df_q.sort_values("dt", ascending=False).reset_index(drop=True)
                rows = []
                for _, r in df_qs.iterrows():
                    d = r.get("dt")
                    if pd.notna(d) and hasattr(d, "strftime"):
                        lab = d.strftime("%Y-%m-%d")
                        sub = f"{getattr(d, 'year', d)}-Q{(getattr(d, 'month', 1)-1)//3+1}"
                    else:
                        lab, sub = str(d)[:10], ""
                    row = {"기간": lab, "분기": sub}
                    for c in value_cols:
                        if c in r:
                            row[c] = _fmt_val(r[c])
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _get_finance_ticker_options() -> list[str]:
    """재무 탭 공통 티커 선택 옵션 (티커 | 종목명)"""
    sales_df = _cached_sales_factset_ticker_list()
    op_df = _cached_op_factset_ticker_list()
    ticker_list_df = pd.concat([sales_df, op_df], ignore_index=True) if (not sales_df.empty or not op_df.empty) else pd.DataFrame()
    if ticker_list_df.empty:
        return []

    has_name = "name" in ticker_list_df.columns or "종목명" in ticker_list_df.columns
    name_col = "name" if "name" in ticker_list_df.columns else ("종목명" if "종목명" in ticker_list_df.columns else None)
    ticker_col = "factset_ticker" if "factset_ticker" in ticker_list_df.columns else "ticker"

    base_df = ticker_list_df.copy()
    base_df[ticker_col] = base_df[ticker_col].astype(str).str.strip()
    base_df = base_df[base_df[ticker_col] != ""].drop_duplicates(subset=[ticker_col], keep="first")
    if base_df.empty:
        return []

    if has_name and name_col:
        base_df[name_col] = base_df[name_col].astype(str).str.strip()
    else:
        base_df["__name_fallback__"] = ""
        name_col = "__name_fallback__"

    name_map_df = _cached_index_constituents_name_map()
    if not name_map_df.empty and "factset_ticker" in name_map_df.columns and "name" in name_map_df.columns:
        idx_name_map = (
            name_map_df.drop_duplicates(subset=["factset_ticker"], keep="first")
            .set_index("factset_ticker")["name"]
            .to_dict()
        )
    else:
        idx_name_map = {}

    base_df["__name_from_ic__"] = base_df[ticker_col].map(idx_name_map)
    base_df["__display_name__"] = base_df["__name_from_ic__"].where(
        base_df["__name_from_ic__"].notna() & (base_df["__name_from_ic__"].astype(str).str.strip() != ""),
        base_df[name_col],
    )
    base_df["__display_name__"] = (
        base_df["__display_name__"].astype(str).str.strip().replace({"": pd.NA}).fillna(base_df[ticker_col])
    )
    base_df["__label__"] = base_df.apply(lambda r: f"{r[ticker_col]} | {r['__display_name__']}", axis=1)
    return sorted(base_df["__label__"].drop_duplicates().tolist())


def _prepare_metric_payload(df: pd.DataFrame):
    """Sales/OP 공통 분석용 시계열/지표 계산"""
    if df is None or df.empty:
        return None
    data = df.copy()
    if "dt" in data.columns:
        data["dt"] = pd.to_datetime(data["dt"], errors="coerce")
    period_type_col = None
    for c in ["period_type", "periodtype", "period"]:
        if c in data.columns:
            period_type_col = c
            break
    if not period_type_col:
        return None

    skip_cols = {"dt", "factset_ticker", "ticker", period_type_col}
    value_cols = [c for c in data.columns if c not in skip_cols]
    value_col = next((c for c in value_cols if c.lower() == "value" or "rev" in c.lower() or "sale" in c.lower() or "revenue" in c.lower()), value_cols[0] if value_cols else None)
    if not value_col:
        return None

    df_y = data[data[period_type_col].astype(str).str.upper().str.strip() == "Y"].copy()
    df_q = data[data[period_type_col].astype(str).str.upper().str.strip() == "Q"].copy()
    df_y = df_y.dropna(subset=["dt"]).sort_values("dt")
    df_q = df_q.dropna(subset=["dt"]).sort_values("dt")

    y_ts = pd.DataFrame()
    q_ts = pd.DataFrame()
    if not df_y.empty:
        y_ts = pd.DataFrame(
            {
                "dt": df_y["dt"],
                "label": df_y["dt"].dt.year.astype(str),
                "value": pd.to_numeric(df_y[value_col], errors="coerce"),
            }
        ).dropna(subset=["value"])
        # 동일 연도 중복은 최신값 1개만 사용
        y_ts = y_ts.sort_values("dt").drop_duplicates(subset=["label"], keep="last").reset_index(drop=True)
    if not df_q.empty:
        q_ts = pd.DataFrame(
            {
                "dt": df_q["dt"],
                "label": df_q["dt"].apply(lambda x: f"{x.year}-Q{(x.month - 1) // 3 + 1}"),
                "value": pd.to_numeric(df_q[value_col], errors="coerce"),
            }
        ).dropna(subset=["value"])
        # 동일 분기 중복은 최신값 1개만 사용
        q_ts = q_ts.sort_values("dt").drop_duplicates(subset=["label"], keep="last").reset_index(drop=True)

    vy = y_ts["value"].reset_index(drop=True)
    vq = q_ts["value"].reset_index(drop=True)
    latest_y = float(vy.iloc[-1]) if len(vy) else None
    latest_q = float(vq.iloc[-1]) if len(vq) else None
    yoy_pct = ((float(vy.iloc[-1]) - float(vy.iloc[-2])) / float(vy.iloc[-2]) * 100) if len(vy) >= 2 and vy.iloc[-2] != 0 else None
    qoq_pct = ((float(vq.iloc[-1]) - float(vq.iloc[-2])) / float(vq.iloc[-2]) * 100) if len(vq) >= 2 and vq.iloc[-2] != 0 else None

    return {
        "year_ts": y_ts,
        "quarter_ts": q_ts,
        "latest_y": latest_y,
        "latest_q": latest_q,
        "yoy_pct": yoy_pct,
        "qoq_pct": qoq_pct,
    }


def _render_재무_혼합(ref_date):
    """재무 혼합 탭: Sales + Operating Profit 동시 비교"""
    st.subheader("Sales + Operating Profit")
    options = _get_finance_ticker_options()
    if not options:
        st.info("종목 목록이 없습니다.")
        return

    selected = st.selectbox(
        "종목",
        ["— 선택 —"] + options,
        key="재무_종목선택_혼합",
        placeholder="티커 또는 종목명 입력 후 선택",
        label_visibility="collapsed",
    )
    if not selected or selected == "— 선택 —":
        return

    sel_ticker = selected.split(" | ")[0].strip()
    sales_df = _get_sales_factset_by_ticker_fast(sel_ticker)
    op_df = _get_op_factset_by_ticker_fast(sel_ticker)
    sales_payload = _prepare_metric_payload(sales_df)
    op_payload = _prepare_metric_payload(op_df)
    if sales_payload is None and op_payload is None:
        st.warning(f"'{sel_ticker}'의 Sales/Operating Profit 데이터를 찾을 수 없습니다.")
        return

    annual_df = pd.DataFrame(columns=["label", "sales", "op"])
    if sales_payload and not sales_payload["year_ts"].empty:
        annual_df = sales_payload["year_ts"][["label", "value"]].rename(columns={"value": "sales"})
    if op_payload and not op_payload["year_ts"].empty:
        op_y = op_payload["year_ts"][["label", "value"]].rename(columns={"value": "op"})
        annual_df = annual_df.merge(op_y, on="label", how="outer") if not annual_df.empty else op_y
    if not annual_df.empty:
        annual_df["year_int"] = pd.to_numeric(annual_df["label"].astype(str).str.extract(r"(\d{4})")[0], errors="coerce")
        annual_df = (
            annual_df.sort_values(["year_int", "label"])
            .drop_duplicates(subset=["label"], keep="last")
            .drop(columns=["year_int"], errors="ignore")
            .reset_index(drop=True)
        )
        annual_df["opm"] = annual_df.apply(
            lambda r: (float(r["op"]) / float(r["sales"]) * 100.0)
            if pd.notna(r.get("op")) and pd.notna(r.get("sales")) and float(r.get("sales")) != 0
            else None,
            axis=1,
        )

    quarter_df = pd.DataFrame(columns=["label", "dt", "sales", "op"])
    if sales_payload and not sales_payload["quarter_ts"].empty:
        quarter_df = sales_payload["quarter_ts"][["label", "dt", "value"]].rename(columns={"value": "sales"})
    if op_payload and not op_payload["quarter_ts"].empty:
        op_q = op_payload["quarter_ts"][["label", "dt", "value"]].rename(columns={"value": "op"})
        quarter_df = quarter_df.merge(op_q, on=["label", "dt"], how="outer") if not quarter_df.empty else op_q
    if not quarter_df.empty:
        quarter_df = (
            quarter_df.sort_values("dt")
            .drop_duplicates(subset=["label"], keep="last")
            .reset_index(drop=True)
        )
        # 혼합 탭 분기 차트는 최근 5개년(20개 분기)만 표시
        quarter_df = quarter_df.tail(20).reset_index(drop=True)
        quarter_df["opm"] = quarter_df.apply(
            lambda r: (float(r["op"]) / float(r["sales"]) * 100.0)
            if pd.notna(r.get("op")) and pd.notna(r.get("sales")) and float(r.get("sales")) != 0
            else None,
            axis=1,
        )

    st.markdown("---")
    st.markdown("#### 연도별 Sales / OP / OPM")
    fig_y = make_subplots(specs=[[{"secondary_y": True}]])
    if not annual_df.empty and "sales" in annual_df.columns:
        fig_y.add_trace(
            go.Bar(
                x=annual_df["label"],
                y=annual_df["sales"],
                name="Sales",
                marker_color="#64b5f6",
                opacity=0.85,
            ),
            secondary_y=False,
        )
    if not annual_df.empty and "op" in annual_df.columns:
        fig_y.add_trace(
            go.Bar(
                x=annual_df["label"],
                y=annual_df["op"],
                name="Operating Profit",
                marker_color="#81c784",
                opacity=0.85,
            ),
            secondary_y=False,
        )
    if not annual_df.empty and "opm" in annual_df.columns:
        fig_y.add_trace(
            go.Scatter(
                x=annual_df["label"],
                y=annual_df["opm"],
                name="OPM (%)",
                mode="lines+markers+text",
                text=[f"{v:.1f}%" if pd.notna(v) else "" for v in annual_df["opm"]],
                textposition="top center",
                textfont=dict(size=14),
                line=dict(color="#fb8c00", width=2.5),
                marker=dict(size=8),
            ),
            secondary_y=True,
        )
    fig_y.update_layout(
        barmode="group",
        height=560,
        xaxis_title="",
        template="plotly_white",
        font=dict(size=15),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(tickfont=dict(size=14), title_font=dict(size=16)),
    )
    fig_y.update_yaxes(title_text="Value", secondary_y=False, tickfont=dict(size=14), title_font=dict(size=16))
    # 보조축은 OPM 라인 계산용으로만 사용, 축 선/그리드는 숨김
    fig_y.update_yaxes(
        title_text="",
        secondary_y=True,
        showgrid=False,
        zeroline=False,
        showline=False,
        showticklabels=False,
        ticks="",
    )
    st.plotly_chart(fig_y, use_container_width=True, config=dict(displayModeBar=False))

    st.markdown("#### 분기별 Sales / OP / OPM")
    fig_q = make_subplots(specs=[[{"secondary_y": True}]])
    if not quarter_df.empty and "sales" in quarter_df.columns:
        fig_q.add_trace(
            go.Bar(
                x=quarter_df["label"],
                y=quarter_df["sales"],
                name="Sales",
                marker_color="#90caf9",
                opacity=0.85,
            ),
            secondary_y=False,
        )
    if not quarter_df.empty and "op" in quarter_df.columns:
        fig_q.add_trace(
            go.Bar(
                x=quarter_df["label"],
                y=quarter_df["op"],
                name="Operating Profit",
                marker_color="#a5d6a7",
                opacity=0.85,
            ),
            secondary_y=False,
        )
    if not quarter_df.empty and "opm" in quarter_df.columns:
        fig_q.add_trace(
            go.Scatter(
                x=quarter_df["label"],
                y=quarter_df["opm"],
                name="OPM (%)",
                mode="lines+markers+text",
                text=[f"{v:.1f}%" if pd.notna(v) else "" for v in quarter_df["opm"]],
                textposition="top center",
                textfont=dict(size=12),
                line=dict(color="#fb8c00", width=2.5),
                marker=dict(size=7),
            ),
            secondary_y=True,
        )

    # 분기 데이터는 모두 보이되, x축 라벨은 최신 분기와 같은 분기만 연 1회 표시
    latest_quarter = int(quarter_df["dt"].iloc[-1].quarter) if not quarter_df.empty else None
    q_tick_df = (
        quarter_df[quarter_df["dt"].dt.quarter == latest_quarter][["label", "dt"]].copy()
        if latest_quarter is not None else pd.DataFrame(columns=["label", "dt"])
    )
    if not q_tick_df.empty:
        q_tick_df["표시라벨"] = q_tick_df["label"].astype(str).str.replace("-Q", ".Q", regex=False)

    fig_q.update_layout(
        barmode="group",
        height=560,
        xaxis_title="",
        template="plotly_white",
        font=dict(size=15),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(
            tickfont=dict(size=14),
            tickangle=-35,
            tickmode="array",
            tickvals=q_tick_df["label"].tolist() if not q_tick_df.empty else None,
            ticktext=q_tick_df["표시라벨"].tolist() if not q_tick_df.empty else None,
        ),
    )
    fig_q.update_yaxes(title_text="Value", secondary_y=False, tickfont=dict(size=14), title_font=dict(size=16))
    fig_q.update_yaxes(
        title_text="",
        secondary_y=True,
        showgrid=False,
        zeroline=False,
        showline=False,
        showticklabels=False,
        ticks="",
    )
    st.plotly_chart(fig_q, use_container_width=True, config=dict(displayModeBar=False))


def _render_재무(ref_date):
    """재무 탭 엔트리: 단일/혼합 탭 제공"""
    fin_labels = ["혼합", "단일 지표"]
    if "stock_fin_tab" not in st.session_state:
        st.session_state["stock_fin_tab"] = "혼합"
    selected_fin = st.session_state.get("stock_fin_tab", "혼합")

    st.markdown(
        """
        <style>
        .st-key-kbam_stock_fin_tabs { margin-bottom: 8px; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with st.container(key="kbam_stock_fin_tabs"):
        selected_fin = st.radio(
            "재무 하위 탭",
            options=fin_labels,
            index=fin_labels.index(selected_fin) if selected_fin in fin_labels else 0,
            horizontal=True,
            label_visibility="collapsed",
            key="stock_fin_tab_nav",
        )
    st.session_state["stock_fin_tab"] = selected_fin

    if selected_fin == "혼합":
        _render_재무_혼합(ref_date)
    else:
        _render_재무_단일(ref_date)