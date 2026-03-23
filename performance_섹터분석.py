"""
섹터 분석 탭 - index_constituents 기반 섹터 비중/기여도/차트
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from call import execute_custom_query
from utils import get_business_day_by_country


MAJOR_INDICES_FOR_SECTOR = [
    "SPX Index",
    "NDX Index",
    "HSCEI Index",
    "HSTECH Index",
    "NIFTY Index",
    "SPEHYDUP Index",
    "SX5E Index",
]

# BUSINESS_DAY 테이블 매칭이 안 될 때 사용할 Index별 국가 코드 (소문자 → utils에서 대문자로 사용)
INDEX_TO_COUNTRY = {
    "SPX Index": "US",
    "NDX Index": "US",
    "HSCEI Index": "HK",
    "HSTECH Index": "HK",
    "SPEHYDUP Index": "US",
    "NIFTY Index": "IN",
    "SX5E Index": "EU",
}


def _get_latest_dt_for_index(index_name: str):
    """DB에서 해당 Index의 가장 최근 dt(날짜) 반환. 서버 날짜와 무관하게 실제 데이터 기준."""
    query = f"""
        SELECT MAX(dt)::date AS max_dt
        FROM index_constituents
        WHERE "index" = '{index_name}'
          AND index_weight IS NOT NULL
          AND local_price IS NOT NULL
    """
    rows = execute_custom_query(query)
    if not rows or rows[0].get("max_dt") is None:
        return None
    return rows[0]["max_dt"]


def _load_index_constituents(index_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    index_constituents에서 섹터 분석에 필요한 컬럼만 조회.
    사용 컬럼: dt, index, ticker, bb_ticker, name, gics_name, local_price, index_market_cap, index_weight
    """
    query = f"""
        SELECT
            dt,
            "index" as index_name,
            ticker,
            bb_ticker,
            name,
            gics_name,
            local_price,
            index_market_cap,
            index_weight
        FROM index_constituents
        WHERE "index" = '{index_name}'
          AND dt >= '{start_date}'
          AND dt <= '{end_date}'
          AND index_weight IS NOT NULL
          AND local_price IS NOT NULL
        ORDER BY dt, ticker
    """
    data = execute_custom_query(query)
    df = pd.DataFrame(data)
    if df.empty:
        return df

    df["dt"] = pd.to_datetime(df["dt"])
    df["index_name"] = df["index_name"].astype(str).str.strip()
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["gics_name"] = df["gics_name"].astype(str).str.strip()
    df["local_price"] = pd.to_numeric(df["local_price"], errors="coerce")
    df["index_weight"] = pd.to_numeric(df["index_weight"], errors="coerce")
    df = df.dropna(subset=["dt", "ticker", "gics_name", "local_price", "index_weight"])

    # 동일 dt/ticker 중복 제거 (마지막 값 유지)
    df = df.sort_values(["dt", "ticker"]).drop_duplicates(subset=["dt", "ticker"], keep="last")
    return df


def _pick_anchor_dates(df: pd.DataFrame, end_date: datetime.date) -> tuple[pd.Timestamp, pd.Timestamp] | tuple[None, None]:
    """
    end_date 이하의 가장 최근 날짜(anchor)와 그 직전 날짜(prev) 반환.
    """
    if df.empty:
        return None, None

    dates = sorted(df["dt"].dt.normalize().unique())
    end_ts = pd.to_datetime(end_date).normalize()
    valid = [d for d in dates if d <= end_ts]
    if len(valid) < 2:
        return None, None
    anchor = valid[-1]
    prev = valid[-2]
    return anchor, prev


def _sector_weights(df_on_date: pd.DataFrame) -> pd.DataFrame:
    """해당 날짜의 GICS별 비중 합(index_weight sum)."""
    w = (
        df_on_date.groupby("gics_name", as_index=False)["index_weight"]
        .sum()
        .rename(columns={"index_weight": "weight"})
    )
    w["weight_pct"] = w["weight"] * 100.0
    w = w.sort_values("weight", ascending=False)
    return w


def _constituents_daily_returns(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> pd.DataFrame:
    """전일 대비 종목별 수익률·비중(기준일). 반환: ticker, name, gics_name, weight_pct, ret_pct."""
    prev = prev_df[["ticker", "gics_name", "index_weight", "local_price"]].copy()
    prev = prev.rename(columns={"index_weight": "w_prev", "local_price": "p_prev"})
    curr = curr_df[["ticker", "name", "gics_name", "local_price", "index_weight"]].copy()
    curr = curr.rename(columns={"local_price": "p_curr", "index_weight": "weight_curr"})
    m = prev.merge(curr, on=["ticker", "gics_name"], how="inner")
    m = m.dropna(subset=["w_prev", "p_prev", "p_curr", "weight_curr"])
    m = m[m["p_prev"] > 0]
    if m.empty:
        return pd.DataFrame(columns=["ticker", "name", "gics_name", "weight_pct", "ret_pct"])
    m["ret_pct"] = (m["p_curr"] - m["p_prev"]) / m["p_prev"] * 100.0
    m["weight_pct"] = m["weight_curr"] * 100.0
    return m[["ticker", "name", "gics_name", "weight_pct", "ret_pct"]].copy()


def _sector_daily_contribution(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> pd.DataFrame:
    """
    전일(prev) 비중 고정 + 가격 변화로 섹터별 일일 수익률 기여도(%) 계산.
    ret(%) = (P_t - P_{t-1}) / P_{t-1} * 100
    contrib(%) = ret(%) * weight_{t-1}
    섹터별 contrib 합을 반환.
    """
    prev = prev_df[["ticker", "gics_name", "index_weight", "local_price"]].copy()
    prev = prev.rename(columns={"index_weight": "w_prev", "local_price": "p_prev"})

    curr = curr_df[["ticker", "local_price"]].copy().rename(columns={"local_price": "p_curr"})

    m = prev.merge(curr, on="ticker", how="inner")
    m = m.dropna(subset=["w_prev", "p_prev", "p_curr"])
    m = m[m["p_prev"] > 0]
    if m.empty:
        return pd.DataFrame(columns=["gics_name", "stock_count", "weight_sum", "contribution"])

    m["ret_pct"] = (m["p_curr"] - m["p_prev"]) / m["p_prev"] * 100.0
    m["contrib_pct"] = m["ret_pct"] * m["w_prev"]

    out = (
        m.groupby("gics_name", as_index=False)
        .agg(
            stock_count=("ticker", "nunique"),
            weight_sum=("w_prev", "sum"),
            contribution=("contrib_pct", "sum"),
        )
        .sort_values("weight_sum", ascending=False)
    )
    out["weight_sum_pct"] = out["weight_sum"] * 100.0
    return out


def _sector_contribution_timeseries(df: pd.DataFrame, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """
    기간 내 섹터별 누적 기여도(일일 기여도 누적합) 시계열 생성.
    """
    if df.empty:
        return pd.DataFrame()

    # 날짜 리스트 (start~end 범위 내, df에 실제 존재하는 dt만)
    start_ts = pd.to_datetime(start_date).normalize()
    end_ts = pd.to_datetime(end_date).normalize()
    dates = sorted(d for d in df["dt"].dt.normalize().unique() if start_ts <= d <= end_ts)
    if len(dates) < 2:
        return pd.DataFrame()

    rows = []
    for i in range(1, len(dates)):
        prev_d = dates[i - 1]
        curr_d = dates[i]
        prev_df = df[df["dt"].dt.normalize() == prev_d]
        curr_df = df[df["dt"].dt.normalize() == curr_d]

        daily = _sector_daily_contribution(prev_df, curr_df)
        if daily.empty:
            continue

        daily = daily[["gics_name", "contribution"]].copy()
        daily["dt"] = curr_d
        rows.append(daily)

    if not rows:
        return pd.DataFrame()

    ts = pd.concat(rows, ignore_index=True)
    ts = ts.sort_values(["gics_name", "dt"])
    ts["cumulative_contribution"] = ts.groupby("gics_name")["contribution"].cumsum()

    # 기간 첫 날(dates[0])을 0%로 추가해 차트가 0%에서 시작하도록 함
    first_date = dates[0]
    sectors_in_ts = ts["gics_name"].unique().tolist()
    start_rows = pd.DataFrame({
        "gics_name": sectors_in_ts,
        "contribution": 0.0,
        "dt": first_date,
        "cumulative_contribution": 0.0,
    })
    ts = pd.concat([start_rows, ts], ignore_index=True).sort_values(["gics_name", "dt"]).reset_index(drop=True)
    return ts


def render():
    """섹터 분석 탭 렌더링"""
    st.header("섹터 분석")

    # Index 선택만 상단에
    selected_index = st.selectbox("Index 선택", MAJOR_INDICES_FOR_SECTOR, index=0)

    try:
        # 기준일·전일 = US 영업일 기준 (우선). BUSINESS_DAY 매칭 안 되면 Index별 국가(hk/us/in/eu)로 fallback
        today = datetime.now().date()
        try:
            anchor_date = get_business_day_by_country(today, 1, "US")
            prev_date = get_business_day_by_country(anchor_date, 1, "US")
            bday_country = "US"
        except Exception:
            country = INDEX_TO_COUNTRY.get(selected_index, "US")
            anchor_date = get_business_day_by_country(today, 1, country)
            prev_date = get_business_day_by_country(anchor_date, 1, country)
            bday_country = country

        fetch_end = today.strftime("%Y-%m-%d")
        fetch_start = (anchor_date - timedelta(days=400)).strftime("%Y-%m-%d")

        with st.spinner("index_constituents 조회 중..."):
            df = _load_index_constituents(selected_index, fetch_start, fetch_end)

        if df.empty:
            st.warning("조회된 데이터가 없습니다.")
            return

        available_dates = sorted(df["dt"].dt.normalize().unique())
        if not available_dates:
            st.warning("조회된 날짜가 없습니다.")
            return
        min_avail = pd.Timestamp(available_dates[0]).date()
        max_avail = pd.Timestamp(available_dates[-1]).date()
        default_anchor = anchor_date if min_avail <= anchor_date <= max_avail else max_avail

        # 1) GICS 비중 (기준일) — 기준일 선택 가능
        st.subheader("1) GICS 비중 (기준일)")
        anchor_option = st.date_input(
            "기준일 선택",
            value=default_anchor,
            min_value=min_avail,
            max_value=max_avail,
            key="sector_anchor_date",
        )
        anchor_date = anchor_option
        prev_date = get_business_day_by_country(anchor_date, 1, bday_country)
        anchor_ts = pd.to_datetime(anchor_date).normalize()
        prev_ts = pd.to_datetime(prev_date).normalize()

        df_anchor = df[df["dt"].dt.normalize() == anchor_ts].copy()
        df_prev = df[df["dt"].dt.normalize() == prev_ts].copy()

        if df_anchor.empty:
            st.warning(f"기준일({anchor_date}) 데이터가 해당 Index에 없습니다. DB 적재 여부를 확인해 주세요.")
            return

        st.caption(f"**선택 Index**: `{selected_index}`  |  **기준일**: `{anchor_date}`  |  **전일**({bday_country} 영업일): `{prev_date}`")

        weights = _sector_weights(df_anchor)

        fig_w = go.Figure()
        fig_w.add_trace(
            go.Bar(
                x=weights["gics_name"],
                y=weights["weight_pct"],
                text=[f"{v:.2f}%" for v in weights["weight_pct"]],
                textposition="auto",
                textfont=dict(size=16),
            )
        )
        fig_w.update_layout(
            height=420,
            xaxis_title="GICS",
            yaxis_title="Weight (%)",
            margin=dict(l=24, r=24, t=36, b=120),
            font=dict(size=16),
            xaxis=dict(tickfont=dict(size=15), title_font=dict(size=17)),
            yaxis=dict(tickfont=dict(size=15), title_font=dict(size=17)),
        )
        st.plotly_chart(fig_w, use_container_width=True)

        # 2) 섹터 요약: 섹터명 / 비중합 / 종목개수, 섹터 선택 시 해당 섹터 종목(티커/종목명/비중/수익률)
        st.subheader("2) 섹터 요약")
        if df_prev.empty:
            st.info(f"전일(영업일 **{prev_date}**) 데이터가 조회된 구간에 없어 전일 대비 기여도를 계산할 수 없습니다. (기준일 데이터는 사용 중입니다.)")
        else:
            sector_summary = _sector_daily_contribution(df_prev, df_anchor)
            if sector_summary.empty:
                st.warning("전일 대비 기여도를 계산할 수 없습니다. (종목 매칭/가격 데이터 확인 필요)")
            else:
                display = sector_summary.copy()
                display["비중합(%)"] = display["weight_sum_pct"].round(2)
                display = display[["gics_name", "weight_sum_pct", "stock_count"]]
                display = display.rename(columns={"gics_name": "섹터명", "weight_sum_pct": "비중합(%)", "stock_count": "종목개수"})
                display["비중합(%)"] = display["비중합(%)"].round(2)
                display = display.sort_values("비중합(%)", ascending=False).reset_index(drop=True)
                styled_display = (
                    display.style
                    .format({"비중합(%)": "{:.2f}%"})
                    .set_table_styles([{"selector": "th, td", "props": [("font-size", "16px")]}])
                )
                st.dataframe(styled_display, use_container_width=True, hide_index=True)

                constituents_ret = _constituents_daily_returns(df_prev, df_anchor)
                if not constituents_ret.empty:
                    sector_options = ["— 선택 —"] + display["섹터명"].tolist()
                    st.markdown('<p style="font-size:18px; font-weight:600;">섹터를 선택하면 해당 섹터 종목이 아래에 표시됩니다.</p>', unsafe_allow_html=True)
                    selected_sector = st.selectbox("섹터 선택", sector_options, key="sector_select", label_visibility="collapsed")
                    if selected_sector and selected_sector != "— 선택 —":
                        sub = constituents_ret[constituents_ret["gics_name"] == selected_sector].copy()
                        sub = sub.sort_values("weight_pct", ascending=False)
                        sub_display = sub[["ticker", "name", "weight_pct", "ret_pct"]].copy()
                        sub_display = sub_display.rename(columns={"ticker": "티커", "name": "종목명", "weight_pct": "비중(%)", "ret_pct": "수익률(%)"})
                        sub_display["비중(%)"] = sub_display["비중(%)"].round(2)
                        sub_display["수익률(%)"] = sub_display["수익률(%)"].round(2)
                        def _color_ret(v):
                            if pd.isna(v): return ""
                            try:
                                f = float(v)
                                if f > 0: return "color: #2e7d32; font-weight: bold;"
                                if f < 0: return "color: #c62828; font-weight: bold;"
                            except (TypeError, ValueError): pass
                            return ""
                        styled_sub = (
                            sub_display.style
                            .format({"비중(%)": "{:.2f}", "수익률(%)": "{:.2f}"})
                            .applymap(_color_ret, subset=["수익률(%)"])
                            .set_table_styles([
                                {"selector": "th, td", "props": [("font-size", "17px")]},
                            ])
                        )
                        st.dataframe(styled_sub, use_container_width=True, hide_index=True)

        # 3) 섹터 기여 수익률 (전일 대비) — 비중에 따른 트리맵: 크기=비중, 색=기여 수익률(빨강~초록)
        st.subheader("3) 섹터 기여 수익률 (전일 대비)")
        st.caption("타일 크기 = 섹터 비중, 색상 = 기여 수익률(빨강: 음수, 초록: 양수)")
        if not df_prev.empty:
            _contrib = _sector_daily_contribution(df_prev, df_anchor)
            if not _contrib.empty:
                _contrib = _contrib[_contrib["weight_sum"] > 0].copy()
                if not _contrib.empty:
                    # 비중(값)으로 트리맵 영역, 기여도로 색상 — 루트(SECTOR) 없이 섹터만 타일로
                    weight_pct = (_contrib["weight_sum"] * 100).tolist()
                    labels = _contrib["gics_name"].astype(str).tolist()
                    parents = [""] * len(_contrib)
                    values = weight_pct
                    contrib_vals = _contrib["contribution"].round(2)
                    text_list = [f"{contrib_vals.iloc[i]:+.2f}%" for i in range(len(_contrib))]
                    # 0을 중간으로 두어 음수=빨강·양수=초록만 나오게 대칭 범위 사용
                    max_abs = float(contrib_vals.abs().max()) if len(contrib_vals) else 1.0
                    if max_abs < 0.01:
                        max_abs = 0.5
                    cmin, cmax = -max_abs, max_abs
                    # 기여도: 음수=빨강, 양수=초록. 타일 안 섹터·수익률 글자 크게, 배경 없음
                    fig_treemap = go.Figure(go.Treemap(
                        labels=labels,
                        parents=parents,
                        values=values,
                        text=text_list,
                        textinfo="label+text",
                        textposition="middle center",
                        textfont=dict(size=22, color="#212121"),
                        hovertemplate="%{label}<br>비중: %{value:.2f}%<br>기여도: %{customdata:.2f}%<extra></extra>",
                        customdata=contrib_vals.tolist(),
                        marker=dict(
                            colors=contrib_vals.tolist(),
                            colorscale=[[0, "#c62828"], [0.5, "#f5f5f5"], [1, "#2e7d32"]],
                            cmin=cmin,
                            cmax=cmax,
                            line=dict(width=1, color="#e0e0e0"),
                            colorbar=dict(
                                title=dict(text="기여도 (%)", font=dict(size=16)),
                                thickness=20,
                                len=0.55,
                                tickformat=".2f",
                                tickfont=dict(size=14),
                                outlinewidth=0,
                            ),
                        ),
                        pathbar=dict(visible=False),
                    ))
                    fig_treemap.update_layout(
                        height=560,
                        margin=dict(l=80, r=80, t=56, b=56),
                        paper_bgcolor="#ffffff",
                        plot_bgcolor="#fafafa",
                        font=dict(size=22, color="#212121"),
                        autosize=True,
                    )
                    st.plotly_chart(fig_treemap, use_container_width=True)
                else:
                    st.info("비중 데이터가 없어 트리맵을 그릴 수 없습니다.")
            else:
                st.warning("전일 대비 섹터 기여도를 계산할 수 없습니다.")
        else:
            st.warning("전일 데이터가 없습니다.")

        # 4) 섹터별 누적 수익률 — 주요 지수와 동일 양식: 기간 선택 → Top N 메트릭(기본 Top5) → 전체 테이블(expander) → 차트 + 기간별 수익률 표
        st.subheader("4) 섹터별 누적 수익률")
        ytd_start = datetime(anchor_date.year, 1, 1).date()
        col_start, col_end = st.columns(2)
        with col_start:
            chart_start = st.date_input("시작일", value=ytd_start, min_value=min_avail, max_value=max_avail, key="sector_chart_start")
        with col_end:
            chart_end = st.date_input("종료일", value=anchor_date, min_value=min_avail, max_value=max_avail, key="sector_chart_end")
        ts = _sector_contribution_timeseries(df, start_date=chart_start, end_date=chart_end)
        if ts.empty:
            st.warning("기간 내 섹터 누적 기여도 시계열을 만들 수 없습니다.")
        else:
            # 수익률이 0.03 형태면 3%로 표시
            plot_ts = ts.copy()
            if plot_ts["cumulative_contribution"].abs().max() < 1.5 and plot_ts["cumulative_contribution"].abs().max() > 0:
                plot_ts["cumulative_contribution"] = plot_ts["cumulative_contribution"] * 100.0

            final_returns = plot_ts.groupby("gics_name")["cumulative_contribution"].last().sort_values(ascending=False)
            final_returns = final_returns[final_returns.notna()]

            if not final_returns.empty:
                # 메트릭으로 볼 섹터 선택 (기본 Top5)
                all_sectors_for_metric = list(final_returns.index)
                default_top5 = list(final_returns.head(5).index)
                selected_for_metric = st.multiselect(
                    "메트릭으로 볼 섹터 선택 (기본: Top5)",
                    options=all_sectors_for_metric,
                    default=default_top5,
                    key="sector_metric_select",
                )
                metric_targets = selected_for_metric or default_top5

                st.markdown("**최종 누적 수익률**")
                metric_cols = st.columns(len(metric_targets))
                for idx, sector_name in enumerate(metric_targets):
                    return_val = final_returns.get(sector_name, None)
                    with metric_cols[idx]:
                        if return_val is None or pd.isna(return_val):
                            continue
                        delta_prefix = "+" if return_val >= 0 else ""
                        st.metric(
                            label=sector_name,
                            value=f"{return_val:.2f}%",
                            delta=None,
                        )
                        delta_color_hex = "#2e7d32" if return_val >= 0 else "#c62828"
                        arrow = "▲" if return_val >= 0 else "▼"
                        st.markdown(
                            f"<div style='margin-top:4px; font-size:13px; font-weight:700; color:{delta_color_hex};'>{arrow} {delta_prefix}{return_val:.2f}%</div>",
                            unsafe_allow_html=True,
                        )

                # 전체 섹터 수익률 테이블 (expander, 주요 지수와 동일)
                with st.expander("전체 섹터 누적 수익률 보기", expanded=False):
                    valid_returns = final_returns[final_returns.notna()]
                    if not valid_returns.empty:
                        returns_df = pd.DataFrame(
                            {
                                "섹터명": valid_returns.index,
                                "수익률(%)": [f"{val:.2f}%" if pd.notna(val) else "N/A" for val in valid_returns.values],
                            }
                        )
                        returns_df["순위"] = range(1, len(returns_df) + 1)
                        returns_df = returns_df[["순위", "섹터명", "수익률(%)"]]
                        st.dataframe(returns_df, use_container_width=True, hide_index=True)

                # 차트: 먼저 표시 (기본 Top5, 나머지 추가 가능)
                sectors = sorted(ts["gics_name"].unique().tolist())
                chart_default_top5 = [s for s in final_returns.head(5).index.tolist() if s in sectors]
                selected_sectors = st.multiselect(
                    "표시할 섹터 선택 (기본: Top5)",
                    options=sectors,
                    default=chart_default_top5,
                    key="sector_cumul_select",
                )
                plot_ts_sel = plot_ts[plot_ts["gics_name"].isin(selected_sectors)].copy() if selected_sectors else plot_ts.copy()

                distinct_colors = [
                    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
                    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
                ]
                color_map = {g: distinct_colors[i % len(distinct_colors)] for i, g in enumerate(plot_ts_sel["gics_name"].unique())}

                fig = go.Figure()
                for gics in plot_ts_sel["gics_name"].unique():
                    d = plot_ts_sel[plot_ts_sel["gics_name"] == gics].sort_values("dt")
                    r = final_returns.get(gics, None) if not final_returns.empty else None
                    fig.add_trace(
                        go.Scatter(
                            x=d["dt"],
                            y=d["cumulative_contribution"],
                            mode="lines",
                            name=gics,
                            line=dict(color=color_map.get(gics, "#888"), width=2),
                            hovertemplate=f"<b>{gics}</b><br>%{{x|%Y-%m-%d}}<br>누적 수익률: %{{y:.2f}}%<br>" + (f"최종: {r:.2f}%<extra></extra>" if r is not None and not pd.isna(r) else "<extra></extra>"),
                        )
                    )
                fig.update_layout(
                    title="",
                    height=500,
                    xaxis_title="날짜",
                    yaxis_title="누적 수익률 (%)",
                    yaxis_tickformat=".2f",
                    hovermode="x unified",
                    legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02, font=dict(size=12)),
                    margin=dict(l=20, r=20, t=20, b=20),
                    template="plotly_white",
                    xaxis=dict(showgrid=True, gridwidth=1, gridcolor="lightgray"),
                    yaxis=dict(showgrid=True, gridwidth=1, gridcolor="lightgray", zeroline=True, zerolinecolor="black", zerolinewidth=1),
                )
                st.plotly_chart(fig, use_container_width=True)

                # --- 섹터별 기간 수익률 표 (1D, 1W, MTD, 1M, 3M, 6M, YTD, 1Y) — 차트 아래 ---
                st.markdown("---")
                st.markdown("**섹터별 기간 수익률 비교 (기준: 종료일)**")

                def _get_period_bounds(base_date: datetime.date):
                    """기준일자 기준 각 기간의 (start, end) 반환"""
                    return {
                        "1D": (base_date - timedelta(days=1), base_date),
                        "1W": (base_date - timedelta(days=7), base_date),
                        "1M": (base_date - timedelta(days=30), base_date),
                        "3M": (base_date - timedelta(days=90), base_date),
                        "6M": (base_date - timedelta(days=180), base_date),
                        "1Y": (base_date - timedelta(days=365), base_date),
                        "MTD": (base_date.replace(day=1), base_date),
                        "YTD": (base_date.replace(month=1, day=1), base_date),
                    }

                period_bounds = _get_period_bounds(chart_end)

                def _calc_sector_period_return(sector_df: pd.DataFrame, start_bound: datetime.date, end_bound: datetime.date):
                    """섹터별 누적 기여도 시계열에서 특정 기간 수익률(%) 추정: 구간 내 누적 변화량."""
                    if sector_df.empty:
                        return None
                    df_tmp = sector_df.copy()
                    if not pd.api.types.is_datetime64_any_dtype(df_tmp["dt"]):
                        df_tmp["dt"] = pd.to_datetime(df_tmp["dt"])
                    df_tmp["dt_date"] = df_tmp["dt"].dt.date

                    # 종료 시점
                    end_candidates = df_tmp[df_tmp["dt_date"] <= end_bound]
                    if end_candidates.empty:
                        end_candidates = df_tmp[df_tmp["dt_date"] >= end_bound]
                        if end_candidates.empty:
                            return None
                        end_row = end_candidates.iloc[0]
                    else:
                        end_row = end_candidates.iloc[-1]

                    # 시작 직전 누적값 (start_bound 이전 가장 최근 값)
                    start_candidates = df_tmp[df_tmp["dt_date"] < start_bound]
                    if start_candidates.empty:
                        base_val = 0.0
                    else:
                        base_val = float(start_candidates.iloc[-1]["cumulative_contribution"])

                    end_val = float(end_row["cumulative_contribution"])
                    return end_val - base_val

                comparison_rows = []
                for sector_name in sorted(plot_ts["gics_name"].unique()):
                    sector_data = plot_ts[plot_ts["gics_name"] == sector_name].sort_values("dt")
                    if sector_data.empty:
                        continue
                    row = {"섹터명": sector_name}
                    for period_name, (start_bound, end_bound) in period_bounds.items():
                        val = _calc_sector_period_return(sector_data, start_bound, end_bound)
                        row[period_name] = val
                    comparison_rows.append(row)

                if comparison_rows:
                    comparison_df = pd.DataFrame(comparison_rows)
                    desired_cols = ["1D", "1W", "MTD", "1M", "3M", "6M", "YTD", "1Y"]
                    available_cols = [c for c in desired_cols if c in comparison_df.columns]
                    col_order = ["섹터명"] + available_cols

                    # 정렬 기준 선택 (내림차순, 기본 YTD)
                    sort_options = ["정렬 안함"] + available_cols
                    default_sort_idx = sort_options.index("YTD") if "YTD" in sort_options else 0
                    selected_sort = st.selectbox(
                        "정렬 기준 컬럼 선택 (내림차순)",
                        options=sort_options,
                        index=default_sort_idx,
                        key="sector_period_sort_select",
                    )

                    if selected_sort != "정렬 안함" and selected_sort in comparison_df.columns:
                        sort_vals = []
                        for idx in comparison_df.index:
                            v = comparison_df.loc[idx, selected_sort]
                            if v is None or pd.isna(v):
                                sort_vals.append(-999999)
                            else:
                                sort_vals.append(float(v))
                        comparison_df = comparison_df.copy()
                        comparison_df["_sort_temp"] = sort_vals
                        comparison_df = comparison_df.sort_values("_sort_temp", ascending=False, na_position="last").drop(columns="_sort_temp")

                    # 포맷팅
                    for c in available_cols:
                        comparison_df[c] = comparison_df[c].apply(
                            lambda x: f"{x:.2f}%" if (x is not None and pd.notna(x) and isinstance(x, (int, float))) else "N/A"
                        )
                    comparison_df = comparison_df[col_order]

                    # 색상 스타일 (주요 지수와 유사)
                    def _color_returns(val):
                        if val == "N/A":
                            return ""
                        try:
                            r = float(val.rstrip("%"))
                            if abs(r) < 1e-12:
                                return "background-color: #f8f9fa; color: #6b7280;"
                            # 연속 그라데이션: 상승(+) 초록, 하락(-) 빨강
                            strength = min(abs(r) / 8.0, 1.0)
                            alpha = 0.10 + (0.32 * strength)
                            if r > 0:
                                return f"background-color: rgba(46, 125, 50, {alpha:.3f}); color: #1b5e20; font-weight: 700"
                            return f"background-color: rgba(198, 40, 40, {alpha:.3f}); color: #7f1d1d; font-weight: 700"
                        except Exception:
                            return ""

                    styled_comp_df = comparison_df.style
                    for c in available_cols:
                        styled_comp_df = styled_comp_df.applymap(_color_returns, subset=[c])

                    st.markdown(
                        """
                    <style>
                    .dataframe {
                        font-size: 16px !important;
                    }
                    .dataframe th {
                        font-size: 18px !important;
                        font-weight: bold !important;
                        padding: 12px !important;
                        cursor: pointer;
                    }
                    .dataframe td {
                        font-size: 16px !important;
                        padding: 10px !important;
                    }
                    </style>
                    """,
                        unsafe_allow_html=True,
                    )
                    st.dataframe(styled_comp_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"오류 발생: {e}")
        import traceback
        with st.expander("상세 오류 정보"):
            st.code(traceback.format_exc())