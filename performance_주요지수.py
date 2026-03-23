"""
주요 지수 탭 - 지수별 누적 수익률 비교 및 지수별 수익률 비교
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from call import get_price_major_index_for_comparison, execute_custom_query
from utils import get_business_day, get_period_options, get_index_country_code


def render():
    """주요 지수 탭 렌더링"""
    # 기간 선택 옵션 및 라벨 가져오기
    period_options, _ = get_period_options()
    
    today = datetime.now().date()
    
    # 전역 기간 선택 (세션 상태 사용) - 기본은 YTD
    if 'selected_period' not in st.session_state:
        st.session_state.selected_period = 'YTD'
    
    # 기간 선택 UI (차트 위쪽에 배치)
    st.markdown("### 지수별 누적 수익률 비교")
    selected_period = st.radio(
        "",
        options=period_options,
        horizontal=True,
        index=period_options.index(st.session_state.selected_period) if st.session_state.selected_period in period_options else 0,
        label_visibility="collapsed",
        key="period_radio"
    )
    
    # 기준일자: 위(최종 수익률·차트)와 아래(지수별 수익률 비교 표)가 동일 수치가 되도록 단일 기준 사용
    if 'comparison_base_date' not in st.session_state:
        st.session_state.comparison_base_date = get_business_day(today, 1)
    comparison_base_date = st.date_input(
        "기준일자",
        value=st.session_state.comparison_base_date,
        max_value=today - timedelta(days=1),
        key="comparison_base_date_input"
    )
    st.session_state.comparison_base_date = comparison_base_date
    
    # 기간 선택이 변경되면 세션 업데이트
    if st.session_state.selected_period != selected_period:
        st.session_state.selected_period = selected_period
        st.rerun()
    
    # price_major_index DB ticker -> 표시명 (지수별 수익률 비교 표와 동일)
    _ticker_to_display = {
        'SPX Index': 'SPX-SPX', 'SPEHYDUP Index': 'SPHYDA-USA', 'SPHYD Index': 'SPHYDA-USA',
        'NDX Index': 'NDX-USA', 'SX5E Index': 'ESX-STX', 'HSCEI Index': 'HSCEI-HKX',
        'NIFTY Index': 'NSENIF-NSE', 'VN30 Index': 'VN30-STC', 'NKY Index': 'NIK-NKX', 'KOSPI Index': 'KOSPI-KRX',
    }
    _db_tickers = list(_ticker_to_display.keys())
    
    try:
        _end_str = comparison_base_date.strftime("%Y-%m-%d")
        _fetch_start = (comparison_base_date - timedelta(days=1200)).strftime("%Y-%m-%d")
        with st.spinner("지수별 수익률 데이터를 조회하는 중..."):
            _price_data = get_price_major_index_for_comparison(
                fetch_start_date=_fetch_start,
                end_date_str=_end_str,
                ticker_list=_db_tickers,
            )
        _comparison_df = pd.DataFrame(_price_data)
        
        if not _comparison_df.empty:
            _comparison_df['dt'] = pd.to_datetime(_comparison_df['dt'])
            _comparison_df['index_name'] = _comparison_df['index_name'].astype(str).str.strip()
            _comparison_df['display_name'] = _comparison_df['index_name'].map(_ticker_to_display)
            _comparison_df = _comparison_df[_comparison_df['display_name'].notna()].copy()
            _available = list(dict.fromkeys(_ticker_to_display[t] for t in _comparison_df['index_name'].unique() if t in _ticker_to_display))
        else:
            _available = []
        
        def _period_bounds(base_date):
            # YTD: 연말(전년 12/31) 종가 ~ 기준일로 통일 (1/1 데이터 유무와 무관하게 27.06% 등 동일 수치)
            ytd_start = base_date.replace(month=1, day=1) - timedelta(days=1)
            return {
                '1D': (base_date - timedelta(days=1), base_date),
                '1W': (base_date - timedelta(days=7), base_date),
                '1M': (base_date - timedelta(days=30), base_date),
                '3M': (base_date - timedelta(days=90), base_date),
                '6M': (base_date - timedelta(days=180), base_date),
                '1Y': (base_date - timedelta(days=365), base_date),
                'MTD': (base_date.replace(day=1), base_date),
                'YTD': (ytd_start, base_date),
            }
        
        def _calc_return(idx_data: pd.DataFrame, start_b: datetime.date, end_b: datetime.date):
            if idx_data.empty:
                return None
            try:
                idx_data = idx_data.copy()
                idx_data['dt'] = pd.to_datetime(idx_data['dt'])
                idx_data['dt_date'] = idx_data['dt'].dt.date
                start_c = idx_data[idx_data['dt_date'] <= start_b]
                start_c = start_c if not start_c.empty else idx_data[idx_data['dt_date'] >= start_b]
                if start_c.empty:
                    return None
                start_row = start_c.iloc[-1] if (idx_data['dt_date'] <= start_b).any() else start_c.iloc[0]
                end_c = idx_data[idx_data['dt_date'] <= end_b]
                end_c = end_c if not end_c.empty else idx_data[idx_data['dt_date'] >= end_b]
                if end_c.empty:
                    return None
                end_row = end_c.iloc[-1] if (idx_data['dt_date'] <= end_b).any() else end_c.iloc[0]
                sp, ep = float(start_row['price']), float(end_row['price'])
                if pd.isna(sp) or pd.isna(ep) or sp == 0:
                    return None
                return (ep - sp) / sp * 100
            except Exception:
                return None

        def _get_country_business_days(start_b: datetime.date, end_b: datetime.date, country_code: str):
            try:
                # business_day 테이블의 국가 컬럼명을 동적으로 탐색 (예: KR vs KRW)
                candidate_map = {
                    'US': ['US', 'USA'],
                    'HK': ['HK', 'HKG'],
                    'IN': ['IN', 'IND'],
                    'JP': ['JP', 'JPN'],
                    'VN': ['VN', 'VNM'],
                    'EU': ['EU', 'EUR', 'EMU'],
                    'KR': ['KR', 'KRW', 'KOR', 'KRX'],
                }
                candidates = candidate_map.get(country_code.upper(), [country_code.upper()])
                col_query = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'business_day'
                """
                col_rows = execute_custom_query(col_query)
                if not col_rows:
                    return None

                available_cols = {str(r.get('column_name', '')).upper() for r in col_rows if isinstance(r, dict)}
                target_col = next((c for c in candidates if c in available_cols), None)
                if target_col is None:
                    return None

                query = f"""
                    SELECT dt
                    FROM business_day
                    WHERE dt >= '{start_b.strftime('%Y-%m-%d')}'
                      AND dt <= '{end_b.strftime('%Y-%m-%d')}'
                      AND "{target_col}" = 1
                    ORDER BY dt
                """
                rows = execute_custom_query(query)
                if not rows:
                    return None
                dates = []
                for r in rows:
                    # execute_custom_query는 보통 dict 리스트를 반환
                    if isinstance(r, dict):
                        dt_val = r.get('dt')
                    else:
                        # 예외적으로 tuple/list 형태가 올 때도 방어
                        dt_val = r[0] if isinstance(r, (list, tuple)) and r else None
                    if dt_val is not None:
                        d = pd.to_datetime(dt_val).date()
                        # 주말은 항상 제외 (캘린더 데이터 이상값 방어)
                        if d.weekday() < 5:
                            dates.append(d)
                return set(dates)
            except Exception:
                return None
        
        _bounds = _period_bounds(comparison_base_date)
        _start_b, _end_b = _bounds.get(selected_period, (comparison_base_date - timedelta(days=30), comparison_base_date))
        
        # 최종 수익률 = 지수별 수익률 비교 표와 동일한 정의
        final_returns = pd.Series(dtype=float)
        for _dn in _available:
            _idx_data = _comparison_df[_comparison_df['display_name'] == _dn].sort_values('dt')
            _r = _calc_return(_idx_data, _start_b, _end_b)
            if _r is not None:
                final_returns[_dn] = _r
        final_returns = final_returns.sort_values(ascending=False)
        valid_returns = final_returns[final_returns.notna()]

        # 제목 색상은 항상 YTD 수익률 기준으로 판단
        ytd_returns = pd.Series(dtype=float)
        ytd_start_b, ytd_end_b = _bounds.get('YTD', (_start_b, _end_b))
        for _dn in _available:
            _idx_data = _comparison_df[_comparison_df['display_name'] == _dn].sort_values('dt')
            _ytd_r = _calc_return(_idx_data, ytd_start_b, ytd_end_b)
            if _ytd_r is not None:
                ytd_returns[_dn] = _ytd_r

        # Top 5 카드 하단 화살표는 "전일 대비" 변동률로 표시
        daily_changes = pd.Series(dtype=float)
        for _dn in _available:
            _idx_data = _comparison_df[_comparison_df['display_name'] == _dn].sort_values('dt').copy()
            if _idx_data.empty:
                continue
            _idx_data['dt_date'] = pd.to_datetime(_idx_data['dt'], errors='coerce').dt.date
            _idx_data = _idx_data[_idx_data['dt_date'].notna()]
            _idx_data = _idx_data[_idx_data['dt_date'] <= comparison_base_date]
            _idx_data['price'] = pd.to_numeric(_idx_data['price'], errors='coerce')
            _idx_data = _idx_data.dropna(subset=['price']).sort_values('dt_date')
            if len(_idx_data) < 2:
                continue
            _prev = float(_idx_data.iloc[-2]['price'])
            _curr = float(_idx_data.iloc[-1]['price'])
            if _prev > 0:
                daily_changes[_dn] = (_curr - _prev) / _prev * 100.0
        
        st.caption(f"**기간** ({selected_period}): {_start_b} ~ {_end_b} (기준일자: {comparison_base_date})")
        
        if not final_returns.empty:
            # 최종 수익률을 메트릭 카드로 상단에 표시 (상위 5개, 지수별 수익률 비교 표와 동일 수치)
            st.subheader("최종 수익률 Top 5")
            top5_cols = st.columns(5)
            for idx, (index_name, return_val) in enumerate(final_returns.head(5).items()):
                with top5_cols[idx]:
                    if return_val is None or pd.isna(return_val):
                        continue
                    daily_val = daily_changes.get(index_name, pd.NA)
                    
                    st.metric(
                        label=index_name.replace(" Index", ""),
                        value=f"{return_val:.2f}%",
                        delta=None,
                    )
                    if pd.notna(daily_val):
                        delta_prefix = "+" if float(daily_val) >= 0 else ""
                        delta_color_hex = "#2e7d32" if float(daily_val) >= 0 else "#c62828"
                        arrow = "▲" if float(daily_val) >= 0 else "▼"
                        st.markdown(
                            f"<div style='margin-top:6px; font-size:22px; font-weight:800; line-height:1.15; color:{delta_color_hex};'>{arrow} {delta_prefix}{float(daily_val):.2f}%</div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            "<div style='margin-top:6px; font-size:20px; font-weight:700; line-height:1.15; color:#6b7280;'>전일 대비 -</div>",
                            unsafe_allow_html=True,
                        )
            
            # Plotly 차트: 기준일자까지 동일 가격 데이터로 누적 수익률 (표와 수치 일치)
            if not final_returns.empty and not _comparison_df.empty:
                valid_final_returns = final_returns[final_returns.notna()]
                if not valid_final_returns.empty:
                    _comparison_df['dt_date'] = _comparison_df['dt'].dt.date
                    distinct_colors = [
                        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
                    ]
                    additional_colors = ['#ff9896', '#c5b0d5', '#c49c94', '#f7b6d3', '#dbdb8d']
                    all_colors = distinct_colors + additional_colors
                    color_map = {name: all_colors[i % len(all_colors)] for i, name in enumerate(valid_final_returns.index)}
                    fig = go.Figure()
                    
                    for index_name in valid_final_returns.index:
                        idx_data = _comparison_df[_comparison_df['display_name'] == index_name].sort_values('dt').copy()
                        if idx_data.empty:
                            continue
                        start_c = idx_data[idx_data['dt_date'] <= _start_b]
                        if start_c.empty:
                            start_c = idx_data[idx_data['dt_date'] >= _start_b]
                        if start_c.empty:
                            continue
                        base_price = float(start_c.iloc[-1]['price']) if (idx_data['dt_date'] <= _start_b).any() else float(start_c.iloc[0]['price'])
                        window = idx_data[(idx_data['dt_date'] >= _start_b) & (idx_data['dt_date'] <= _end_b)]
                        if window.empty:
                            continue
                        window = window.copy()
                        window['cumulative_return'] = (window['price'].astype(float) - base_price) / base_price * 100
                        return_val = valid_final_returns[index_name]
                        line_width = 3.0 if abs(return_val) > 2 else 2.0
                        line_dash = 'dash' if return_val < 0 else 'solid'
                        fig.add_trace(go.Scatter(
                            x=window['dt'],
                            y=window['cumulative_return'],
                            mode='lines',
                            name=index_name.replace(" Index", ""),
                            line=dict(color=color_map[index_name], width=line_width, dash=line_dash),
                            hovertemplate=f'<b>{index_name.replace(" Index", "")}</b><br>날짜: %{{x}}<br>수익률: %{{y:.2f}}%<br>최종: {return_val:.2f}%<extra></extra>'
                        ))
                    
                    fig.update_layout(
                        title="",
                        xaxis_title="날짜",
                        yaxis_title="수익률 (%)",
                        hovermode='x unified',
                        legend=dict(
                            orientation="v",
                            yanchor="top",
                            y=1,
                            xanchor="left",
                            x=1.02,
                            font=dict(size=20)
                        ),
                        height=600,
                        template='plotly_white',
                        xaxis=dict(
                            showgrid=True,
                            gridwidth=1,
                            gridcolor='lightgray',
                            title_font=dict(size=24),
                            tickfont=dict(size=20)
                        ),
                        yaxis=dict(
                            showgrid=True,
                            gridwidth=1,
                            gridcolor='lightgray',
                            zeroline=True,
                            zerolinecolor='black',
                            zerolinewidth=1,
                            title_font=dict(size=24),
                            tickfont=dict(size=20)
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)

            # 전체 수익률을 정렬된 테이블로 표시 (차트 아래)
            with st.expander("전체 지수 수익률 보기", expanded=False):
                if not valid_returns.empty:
                    returns_df = pd.DataFrame({
                        '지수명': valid_returns.index,
                        '수익률(%)': [f"{val:.2f}%" if pd.notna(val) else "N/A" for val in valid_returns.values]
                    })
                    returns_df['순위'] = range(1, len(returns_df) + 1)
                    returns_df = returns_df[['순위', '지수명', '수익률(%)']]
                    
                    def color_returns(val):
                        try:
                            return_val = float(val.rstrip('%'))
                            if abs(return_val) < 1e-12:
                                return "background-color: #f8f9fa; color: #6b7280;"
                            # 연속 그라데이션: 상승(+) 초록, 하락(-) 빨강
                            strength = min(abs(return_val) / 8.0, 1.0)
                            alpha = 0.10 + (0.32 * strength)
                            if return_val > 0:
                                return f"background-color: rgba(46, 125, 50, {alpha:.3f}); color: #1b5e20; font-weight: 700"
                            return f"background-color: rgba(198, 40, 40, {alpha:.3f}); color: #7f1d1d; font-weight: 700"
                        except:
                            return ''
                    
                    styled_df = returns_df.style.applymap(color_returns, subset=['수익률(%)'])
                    st.markdown("""
                    <style>
                    .dataframe {
                        font-size: 16px !important;
                    }
                    .dataframe th {
                        font-size: 18px !important;
                        font-weight: bold !important;
                        padding: 12px !important;
                    }
                    .dataframe td {
                        font-size: 16px !important;
                        padding: 10px !important;
                    }
                    </style>
                    """, unsafe_allow_html=True)
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)

        else:
            st.warning("표시할 수익률 데이터가 없습니다. 기준일자를 확인해주세요.")
        
        # 지수간 비교 테이블 (위와 동일한 기준일자·가격 데이터 사용)
        st.markdown("---")
        st.subheader("지수별 수익률 비교")
        st.caption("YTD = 해당 연도 1월 1일 **이전 최종 거래일**(연말 종가) ~ 기준일.")
        
        comparison_indices_df = _comparison_df
        available_indices = _available
        
        selected_indices_for_comparison = st.multiselect(
            "비교할 지수 선택",
            options=available_indices,
            default=available_indices,
            format_func=lambda x: x.replace(" Index", "")
        )
        
        if selected_indices_for_comparison:
                period_bounds = _period_bounds(comparison_base_date)
                
                if comparison_indices_df.empty:
                    st.warning("지수별 수익률 비교를 위한 데이터를 가져올 수 없습니다.")
                else:
                    comparison_data = []
                    for display_name in selected_indices_for_comparison:
                        index_data = comparison_indices_df[
                            comparison_indices_df['display_name'] == display_name
                        ].sort_values('dt').copy()
                        
                        if not index_data.empty:
                            row_data = {
                                '지수명': display_name.replace(" Index", "") if " Index" in str(display_name) else display_name
                            }
                            
                            for period_name, (start_bound, end_bound) in period_bounds.items():
                                return_val = _calc_return(index_data, start_bound, end_bound)
                                row_data[period_name] = return_val
                            
                            comparison_data.append(row_data)
                    
                    if not comparison_data:
                        st.warning("선택한 지수에 대한 데이터를 찾을 수 없습니다.")
                    
                    if comparison_data:
                        comparison_df = pd.DataFrame(comparison_data)
                        comparison_df_raw = comparison_df.copy()
                        
                        # 원하는 컬럼 순서 정의: 1D -> 1W -> MTD -> 1M -> 3M -> 6M -> YTD -> 1Y
                        desired_column_order = ['1D', '1W', 'MTD', '1M', '3M', '6M', 'YTD', '1Y']
                        # period_bounds에 있는 컬럼만 사용
                        available_columns = [col for col in desired_column_order if col in comparison_df.columns]
                        column_order = ['지수명'] + available_columns
                        
                        # 정렬 옵션 설정 (기본 YTD 내림차순)
                        available_sort_columns = [col for col in desired_column_order if col in comparison_df.columns]
                        if 'comparison_sort_column' not in st.session_state:
                            st.session_state.comparison_sort_column = 'YTD' if 'YTD' in available_sort_columns else '정렬 안함'
                        
                        sort_options = ['정렬 안함'] + available_sort_columns
                        
                        # 현재 선택된 정렬 기준의 인덱스 찾기
                        current_index = 0
                        if st.session_state.comparison_sort_column in sort_options:
                            current_index = sort_options.index(st.session_state.comparison_sort_column)
                        
                        selected_sort = st.selectbox(
                            "정렬 기준 컬럼 선택 (내림차순)",
                            options=sort_options,
                            index=current_index,
                            key="comparison_sort_select"
                        )
                        
                        # 선택된 정렬 기준 저장
                        st.session_state.comparison_sort_column = selected_sort
                        
                        # 정렬 수행 (문자열 포맷팅 전에 숫자 값으로 정렬)
                        if selected_sort != '정렬 안함' and selected_sort in comparison_df.columns:
                            # 정렬용 임시 컬럼 생성 (숫자 값으로 변환)
                            sort_values = []
                            for idx in comparison_df.index:
                                val = comparison_df.loc[idx, selected_sort]
                                if val is None or pd.isna(val):
                                    sort_values.append(-999999)
                                elif isinstance(val, (int, float)):
                                    sort_values.append(float(val))
                                else:
                                    # 이미 문자열인 경우
                                    try:
                                        sort_values.append(float(str(val).rstrip('%')))
                                    except:
                                        sort_values.append(-999999)
                            
                            # 정렬용 컬럼 추가
                            comparison_df = comparison_df.copy()
                            comparison_df['_sort_temp'] = sort_values
                            
                            # 내림차순 정렬 (큰 값부터 작은 값 순서로)
                            comparison_df = comparison_df.sort_values('_sort_temp', ascending=False, na_position='last').reset_index(drop=True)
                            
                            # 정렬용 임시 컬럼 제거
                            comparison_df = comparison_df.drop('_sort_temp', axis=1)
                        
                        # 정렬 후에 문자열로 포맷팅
                        for period_name in available_columns:
                            if period_name in comparison_df.columns:
                                comparison_df[period_name] = comparison_df[period_name].apply(
                                    lambda x: f"{x:.2f}%" if (x is not None and pd.notna(x) and isinstance(x, (int, float))) else "N/A"
                                )
                        
                        # 최종 컬럼 순서 적용 (정렬된 행 순서는 유지)
                        comparison_df = comparison_df[column_order]
                        
                        def color_comparison_returns(val):
                            if val == "N/A":
                                return ''
                            try:
                                return_val = float(val.rstrip('%'))
                                if abs(return_val) < 1e-12:
                                    return "background-color: #f8f9fa; color: #6b7280;"
                                # 연속 그라데이션: 상승(+) 초록, 하락(-) 빨강
                                strength = min(abs(return_val) / 8.0, 1.0)
                                alpha = 0.10 + (0.32 * strength)
                                if return_val > 0:
                                    return f"background-color: rgba(46, 125, 50, {alpha:.3f}); color: #1b5e20; font-weight: 700"
                                return f"background-color: rgba(198, 40, 40, {alpha:.3f}); color: #7f1d1d; font-weight: 700"
                            except:
                                return ''
                        
                        styled_comparison_df = comparison_df.style
                        for period_name in available_columns:
                            if period_name in comparison_df.columns:
                                styled_comparison_df = styled_comparison_df.applymap(
                                    color_comparison_returns,
                                    subset=[period_name]
                                )
                        
                        st.markdown("""
                        <style>
                        .dataframe {
                            font-size: 32px !important;
                        }
                        .dataframe th {
                            font-size: 36px !important;
                            font-weight: bold !important;
                            padding: 24px !important;
                            cursor: pointer;
                        }
                        .dataframe td {
                            font-size: 32px !important;
                            padding: 20px !important;
                        }
                        </style>
                        """, unsafe_allow_html=True)
                        st.dataframe(styled_comparison_df, use_container_width=True, hide_index=True)

                        # 표 하단 Top/Bottom 요약 (원본 숫자 기준)
                        summary_period_order = ["1D", "1W", "1M", "3M", "YTD"]
                        summary_rows = []
                        for period_key in summary_period_order:
                            if period_key in comparison_df_raw.columns:
                                period_vals = pd.to_numeric(comparison_df_raw[period_key], errors="coerce")
                                valid_mask = period_vals.notna()
                                if valid_mask.any():
                                    tmp = comparison_df_raw.loc[valid_mask, ["지수명"]].copy()
                                    tmp["val"] = period_vals[valid_mask].astype(float).values
                                    top_row = tmp.loc[tmp["val"].idxmax()]
                                    bot_row = tmp.loc[tmp["val"].idxmin()]
                                    summary_rows.append({
                                        "기간": period_key,
                                        "Top 지수": str(top_row["지수명"]),
                                        "Top(%)": float(top_row["val"]),
                                        "Bottom 지수": str(bot_row["지수명"]),
                                        "Bottom(%)": float(bot_row["val"]),
                                    })
                        if summary_rows:
                            summary_df = pd.DataFrame(summary_rows)
                            summary_df["기간"] = pd.Categorical(
                                summary_df["기간"],
                                categories=summary_period_order,
                                ordered=True
                            )
                            summary_df = summary_df.sort_values("기간")

                            def _color_pct(val):
                                if isinstance(val, str):
                                    try:
                                        val = float(val.replace("%", ""))
                                    except Exception:
                                        return ""
                                if val > 0:
                                    return "color:#2e7d32; font-weight:700"
                                if val < 0:
                                    return "color:#c62828; font-weight:700"
                                return "color:#616161; font-weight:600"

                            display_df = summary_df.copy()
                            display_df["Top(%)"] = display_df["Top(%)"].map(lambda x: f"{x:+.2f}%")
                            display_df["Bottom(%)"] = display_df["Bottom(%)"].map(lambda x: f"{x:+.2f}%")
                            styled_summary_df = (
                                display_df.style
                                .applymap(_color_pct, subset=["Top(%)", "Bottom(%)"])
                                .set_properties(subset=["기간"], **{"font-weight": "700", "color": "#444"})
                            )
                            st.dataframe(styled_summary_df, use_container_width=True, hide_index=True)
        else:
            st.info("비교할 지수를 선택해주세요.")

        # 지수 일별 수익률 (지수별 수익률 비교 하단 배치)
        st.markdown("---")
        st.subheader("지수 일별 수익률 (영업일 기준)")
        if not valid_returns.empty:
            selected_daily_index = st.selectbox(
                "지수 선택",
                options=list(valid_returns.index),
                index=0,
                format_func=lambda x: x.replace(" Index", ""),
                key="selected_daily_index"
            )

            selected_index_data = _comparison_df[
                _comparison_df['display_name'] == selected_daily_index
            ].sort_values('dt').copy()

            if selected_index_data.empty:
                st.info("선택한 지수의 가격 데이터가 없습니다.")
            else:
                selected_index_data['dt_date'] = selected_index_data['dt'].dt.date
                daily_window = selected_index_data[
                    (selected_index_data['dt_date'] >= _start_b) &
                    (selected_index_data['dt_date'] <= _end_b)
                ].copy()

                # 1차: 주말은 항상 제거
                daily_window = daily_window[daily_window['dt_date'].apply(lambda d: d.weekday() < 5)].copy()

                # 2차: 국가별 영업일 캘린더 기준으로 비영업일 제거
                db_index_name = str(selected_index_data['index_name'].iloc[0]).strip()
                country_code = get_index_country_code(db_index_name)
                business_days = _get_country_business_days(_start_b, _end_b, country_code)
                if business_days is not None:
                    daily_window = daily_window[daily_window['dt_date'].isin(business_days)].copy()

                if daily_window.empty:
                    st.info("선택한 기간의 데이터가 없습니다.")
                else:
                    daily_window['price'] = daily_window['price'].astype(float)
                    daily_window['daily_return_pct'] = daily_window['price'].pct_change() * 100
                    daily_returns_df = daily_window[['dt', 'price', 'daily_return_pct']].dropna(
                        subset=['daily_return_pct']
                    ).copy()

                    if daily_returns_df.empty:
                        st.info("일별 수익률 계산을 위한 데이터가 부족합니다.")
                    else:
                        st.caption(
                            f"{selected_daily_index.replace(' Index', '')} | "
                            f"{selected_period} ({_start_b} ~ {_end_b})"
                        )

                        # 0.00% 수익률은 시각적 노이즈가 커서 차트에서 제외
                        chart_df = daily_returns_df[
                            daily_returns_df['daily_return_pct'].abs() > 1e-12
                        ].copy().reset_index(drop=True)
                        if chart_df.empty:
                            st.info("선택 기간 내 0%를 제외하면 표시할 일별 수익률이 없습니다.")
                        else:
                            chart_df['trade_day'] = pd.to_datetime(chart_df['dt']).dt.strftime('%Y-%m-%d')

                            fig_daily = go.Figure()
                            fig_daily.add_trace(go.Bar(
                                x=chart_df['trade_day'],
                                y=chart_df['daily_return_pct'],
                                name='일별 수익률(%)',
                                marker_color=[
                                    '#2ca02c' if x >= 0 else '#d62728'
                                    for x in chart_df['daily_return_pct']
                                ],
                                customdata=chart_df['trade_day'],
                                hovertemplate='날짜: %{customdata}<br>일별 수익률: %{y:.2f}%<extra></extra>'
                            ))
                            tick_step = max(1, len(chart_df) // 10)
                            tickvals = chart_df['trade_day'][::tick_step]
                            ticktext = chart_df['trade_day'][::tick_step]
                            fig_daily.update_layout(
                                height=320,
                                template='plotly_white',
                                margin=dict(l=10, r=10, t=10, b=10),
                                xaxis_title="날짜",
                                yaxis_title="일별 수익률 (%)",
                                bargap=0.05,
                                xaxis=dict(
                                    type='category',
                                    categoryorder='array',
                                    categoryarray=chart_df['trade_day'].tolist(),
                                    tickmode='array',
                                    tickvals=tickvals,
                                    ticktext=ticktext
                                ),
                                showlegend=False
                            )
                            st.plotly_chart(fig_daily, use_container_width=True)

                        view_df = daily_returns_df.copy()
                        view_df['날짜'] = pd.to_datetime(view_df['dt']).dt.strftime('%Y-%m-%d')
                        view_df['종가'] = view_df['price'].map(lambda x: f"{x:,.2f}")
                        view_df['일별 수익률(%)'] = view_df['daily_return_pct'].map(lambda x: f"{x:.2f}%")
                        view_df = view_df[['날짜', '종가', '일별 수익률(%)']].sort_values('날짜', ascending=False)

                        def color_daily_return(val):
                            try:
                                r = float(str(val).replace('%', '').strip())
                                if r > 0:
                                    return 'color: #2e7d32; font-weight: 600'
                                if r < 0:
                                    return 'color: #c62828; font-weight: 600'
                                return 'color: #616161'
                            except Exception:
                                return ''

                        styled_view_df = view_df.style.applymap(
                            color_daily_return,
                            subset=['일별 수익률(%)']
                        )
                        st.dataframe(styled_view_df, use_container_width=True, hide_index=True)

                        ma_windows_default = [20, 60, 120, 200]
                        ma_window_options = [5, 10, 20, 50, 60, 120, 150, 200]
                        # MA 색상 고정 (선/라벨 공통 사용)
                        ma_color_map = {
                            5: "#17becf",
                            10: "#2ca02c",
                            20: "#ff7f0e",
                            50: "#9467bd",
                            60: "#e377c2",
                            120: "#1f77b4",
                            150: "#8c564b",
                            200: "#d62728",
                        }
                        # 지수별 MA 기본값 오버라이드 (코드에서 고정하고 싶으면 여기 사용)
                        ma_windows_by_index = {}
                        st.subheader("지수 이동평균선(MA)")
                        st.caption("수익률은 YTD 기준입니다.")
                        all_dates = pd.to_datetime(_comparison_df['dt'], errors='coerce').dropna()
                        if all_dates.empty:
                            st.info("전체 지수 차트를 표시할 데이터가 없습니다.")
                        else:
                            min_date = all_dates.min().date()
                            max_date = all_dates.max().date()
                            default_start = max(min_date, max_date - timedelta(days=365))
                            range_start, range_end = st.slider(
                                "차트 기간",
                                min_value=min_date,
                                max_value=max_date,
                                value=(default_start, max_date),
                                format="YYYY-MM-DD",
                                label_visibility="collapsed",
                                key="all_index_ma_date_range"
                            )

                            # 상위 9개 지수를 3x3으로 고정 표시
                            grid_indices = list(valid_returns.index)[:9]
                            if not grid_indices:
                                st.info("표시할 지수 데이터가 없습니다.")
                            else:
                                # 지수별 MA 설정 (필요 시 개별 조정)
                                with st.expander("지수별 MA 설정", expanded=False):
                                    for idx_name in grid_indices:
                                        safe_key = "".join(ch if ch.isalnum() else "_" for ch in idx_name)
                                        default_windows = sorted(ma_windows_by_index.get(idx_name, ma_windows_default))
                                        selected_windows = st.multiselect(
                                            idx_name,
                                            options=ma_window_options,
                                            default=default_windows,
                                            key=f"ma_windows_{safe_key}"
                                        )
                                        ma_windows_by_index[idx_name] = (
                                            sorted(set(selected_windows)) if selected_windows else ma_windows_default
                                        )

                                for i in range(0, len(grid_indices), 3):
                                    cols = st.columns(3)
                                    for j, idx_name in enumerate(grid_indices[i:i + 3]):
                                        with cols[j]:
                                            mini_df = _comparison_df[
                                                _comparison_df['display_name'] == idx_name
                                            ].sort_values('dt').copy()
                                            if mini_df.empty:
                                                st.info(f"{idx_name}: 데이터 없음")
                                                continue

                                            mini_df['dt'] = pd.to_datetime(mini_df['dt'], errors='coerce')
                                            mini_df = mini_df.dropna(subset=['dt'])
                                            mini_df = mini_df[
                                                (mini_df['dt'].dt.date >= range_start) &
                                                (mini_df['dt'].dt.date <= range_end)
                                            ].copy()
                                            mini_df['dt_date'] = mini_df['dt'].dt.date
                                            # 1차: 주말 제거
                                            mini_df = mini_df[mini_df['dt_date'].apply(lambda d: d.weekday() < 5)].copy()
                                            # 2차: 국가별 영업일 캘린더 적용
                                            db_index_name = str(mini_df['index_name'].iloc[0]).strip() if 'index_name' in mini_df.columns and not mini_df.empty else ""
                                            if db_index_name:
                                                country_code = get_index_country_code(db_index_name)
                                                business_days = _get_country_business_days(range_start, range_end, country_code)
                                                if business_days is not None:
                                                    mini_df = mini_df[mini_df['dt_date'].isin(business_days)].copy()
                                            if mini_df.empty:
                                                st.info(f"{idx_name}: 기간 데이터 없음")
                                                continue

                                            mini_df['price'] = pd.to_numeric(mini_df['price'], errors='coerce')
                                            mini_df = mini_df.dropna(subset=['price'])
                                            if mini_df.empty:
                                                st.info(f"{idx_name}: 가격 데이터 없음")
                                                continue

                                            index_ma_windows = ma_windows_by_index.get(idx_name, ma_windows_default)
                                            for w in index_ma_windows:
                                                mini_df[f"MA_{w}"] = mini_df['price'].rolling(
                                                    window=w, min_periods=w
                                                ).mean()

                                            ret_val = valid_returns.get(idx_name, None)
                                            ytd_ret_val = ytd_returns.get(idx_name, None)
                                            latest_price = float(mini_df["price"].iloc[-1])
                                            title_text = idx_name.replace(" Index", "")
                                            if ret_val is not None and pd.notna(ret_val):
                                                title_text += f" ({latest_price:,.0f}pt, {ret_val:+.2f}%)"
                                            else:
                                                title_text += f" ({latest_price:,.0f}pt)"
                                            if ytd_ret_val is not None and pd.notna(ytd_ret_val):
                                                title_color = "#2e7d32" if ytd_ret_val >= 0 else "#c62828"
                                            else:
                                                title_color = "#1f2a44"

                                            mini_fig = go.Figure()
                                            mini_fig.add_trace(go.Scatter(
                                                x=mini_df['dt'],
                                                y=mini_df['price'],
                                                mode='lines',
                                                name='Close',
                                                line=dict(color='black', width=1.8),
                                                showlegend=True,
                                                hovertemplate='날짜: %{x}<br>종가: %{y:,.2f}<extra></extra>'
                                            ))
                                            for w in index_ma_windows:
                                                col = f"MA_{w}"
                                                if mini_df[col].notna().any():
                                                    mini_fig.add_trace(go.Scatter(
                                                        x=mini_df['dt'],
                                                        y=mini_df[col],
                                                        mode='lines',
                                                        name=col,
                                                        line=dict(
                                                            color=ma_color_map.get(w, "#2ca02c"),
                                                            width=1.2
                                                        ),
                                                        opacity=0.9,
                                                        showlegend=True
                                                    ))

                                            ma_label = (
                                                f"<span style='color:#666'>"
                                                f"MA({','.join(str(w) for w in index_ma_windows)})"
                                                f"</span>"
                                            )
                                            ma_value_parts = []
                                            for w in index_ma_windows:
                                                col = f"MA_{w}"
                                                if col in mini_df.columns and mini_df[col].notna().any():
                                                    latest_val = float(mini_df[col].dropna().iloc[-1])
                                                    color = ma_color_map.get(w, "#2ca02c")
                                                    ma_value_parts.append(
                                                        f"<span style='color:{color}'>MA{w}: {latest_val:,.0f}</span>"
                                                    )
                                            if ma_value_parts:
                                                ma_label += " &nbsp;&nbsp; " + " &nbsp;&nbsp; ".join(ma_value_parts)

                                            mini_fig.update_layout(
                                                title=dict(
                                                    text=title_text,
                                                    font=dict(color=title_color, size=16)
                                                ),
                                                height=250,
                                                template='plotly_white',
                                                margin=dict(l=8, r=8, t=36, b=8),
                                                xaxis_title="",
                                                yaxis_title="",
                                                hovermode='x unified',
                                                showlegend=True,
                                                legend=dict(
                                                    orientation="h",
                                                    yanchor="top",
                                                    y=0.98,
                                                    xanchor="left",
                                                    x=0.01,
                                                    bgcolor="rgba(255,255,255,0.65)",
                                                    font=dict(size=9),
                                                    itemclick="toggle",
                                                    itemdoubleclick="toggleothers"
                                                )
                                            )
                                            mini_fig.add_annotation(
                                                xref="paper",
                                                yref="paper",
                                                x=0.5,
                                                y=1.12,
                                                text=ma_label,
                                                showarrow=False,
                                                font=dict(size=10, color="#444"),
                                                align="center",
                                                xanchor="center",
                                            )
                                            st.plotly_chart(mini_fig, use_container_width=True)
        else:
            st.info("일별 수익률을 표시할 지수가 없습니다.")
    except Exception as e:
        st.error(f"오류 발생: {e}")
        import traceback
        with st.expander("상세 오류 정보"):
            st.code(traceback.format_exc())