"""
Strategy 성과 추적 모듈
BM(Benchmark)의 수익률과 종목을 추적하는 기능
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from call import get_index_constituents_data, get_bm_gics_sector_weights, get_bm_stock_weights, get_daily_sector_contributions, execute_custom_query, with_connection, calculate_strategy_portfolio_returns
from verification import render_verification
from utils import get_business_day, get_business_day_by_country, get_index_country_code, get_period_dates_from_base_date
from typing import Optional
from datetime import date
from psycopg2.extensions import connection as Connection


def render():
    """Strategy 성과 추적 페이지 렌더링"""
    st.header("Strategy 모니터링")
    
    # 사용 가능한 지수 목록 가져오기
    try:
        with st.spinner("지수 목록을 불러오는 중..."):
            # index_constituents 테이블에서 고유한 지수명 가져오기
            # 최근 데이터만 샘플링하여 지수 목록 가져오기
            from datetime import datetime, timedelta
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
            df_sample = get_index_constituents_data(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d")
            )
            if not df_sample.empty and 'index_name' in df_sample.columns:
                available_indices = sorted(df_sample['index_name'].unique().tolist())
            else:
                available_indices = []
    except Exception as e:
        st.error(f"지수 목록을 불러오는 중 오류가 발생했습니다: {str(e)}")
        available_indices = []
    
    if not available_indices:
        st.warning("사용 가능한 지수가 없습니다.")
        return
    
    # 지수 선택 및 기준일자 선택
    col1, col2 = st.columns([2, 1])
    with col1:
        selected_index = st.selectbox(
            "BM(Benchmark) 선택",
            options=available_indices,
            index=0 if available_indices else None,
            key="strategy_bm_index"
        )
    
    with col2:
        # 기준일자 선택
        today = datetime.now().date()
        base_date = st.date_input(
            "기준일자",
            value=get_business_day(today, -1),
            key="strategy_base_date"
        )
    
    # 기준일자부터 최근까지의 데이터 조회
    # 기준일자 이전 충분한 기간부터 데이터를 조회 (calculate_bm_returns에서 기준일자 이하의 가장 가까운 날짜를 찾음)
    # 기준일자가 미래 날짜일 경우를 대비하여 기준일자 이전 90일부터 조회
    data_start_date = get_business_day(base_date, 90)
    
    # 데이터 조회
    if selected_index:
        with st.spinner(f"{selected_index} 데이터를 불러오는 중..."):
            try:
                # 기준일자 이전부터 최근까지의 데이터 조회 (end_date는 None으로 설정하여 최근까지 가져옴)
                df = get_index_constituents_data(
                    index_name=selected_index,
                    start_date=data_start_date.strftime("%Y-%m-%d"),
                    end_date=None
                )
                
                if df.empty:
                    st.warning(f"{selected_index}에 대한 데이터를 찾을 수 없습니다.")
                    return
                
                # 기준일자 로직:
                # - 기준일자가 오늘자-1영업일인 경우: 시작일 = 기준일자-1영업일, 종료일 = 기준일자
                #   예: 기준일자 12/10이면 12/09 종가와 12/10 종가 비교 (12/09~12/10 차트)
                # - 기준일자가 과거 날짜인 경우: 
                #   - 계산 시작일 = 기준일자의 1영업일 전 (주가 조회용, 표에는 표시 안 함)
                #   - 표시 시작일 = 기준일자 (표에 표시할 시작일)
                #   - 종료일 = 오늘자-1영업일
                #   예: 기준일자 12/01이면 12/01~12/10의 일별 수익률과 누적 수익률 차트 표시
                #       (하지만 누적 수익률 계산은 12/01의 1영업일 전 주가를 기준으로 함)
                
                # 지수명에서 국가 코드 추출
                country_code = get_index_country_code(selected_index)
                
                today = datetime.now().date()
                # 오늘자 - 1영업일 (국내 기준, 표시용)
                latest_available_date = get_business_day(today, 1)
                
                if base_date == latest_available_date:
                    # 기준일자가 오늘자-1영업일인 경우: 해당 국가 기준 1영업일 전과 기준일자 비교
                    calculation_start_date = get_business_day_by_country(base_date, 1, country_code)  # 해당 국가 기준 1영업일 전 (예: 12/09) - 계산 및 표시 시작일
                    display_start_date = calculation_start_date  # 표시 시작일도 동일
                    initial_end_date = base_date  # 기준일자 (예: 12/10)
                else:
                    # 기준일자가 과거 날짜인 경우
                    # 해당 국가 기준으로 기준일자의 1영업일 전 계산 (주가 조회용, 표에는 표시 안 함)
                    calculation_start_date = get_business_day_by_country(base_date, 1, country_code)
                    display_start_date = base_date  # 기준일자 (표에 표시할 시작일, 예: 12/01)
                    initial_end_date = latest_available_date  # 오늘자 - 1영업일 (예: 12/10)
                
                # MP_WEIGHT 데이터 조회하여 실제 종료일 결정
                # end_date를 None으로 설정하여 기준일자 이후의 모든 데이터를 조회
                from call import get_mp_weight_data
                mp_weight_data = get_mp_weight_data(
                    start_date=base_date.strftime("%Y-%m-%d"),
                    end_date=None  # None으로 설정하여 기준일자 이후의 모든 데이터 조회
                )
                
                # MP_WEIGHT 데이터가 있고 기준일자 이후 데이터가 있으면, 그 마지막 날짜를 종료일로 설정
                if not mp_weight_data.empty and 'dt' in mp_weight_data.columns:
                    # 기준일자 이후의 데이터만 필터링
                    mp_weight_after_base = mp_weight_data[mp_weight_data['dt'].dt.date >= base_date]
                    if not mp_weight_after_base.empty:
                        # MP_WEIGHT 데이터의 마지막 날짜를 종료일로 설정 (MP_WEIGHT에 있는 날까지만 표시)
                        actual_end_date = mp_weight_after_base['dt'].max().date()
                    else:
                        # 기준일자 이후 MP_WEIGHT 데이터가 없으면 initial_end_date 사용
                        actual_end_date = initial_end_date
                else:
                    # MP_WEIGHT 데이터가 없으면 initial_end_date 사용
                    actual_end_date = initial_end_date
                
                # 종료일은 표시 시작일보다 크거나 같아야 함
                if actual_end_date < display_start_date:
                    st.warning(f"종료일({actual_end_date.strftime('%Y-%m-%d')})이 시작일({display_start_date.strftime('%Y-%m-%d')})보다 이전입니다.")
                    return
                
                # 비중 정보는 index_constituents에서 가져오되, 가격은 PRICE_INDEX에서 가져옴
                # 따라서 df_filtered는 비중 정보 확인용으로만 사용
                df_filtered = df.copy()
                
                # 데이터 확인 정보 (한 번만 표시)
                st.caption(f"조회된 데이터: {len(df_filtered)}건 | 날짜 범위: {df_filtered['dt'].min().strftime('%Y-%m-%d') if not df_filtered.empty else 'N/A'} ~ {df_filtered['dt'].max().strftime('%Y-%m-%d') if not df_filtered.empty else 'N/A'}")
                st.caption(f"기준일자: {base_date.strftime('%Y-%m-%d')} | 시작일: {display_start_date.strftime('%Y-%m-%d')} | 종료일: {actual_end_date.strftime('%Y-%m-%d')}")
                
                # BM 수익률 계산 (계산 시작일부터 종료일까지 조회, 누적 수익률은 계산 시작일 기준)
                # PRICE_INDEX 테이블에서 지수 가격을 직접 가져와서 계산
                # 기준일자 2025/12/01이면 계산 시작일은 12/01의 1영업일 전이지만, 표시는 12/01부터
                bm_returns = calculate_bm_returns(calculation_start_date, actual_end_date, index_name=selected_index, display_start_date=display_start_date)
                bm_returns_sorted = None  # 전략 포트폴리오 섹션에서 사용하기 위해 초기화
                
                if not bm_returns.empty:
                    # 기준일자(표시 시작일)를 0%로 재계산
                    # 기준일자의 종가가 기준이므로, 기준일자의 일별 수익률과 누적 수익률은 모두 0%
                    bm_returns_sorted = bm_returns.sort_values('dt').copy()
                    bm_returns_sorted['daily_return'] = 0.0
                    
                    if 'bm_value' not in bm_returns_sorted.columns:
                        st.warning("BM 가치 데이터가 없습니다.")
                    else:
                        # 기준일자(첫 번째 날짜)의 가격을 기준으로 재계산
                        if len(bm_returns_sorted) > 0:
                            base_idx = bm_returns_sorted.index[0]
                            base_bm_value = bm_returns_sorted.loc[base_idx, 'bm_value']
                            
                            # 기준일자의 일별 수익률과 누적 수익률은 0%
                            bm_returns_sorted.loc[base_idx, 'daily_return'] = 0.0
                            bm_returns_sorted.loc[base_idx, 'cumulative_return'] = 0.0
                            
                            prev_bm_value = base_bm_value
                            
                            # 나머지 날짜들의 일별 수익률과 누적 수익률 재계산 (기준일자 대비)
                            for idx in bm_returns_sorted.index[1:]:
                                current_bm_value = bm_returns_sorted.loc[idx, 'bm_value']
                                
                                if prev_bm_value is not None and prev_bm_value > 0 and not pd.isna(current_bm_value) and current_bm_value > 0:
                                    # 일별 수익률: 전일 대비
                                    daily_return = ((current_bm_value - prev_bm_value) / prev_bm_value) * 100
                                    bm_returns_sorted.loc[idx, 'daily_return'] = daily_return
                                    
                                    # 누적 수익률: 기준일자 대비
                                    cumulative_return = ((current_bm_value - base_bm_value) / base_bm_value) * 100
                                    bm_returns_sorted.loc[idx, 'cumulative_return'] = cumulative_return
                                
                                prev_bm_value = current_bm_value
                
                # ========== BM vs 전략 포트폴리오 수익률 ==========
                st.subheader("BM vs 전략 포트폴리오 수익률")
                
                # 전략 포트폴리오 수익률 계산
                # BM 수익률은 위에서 계산한 bm_returns_sorted를 그대로 사용
                try:
                    with st.spinner("전략 포트폴리오 수익률을 계산하는 중..."):
                        # BM 수익률이 있는 경우에만 전략 포트폴리오 계산
                        if not bm_returns.empty and bm_returns_sorted is not None:
                            # bm_returns_sorted는 위의 "BM별 수익률" 섹션에서 이미 계산됨
                            strategy_returns = calculate_strategy_portfolio_returns(
                                index_name=selected_index,
                                base_date=base_date.strftime("%Y-%m-%d"),
                                end_date=actual_end_date.strftime("%Y-%m-%d"),
                                bm_returns_df=bm_returns_sorted  # BM 수익률 전달 (위에서 계산된 값 사용)
                            )
                            
                            # 디버깅 정보 출력
                            if strategy_returns.empty:
                                # mp_weight_data는 이미 위에서 조회했으므로 재사용
                                if mp_weight_data.empty:
                                    st.warning(f"mp_weight 테이블에 데이터가 없습니다. 기준일자: {base_date.strftime('%Y-%m-%d')}, 종료일자: {actual_end_date.strftime('%Y-%m-%d')}")
                                else:
                                    # 더 자세한 디버깅 정보
                                    # get_index_constituents_data는 이미 파일 상단에서 import됨
                                    bm_data_check = get_index_constituents_data(
                                        index_name=selected_index,
                                        start_date=base_date.strftime("%Y-%m-%d"),
                                        end_date=actual_end_date.strftime("%Y-%m-%d")
                                    )
                                    debug_info = []
                                    if bm_data_check.empty:
                                        debug_info.append("BM 구성종목 데이터가 없습니다")
                                    else:
                                        debug_info.append(f"BM 구성종목 데이터: {len(bm_data_check)}건")
                                        dates_check = sorted(bm_data_check['dt'].unique())
                                        if dates_check:
                                            base_date_obj = pd.to_datetime(base_date).date()
                                            base_data_check = bm_data_check[bm_data_check['dt'].dt.date <= base_date_obj]
                                            if base_data_check.empty:
                                                debug_info.append("기준일자 데이터가 없습니다")
                                            else:
                                                base_actual_date_check = base_data_check['dt'].max().date()
                                                debug_info.append(f"기준일자: {base_actual_date_check}")
                                    
                                    # 기준일자의 stock_price 데이터 확인
                                    from call import execute_custom_query, get_table_info
                                    try:
                                        stock_price_table_info = get_table_info("stock_price")
                                        stock_price_column_names = [col['column_name'] for col in stock_price_table_info]
                                        
                                        ticker_col = None
                                        for col in ['ticker', 'stock_name', 'stock', 'symbol', 'name']:
                                            if col in stock_price_column_names:
                                                ticker_col = col
                                                break
                                        
                                        price_col = None
                                        for col in ['price', 'close', 'close_price', 'value']:
                                            if col in stock_price_column_names:
                                                price_col = col
                                                break
                                        
                                        if ticker_col and price_col and not bm_data_check.empty:
                                            base_actual_date_check = bm_data_check[bm_data_check['dt'].dt.date <= base_date].iloc[-1]['dt'].date() if not bm_data_check[bm_data_check['dt'].dt.date <= base_date].empty else None
                                            if base_actual_date_check:
                                                # BM 종목과 mp_weight 종목 모두 포함
                                                bm_stocks = set(bm_data_check['stock_name'].unique())
                                                mp_stocks = set(mp_weight_data['stock_name'].unique())
                                                all_stocks_check = list(bm_stocks | mp_stocks)[:10]  # 처음 10개만
                                                if all_stocks_check:
                                                    stock_names_str_check = "', '".join(all_stocks_check)
                                                    price_check_query = f"""
                                                        SELECT COUNT(*) as cnt
                                                        FROM stock_price
                                                        WHERE {ticker_col} IN ('{stock_names_str_check}')
                                                        AND dt = '{base_actual_date_check}'
                                                    """
                                                    price_check_result = execute_custom_query(price_check_query)
                                                    if price_check_result:
                                                        price_count = price_check_result[0].get('cnt', 0)
                                                        debug_info.append(f"기준일자({base_actual_date_check}) stock_price 데이터: {price_count}건 (샘플 종목 {len(all_stocks_check)}개 중)")
                                    except Exception as e:
                                        debug_info.append(f"stock_price 확인 중 오류: {str(e)}")
                                    
                                    st.warning(f"전략 포트폴리오 수익률 데이터가 없습니다.\n"
                                             f"- mp_weight 데이터: {len(mp_weight_data)}건 (날짜 범위: {mp_weight_data['dt'].min()} ~ {mp_weight_data['dt'].max()})\n"
                                             f"- {', '.join(debug_info)}")
                            
                            if not strategy_returns.empty:
                                strategy_returns_sorted = strategy_returns.sort_values('dt').copy()
                                
                                # BM과 전략 포트폴리오 비교 차트
                                # BM 수익률은 이미 계산된 bm_returns_sorted 사용 (위의 "BM별 수익률" 섹션에서 계산됨)
                                
                                # 날짜 기준으로 병합
                                merged_df = pd.merge(
                                    bm_returns_sorted[['dt', 'cumulative_return']],
                                    strategy_returns_sorted[['dt', 'strategy_cumulative_return']],
                                    on='dt',
                                    how='outer'
                                )
                                merged_df = merged_df.sort_values('dt')
                                
                                # 차트 생성
                                fig_strategy = go.Figure()
                                
                                # BM 누적 수익률
                                fig_strategy.add_trace(go.Scatter(
                                    x=merged_df['dt'],
                                    y=merged_df['cumulative_return'],
                                    mode='lines+markers',
                                    name='BM 누적 수익률',
                                    line=dict(color='#1f77b4', width=2),
                                    marker=dict(size=4),
                                    hovertemplate='날짜: %{x}<br>BM 누적 수익률: %{y:.2f}%<extra></extra>'
                                ))
                                
                                # 전략 포트폴리오 누적 수익률
                                fig_strategy.add_trace(go.Scatter(
                                    x=merged_df['dt'],
                                    y=merged_df['strategy_cumulative_return'],
                                    mode='lines+markers',
                                    name='전략 포트폴리오 누적 수익률',
                                    line=dict(color='#ff7f0e', width=2),
                                    marker=dict(size=4),
                                    hovertemplate='날짜: %{x}<br>전략 포트폴리오 누적 수익률: %{y:.2f}%<extra></extra>'
                                ))
                                
                                fig_strategy.update_layout(
                                    title="BM vs 전략 포트폴리오 누적 수익률 비교",
                                    xaxis_title="날짜",
                                    yaxis_title="누적 수익률 (%)",
                                    hovermode='x unified',
                                    height=400,
                                    showlegend=True,
                                    xaxis=dict(
                                        showgrid=True,
                                        gridcolor='lightgray',
                                        type='date'
                                    ),
                                    yaxis=dict(
                                        showgrid=True,
                                        gridcolor='lightgray'
                                    )
                                )
                                
                                st.plotly_chart(fig_strategy, use_container_width=True)
                                
                                # 최종 수익률 비교
                                bm_final_return = bm_returns_sorted.iloc[-1]['cumulative_return'] if len(bm_returns_sorted) > 0 else 0
                                strategy_final_return = strategy_returns_sorted.iloc[-1]['strategy_cumulative_return'] if len(strategy_returns_sorted) > 0 else 0
                                excess_return = strategy_final_return - bm_final_return
                                
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric(
                                        "BM 최종 수익률",
                                        f"{bm_final_return:.2f}%",
                                        delta=None
                                    )
                                with col2:
                                    st.metric(
                                        "전략 포트폴리오 최종 수익률",
                                        f"{strategy_final_return:.2f}%",
                                        delta=f"{excess_return:.2f}%"
                                    )
                                with col3:
                                    st.metric(
                                        "초과 수익률",
                                        f"{excess_return:.2f}%",
                                        delta=None
                                    )
                                
                                # 전략 포트폴리오 비중 검증
                                render_verification(
                                    index_name=selected_index,
                                    base_date=base_date.strftime("%Y-%m-%d"),
                                    end_date=actual_end_date.strftime("%Y-%m-%d")
                                )
                                
                                # 일별 수익률 비교 표
                                with st.expander("일별 수익률 및 누적 수익률 보기", expanded=False):
                                    # 일별 수익률 계산
                                    merged_df['bm_daily_return'] = merged_df['cumulative_return'].diff()
                                    merged_df['strategy_daily_return'] = merged_df['strategy_cumulative_return'].diff()
                                
                                    display_df = pd.DataFrame({
                                        '날짜': merged_df['dt'].dt.strftime('%Y-%m-%d'),
                                        'BM 일별 수익률 (%)': merged_df['bm_daily_return'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A"),
                                        '전략 포트폴리오 일별 수익률 (%)': merged_df['strategy_daily_return'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A"),
                                        'BM 누적 수익률 (%)': merged_df['cumulative_return'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A"),
                                        '전략 포트폴리오 누적 수익률 (%)': merged_df['strategy_cumulative_return'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
                                    })
                                
                                    # 스타일링 적용
                                    def color_daily_returns(val):
                                        try:
                                            return_val = float(val.rstrip('%'))
                                            if return_val >= 2:
                                                return 'background-color: #d4edda; color: #155724; font-weight: bold'
                                            elif return_val >= 0:
                                                return 'background-color: #fff3cd; color: #856404'
                                            elif return_val >= -2:
                                                return 'background-color: #f8d7da; color: #721c24'
                                            else:
                                                return 'background-color: #f5c6cb; color: #721c24; font-weight: bold'
                                        except:
                                            return ''
                                
                                    styled_df = display_df.style.applymap(color_daily_returns, subset=['BM 일별 수익률 (%)', '전략 포트폴리오 일별 수익률 (%)'])
                                
                                    st.markdown("""
                                    <style>
                                    .dataframe {
                                        font-size: 14px !important;
                                    }
                                    .dataframe th {
                                        font-size: 16px !important;
                                        font-weight: bold !important;
                                        padding: 10px !important;
                                    }
                                    .dataframe td {
                                        font-size: 14px !important;
                                        padding: 8px !important;
                                    }
                                    </style>
                                    """, unsafe_allow_html=True)
                                    st.dataframe(styled_df, use_container_width=True, hide_index=True)
                            else:
                                # strategy_returns가 비어있을 때
                                st.warning("전략 포트폴리오 수익률 데이터가 없습니다. mp_weight 테이블에 데이터가 있는지 확인해주세요.")
                        else:
                            st.warning("전략 포트폴리오 수익률 데이터를 계산할 수 없습니다. mp_weight 테이블에 데이터가 있는지 확인해주세요.")
                except Exception as e:
                    st.error(f"전략 포트폴리오 수익률을 계산하는 중 오류가 발생했습니다: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
                
                # ========== GICS 섹터별 비중 ==========
                st.subheader("GICS 섹터별 비중")
                
                # index_constituents 테이블에서 GICS SECTOR별 비중 및 성과 가져오기
                try:
                    with st.spinner(f"GICS SECTOR 정보를 불러오는 중..."):
                        # 기준일자(base_date)를 base_date로 전달하여 기준일자 기여도는 제외
                        gics_data = get_bm_gics_sector_weights(
                            index_name=selected_index,
                            base_date=base_date.strftime("%Y-%m-%d"),  # 기준일자 전달 (기준일자 기여도 제외)
                            end_date=actual_end_date.strftime("%Y-%m-%d")  # 비중 표시 및 BM 성과 계산 종료일
                        )
                        
                        if not gics_data.empty:
                            # 표시할 컬럼 선택
                            display_cols = []
                            if 'gics_name' in gics_data.columns:
                                display_cols.append('gics_name')
                            if 'stock_count' in gics_data.columns:
                                display_cols.append('stock_count')
                            if 'bm_weight_pct' in gics_data.columns:
                                display_cols.append('bm_weight_pct')
                            if 'bm_performance' in gics_data.columns:
                                display_cols.append('bm_performance')
                            
                            if display_cols:
                                display_df = gics_data[display_cols].copy()
                                
                                # BM 성과 값 검증 및 포맷팅 (이상한 값 처리)
                                if 'bm_performance' in display_df.columns:
                                    # NaN, inf, 또는 이상한 값 처리
                                    display_df['bm_performance'] = pd.to_numeric(display_df['bm_performance'], errors='coerce')
                                    display_df['bm_performance'] = display_df['bm_performance'].fillna(0)
                                    # 매우 큰 값이나 inf 값 처리
                                    display_df['bm_performance'] = display_df['bm_performance'].replace([float('inf'), float('-inf')], 0)
                                    # 합리적인 범위로 제한 (예: -100% ~ 100%)
                                    display_df['bm_performance'] = display_df['bm_performance'].clip(-100, 100)
                                
                                # 컬럼명 한글화
                                column_mapping = {
                                    'gics_name': 'GICS Sector',
                                    'stock_count': '종목 수',
                                    'bm_weight_pct': 'BM 비중',
                                    'bm_performance': '기여 성과'
                                }
                                display_df.columns = [column_mapping.get(col, col) for col in display_df.columns]
                                
                                st.markdown(f"**{selected_index} | 기준일자: {actual_end_date.strftime('%Y-%m-%d')}**")
                                
                                # 비중 합계 표시
                                if 'BM 비중' in display_df.columns:
                                    total_weight = display_df['BM 비중'].sum()
                                    st.caption(f"총 비중: {total_weight:.2f}%")
                                
                                # 스타일링 적용
                                format_dict = {}
                                if 'BM 비중' in display_df.columns:
                                    format_dict['BM 비중'] = '{:.2f}%'
                                if '기여 성과' in display_df.columns:
                                    format_dict['기여 성과'] = '{:.2f}%'
                                
                                styled_df = display_df.style.format(format_dict)
                                
                                st.dataframe(
                                    styled_df,
                                    use_container_width=True,
                                    hide_index=True
                                )
                                
                                # 일자별 섹터 기여도 표시 (BM 일별 수익률처럼)
                                with st.expander("일자별 섹터 기여도 보기", expanded=False):
                                    try:
                                        # 기준일자(base_date)를 base_date로 전달하여 기준일자 데이터는 제외
                                        daily_sector_data = get_daily_sector_contributions(
                                            index_name=selected_index,
                                            base_date=base_date.strftime("%Y-%m-%d"),  # 기준일자 전달
                                            end_date=actual_end_date.strftime("%Y-%m-%d")
                                        )
                                        
                                        if not daily_sector_data.empty:
                                            # 날짜별로 정렬
                                            daily_sector_data = daily_sector_data.sort_values('dt')
                                            
                                            # 날짜별로 그룹화하여 일별 합계 계산
                                            dates = sorted(daily_sector_data['dt'].unique())
                                            
                                            # 일별 섹터 기여도 표 (BM 일별 수익률 표와 유사한 형식)
                                            display_rows = []
                                            
                                            for date in dates:
                                                date_data = daily_sector_data[daily_sector_data['dt'] == date]
                                                
                                                # 섹터별 일별 기여도
                                                sector_contributions = {}
                                                daily_total = 0.0
                                                
                                                for _, row in date_data.iterrows():
                                                    gics_name = row['gics_name']
                                                    daily_contrib = row['daily_contribution']
                                                    sector_contributions[gics_name] = daily_contrib
                                                    daily_total += daily_contrib
                                                
                                                # 모든 섹터 포함 (없는 섹터는 0으로)
                                                all_sectors = sorted(daily_sector_data['gics_name'].unique())
                                                row_data = {'날짜': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)}
                                                
                                                for sector in all_sectors:
                                                    row_data[sector] = sector_contributions.get(sector, 0.0)
                                                
                                                row_data['일별 합계'] = daily_total
                                                display_rows.append(row_data)
                                            
                                            display_df = pd.DataFrame(display_rows)
                                            
                                            # 스타일링
                                            def color_daily_contributions(val):
                                                try:
                                                    return_val = float(val)
                                                    if return_val >= 0.5:
                                                        return 'background-color: #d4edda; color: #155724; font-weight: bold'
                                                    elif return_val >= 0:
                                                        return 'background-color: #fff3cd; color: #856404'
                                                    elif return_val >= -0.5:
                                                        return 'background-color: #f8d7da; color: #721c24'
                                                    else:
                                                        return 'background-color: #f5c6cb; color: #721c24; font-weight: bold'
                                                except:
                                                    return ''
                                            
                                            # 숫자 컬럼에만 스타일 적용
                                            numeric_cols = [col for col in display_df.columns if col != '날짜']
                                            styled_df = display_df.style.applymap(color_daily_contributions, subset=numeric_cols)
                                            
                                            # 포맷팅
                                            format_dict = {}
                                            for col in numeric_cols:
                                                format_dict[col] = '{:.2f}%'
                                            styled_df = styled_df.format(format_dict)
                                            
                                            st.markdown("""
                                            <style>
                                            .dataframe {
                                                font-size: 14px !important;
                                            }
                                            .dataframe th {
                                                font-size: 16px !important;
                                                font-weight: bold !important;
                                                padding: 10px !important;
                                            }
                                            .dataframe td {
                                                font-size: 14px !important;
                                                padding: 8px !important;
                                            }
                                            </style>
                                            """, unsafe_allow_html=True)
                                            st.dataframe(styled_df, use_container_width=True, hide_index=True)
                                            
                                            # 누적 기여도 차트
                                            cumulative_pivot_df = daily_sector_data.pivot_table(
                                                index='dt',
                                                columns='gics_name',
                                                values='cumulative_contribution',
                                                aggfunc='last',
                                                fill_value=0.0
                                            )
                                            
                                            fig_sector = go.Figure()
                                            
                                            for gics_name in cumulative_pivot_df.columns:
                                                fig_sector.add_trace(go.Scatter(
                                                    x=cumulative_pivot_df.index,
                                                    y=cumulative_pivot_df[gics_name],
                                                    mode='lines+markers',
                                                    name=gics_name,
                                                    hovertemplate=f'{gics_name}<br>날짜: %{{x}}<br>누적 기여도: %{{y:.2f}}%<extra></extra>'
                                                ))
                                            
                                            fig_sector.update_layout(
                                                title="섹터별 누적 기여도",
                                                xaxis_title="날짜",
                                                yaxis_title="누적 기여도 (%)",
                                                hovermode='x unified',
                                                height=400,
                                                showlegend=True,
                                                xaxis=dict(
                                                    showgrid=True,
                                                    gridcolor='lightgray',
                                                    type='date'
                                                ),
                                                yaxis=dict(
                                                    showgrid=True,
                                                    gridcolor='lightgray'
                                                )
                                            )
                                            
                                            st.plotly_chart(fig_sector, use_container_width=True)
                                        else:
                                            st.warning("일자별 섹터 기여도 데이터를 찾을 수 없습니다.")
                                    except Exception as e:
                                        st.error(f"일자별 섹터 기여도를 불러오는 중 오류가 발생했습니다: {str(e)}")
                                        import traceback
                                        st.code(traceback.format_exc())
                            else:
                                st.warning("표시할 데이터가 없습니다.")
                        else:
                            st.warning(f"기준일자({base_date.strftime('%Y-%m-%d')})에 대한 GICS SECTOR 정보를 찾을 수 없습니다.")
                except Exception as e:
                    st.error(f"GICS SECTOR 정보를 불러오는 중 오류가 발생했습니다: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
                
                # ========== 종목별 비중 ==========
                st.subheader("종목별 비중")
                
                # index_constituents 테이블에서 종목별 비중 및 성과 가져오기
                try:
                    # 기준일자와 종료일 사용
                    with st.spinner(f"종목별 비중 정보를 불러오는 중..."):
                        # 기준일자(base_date)를 base_date로 전달하여 기준일자 기여도는 제외
                        stock_data = get_bm_stock_weights(
                            index_name=selected_index,
                            base_date=base_date.strftime("%Y-%m-%d"),  # 기준일자 전달 (기준일자 기여도 제외)
                            end_date=actual_end_date.strftime("%Y-%m-%d")  # 비중 표시 및 BM 성과 계산 종료일
                        )
                        
                        if not stock_data.empty:
                            # 표시할 컬럼 확인: 종목명 / 기준일 비중 / 기간 수익률 / 기여성과
                            display_cols = []
                            if 'stock_name' in stock_data.columns:
                                display_cols.append('stock_name')
                            if 'base_weight_pct' in stock_data.columns:
                                display_cols.append('base_weight_pct')
                            if 'period_return' in stock_data.columns:
                                display_cols.append('period_return')
                            if 'contribution' in stock_data.columns:
                                display_cols.append('contribution')
                            
                            if display_cols:
                                display_df = stock_data[display_cols].copy()
                                
                                # 값 검증 및 포맷팅
                                if 'period_return' in display_df.columns:
                                    display_df['period_return'] = pd.to_numeric(display_df['period_return'], errors='coerce')
                                    display_df['period_return'] = display_df['period_return'].fillna(0)
                                    display_df['period_return'] = display_df['period_return'].replace([float('inf'), float('-inf')], 0)
                                    display_df['period_return'] = display_df['period_return'].clip(-100, 100)
                                
                                if 'contribution' in display_df.columns:
                                    display_df['contribution'] = pd.to_numeric(display_df['contribution'], errors='coerce')
                                    display_df['contribution'] = display_df['contribution'].fillna(0)
                                    display_df['contribution'] = display_df['contribution'].replace([float('inf'), float('-inf')], 0)
                                    display_df['contribution'] = display_df['contribution'].clip(-100, 100)
                                
                                # 컬럼명 한글화: 종목명 / 기준일 비중 / 기간 수익률 / 기여성과
                                column_mapping = {
                                    'stock_name': '종목명',
                                    'base_weight_pct': '기준일 비중 (%)',
                                    'period_return': '기간 수익률 (%)',
                                    'contribution': '기여성과 (%)'
                                }
                                display_df.columns = [column_mapping.get(col, col) for col in display_df.columns]
                                
                                st.markdown(f"**BM: {selected_index} | 기준일자: {base_date.strftime('%Y-%m-%d')}**")
                                
                                # 비중 합계 표시
                                if '기준일 비중 (%)' in display_df.columns:
                                    total_weight = display_df['기준일 비중 (%)'].sum()
                                    st.caption(f"총 비중: {total_weight:.2f}% | 종목 수: {len(display_df)}")
                                
                                # 스타일링 적용
                                format_dict = {}
                                if '기준일 비중 (%)' in display_df.columns:
                                    format_dict['기준일 비중 (%)'] = '{:.2f}%'
                                if '기간 수익률 (%)' in display_df.columns:
                                    format_dict['기간 수익률 (%)'] = '{:.2f}%'
                                if '기여성과 (%)' in display_df.columns:
                                    format_dict['기여성과 (%)'] = '{:.2f}%'
                                
                                styled_df = display_df.style.format(format_dict)
                                
                                st.dataframe(
                                    styled_df,
                                    use_container_width=True,
                                    hide_index=True
                                )
                                
                                # 기여성과 TOP10 / WORST10 표시용 원본 데이터
                                holdings_df = stock_data.copy()
                            else:
                                st.warning("표시할 데이터가 없습니다.")
                                holdings_df = pd.DataFrame()
                        else:
                            st.warning(f"기준일자({base_date.strftime('%Y-%m-%d')})에 대한 종목별 비중 정보를 찾을 수 없습니다.")
                            holdings_df = pd.DataFrame()
                except Exception as e:
                    st.error(f"종목별 비중 정보를 불러오는 중 오류가 발생했습니다: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
                    holdings_df = pd.DataFrame()
                
                # ========== 기여성과 TOP10 / WORST10 ==========
                st.subheader("기여성과 TOP10 / WORST10")
                
                if not holdings_df.empty and 'contribution' in holdings_df.columns:
                    # 기여성과 기준으로 정렬
                    holdings_df_sorted = holdings_df.sort_values('contribution', ascending=False)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**기여성과 TOP10**")
                        top_contributions = holdings_df_sorted.head(10).copy()
                        # 종목명 / 기준일 비중 / 기간 수익률 / 기여성과
                        display_cols = ['stock_name', 'base_weight_pct', 'period_return', 'contribution']
                        available_cols = [col for col in display_cols if col in top_contributions.columns]
                        top_display = top_contributions[available_cols].copy()
                        column_mapping = {
                            'stock_name': '종목명',
                            'base_weight_pct': '기준일 비중 (%)',
                            'period_return': '기간 수익률 (%)',
                            'contribution': '기여성과 (%)'
                        }
                        top_display.columns = [column_mapping.get(col, col) for col in top_display.columns]
                        for col in top_display.columns:
                            if col != '종목명':
                                top_display[col] = top_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
                        st.dataframe(top_display, use_container_width=True, hide_index=True)
                    
                    with col2:
                        st.markdown("**기여성과 WORST10**")
                        worst_contributions = holdings_df_sorted.tail(10).sort_values('contribution', ascending=True).copy()
                        # 종목명 / 기준일 비중 / 기간 수익률 / 기여성과
                        display_cols = ['stock_name', 'base_weight_pct', 'period_return', 'contribution']
                        available_cols = [col for col in display_cols if col in worst_contributions.columns]
                        worst_display = worst_contributions[available_cols].copy()
                        column_mapping = {
                            'stock_name': '종목명',
                            'base_weight_pct': '기준일 비중 (%)',
                            'period_return': '기간 수익률 (%)',
                            'contribution': '기여성과 (%)'
                        }
                        worst_display.columns = [column_mapping.get(col, col) for col in worst_display.columns]
                        for col in worst_display.columns:
                            if col != '종목명':
                                worst_display[col] = worst_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
                        st.dataframe(worst_display, use_container_width=True, hide_index=True)
                
            except Exception as e:
                st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {str(e)}")
                import traceback
                st.code(traceback.format_exc())


@with_connection
def calculate_bm_returns(start_date, end_date, index_name: str, display_start_date: Optional[date] = None, connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    BM의 일별 누적 수익률을 계산 (PRICE_INDEX 테이블에서 지수 가격 직접 가져오기)
    
    Args:
        start_date: 계산 시작일 (누적 수익률 계산의 기준일, 주가 조회용)
        end_date: 종료일
        index_name: 지수명 (PRICE_INDEX 테이블에서 가격 조회용, 예: 'NDX Index')
        display_start_date: 표시 시작일 (None이면 start_date와 동일, 이 날짜부터 표에 표시)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 누적 수익률 (dt, cumulative_return, bm_value) - display_start_date부터만 반환
    """
    if not index_name:
        return pd.DataFrame()
    
    # 날짜 범위 확인
    start_date_obj = start_date if hasattr(start_date, 'date') else pd.to_datetime(start_date).date()
    end_date_obj = end_date if hasattr(end_date, 'date') else pd.to_datetime(end_date).date()
    
    start_date_str = start_date_obj.strftime('%Y-%m-%d')
    end_date_str = end_date_obj.strftime('%Y-%m-%d')
    
    # PRICE_INDEX 테이블에서 선택한 지수의 가격 직접 조회
    where_conditions = [
        "value IS NOT NULL",
        "value_type = 'price'",
        f"ticker = '{index_name}'",
        f"dt >= '{start_date_str}'",
        f"dt <= '{end_date_str}'"
    ]
    where_clause = " AND ".join(where_conditions)
    
    query = f"""
        SELECT 
            dt,
            value as price
        FROM price_index
        WHERE {where_clause}
        ORDER BY dt
    """
    
    try:
        price_data = execute_custom_query(query, connection=connection)
        price_df = pd.DataFrame(price_data)
        
        if price_df.empty:
            return pd.DataFrame()
        
        price_df['dt'] = pd.to_datetime(price_df['dt'])
        price_df['dt_date'] = price_df['dt'].dt.date
        
        # 같은 날짜에 대해 집계 (평균 가격 사용)
        price_df = price_df.groupby('dt_date')['price'].mean().reset_index()
        price_df.rename(columns={'dt_date': 'dt'}, inplace=True)
        
        # 가격이 유효한 데이터만 사용
        price_df = price_df[price_df['price'].notna() & (price_df['price'] > 0)]
        if price_df.empty:
            return pd.DataFrame()
        
        # 시작일 이하의 가장 가까운 날짜 찾기
        start_data = price_df[price_df['dt'] <= start_date_obj]
        if start_data.empty:
            return pd.DataFrame()
        
        # 시작일 이후의 데이터만 사용
        base_date = start_data['dt'].max()
        price_df = price_df[price_df['dt'] >= base_date].copy()
        if price_df.empty or len(price_df) < 2:
            return pd.DataFrame()
        
        # 기준일자(첫 날짜)를 기준으로 누적 수익률 계산
        base_value = price_df.iloc[0]['price']
        if base_value == 0 or pd.isna(base_value):
            return pd.DataFrame()
        
        price_df['cumulative_return'] = ((price_df['price'] - base_value) / base_value) * 100
        price_df.rename(columns={'price': 'bm_value'}, inplace=True)
        price_df['dt'] = pd.to_datetime(price_df['dt'])
        
        # display_start_date가 지정된 경우, 해당 날짜부터만 반환 (표시용)
        if display_start_date is not None:
            display_start_obj = display_start_date if hasattr(display_start_date, 'date') else pd.to_datetime(display_start_date).date()
            price_df = price_df[price_df['dt'].dt.date >= display_start_obj].copy()
        
        return price_df[['dt', 'cumulative_return', 'bm_value']]
    except Exception as e:
        # 에러 발생 시 빈 DataFrame 반환
        return pd.DataFrame()


def calculate_stock_returns(df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    """
    종목별 수익률을 계산
    
    Args:
        df: index_constituents 데이터프레임
        start_date: 시작일
        end_date: 종료일
    
    Returns:
        pd.DataFrame: 종목별 수익률 (stock_name, return, weight)
    """
    if df.empty:
        return pd.DataFrame()
    
    results = []
    
    for stock_name in df['stock_name'].unique():
        stock_data = df[df['stock_name'] == stock_name].sort_values('dt')
        
        # 시작일 이하의 가장 가까운 데이터
        start_data = stock_data[stock_data['dt'].dt.date <= start_date]
        if start_data.empty:
            continue
        
        start_price = start_data.iloc[-1]['price']
        start_actual_date = start_data.iloc[-1]['dt'].date()
        
        # 종료일 이하의 가장 가까운 데이터
        end_data = stock_data[stock_data['dt'].dt.date <= end_date]
        if end_data.empty:
            continue
        
        end_price = end_data.iloc[-1]['price']
        end_actual_date = end_data.iloc[-1]['dt'].date()
        
        if start_actual_date >= end_actual_date:
            continue
        
        if start_price == 0 or pd.isna(start_price) or pd.isna(end_price):
            continue
        
        return_pct = ((end_price - start_price) / start_price) * 100
        
        # 최신 비중 가져오기
        latest_weight = stock_data.iloc[-1]['weight'] if 'weight' in stock_data.columns else 0
        
        results.append({
            'stock_name': stock_name,
            'return': return_pct,
            'weight': latest_weight
        })
    
    return pd.DataFrame(results).sort_values('return', ascending=False)