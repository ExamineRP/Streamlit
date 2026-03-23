"""
종목별 기여성과 계산 검증 스크립트
"""
import pandas as pd
from call import get_bm_stock_weights, get_bm_gics_sector_weights, execute_custom_query, with_connection, get_table_info
from psycopg2.extensions import connection as Connection
from typing import Optional
from datetime import datetime, timedelta
from utils import get_business_day, get_business_day_by_country, get_index_country_code

# ... existing code ...

@with_connection
def compare_daily_return_calculations(index_name: str, date: str, connection: Optional[Connection] = None):
    """
    일별 기여도 합계와 BM별 수익률 일별 수익률의 차이를 분석하는 함수
    
    Args:
        index_name: 지수명 (예: 'NDX Index')
        date: 날짜 (YYYY-MM-DD)
    """
    from utils import get_business_day_by_country, get_index_country_code
    
    print("\n" + "="*100)
    print(f"일별 기여도 합계 vs BM별 수익률 일별 수익률 비교 분석")
    print("="*100)
    print(f"지수: {index_name}")
    print(f"날짜: {date}")
    print("="*100)
    
    # 해당 날짜의 1영업일 전 날짜 계산
    date_obj = pd.to_datetime(date).date()
    country_code = get_index_country_code(index_name)
    prev_date = get_business_day_by_country(date_obj, 1, country_code)
    
    print(f"\n비교 날짜:")
    print(f"  전날: {prev_date.strftime('%Y-%m-%d')}")
    print(f"  당일: {date}")
    
    # ==========================================
    # 방법 1: PRICE_INDEX에서 지수 가격 직접 가져오기 (BM별 수익률)
    # ==========================================
    price_index_query = f"""
        SELECT 
            dt,
            value as price
        FROM price_index
        WHERE ticker = '{index_name}'
          AND value IS NOT NULL
          AND value_type = 'price'
          AND dt IN ('{prev_date.strftime('%Y-%m-%d')}', '{date}')
        ORDER BY dt
    """
    
    price_index_data = execute_custom_query(price_index_query, connection=connection)
    price_index_df = pd.DataFrame(price_index_data)
    
    if price_index_df.empty or len(price_index_df) < 2:
        print("\n❌ PRICE_INDEX 데이터를 찾을 수 없습니다.")
        return
    
    price_index_df['dt'] = pd.to_datetime(price_index_df['dt'])
    price_index_df = price_index_df.sort_values('dt')
    
    prev_price_index = float(price_index_df.iloc[0]['price'])
    current_price_index = float(price_index_df.iloc[-1]['price'])
    price_index_daily_return = ((current_price_index - prev_price_index) / prev_price_index) * 100
    
    print(f"\n[방법 1] PRICE_INDEX 지수 가격 (BM별 수익률):")
    print(f"  전날 가격: {prev_price_index:,.2f}")
    print(f"  당일 가격: {current_price_index:,.2f}")
    print(f"  일별 수익률: {price_index_daily_return:.4f}%")
    
    # ==========================================
    # 방법 2: index_constituents 비중 + stock_price 가격으로 BM 가치 계산
    # ==========================================
    table_info = get_table_info("index_constituents", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 컬럼 찾기
    index_col = None
    for col in ['index', 'index_name', 'index_code', 'idx']:
        if col in column_names:
            index_col = col
            break
    
    weight_col = None
    for col in ['index_weight', 'weight', 'weight_pct', 'weight_percent']:
        if col in column_names:
            weight_col = col
            break
    
    stock_col = None
    for col in ['stock', 'stock_name', 'stock_code', 'ticker', 'symbol']:
        if col in column_names:
            stock_col = col
            break
    
    gics_name_col = None
    for col in ['gics_name', 'gics_sector', 'sector', 'gics_sector_name', 'sector_name']:
        if col in column_names:
            gics_name_col = col
            break
    
    # index_constituents에서 비중 가져오기
    constituents_query = f"""
        SELECT 
            dt,
            {stock_col} as stock_name,
            {gics_name_col} as gics_name,
            {weight_col} as weight
        FROM index_constituents
        WHERE {index_col} = '{index_name}'
          AND dt IN ('{prev_date.strftime('%Y-%m-%d')}', '{date}')
          AND {weight_col} IS NOT NULL
        ORDER BY dt, {stock_col}
    """
    
    constituents_data = execute_custom_query(constituents_query, connection=connection)
    constituents_df = pd.DataFrame(constituents_data)
    
    if constituents_df.empty:
        print("\n❌ index_constituents 데이터를 찾을 수 없습니다.")
        return
    
    constituents_df['dt'] = pd.to_datetime(constituents_df['dt'])
    
    # stock_price 테이블 구조 확인
    stock_price_table_info = get_table_info("stock_price", connection=connection)
    stock_price_column_names = [col['column_name'] for col in stock_price_table_info]
    
    # ticker 컬럼 찾기
    ticker_col = None
    for col in ['ticker', 'stock_name', 'stock', 'symbol', 'name']:
        if col in stock_price_column_names:
            ticker_col = col
            break
    
    # price 컬럼 찾기
    price_col_stock = None
    for col in ['price', 'close', 'close_price', 'value']:
        if col in stock_price_column_names:
            price_col_stock = col
            break
    
    if not ticker_col or not price_col_stock:
        print("\n❌ stock_price 테이블 구조를 확인할 수 없습니다.")
        return
    
    # 각 날짜별로 BM 가치 및 섹터별 가치 계산
    dates = [prev_date, date_obj]
    bm_values = {}
    sector_values = {}
    
    for target_date in dates:
        date_constituents = constituents_df[constituents_df['dt'].dt.date == target_date].copy()
        
        if date_constituents.empty:
            continue
        
        stock_names = date_constituents['stock_name'].unique().tolist()
        stock_list = "', '".join([f"{name}" for name in stock_names])
        
        # stock_price에서 가격 가져오기
        stock_price_query = f"""
            SELECT 
                {ticker_col} as stock_name,
                {price_col_stock} as price
            FROM stock_price
            WHERE {ticker_col} IN ('{stock_list}')
              AND dt = '{target_date.strftime('%Y-%m-%d')}'
              AND {price_col_stock} IS NOT NULL
              AND {price_col_stock} > 0
        """
        
        stock_price_data = execute_custom_query(stock_price_query, connection=connection)
        stock_price_df = pd.DataFrame(stock_price_data)
        
        if stock_price_df.empty:
            continue
        
        # 비중과 가격 병합
        merged_df = date_constituents.merge(
            stock_price_df,
            on='stock_name',
            how='inner'
        )
        
        if merged_df.empty:
            continue
        
        # BM 가치 = sum(비중 × 가격)
        merged_df['value'] = merged_df['weight'].astype(float) * merged_df['price'].astype(float)
        bm_value = float(merged_df['value'].sum())
        weight_sum = float(merged_df['weight'].sum())
        
        bm_values[target_date] = {
            'bm_value': bm_value,
            'weight_sum': weight_sum,
            'stock_count': len(merged_df),
            'missing_stocks': len(date_constituents) - len(merged_df),
            'merged_df': merged_df
        }
        
        # 섹터별 가치 계산
        sector_values[target_date] = {}
        for gics_name in merged_df['gics_name'].unique():
            sector_data = merged_df[merged_df['gics_name'] == gics_name]
            sector_value = float((sector_data['weight'].astype(float) * sector_data['price'].astype(float)).sum())
            sector_values[target_date][gics_name] = sector_value
        
        print(f"\n[방법 2] index_constituents 비중 + stock_price 가격 ({target_date.strftime('%Y-%m-%d')}):")
        print(f"  BM 가치: {bm_value:,.2f}")
        print(f"  비중 합계: {weight_sum:.6f} ({weight_sum*100:.4f}%)")
        print(f"  종목 수: {bm_values[target_date]['stock_count']}")
        if bm_values[target_date]['missing_stocks'] > 0:
            print(f"  ⚠️  가격 데이터 없는 종목: {bm_values[target_date]['missing_stocks']}개")
    
    # ==========================================
    # 방법 2의 일별 수익률 계산
    # ==========================================
    if prev_date in bm_values and date_obj in bm_values:
        prev_bm_value = float(bm_values[prev_date]['bm_value'])
        current_bm_value = float(bm_values[date_obj]['bm_value'])
        constituents_daily_return = ((current_bm_value - prev_bm_value) / prev_bm_value) * 100
        
        print(f"\n[방법 2] 일별 수익률 (BM 가치 기준):")
        print(f"  전날 BM 가치: {prev_bm_value:,.2f}")
        print(f"  당일 BM 가치: {current_bm_value:,.2f}")
        print(f"  일별 수익률: {constituents_daily_return:.4f}%")
        
        # ==========================================
        # 방법 3: 섹터별 기여도 합계 계산 (일별 기여도 합계)
        # 전날 비중 고정, 가격 변화만 반영
        # ==========================================
        prev_merged_df = bm_values[prev_date]['merged_df'].copy()
        current_merged_df = bm_values[date_obj]['merged_df'].copy()
        
        # 전날 비중과 가격, 당일 가격 병합
        prev_merged_df = prev_merged_df[['stock_name', 'gics_name', 'weight', 'price']].copy()
        prev_merged_df.rename(columns={'weight': 'prev_weight', 'price': 'prev_price'}, inplace=True)
        
        current_merged_df = current_merged_df[['stock_name', 'price']].copy()
        current_merged_df.rename(columns={'price': 'current_price'}, inplace=True)
        
        # 전날 비중과 당일 가격 병합
        contribution_df = prev_merged_df.merge(
            current_merged_df,
            on='stock_name',
            how='inner'
        )
        
        # 각 종목별 수익률 계산: (당일 가격 - 전날 가격) / 전날 가격 × 100
        contribution_df['ret'] = ((contribution_df['current_price'].astype(float) - contribution_df['prev_price'].astype(float)) / contribution_df['prev_price'].astype(float)) * 100
        
        # 각 종목별 기여도 계산: ret × 전날 비중
        contribution_df['ret_contribution'] = contribution_df['ret'] * contribution_df['prev_weight'].astype(float)
        
        # 섹터별 기여도 합산
        sector_contributions = contribution_df.groupby('gics_name')['ret_contribution'].sum().to_dict()
        
        print(f"\n[방법 3] 섹터별 기여도 계산 (전날 비중 고정, 수익률 × 비중):")
        print("-" * 100)
        print(f"{'섹터명':<30} | {'전날 비중 합계':<18} | {'섹터 ret_contribution 합':<18} | {'일별기여도(%)':<18}")
        print("-" * 100)
        
        total_daily_contribution = 0.0
        
        for gics_name in sorted(sector_contributions.keys()):
            sector_contribution_value = float(sector_contributions.get(gics_name, 0.0))
            
            # 섹터별 전날 비중 합계
            sector_prev_weights = contribution_df[contribution_df['gics_name'] == gics_name]['prev_weight'].astype(float).sum()
            
            # 일별 섹터 기여도 (%) = 섹터 ret_contribution 합
            daily_contribution_pct = sector_contribution_value
            
            total_daily_contribution += daily_contribution_pct
            
            print(f"{gics_name:<30} | {sector_prev_weights:>17.6f} | {sector_contribution_value:>17.4f} | {daily_contribution_pct:>17.4f}")
        
        print("-" * 100)
        print(f"{'일별 기여도 합계':<30} | {'':<18} | {'':<18} | {'':<18} | {'':<18} | {total_daily_contribution:>17.4f}")
        
        # ==========================================
        # 차이 분석
        # ==========================================
        print(f"\n" + "="*100)
        print(f"차이 분석:")
        print(f"  [방법 1] PRICE_INDEX 일별 수익률: {price_index_daily_return:.4f}%")
        print(f"  [방법 2] BM 가치 기준 일별 수익률: {constituents_daily_return:.4f}%")
        print(f"  [방법 3] 섹터별 기여도 합계: {total_daily_contribution:.4f}%")
        print(f"\n  방법 1 vs 방법 2 차이: {abs(price_index_daily_return - constituents_daily_return):.4f}%")
        print(f"  방법 2 vs 방법 3 차이: {abs(constituents_daily_return - total_daily_contribution):.4f}%")
        print(f"  방법 1 vs 방법 3 차이: {abs(price_index_daily_return - total_daily_contribution):.4f}%")
        
        if abs(constituents_daily_return - total_daily_contribution) > 0.01:
            print(f"\n⚠️  경고: BM 가치 기준 일별 수익률과 섹터별 기여도 합계가 일치하지 않습니다!")
            print(f"\n가능한 원인:")
            print(f"  1. 비중 합계 문제:")
            print(f"     - 전날 비중 합계: {bm_values[prev_date]['weight_sum']*100:.4f}%")
            print(f"     - 당일 비중 합계: {bm_values[date_obj]['weight_sum']*100:.4f}%")
            print(f"  2. 종목 누락:")
            print(f"     - 전날 가격 데이터 없는 종목: {bm_values[prev_date]['missing_stocks']}개")
            print(f"     - 당일 가격 데이터 없는 종목: {bm_values[date_obj]['missing_stocks']}개")
            print(f"  3. 비중 변경 영향:")
            print(f"     - 비중이 변경되면 섹터별 기여도 계산 방식에 따라 차이가 발생할 수 있음")
            print(f"     - 섹터별 기여도는 전날 비중을 사용하지만, 실제 BM 가치는 당일 비중을 사용")
            
            # 비중 변경 상세 분석
            if prev_date in bm_values and date_obj in bm_values:
                prev_df = bm_values[prev_date]['merged_df'].copy()
                current_df = bm_values[date_obj]['merged_df'].copy()
                
                # 공통 종목 찾기
                common_stocks = set(prev_df['stock_name']) & set(current_df['stock_name'])
                
                if len(common_stocks) > 0:
                    print(f"\n  4. 비중 변경 상세 분석:")
                    print(f"     - 공통 종목 수: {len(common_stocks)}개")
                    
                    # 비중이 변경된 종목 찾기
                    weight_changes = []
                    for stock in common_stocks:
                        prev_weight = prev_df[prev_df['stock_name'] == stock]['weight'].iloc[0]
                        current_weight = current_df[current_df['stock_name'] == stock]['weight'].iloc[0]
                        if abs(prev_weight - current_weight) > 0.0001:
                            weight_changes.append({
                                'stock': stock,
                                'prev_weight': prev_weight,
                                'current_weight': current_weight,
                                'change': current_weight - prev_weight
                            })
                    
                    if weight_changes:
                        print(f"     - 비중이 변경된 종목: {len(weight_changes)}개")
                        print(f"       (상위 5개만 표시)")
                        for change in sorted(weight_changes, key=lambda x: abs(x['change']), reverse=True)[:5]:
                            print(f"       - {change['stock']}: {change['prev_weight']*100:.4f}% → {change['current_weight']*100:.4f}% (변화: {change['change']*100:+.4f}%)")
        
        if abs(price_index_daily_return - constituents_daily_return) > 0.01:
            print(f"\n⚠️  경고: PRICE_INDEX 일별 수익률과 BM 가치 기준 일별 수익률이 일치하지 않습니다!")
            print(f"\n가능한 원인:")
            print(f"  1. PRICE_INDEX의 지수 가격이 다른 기준일/시간대를 사용할 수 있음")
            print(f"  2. index_constituents의 비중이 실제 지수 구성과 다를 수 있음")
            print(f"  3. stock_price에 가격 데이터가 없는 종목이 있음")
    else:
        print(f"\n❌ BM 가치 계산에 필요한 데이터가 부족합니다.")
    
    print("="*100)


@with_connection
def calculate_daily_and_cumulative_contribution(index_name: str, start_date: str, end_date: str, connection: Optional[Connection] = None):
    """
    방법 3을 이용해서 일별 수익률과 누적 기여도를 계산하는 함수
    
    Args:
        index_name: 지수명 (예: 'NDX Index')
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)
    """
    from utils import get_business_day_by_country, get_index_country_code
    
    print("\n" + "="*100)
    print(f"일별 수익률 및 누적 기여도 계산 (방법 3)")
    print("="*100)
    print(f"지수: {index_name}")
    print(f"기간: {start_date} ~ {end_date}")
    print("="*100)
    
    # 날짜 범위 계산
    start_date_obj = pd.to_datetime(start_date).date()
    end_date_obj = pd.to_datetime(end_date).date()
    country_code = get_index_country_code(index_name)
    
    # 시작일의 전영업일 계산
    prev_start_date = get_business_day_by_country(start_date_obj, 1, country_code)
    
    # 모든 날짜 리스트 생성 (전영업일 포함)
    # business_day 테이블에서 해당 기간의 모든 영업일 가져오기
    from datetime import timedelta
    # 컬럼명이 대소문자 구분 없이 저장되어 있을 수 있으므로 소문자로 변환
    country_code_lower = country_code.lower()
    business_day_query = f"""
        SELECT dt
        FROM business_day
        WHERE dt >= '{prev_start_date.strftime('%Y-%m-%d')}'
          AND dt <= '{end_date_obj.strftime('%Y-%m-%d')}'
          AND {country_code_lower} = 1
        ORDER BY dt
    """
    business_day_data = execute_custom_query(business_day_query, connection=connection)
    
    if business_day_data:
        all_dates = []
        for row in business_day_data:
            # execute_custom_query는 딕셔너리 리스트를 반환
            if isinstance(row, dict):
                dt_value = row.get('dt') or row.get('DT')
            else:
                # 혹시 튜플이나 리스트인 경우를 대비
                dt_value = row[0] if len(row) > 0 else None
            
            if dt_value is not None:
                # 이미 date 객체인 경우
                if isinstance(dt_value, type(prev_start_date)):
                    all_dates.append(dt_value)
                # datetime 객체인 경우
                elif hasattr(dt_value, 'date'):
                    all_dates.append(dt_value.date())
                # 문자열이나 다른 형식인 경우
                else:
                    try:
                        parsed_date = pd.to_datetime(dt_value)
                        if hasattr(parsed_date, 'date'):
                            all_dates.append(parsed_date.date())
                        else:
                            all_dates.append(parsed_date)
                    except Exception as e:
                        continue
        
        # prev_start_date가 포함되어 있는지 확인
        if prev_start_date not in all_dates:
            all_dates.insert(0, prev_start_date)
        all_dates = sorted(set(all_dates))
    else:
        # business_day 테이블에 데이터가 없으면 기본 로직 사용
        all_dates = [prev_start_date]
        current_date = start_date_obj
        while current_date <= end_date_obj:
            all_dates.append(current_date)
            # 다음 영업일 계산 (1일씩 증가하면서 영업일인지 확인)
            next_date = current_date + timedelta(days=1)
            # 최대 10일까지 확인 (주말/공휴일 고려)
            found = False
            for _ in range(10):
                if next_date > end_date_obj:
                    break
                # 주말이 아니면 영업일로 간주
                if next_date.weekday() < 5:
                    current_date = next_date
                    found = True
                    break
                next_date += timedelta(days=1)
            if not found:
                break
    
    print(f"\n계산 기간:")
    print(f"  전영업일: {prev_start_date.strftime('%Y-%m-%d')}")
    for date in all_dates[1:]:
        print(f"  {date.strftime('%Y-%m-%d')}")
    
    # 테이블 구조 확인
    table_info = get_table_info("index_constituents", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 컬럼 찾기
    index_col = None
    for col in ['index', 'index_name', 'index_code', 'idx']:
        if col in column_names:
            index_col = col
            break
    
    weight_col = None
    for col in ['index_weight', 'weight', 'weight_pct', 'weight_percent']:
        if col in column_names:
            weight_col = col
            break
    
    stock_col = None
    for col in ['stock', 'stock_name', 'stock_code', 'ticker', 'symbol']:
        if col in column_names:
            stock_col = col
            break
    
    gics_name_col = None
    for col in ['gics_name', 'gics_sector', 'sector', 'gics_sector_name', 'sector_name']:
        if col in column_names:
            gics_name_col = col
            break
    
    # index_constituents에서 비중 가져오기
    date_list = "', '".join([d.strftime('%Y-%m-%d') for d in all_dates])
    constituents_query = f"""
        SELECT 
            dt,
            {stock_col} as stock_name,
            {gics_name_col} as gics_name,
            {weight_col} as weight
        FROM index_constituents
        WHERE {index_col} = '{index_name}'
          AND dt IN ('{date_list}')
          AND {weight_col} IS NOT NULL
        ORDER BY dt, {stock_col}
    """
    
    constituents_data = execute_custom_query(constituents_query, connection=connection)
    constituents_df = pd.DataFrame(constituents_data)
    
    if constituents_df.empty:
        print("\n❌ index_constituents 데이터를 찾을 수 없습니다.")
        return
    
    constituents_df['dt'] = pd.to_datetime(constituents_df['dt'])
    
    # stock_price 테이블 구조 확인
    stock_price_table_info = get_table_info("stock_price", connection=connection)
    stock_price_column_names = [col['column_name'] for col in stock_price_table_info]
    
    # ticker 컬럼 찾기
    ticker_col = None
    for col in ['ticker', 'stock_name', 'stock', 'symbol', 'name']:
        if col in stock_price_column_names:
            ticker_col = col
            break
    
    # price 컬럼 찾기
    price_col_stock = None
    for col in ['price', 'close', 'close_price', 'value']:
        if col in stock_price_column_names:
            price_col_stock = col
            break
    
    if not ticker_col or not price_col_stock:
        print("\n❌ stock_price 테이블 구조를 확인할 수 없습니다.")
        return
    
    # 각 날짜별로 데이터 가져오기
    date_data_dict = {}
    for target_date in all_dates:
        date_constituents = constituents_df[constituents_df['dt'].dt.date == target_date].copy()
        
        if date_constituents.empty:
            continue
        
        stock_names = date_constituents['stock_name'].unique().tolist()
        stock_list = "', '".join([f"{name}" for name in stock_names])
        
        # stock_price에서 가격 가져오기
        stock_price_query = f"""
            SELECT 
                {ticker_col} as stock_name,
                {price_col_stock} as price
            FROM stock_price
            WHERE {ticker_col} IN ('{stock_list}')
              AND dt = '{target_date.strftime('%Y-%m-%d')}'
              AND {price_col_stock} IS NOT NULL
              AND {price_col_stock} > 0
        """
        
        stock_price_data = execute_custom_query(stock_price_query, connection=connection)
        stock_price_df = pd.DataFrame(stock_price_data)
        
        if stock_price_df.empty:
            continue
        
        # 비중과 가격 병합
        merged_df = date_constituents.merge(
            stock_price_df,
            on='stock_name',
            how='inner'
        )
        
        if not merged_df.empty:
            date_data_dict[target_date] = merged_df
    
    # 일별 수익률 및 기여도 계산
    daily_results = []
    sector_cumulative_contribution = {}  # {gics_name: 누적 기여도}
    
    for i in range(1, len(all_dates)):
        prev_date = all_dates[i-1]
        current_date = all_dates[i]
        
        if prev_date not in date_data_dict or current_date not in date_data_dict:
            continue
        
        prev_df = date_data_dict[prev_date].copy()
        current_df = date_data_dict[current_date].copy()
        
        # 전날 비중과 가격, 당일 가격 병합
        prev_df = prev_df[['stock_name', 'gics_name', 'weight', 'price']].copy()
        prev_df.rename(columns={'weight': 'prev_weight', 'price': 'prev_price'}, inplace=True)
        
        current_df = current_df[['stock_name', 'price']].copy()
        current_df.rename(columns={'price': 'current_price'}, inplace=True)
        
        # 전날 비중과 당일 가격 병합
        contribution_df = prev_df.merge(
            current_df,
            on='stock_name',
            how='inner'
        )
        
        if contribution_df.empty:
            continue
        
        # 각 종목별 수익률 계산: (당일 가격 - 전날 가격) / 전날 가격 × 100
        contribution_df['ret'] = ((contribution_df['current_price'].astype(float) - contribution_df['prev_price'].astype(float)) / contribution_df['prev_price'].astype(float)) * 100
        
        # 각 종목별 기여도 계산: ret × 전날 비중
        contribution_df['ret_contribution'] = contribution_df['ret'] * contribution_df['prev_weight'].astype(float)
        
        # 전체 일별 수익률 = 모든 종목의 ret_contribution 합
        daily_return = contribution_df['ret_contribution'].sum()
        
        # 섹터별 기여도 합산
        sector_contributions = contribution_df.groupby('gics_name')['ret_contribution'].sum().to_dict()
        
        # 누적 기여도 업데이트
        for gics_name, sector_contribution_value in sector_contributions.items():
            if gics_name not in sector_cumulative_contribution:
                sector_cumulative_contribution[gics_name] = 0.0
            sector_cumulative_contribution[gics_name] += float(sector_contribution_value)
        
        daily_results.append({
            'date': current_date,
            'prev_date': prev_date,
            'daily_return': daily_return,
            'sector_contributions': sector_contributions.copy()
        })
    
    # 결과 출력
    print(f"\n일별 수익률:")
    print("-" * 100)
    print(f"{'날짜':<12} | {'전날':<12} | {'일별 수익률 (%)':<18}")
    print("-" * 100)
    
    for result in daily_results:
        print(f"{result['date'].strftime('%Y-%m-%d'):<12} | {result['prev_date'].strftime('%Y-%m-%d'):<12} | {result['daily_return']:>17.4f}")
    
    print(f"\n섹터별 일별 기여도:")
    print("-" * 100)
    print(f"{'날짜':<12} | {'섹터명':<30} | {'일별 기여도 (%)':<18}")
    print("-" * 100)
    
    for result in daily_results:
        for gics_name in sorted(result['sector_contributions'].keys()):
            print(f"{result['date'].strftime('%Y-%m-%d'):<12} | {gics_name:<30} | {result['sector_contributions'][gics_name]:>17.4f}")
    
    print(f"\n섹터별 누적 기여도 ({start_date} ~ {end_date}):")
    print("-" * 100)
    print(f"{'섹터명':<30} | {'누적 기여도 (%)':<18}")
    print("-" * 100)
    
    total_cumulative = 0.0
    for gics_name in sorted(sector_cumulative_contribution.keys()):
        cumulative_value = sector_cumulative_contribution[gics_name]
        total_cumulative += cumulative_value
        print(f"{gics_name:<30} | {cumulative_value:>17.4f}")
    
    print("-" * 100)
    print(f"{'전체 누적 기여도 합계':<30} | {total_cumulative:>17.4f}")
    
    # 전체 기간 일별 수익률 합계
    total_daily_return = sum([r['daily_return'] for r in daily_results])
    print(f"\n전체 기간 일별 수익률 합계: {total_daily_return:.4f}%")
    print(f"전체 기간 누적 기여도 합계: {total_cumulative:.4f}%")
    print("="*100)


if __name__ == "__main__":
    # 비교 분석 실행
    print("일별 기여도 합계 vs BM별 수익률 일별 수익률 비교 분석")
    print("="*80)
    
    compare_daily_return_calculations(
        index_name="NDX Index",
        date="2025-12-02"
    )
    
    # 2025-12-03과 2025-12-04의 일별 수익률 및 누적 기여도 계산
    print("\n\n")
    calculate_daily_and_cumulative_contribution(
        index_name="NDX Index",
        start_date="2025-12-01",
        end_date="2025-12-10"
    )