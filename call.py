import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as Connection
from typing import List, Dict, Optional, Any, Callable
from contextlib import contextmanager
from functools import wraps
from datetime import timedelta
import pandas as pd
import numpy as np
import settings


def get_db_connection():
    """
    PostgreSQL 데이터베이스에 연결하는 함수
    
    Returns:
        Connection: 데이터베이스 연결 객체
    """
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD
    )


@contextmanager
def db_connection():
    """
    데이터베이스 연결을 컨텍스트 매니저로 관리
    
    Usage:
        with db_connection() as conn:
            # 작업 수행
            pass
    """
    conn = get_db_connection()
    try:
        yield conn
    finally:
        conn.close()


def with_connection(func: Callable) -> Callable:
    """
    함수에 자동으로 데이터베이스 연결을 제공하는 데코레이터
    connection 파라미터가 None일 때만 새 연결을 생성하고 관리
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        connection = kwargs.get('connection')
        if connection is None:
            with db_connection() as conn:
                kwargs['connection'] = conn
                return func(*args, **kwargs)
        return func(*args, **kwargs)
    return wrapper


@with_connection
def get_table_names(connection: Optional[Connection] = None) -> List[str]:
    """
    데이터베이스의 모든 테이블 이름을 가져오는 함수
    
    Args:
        connection: 데이터베이스 연결 객체 (None이면 새로 연결)
    
    Returns:
        List[str]: 테이블 이름 리스트
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        return [row[0] for row in cursor.fetchall()]


def _resolve_table_schema(table_name: str, connection: Connection) -> str:
    """
    테이블이 실제로 있는 스키마 반환 (public → market 순으로 탐색).
    PostgreSQL에서 테이블명은 소문자로 저장되므로 table_name을 소문자로 비교.
    """
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_schema IN ('public', 'market')
              AND LOWER(table_name) = LOWER(%s)
            ORDER BY CASE table_schema WHEN 'public' THEN 0 ELSE 1 END
            LIMIT 1
        """, (table_name,))
        row = cursor.fetchone()
        return row["table_schema"] if row else "public"


@with_connection
def get_table_info(table_name: str, connection: Optional[Connection] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    특정 테이블의 컬럼 정보를 가져오는 함수.
    schema가 None이면 public → market 순으로 테이블이 있는 스키마를 자동 탐색.
    """
    if schema is None:
        schema = _resolve_table_schema(table_name, connection)
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s
              AND LOWER(table_name) = LOWER(%s)
            ORDER BY ordinal_position
        """, (schema, table_name))
        return [dict(col) for col in cursor.fetchall()]


@with_connection
def query_table(table_name: str, 
                columns: Optional[List[str]] = None,
                where_clause: Optional[str] = None,
                limit: Optional[int] = None,
                connection: Optional[Connection] = None) -> List[Dict[str, Any]]:
    """
    테이블에서 데이터를 조회하는 함수
    
    Args:
        table_name: 테이블 이름
        columns: 조회할 컬럼 리스트 (None이면 모든 컬럼)
        where_clause: WHERE 절 조건 (예: "id > 100")
        limit: 조회할 행 수 제한
        connection: 데이터베이스 연결 객체 (None이면 새로 연결)
    
    Returns:
        List[Dict]: 조회된 데이터 리스트
    """
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        # 컬럼 선택
        cols = sql.SQL(', ').join(sql.Identifier(col) for col in columns) if columns else sql.SQL('*')
        
        # 쿼리 구성
        query = sql.SQL("SELECT {} FROM {}").format(cols, sql.Identifier(table_name))
        
        # WHERE 절 추가
        if where_clause:
            query = sql.SQL("{} WHERE {}").format(query, sql.SQL(where_clause))
        
        # LIMIT 추가
        if limit:
            query = sql.SQL("{} LIMIT {}").format(query, sql.Literal(limit))
        
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]


@with_connection
def execute_custom_query(query: str, 
                         params: Optional[tuple] = None,
                         connection: Optional[Connection] = None) -> List[Dict[str, Any]]:
    """
    사용자 정의 SQL 쿼리를 실행하는 함수
    
    Args:
        query: 실행할 SQL 쿼리
        params: 쿼리 파라미터 (튜플)
        connection: 데이터베이스 연결 객체 (None이면 새로 연결)
    
    Returns:
        List[Dict]: 조회된 데이터 리스트
    """
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, params or ())
        return [dict(row) for row in cursor.fetchall()]


def select_and_query_table(table_name: Optional[str] = None,
                          columns: Optional[List[str]] = None,
                          where_clause: Optional[str] = None,
                          limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    테이블을 선택하고 조회하는 함수 (대화형)
    
    Args:
        table_name: 테이블 이름 (None이면 목록에서 선택)
        columns: 조회할 컬럼 리스트 (None이면 모든 컬럼)
        where_clause: WHERE 절 조건 (예: "id > 100")
        limit: 조회할 행 수 제한
    
    Returns:
        List[Dict]: 조회된 데이터 리스트
    """
    # 테이블 이름이 제공되지 않으면 목록 표시
    if table_name is None:
        tables = get_table_names()
        if not tables:
            print("조회할 수 있는 테이블이 없습니다.")
            return []
        
        print("\n=== 사용 가능한 테이블 목록 ===")
        for idx, table in enumerate(tables, 1):
            print(f"{idx}. {table}")
        
        try:
            choice = input("\n테이블 번호를 선택하거나 테이블 이름을 직접 입력하세요: ").strip()
            
            # 숫자로 입력한 경우
            if choice.isdigit():
                table_idx = int(choice) - 1
                if 0 <= table_idx < len(tables):
                    table_name = tables[table_idx]
                else:
                    print(f"잘못된 번호입니다. 1-{len(tables)} 사이의 숫자를 입력하세요.")
                    return []
            else:
                # 테이블 이름을 직접 입력한 경우
                if choice in tables:
                    table_name = choice
                else:
                    print(f"'{choice}' 테이블을 찾을 수 없습니다.")
                    return []
        except (ValueError, KeyboardInterrupt):
            print("\n취소되었습니다.")
            return []
    
    # 테이블 조회
    print(f"\n=== '{table_name}' 테이블 조회 중... ===")
    data = query_table(table_name, columns=columns, where_clause=where_clause, limit=limit)
    return data


def interactive_table_query():
    """
    대화형으로 테이블을 선택하고 조회하는 함수
    """
    print("=" * 50)
    print("데이터베이스 테이블 조회 도구")
    print("=" * 50)
    
    # 테이블 목록 가져오기
    tables = get_table_names()
    if not tables:
        print("조회할 수 있는 테이블이 없습니다.")
        return
    
    print("\n=== 사용 가능한 테이블 목록 ===")
    for idx, table in enumerate(tables, 1):
        print(f"{idx}. {table}")
    
    # 테이블 선택
    try:
        choice = input("\n테이블 번호를 선택하거나 테이블 이름을 직접 입력하세요: ").strip()
        
        if choice.isdigit():
            table_idx = int(choice) - 1
            if 0 <= table_idx < len(tables):
                selected_table = tables[table_idx]
            else:
                print(f"잘못된 번호입니다. 1-{len(tables)} 사이의 숫자를 입력하세요.")
                return
        else:
            if choice in tables:
                selected_table = choice
            else:
                print(f"'{choice}' 테이블을 찾을 수 없습니다.")
                return
        
        # 조회 옵션 설정
        print(f"\n선택된 테이블: {selected_table}")
        
        # 컬럼 선택 (선택사항)
        columns_input = input("조회할 컬럼을 쉼표로 구분하여 입력하세요 (전체: Enter): ").strip()
        columns = [col.strip() for col in columns_input.split(',')] if columns_input else None
        
        # WHERE 조건 (선택사항)
        where_clause = input("WHERE 조건을 입력하세요 (예: id > 100, 생략: Enter): ").strip() or None
        
        # LIMIT 설정
        limit_input = input("조회할 행 수를 입력하세요 (전체: Enter): ").strip()
        limit = int(limit_input) if limit_input.isdigit() else None
        
        # 데이터 조회
        print(f"\n=== '{selected_table}' 테이블 데이터 ===")
        data = query_table(selected_table, columns=columns, where_clause=where_clause, limit=limit)
        
        if data:
            print(f"\n총 {len(data)}개의 행이 조회되었습니다.\n")
            for idx, row in enumerate(data, 1):
                print(f"[행 {idx}]")
                for key, value in row.items():
                    print(f"  {key}: {value}")
                print()
        else:
            print("조회된 데이터가 없습니다.")
            
    except (ValueError, KeyboardInterrupt):
        print("\n취소되었습니다.")


@with_connection
def get_index_returns_ranking(period: str = 'daily',
                              price_column: Optional[str] = None,
                              connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    index_constituents 테이블에서 각 index별 수익률 순위를 계산하는 함수
    
    Args:
        period: 'daily', 'weekly', 'monthly' 중 하나
        price_column: 가격 컬럼명 (None이면 자동 감지)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: index별 수익률과 순위가 포함된 데이터프레임
    """
    # 테이블 구조 확인
    table_info = get_table_info("index_constituents", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 필수 컬럼 확인
    if 'dt' not in column_names:
        raise ValueError("'dt' 컬럼을 찾을 수 없습니다.")
    
    # index 컬럼 찾기 (index, index_name, index_code 등 가능)
    index_col = None
    for col in ['index', 'index_name', 'index_code', 'idx']:
        if col in column_names:
            index_col = col
            break
    
    if index_col is None:
        raise ValueError("index 컬럼을 찾을 수 없습니다. 'index', 'index_name', 'index_code', 'idx' 중 하나가 필요합니다.")
    
    # 가격 컬럼 자동 감지
    if price_column is None:
        price_candidates = ['price', 'close', 'close_price', 'value', 'nav']
        for col in price_candidates:
            if col in column_names:
                price_column = col
                break
        
        if price_column is None:
            # 숫자형 컬럼 중에서 선택
            numeric_cols = [col['column_name'] for col in table_info 
                          if col['data_type'] in ['numeric', 'double precision', 'real', 'bigint', 'integer'] 
                          and col['column_name'] not in ['dt', index_col]]
            if numeric_cols:
                price_column = numeric_cols[0]
                print(f"가격 컬럼을 자동으로 '{price_column}'로 선택했습니다.")
            else:
                raise ValueError("가격 컬럼을 찾을 수 없습니다. price_column 파라미터를 지정해주세요.")
    
    # 데이터 조회
    query = f"""
        SELECT 
            dt,
            {index_col} as index_name,
            {price_column} as price
        FROM index_constituents
        WHERE {price_column} IS NOT NULL
        ORDER BY dt, {index_col}
    """
    
    data = execute_custom_query(query, connection=connection)
    df = pd.DataFrame(data)
    
    if df.empty:
        return pd.DataFrame()
    
    # dt를 datetime으로 변환
    df['dt'] = pd.to_datetime(df['dt'])
    df = df.sort_values(['dt', 'index_name'])
    
    # 수익률 계산을 위한 그룹화
    period_map = {
        'daily': 'D',
        'weekly': 'W',
        'monthly': 'M'
    }
    
    if period not in period_map:
        raise ValueError("period는 'daily', 'weekly', 'monthly' 중 하나여야 합니다.")
    
    # 기간별로 그룹화하여 수익률 계산
    results = []
    
    for index_name in df['index_name'].unique():
        index_df = df[df['index_name'] == index_name].copy()
        index_df = index_df.sort_values('dt')
        
        if period == 'daily':
            # 일별 수익률: 전일 대비
            index_df['prev_price'] = index_df['price'].shift(1)
            index_df['return'] = (index_df['price'] - index_df['prev_price']) / index_df['prev_price'] * 100
            index_df['period'] = index_df['dt'].dt.date
            
        elif period == 'weekly':
            # 주별 수익률: 전주 동일 요일 대비
            index_df['week'] = index_df['dt'].dt.to_period('W')
            index_df['prev_week_price'] = index_df.groupby('week')['price'].shift(1)
            index_df['return'] = (index_df['price'] - index_df['prev_week_price']) / index_df['prev_week_price'] * 100
            index_df['period'] = index_df['week'].astype(str)
            
        elif period == 'monthly':
            # 월별 수익률: 전월 동일 일자 대비
            index_df['month'] = index_df['dt'].dt.to_period('M')
            index_df['prev_month_price'] = index_df.groupby('month')['price'].shift(1)
            index_df['return'] = (index_df['price'] - index_df['prev_month_price']) / index_df['prev_month_price'] * 100
            index_df['period'] = index_df['month'].astype(str)
        
        # 유효한 수익률만 필터링
        valid_df = index_df[index_df['return'].notna()].copy()
        
        for period_val in valid_df['period'].unique():
            period_data = valid_df[valid_df['period'] == period_val].iloc[-1]  # 해당 기간의 마지막 데이터
            results.append({
                'period': period_val,
                'index_name': index_name,
                'dt': period_data['dt'],
                'price': period_data['price'],
                'return': period_data['return']
            })
    
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        return result_df
    
    # 각 기간별로 수익률 순위 계산
    result_df['rank'] = result_df.groupby('period')['return'].rank(ascending=False, method='min').astype(int)
    result_df = result_df.sort_values(['period', 'rank'])
    
    return result_df


@with_connection
def get_index_constituents_data(index_name: Optional[str] = None,
                                start_date: Optional[str] = None,
                                end_date: Optional[str] = None,
                                connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    index_constituents 테이블에서 BM(Benchmark) 데이터를 조회하는 함수
    일자별 종목의 비중과 가격 정보를 반환
    
    Args:
        index_name: 지수명 (None이면 전체)
        start_date: 시작 날짜 (YYYY-MM-DD 형식, None이면 제한 없음)
        end_date: 종료 날짜 (YYYY-MM-DD 형식, None이면 제한 없음)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 종목별 비중과 가격 데이터프레임 (dt, index_name, stock_name, weight, price)
    """
    # 테이블 구조 확인
    table_info = get_table_info("index_constituents", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 필수 컬럼 확인
    if 'dt' not in column_names:
        raise ValueError("'dt' 컬럼을 찾을 수 없습니다.")
    
    # index 컬럼 찾기
    index_col = None
    for col in ['index', 'index_name', 'index_code', 'idx']:
        if col in column_names:
            index_col = col
            break
    
    if index_col is None:
        raise ValueError("index 컬럼을 찾을 수 없습니다.")
    
    # 종목명 컬럼 찾기
    stock_col = None
    for col in ['stock', 'stock_name', 'ticker', 'symbol', 'name']:
        if col in column_names:
            stock_col = col
            break
    
    if stock_col is None:
        raise ValueError("종목명 컬럼을 찾을 수 없습니다.")
    
    # 가격 컬럼 자동 감지 (local_price 우선)
    price_candidates = ['local_price', 'price', 'close', 'close_price', 'value', 'nav']
    price_column = None
    for col in price_candidates:
        if col in column_names:
            price_column = col
            break
    
    if price_column is None:
        numeric_cols = [col['column_name'] for col in table_info 
                      if col['data_type'] in ['numeric', 'double precision', 'real', 'bigint', 'integer'] 
                      and col['column_name'] not in ['dt', index_col, stock_col]]
        if numeric_cols:
            price_column = numeric_cols[0]
        else:
            raise ValueError("가격 컬럼을 찾을 수 없습니다.")
    
    # 비중 컬럼 찾기 (index_weight 우선)
    weight_candidates = ['index_weight', 'weight', 'weight_pct', 'weight_percent', 'allocation', 'pct']
    weight_column = None
    for col in weight_candidates:
        if col in column_names:
            weight_column = col
            break
    
    # 쿼리 구성
    where_conditions = [f"{price_column} IS NOT NULL", f"{price_column} > 0"]
    
    if index_name:
        where_conditions.append(f"{index_col} = '{index_name}'")
    
    if start_date:
        where_conditions.append(f"dt >= '{start_date}'")
    if end_date:
        where_conditions.append(f"dt <= '{end_date}'")
    
    where_clause = " AND ".join(where_conditions)
    
    # GICS 섹터 컬럼 찾기 (gics_name 우선)
    gics_col = None
    for col in ['gics_name', 'gics_sector', 'sector', 'gics_sector_name', 'sector_name', 'gics_sector_code']:
        if col in column_names:
            gics_col = col
            break
    
    # GICS Industry Group 컬럼 찾기
    gics_industry_col = None
    for col in ['gics_industry_group', 'industry_group', 'gics_industry_group_name', 'industry_group_name']:
        if col in column_names:
            gics_industry_col = col
            break
    
    # SELECT 컬럼 구성
    select_cols = [f"dt", f"{index_col} as index_name", f"{stock_col} as stock_name", f"{price_column} as price"]
    if weight_column:
        select_cols.append(f"{weight_column} as weight")
    if gics_col:
        select_cols.append(f"{gics_col} as gics_sector")
    if gics_industry_col:
        select_cols.append(f"{gics_industry_col} as gics_industry_group")
    
    query = f"""
        SELECT 
            {', '.join(select_cols)}
        FROM index_constituents
        WHERE {where_clause}
        ORDER BY dt, {index_col}, {stock_col}
    """
    
    data = execute_custom_query(query, connection=connection)
    df = pd.DataFrame(data)
    
    if df.empty:
        return pd.DataFrame()
    
    # dt를 datetime으로 변환
    df['dt'] = pd.to_datetime(df['dt'])
    df = df.sort_values(['dt', 'index_name', 'stock_name'])
    
    # weight 컬럼이 없으면 기본값 0 설정
    if 'weight' not in df.columns:
        df['weight'] = 0.0
    
    return df


@with_connection
def get_bm_gics_sector_weights(index_name: Optional[str] = None,
                                base_date: Optional[str] = None,
                                end_date: Optional[str] = None,
                                connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    index_constituents 테이블에서 BM GICS SECTOR별 비중과 성과를 조회하는 함수
    gics_name별로 종목 수, 비중, 성과를 계산
    
    Args:
        index_name: 지수명 (None이면 전체)
        base_date: 기준일자 (BM 성과 계산 시작일, YYYY-MM-DD 형식)
        end_date: 종료일자 (비중 표시 및 BM 성과 계산 종료일, YYYY-MM-DD 형식, None이면 최신 데이터)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: GICS SECTOR별 데이터프레임 (gics_name, stock_count, bm_weight_pct, bm_performance)
    """
    from datetime import datetime, timedelta
    from utils import get_business_day_by_country, get_index_country_code
    
    # 테이블 구조 확인
    table_info = get_table_info("index_constituents", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 필수 컬럼 확인
    if 'dt' not in column_names:
        raise ValueError("'dt' 컬럼을 찾을 수 없습니다.")
    
    # index 컬럼 찾기
    index_col = None
    for col in ['index', 'index_name', 'index_code', 'idx']:
        if col in column_names:
            index_col = col
            break
    
    if index_col is None:
        raise ValueError("index 컬럼을 찾을 수 없습니다.")
    
    # gics_name 컬럼 찾기
    gics_name_col = None
    for col in ['gics_name', 'gics_sector', 'sector', 'gics_sector_name', 'sector_name']:
        if col in column_names:
            gics_name_col = col
            break
    
    if gics_name_col is None:
        raise ValueError("gics_name 컬럼을 찾을 수 없습니다.")
    
    # index_weight 컬럼 찾기
    weight_col = None
    for col in ['index_weight', 'weight', 'weight_pct', 'weight_percent']:
        if col in column_names:
            weight_col = col
            break
    
    if weight_col is None:
        raise ValueError("index_weight 컬럼을 찾을 수 없습니다.")
    
    # 종목명 컬럼 찾기
    stock_col = None
    for col in ['stock', 'stock_name', 'ticker', 'symbol', 'name']:
        if col in column_names:
            stock_col = col
            break
    
    if stock_col is None:
        raise ValueError("종목명 컬럼을 찾을 수 없습니다.")
    
    # 기준일자에 가장 가까운 날짜 찾기
    # 최종 날짜(end_date) 확인 - 비중 표시용
    end_date_where_conditions = [
        f"{gics_name_col} IS NOT NULL",
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        end_date_where_conditions.append(f"{index_col} = '{index_name}'")
    
    if end_date:
        end_date_where_conditions.append(f"dt <= '{end_date}'")
    
    end_date_where_clause = " AND ".join(end_date_where_conditions)
    
    end_date_query = f"""
        SELECT MAX(dt) as max_dt
        FROM index_constituents
        WHERE {end_date_where_clause}
    """
    
    end_date_result = execute_custom_query(end_date_query, connection=connection)
    if not end_date_result or not end_date_result[0] or not end_date_result[0].get('max_dt'):
        return pd.DataFrame()
    
    final_date = end_date_result[0]['max_dt']
    if isinstance(final_date, str):
        final_date_obj = pd.to_datetime(final_date).date()
    elif hasattr(final_date, 'date'):
        final_date_obj = final_date.date()
    else:
        final_date_obj = final_date
    
    # 기준일자(base_date) 확인 - BM 성과 계산 시작일
    base_date_where_conditions = [
        f"{gics_name_col} IS NOT NULL",
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        base_date_where_conditions.append(f"{index_col} = '{index_name}'")
    
    if base_date:
        base_date_where_conditions.append(f"dt <= '{base_date}'")
    
    base_date_where_clause = " AND ".join(base_date_where_conditions)
    
    base_date_query = f"""
        SELECT MAX(dt) as max_dt
        FROM index_constituents
        WHERE {base_date_where_clause}
    """
    
    base_date_result = execute_custom_query(base_date_query, connection=connection)
    if not base_date_result or not base_date_result[0] or not base_date_result[0].get('max_dt'):
        return pd.DataFrame()
    
    start_date = base_date_result[0]['max_dt']
    if isinstance(start_date, str):
        start_date_obj = pd.to_datetime(start_date).date()
    elif hasattr(start_date, 'date'):
        start_date_obj = start_date.date()
    else:
        start_date_obj = start_date
    
    # 기준일자부터 최종 날짜까지의 데이터 가져오기 (BM 성과 계산용 - 가격과 비중 모두 필요)
    performance_where_conditions = [
        f"dt >= '{start_date_obj}'",
        f"dt <= '{final_date_obj}'",
        f"{gics_name_col} IS NOT NULL",
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        performance_where_conditions.append(f"{index_col} = '{index_name}'")
    
    performance_where_clause = " AND ".join(performance_where_conditions)
    
    # index_constituents 테이블에서 가격 컬럼 찾기
    price_col = None
    for col in ['local_price', 'price', 'close', 'close_price', 'value']:
        if col in column_names:
            price_col = col
            break
    
    if price_col is None:
        # 가격 컬럼이 없으면 숫자형 컬럼 중 하나 선택
        numeric_cols = [col['column_name'] for col in table_info 
                      if col['data_type'] in ['numeric', 'double precision', 'real', 'bigint', 'integer'] 
                      and col['column_name'] not in ['dt', index_col, stock_col, gics_name_col, weight_col]]
        if numeric_cols:
            price_col = numeric_cols[0]
        else:
            price_col = None
    
    # performance_query에 가격 컬럼 포함
    if price_col:
        performance_query = f"""
            SELECT 
                dt,
                {gics_name_col} as gics_name,
                {stock_col} as stock_name,
                {weight_col} as weight,
                {price_col} as price
            FROM index_constituents
            WHERE {performance_where_clause}
              AND {price_col} IS NOT NULL
              AND {price_col} > 0
            ORDER BY dt, {gics_name_col}, {stock_col}
        """
    else:
        # 가격 컬럼이 없으면 가격 없이 조회
        performance_query = f"""
            SELECT 
                dt,
                {gics_name_col} as gics_name,
                {stock_col} as stock_name,
                {weight_col} as weight
            FROM index_constituents
            WHERE {performance_where_clause}
            ORDER BY dt, {gics_name_col}, {stock_col}
        """
    
    performance_data = execute_custom_query(performance_query, connection=connection)
    performance_df = pd.DataFrame(performance_data)
    
    # dt 컬럼을 datetime으로 변환
    if not performance_df.empty and 'dt' in performance_df.columns:
        performance_df['dt'] = pd.to_datetime(performance_df['dt'])
    
    # 가격 컬럼이 없으면 None으로 추가
    if 'price' not in performance_df.columns:
        performance_df['price'] = None
    
    # 최종 날짜의 비중 데이터 가져오기 (비중 표시용)
    final_weight_where_conditions = [
        f"dt = '{final_date_obj}'",
        f"{gics_name_col} IS NOT NULL",
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        final_weight_where_conditions.append(f"{index_col} = '{index_name}'")
    
    final_weight_where_clause = " AND ".join(final_weight_where_conditions)
    
    final_weight_query = f"""
        SELECT 
            {gics_name_col} as gics_name,
            {stock_col} as stock_name,
            {weight_col} as weight
        FROM index_constituents
        WHERE {final_weight_where_clause}
        ORDER BY {gics_name_col}, {stock_col}
    """
    
    final_weight_data = execute_custom_query(final_weight_query, connection=connection)
    final_weight_df = pd.DataFrame(final_weight_data)
    
    if performance_df.empty or final_weight_df.empty:
        return pd.DataFrame()
    
    # dt 컬럼은 이미 위에서 변환됨
    performance_df = performance_df.sort_values(['dt', 'gics_name', 'stock_name'])
    
    # 날짜별로 그룹화하여 일별 섹터별 기여도 계산 (방법 3 사용)
    dates = sorted(performance_df['dt'].unique())
    sector_cumulative_performance = {}  # {gics_name: 누적 기여도}
    
    prev_date = None
    for date in sorted(dates):
        if date < pd.to_datetime(start_date_obj):
            continue
        if date > pd.to_datetime(final_date_obj):
            break
        
        # 기준일자(start_date_obj)는 건너뛰고, 그 다음 날부터 기여도 계산
        if date.date() == start_date_obj:
            # 기준일자는 초기화만 하고 기여도 계산하지 않음
            current_date_data = performance_df[performance_df['dt'] == date].copy()
            if 'price' in current_date_data.columns:
                current_date_data = current_date_data[current_date_data['price'].notna() & (current_date_data['price'] > 0) & current_date_data['weight'].notna()]
            else:
                current_date_data = current_date_data[current_date_data['weight'].notna()]
            
            if not current_date_data.empty:
                # 기준일자에 섹터별 초기화
                for gics_name in current_date_data['gics_name'].unique():
                    sector_cumulative_performance[gics_name] = 0.0
                prev_date = date
            continue
        
        current_date_data = performance_df[performance_df['dt'] == date].copy()
        # price 컬럼이 있는지 확인하고 필터링
        if 'price' in current_date_data.columns:
            current_date_data = current_date_data[current_date_data['price'].notna() & (current_date_data['price'] > 0) & current_date_data['weight'].notna()]
        else:
            # price 컬럼이 없으면 weight만 확인
            current_date_data = current_date_data[current_date_data['weight'].notna()]
        
        if current_date_data.empty:
            prev_date = date
            continue
        
        if prev_date is None:
            # 기준일자 이후 첫 날짜 (prev_date가 None이면 기준일자 데이터가 없었던 경우)
            for gics_name in current_date_data['gics_name'].unique():
                sector_cumulative_performance[gics_name] = 0.0
            prev_date = date
            continue
        
        prev_date_data = performance_df[performance_df['dt'] == prev_date].copy()
        # price 컬럼이 있는지 확인하고 필터링
        if 'price' in prev_date_data.columns:
            prev_date_data = prev_date_data[prev_date_data['price'].notna() & (prev_date_data['price'] > 0) & prev_date_data['weight'].notna()]
        else:
            # price 컬럼이 없으면 weight만 확인
            prev_date_data = prev_date_data[prev_date_data['weight'].notna()]
        
        if prev_date_data.empty:
            prev_date = date
            continue
        
        # price 컬럼이 없으면 기여도 계산 불가
        if 'price' not in current_date_data.columns or 'price' not in prev_date_data.columns:
            prev_date = date
            continue
        
        # 일별로 섹터별 기여도 계산 (방법 3: ret × 전날 비중)
        # 전날 비중과 가격, 당일 가격 병합
        prev_date_data = prev_date_data[['stock_name', 'gics_name', 'weight', 'price']].copy()
        prev_date_data.rename(columns={'weight': 'prev_weight', 'price': 'prev_price'}, inplace=True)
        
        current_date_data = current_date_data[['stock_name', 'price']].copy()
        current_date_data.rename(columns={'price': 'current_price'}, inplace=True)
        
        # 전날 비중과 당일 가격 병합
        contribution_df = prev_date_data.merge(
            current_date_data,
            on='stock_name',
            how='inner'
        )
        
        if not contribution_df.empty:
            # 각 종목별 수익률 계산: (당일 가격 - 전날 가격) / 전날 가격 × 100
            contribution_df['ret'] = ((contribution_df['current_price'].astype(float) - contribution_df['prev_price'].astype(float)) / contribution_df['prev_price'].astype(float)) * 100
            
            # 각 종목별 기여도 계산: ret × 전날 비중
            contribution_df['ret_contribution'] = contribution_df['ret'] * contribution_df['prev_weight'].astype(float)
            
            # 섹터별 기여도 합산
            sector_contributions = contribution_df.groupby('gics_name')['ret_contribution'].sum().to_dict()
            
            # 누적 기여도 업데이트
            for gics_name, sector_contribution_value in sector_contributions.items():
                if gics_name not in sector_cumulative_performance:
                    sector_cumulative_performance[gics_name] = 0.0
                
                # 일별 섹터 기여도 (%) = 섹터 ret_contribution 합
                daily_contribution = float(sector_contribution_value)
                sector_cumulative_performance[gics_name] += daily_contribution
        
        prev_date = date
    
    # 최종 날짜의 비중 정보 (비중 표시용)
    final_weight_dict = {}
    for _, row in final_weight_df.iterrows():
        stock_name = row['stock_name']
        gics_name = row['gics_name']
        weight = float(row['weight']) if pd.notna(row['weight']) else 0.0
        
        if gics_name not in final_weight_dict:
            final_weight_dict[gics_name] = {
                'stock_count': 0,
                'total_weight': 0,
                'stocks': []
            }
        
        final_weight_dict[gics_name]['stock_count'] += 1
        final_weight_dict[gics_name]['total_weight'] += weight
        final_weight_dict[gics_name]['stocks'].append(stock_name)
    
    # GICS 섹터별 집계
    sector_data = {}
    for gics_name, cumulative_performance in sector_cumulative_performance.items():
        # 최종 날짜의 비중 정보 (표시용)
        if gics_name not in final_weight_dict:
            continue
        
        weight_info = final_weight_dict[gics_name]
        
        # 섹터별 BM 성과 = 일별 기여도의 누적합
        sector_performance = cumulative_performance
        
        sector_data[gics_name] = {
            'stock_count': weight_info['stock_count'],
            'total_weight': weight_info['total_weight'],  # 최종 날짜 비중 (표시용)
            'weighted_return_sum': sector_performance
        }
    
    # DataFrame 생성
    results = []
    for gics_name, data in sector_data.items():
        # bm_performance 값 검증 및 제한
        bm_performance = data['weighted_return_sum']
        if pd.isna(bm_performance) or np.isinf(bm_performance):
            bm_performance = 0.0
        # 합리적인 범위로 제한
        bm_performance = max(-100.0, min(100.0, bm_performance))
        
        results.append({
            'gics_name': gics_name,
            'stock_count': data['stock_count'],
            'bm_weight_pct': data['total_weight'] * 100,  # 비중을 퍼센트로 변환
            'bm_performance': bm_performance  # 비중 * 수익률의 합
        })
    
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        return pd.DataFrame()
    
    # 비중 기준 정렬
    result_df = result_df.sort_values('bm_weight_pct', ascending=False)
    
    return result_df[['gics_name', 'stock_count', 'bm_weight_pct', 'bm_performance']]


@with_connection
def get_bm_stock_weights(index_name: Optional[str] = None,
                        base_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    index_constituents 테이블에서 BM 종목별 비중과 성과를 조회하는 함수
    종목별로 비중, 누적 수익률, 기여도를 계산
    
    Args:
        index_name: 지수명 (None이면 전체)
        base_date: 기준일자 (BM 성과 계산 시작일, YYYY-MM-DD 형식)
        end_date: 종료일자 (비중 표시 및 BM 성과 계산 종료일, YYYY-MM-DD 형식, None이면 최신 데이터)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 종목별 데이터프레임 (stock_name, weight_pct, cumulative_return, contribution)
    """
    from datetime import datetime, timedelta
    from utils import get_business_day_by_country, get_index_country_code
    import numpy as np
    
    # 테이블 구조 확인
    table_info = get_table_info("index_constituents", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 필수 컬럼 확인
    if 'dt' not in column_names:
        raise ValueError("'dt' 컬럼을 찾을 수 없습니다.")
    
    # index 컬럼 찾기
    index_col = None
    for col in ['index', 'index_name', 'index_code', 'idx']:
        if col in column_names:
            index_col = col
            break
    
    if index_col is None:
        raise ValueError("index 컬럼을 찾을 수 없습니다.")
    
    # index_weight 컬럼 찾기
    weight_col = None
    for col in ['index_weight', 'weight', 'weight_pct', 'weight_percent']:
        if col in column_names:
            weight_col = col
            break
    
    if weight_col is None:
        raise ValueError("index_weight 컬럼을 찾을 수 없습니다.")
    
    # 종목명 컬럼 찾기
    stock_col = None
    for col in ['stock', 'stock_name', 'ticker', 'symbol', 'name']:
        if col in column_names:
            stock_col = col
            break
    
    if stock_col is None:
        raise ValueError("종목명 컬럼을 찾을 수 없습니다.")
    
    # 기준일자에 가장 가까운 날짜 찾기
    # 최종 날짜(end_date) 확인 - 비중 표시용
    end_date_where_conditions = [
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        end_date_where_conditions.append(f"{index_col} = '{index_name}'")
    
    if end_date:
        end_date_where_conditions.append(f"dt <= '{end_date}'")
    
    end_date_where_clause = " AND ".join(end_date_where_conditions)
    
    end_date_query = f"""
        SELECT MAX(dt) as max_dt
        FROM index_constituents
        WHERE {end_date_where_clause}
    """
    
    end_date_result = execute_custom_query(end_date_query, connection=connection)
    if not end_date_result or not end_date_result[0] or not end_date_result[0].get('max_dt'):
        return pd.DataFrame()
    
    final_date = end_date_result[0]['max_dt']
    if isinstance(final_date, str):
        final_date_obj = pd.to_datetime(final_date).date()
    elif hasattr(final_date, 'date'):
        final_date_obj = final_date.date()
    else:
        final_date_obj = final_date
    
    # 기준일자(base_date) 확인 - BM 성과 계산 시작일
    base_date_where_conditions = [
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        base_date_where_conditions.append(f"{index_col} = '{index_name}'")
    
    if base_date:
        base_date_where_conditions.append(f"dt <= '{base_date}'")
    
    base_date_where_clause = " AND ".join(base_date_where_conditions)
    
    base_date_query = f"""
        SELECT MAX(dt) as max_dt
        FROM index_constituents
        WHERE {base_date_where_clause}
    """
    
    base_date_result = execute_custom_query(base_date_query, connection=connection)
    if not base_date_result or not base_date_result[0] or not base_date_result[0].get('max_dt'):
        return pd.DataFrame()
    
    start_date = base_date_result[0]['max_dt']
    if isinstance(start_date, str):
        start_date_obj = pd.to_datetime(start_date).date()
    elif hasattr(start_date, 'date'):
        start_date_obj = start_date.date()
    else:
        start_date_obj = start_date
    
    # 기준일자부터 최종 날짜까지의 데이터 가져오기 (BM 성과 계산용)
    performance_where_conditions = [
        f"dt >= '{start_date_obj}'",
        f"dt <= '{final_date_obj}'",
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        performance_where_conditions.append(f"{index_col} = '{index_name}'")
    
    performance_where_clause = " AND ".join(performance_where_conditions)
    
    performance_query = f"""
        SELECT 
            dt,
            {stock_col} as stock_name,
            {weight_col} as weight
        FROM index_constituents
        WHERE {performance_where_clause}
        ORDER BY dt, {stock_col}
    """
    
    performance_data = execute_custom_query(performance_query, connection=connection)
    performance_df = pd.DataFrame(performance_data)
    
    # dt 컬럼을 datetime으로 변환
    if not performance_df.empty:
        performance_df['dt'] = pd.to_datetime(performance_df['dt'])
    
    # stock_price 테이블에서 각 종목의 가격 가져오기 (기여도 계산용)
    stock_names_from_perf = []
    dates = []
    if not performance_df.empty:
        stock_names_from_perf = performance_df['stock_name'].unique().tolist()
        dates = sorted(performance_df['dt'].unique())
    
    if stock_names_from_perf and len(dates) > 0:
        # 날짜 범위
        start_date_str = dates[0].strftime('%Y-%m-%d')
        end_date_str = dates[-1].strftime('%Y-%m-%d')
        
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
        
        if ticker_col and price_col_stock:
            # stock_price 테이블에서 종목별 가격 조회
            stock_list = "', '".join([f"{name}" for name in stock_names_from_perf])
            price_where_conditions = [
                f"{price_col_stock} IS NOT NULL",
                f"{price_col_stock} > 0",
                f"{ticker_col} IN ('{stock_list}')",
                f"dt >= '{start_date_str}'",
                f"dt <= '{end_date_str}'"
            ]
            price_where_clause = " AND ".join(price_where_conditions)
            
            price_query = f"""
                SELECT 
                    dt,
                    {ticker_col} as stock_name,
                    {price_col_stock} as price
                FROM stock_price
                WHERE {price_where_clause}
                ORDER BY dt, {ticker_col}
            """
            
            try:
                price_data = execute_custom_query(price_query, connection=connection)
                price_df = pd.DataFrame(price_data)
                
                if not price_df.empty:
                    price_df['dt'] = pd.to_datetime(price_df['dt'])
                    price_df['dt_date'] = price_df['dt'].dt.date
                    
                    # 같은 날짜, 같은 종목에 대해 집계 (평균 가격 사용)
                    price_df_grouped = price_df.groupby(['dt_date', 'stock_name'])['price'].mean().reset_index()
                    
                    # performance_df의 가격을 stock_price에서 가져온 가격으로 교체
                    performance_df['dt_date'] = performance_df['dt'].dt.date
                    # 기존 price 컬럼이 있으면 제거 (stock_price에서 가져온 가격으로 교체)
                    if 'price' in performance_df.columns:
                        performance_df = performance_df.drop('price', axis=1)
                    performance_df = performance_df.merge(
                        price_df_grouped[['dt_date', 'stock_name', 'price']],
                        on=['dt_date', 'stock_name'],
                        how='left'
                    )
                    performance_df = performance_df.drop('dt_date', axis=1)
                    # price 컬럼이 제대로 병합되었는지 확인
                    if 'price' not in performance_df.columns:
                        performance_df['price'] = None
                else:
                    # 가격 데이터가 없으면 price 컬럼을 None으로 추가
                    if 'price' not in performance_df.columns:
                        performance_df['price'] = None
            except Exception as e:
                # stock_price 조회 실패 시 price 컬럼을 None으로 추가
                if 'price' not in performance_df.columns:
                    performance_df['price'] = None
    
    # 기준일자의 비중 데이터 가져오기 (기준일 비중 표시용)
    base_weight_where_conditions = [
        f"dt = '{start_date_obj}'",
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        base_weight_where_conditions.append(f"{index_col} = '{index_name}'")
    
    base_weight_where_clause = " AND ".join(base_weight_where_conditions)
    
    base_weight_query = f"""
        SELECT 
            {stock_col} as stock_name,
            {weight_col} as weight
        FROM index_constituents
        WHERE {base_weight_where_clause}
        ORDER BY {stock_col}
    """
    
    base_weight_data = execute_custom_query(base_weight_query, connection=connection)
    base_weight_df = pd.DataFrame(base_weight_data)
    
    if base_weight_df.empty:
        return pd.DataFrame()
    
    # stock_price 테이블에서 기준일자와 각 종목의 마지막 일자 가격 가져오기
    stock_names = base_weight_df['stock_name'].unique().tolist()
    
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
    
    # 기준일자와 최종일자의 가격 조회
    base_prices = {}  # {stock_name: 기준일자 가격}
    final_prices = {}  # {stock_name: 최종일자 가격}
    
    if ticker_col and price_col_stock and stock_names:
        stock_list = "', '".join([f"{name}" for name in stock_names])
        
        # 기준일자 가격 조회
        base_price_query = f"""
            SELECT 
                {ticker_col} as stock_name,
                {price_col_stock} as price
            FROM stock_price
            WHERE {ticker_col} IN ('{stock_list}')
              AND dt = '{start_date_obj}'
              AND {price_col_stock} IS NOT NULL
              AND {price_col_stock} > 0
            ORDER BY {ticker_col}
        """
        
        try:
            base_price_data = execute_custom_query(base_price_query, connection=connection)
            if base_price_data:
                for row in base_price_data:
                    stock_name = row.get('stock_name') or row[0]
                    price = float(row.get('price') or row[1])
                    base_prices[stock_name] = price
        except:
            pass
        
        # 각 종목의 마지막 일자 가격 조회 (end_date 이하의 가장 최근 가격)
        # 각 종목별로 마지막 일자를 찾기 위해 서브쿼리 사용
        final_price_query = f"""
            SELECT DISTINCT ON ({ticker_col})
                {ticker_col} as stock_name,
                dt,
                {price_col_stock} as price
            FROM stock_price
            WHERE {ticker_col} IN ('{stock_list}')
              AND dt <= '{final_date_obj}'
              AND {price_col_stock} IS NOT NULL
              AND {price_col_stock} > 0
            ORDER BY {ticker_col}, dt DESC
        """
        
        try:
            final_price_data = execute_custom_query(final_price_query, connection=connection)
            if final_price_data:
                for row in final_price_data:
                    stock_name = row.get('stock_name') or row[0]
                    price = float(row.get('price') or row[2] if len(row) > 2 else row[1])
                    final_prices[stock_name] = price
        except:
            pass
    
    # dt 컬럼은 이미 위에서 변환됨
    if not performance_df.empty:
        performance_df = performance_df.sort_values(['dt', 'stock_name'])
    
    # 날짜 리스트 추출
    dates = []
    if not performance_df.empty and 'dt' in performance_df.columns:
        dates = sorted(performance_df['dt'].unique())
    
    # 날짜별로 그룹화하여 일별 종목별 기여도 계산 (기준일자 제외)
    # 기준일자가 12/01, 최종날짜가 12/10이라면
    # 12/02부터 12/10까지 일별 기여성과의 누적합을 계산
    # 12/02 일별 기여성과 = (12/02 가격 - 12/01 가격) / 12/01 가격 * 100 * 12/01 비중
    # 12/03 일별 기여성과 = (12/03 가격 - 12/02 가격) / 12/02 가격 * 100 * 12/02 비중
    stock_cumulative_performance = {}  # {stock_name: 누적 기여도}
    
    # performance_df가 있는 경우에만 일별 기여도 계산
    if not performance_df.empty and len(dates) > 0:
        prev_date = None
        for date in sorted(dates):
            if date < pd.to_datetime(start_date_obj):
                continue
            if date > pd.to_datetime(final_date_obj):
                break
            
            # 기준일자(start_date_obj)는 건너뛰고, 그 다음 날부터 기여도 계산
            if date.date() == start_date_obj:
                # 기준일자는 초기화만 하고 기여도 계산하지 않음
                current_date_data = performance_df[performance_df['dt'] == date].copy()
                if 'price' in current_date_data.columns:
                    current_date_data = current_date_data[current_date_data['price'].notna() & (current_date_data['price'] > 0) & current_date_data['weight'].notna()]
                else:
                    current_date_data = current_date_data[current_date_data['weight'].notna()]
                
                if not current_date_data.empty:
                    # 기준일자에 종목별 초기화
                    for stock_name in current_date_data['stock_name'].unique():
                        stock_cumulative_performance[stock_name] = 0.0
                    prev_date = date
                continue
            
            current_date_data = performance_df[performance_df['dt'] == date].copy()
            # price 컬럼이 있는지 확인하고 필터링
            if 'price' in current_date_data.columns:
                current_date_data = current_date_data[current_date_data['price'].notna() & (current_date_data['price'] > 0) & current_date_data['weight'].notna()]
            else:
                # price 컬럼이 없으면 weight만 확인
                current_date_data = current_date_data[current_date_data['weight'].notna()]
            
            if current_date_data.empty:
                prev_date = date
                continue
            
            if prev_date is None:
                # 기준일자 이후 첫 날짜 (prev_date가 None이면 기준일자 데이터가 없었던 경우)
                for stock_name in current_date_data['stock_name'].unique():
                    stock_cumulative_performance[stock_name] = 0.0
                prev_date = date
                continue
            
            prev_date_data = performance_df[performance_df['dt'] == prev_date].copy()
            # price 컬럼이 있는지 확인하고 필터링
            if 'price' in prev_date_data.columns:
                prev_date_data = prev_date_data[prev_date_data['price'].notna() & (prev_date_data['price'] > 0) & prev_date_data['weight'].notna()]
            else:
                # price 컬럼이 없으면 weight만 확인
                prev_date_data = prev_date_data[prev_date_data['weight'].notna()]
            
            if prev_date_data.empty:
                prev_date = date
                continue
            
            # price 컬럼이 없으면 기여도 계산 불가
            if 'price' not in current_date_data.columns or 'price' not in prev_date_data.columns:
                prev_date = date
                continue
            
            # 일별로 종목별 기여도 계산 (방법 3: ret × 전날 비중)
            prev_date_data = prev_date_data[['stock_name', 'weight', 'price']].copy()
            prev_date_data.rename(columns={'weight': 'prev_weight', 'price': 'prev_price'}, inplace=True)
            
            current_date_data = current_date_data[['stock_name', 'price']].copy()
            current_date_data.rename(columns={'price': 'current_price'}, inplace=True)
            
            # 전날 비중과 당일 가격 병합
            contribution_df = prev_date_data.merge(
                current_date_data,
                on='stock_name',
                how='inner'
            )
            
            if not contribution_df.empty:
                # 각 종목별 수익률 계산: (당일 가격 - 전날 가격) / 전날 가격 × 100
                contribution_df['ret'] = ((contribution_df['current_price'].astype(float) - contribution_df['prev_price'].astype(float)) / contribution_df['prev_price'].astype(float)) * 100
                
                # 각 종목별 기여도 계산: ret × 전날 비중
                contribution_df['ret_contribution'] = contribution_df['ret'] * contribution_df['prev_weight'].astype(float)
                
                # 종목별 기여도 업데이트
                for _, row in contribution_df.iterrows():
                    stock_name = row['stock_name']
                    ret_contribution = float(row['ret_contribution'])
                    
                    if stock_name not in stock_cumulative_performance:
                        stock_cumulative_performance[stock_name] = 0.0
                    
                    # 누적 기여도 업데이트
                    stock_cumulative_performance[stock_name] += ret_contribution
            
            prev_date = date
    
    # DataFrame 생성: 종목명 / 기준일 비중 / 기간 수익률 / 기여성과
    results = []
    for _, row in base_weight_df.iterrows():
        stock_name = row['stock_name']
        base_weight = float(row['weight']) if pd.notna(row['weight']) else 0.0
        
        # 기간 수익률 계산: (마지막 일자 가격 - 기준일자 가격) / 기준일자 가격 * 100
        base_price = base_prices.get(stock_name)
        final_price = final_prices.get(stock_name)
        
        period_return = 0.0
        if base_price and final_price and base_price > 0:
            period_return = ((final_price - base_price) / base_price) * 100
        
        # 기여성과: 기준일자를 제외한 누적 기여도
        contribution = stock_cumulative_performance.get(stock_name, 0.0)
        
        # 값 검증 및 제한
        if pd.isna(period_return) or np.isinf(period_return):
            period_return = 0.0
        period_return = max(-100.0, min(100.0, period_return))
        
        if pd.isna(contribution) or np.isinf(contribution):
            contribution = 0.0
        contribution = max(-100.0, min(100.0, contribution))
        
        results.append({
            'stock_name': stock_name,
            'base_weight_pct': base_weight * 100,  # 기준일 비중을 퍼센트로 변환
            'period_return': period_return,  # 기간 수익률
            'contribution': contribution  # 기여성과
        })
    
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        return pd.DataFrame()
    
    # 기준일 비중 기준 정렬
    result_df = result_df.sort_values('base_weight_pct', ascending=False)
    
    return result_df[['stock_name', 'base_weight_pct', 'period_return', 'contribution']]


@with_connection
def get_daily_sector_contributions(index_name: Optional[str] = None,
                                   base_date: Optional[str] = None,
                                   end_date: Optional[str] = None,
                                   connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    일자별 섹터별 기여도를 계산하는 함수 (방법 3 사용)
    
    Args:
        index_name: 지수명 (None이면 전체)
        base_date: 기준일자 (BM 성과 계산 시작일, YYYY-MM-DD 형식)
        end_date: 종료일자 (BM 성과 계산 종료일, YYYY-MM-DD 형식, None이면 최신 데이터)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 일자별 섹터별 기여도 (dt, gics_name, daily_contribution, cumulative_contribution)
    """
    from datetime import datetime, timedelta
    from utils import get_business_day_by_country, get_index_country_code
    
    # 테이블 구조 확인
    table_info = get_table_info("index_constituents", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 필수 컬럼 확인
    if 'dt' not in column_names:
        raise ValueError("'dt' 컬럼을 찾을 수 없습니다.")
    
    # index 컬럼 찾기
    index_col = None
    for col in ['index', 'index_name', 'index_code', 'idx']:
        if col in column_names:
            index_col = col
            break
    
    if index_col is None:
        raise ValueError("index 컬럼을 찾을 수 없습니다.")
    
    # gics_name 컬럼 찾기
    gics_name_col = None
    for col in ['gics_name', 'gics_sector', 'sector', 'gics_sector_name', 'sector_name']:
        if col in column_names:
            gics_name_col = col
            break
    
    if gics_name_col is None:
        raise ValueError("gics_name 컬럼을 찾을 수 없습니다.")
    
    # index_weight 컬럼 찾기
    weight_col = None
    for col in ['index_weight', 'weight', 'weight_pct', 'weight_percent']:
        if col in column_names:
            weight_col = col
            break
    
    if weight_col is None:
        raise ValueError("index_weight 컬럼을 찾을 수 없습니다.")
    
    # 가격 컬럼 자동 감지 (local_price 우선)
    price_candidates = ['local_price', 'price', 'close', 'close_price', 'value', 'nav']
    price_col = None
    for col in price_candidates:
        if col in column_names:
            price_col = col
            break
    
    if price_col is None:
        raise ValueError("가격 컬럼을 찾을 수 없습니다.")
    
    # 종목명 컬럼 찾기
    stock_col = None
    for col in ['stock', 'stock_name', 'ticker', 'symbol', 'name']:
        if col in column_names:
            stock_col = col
            break
    
    if stock_col is None:
        raise ValueError("종목명 컬럼을 찾을 수 없습니다.")
    
    # 기준일자에 가장 가까운 날짜 찾기
    # 최종 날짜(end_date) 확인
    end_date_where_conditions = [
        f"{gics_name_col} IS NOT NULL",
        f"{weight_col} IS NOT NULL",
        f"{price_col} IS NOT NULL",
        f"{price_col} > 0"
    ]
    
    if index_name:
        end_date_where_conditions.append(f"{index_col} = '{index_name}'")
    
    if end_date:
        end_date_where_conditions.append(f"dt <= '{end_date}'")
    
    end_date_where_clause = " AND ".join(end_date_where_conditions)
    
    end_date_query = f"""
        SELECT MAX(dt) as max_dt
        FROM index_constituents
        WHERE {end_date_where_clause}
    """
    
    end_date_result = execute_custom_query(end_date_query, connection=connection)
    if not end_date_result or not end_date_result[0] or not end_date_result[0].get('max_dt'):
        return pd.DataFrame()
    
    final_date = end_date_result[0]['max_dt']
    if isinstance(final_date, str):
        final_date_obj = pd.to_datetime(final_date).date()
    elif hasattr(final_date, 'date'):
        final_date_obj = final_date.date()
    else:
        final_date_obj = final_date
    
    # 기준일자(base_date) 확인
    base_date_where_conditions = [
        f"{gics_name_col} IS NOT NULL",
        f"{price_col} IS NOT NULL",
        f"{price_col} > 0"
    ]
    
    if index_name:
        base_date_where_conditions.append(f"{index_col} = '{index_name}'")
    
    if base_date:
        base_date_where_conditions.append(f"dt <= '{base_date}'")
    
    base_date_where_clause = " AND ".join(base_date_where_conditions)
    
    base_date_query = f"""
        SELECT MAX(dt) as max_dt
        FROM index_constituents
        WHERE {base_date_where_clause}
    """
    
    base_date_result = execute_custom_query(base_date_query, connection=connection)
    if not base_date_result or not base_date_result[0] or not base_date_result[0].get('max_dt'):
        return pd.DataFrame()
    
    start_date = base_date_result[0]['max_dt']
    if isinstance(start_date, str):
        start_date_obj = pd.to_datetime(start_date).date()
    elif hasattr(start_date, 'date'):
        start_date_obj = start_date.date()
    else:
        start_date_obj = start_date
    
    # 기준일자부터 최종 날짜까지의 데이터 가져오기
    performance_where_conditions = [
        f"dt >= '{start_date_obj}'",
        f"dt <= '{final_date_obj}'",
        f"{gics_name_col} IS NOT NULL",
        f"{price_col} IS NOT NULL",
        f"{price_col} > 0",
        f"{weight_col} IS NOT NULL"
    ]
    
    if index_name:
        performance_where_conditions.append(f"{index_col} = '{index_name}'")
    
    performance_where_clause = " AND ".join(performance_where_conditions)
    
    performance_query = f"""
        SELECT 
            dt,
            {gics_name_col} as gics_name,
            {stock_col} as stock_name,
            {weight_col} as weight,
            {price_col} as price
        FROM index_constituents
        WHERE {performance_where_clause}
        ORDER BY dt, {gics_name_col}, {stock_col}
    """
    
    performance_data = execute_custom_query(performance_query, connection=connection)
    performance_df = pd.DataFrame(performance_data)
    
    # dt 컬럼을 datetime으로 변환
    if not performance_df.empty and 'dt' in performance_df.columns:
        performance_df['dt'] = pd.to_datetime(performance_df['dt'])
    
    # stock_price 테이블에서 각 종목의 가격 가져오기
    if not performance_df.empty and 'stock_name' in performance_df.columns:
        stock_names = performance_df['stock_name'].unique().tolist()
        dates = sorted(performance_df['dt'].unique())
        
        if stock_names and len(dates) > 0:
            # 날짜 범위
            start_date_str = dates[0].strftime('%Y-%m-%d')
            end_date_str = dates[-1].strftime('%Y-%m-%d')
            
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
            
            if ticker_col and price_col_stock:
                # stock_price 테이블에서 종목별 가격 조회
                stock_list = "', '".join([f"{name}" for name in stock_names])
                price_where_conditions = [
                    f"{price_col_stock} IS NOT NULL",
                    f"{price_col_stock} > 0",
                    f"{ticker_col} IN ('{stock_list}')",
                    f"dt >= '{start_date_str}'",
                    f"dt <= '{end_date_str}'"
                ]
                price_where_clause = " AND ".join(price_where_conditions)
                
                price_query = f"""
                    SELECT 
                        dt,
                        {ticker_col} as stock_name,
                        {price_col_stock} as price
                    FROM stock_price
                    WHERE {price_where_clause}
                    ORDER BY dt, {ticker_col}
                """
                
                try:
                    price_data = execute_custom_query(price_query, connection=connection)
                    price_df = pd.DataFrame(price_data)
                    
                    if not price_df.empty:
                        price_df['dt'] = pd.to_datetime(price_df['dt'])
                        price_df['dt_date'] = price_df['dt'].dt.date
                        
                        # 같은 날짜, 같은 종목에 대해 집계 (평균 가격 사용)
                        price_df = price_df.groupby(['dt_date', 'stock_name'])['price'].mean().reset_index()
                        price_df.rename(columns={'dt_date': 'dt'}, inplace=True)
                        
                        # performance_df의 가격을 stock_price에서 가져온 가격으로 교체
                        performance_df['dt_date'] = performance_df['dt'].dt.date
                        performance_df = performance_df.merge(
                            price_df[['dt', 'stock_name', 'price']],
                            on=['dt', 'stock_name'],
                            how='left',
                            suffixes=('_old', '')
                        )
                        # stock_price에서 가격을 찾지 못한 경우 기존 가격 유지
                        if 'price_old' in performance_df.columns:
                            performance_df['price'] = performance_df['price'].fillna(performance_df['price_old'])
                            performance_df = performance_df.drop('price_old', axis=1)
                        performance_df = performance_df.drop('dt_date', axis=1)
                except Exception as e:
                    # stock_price 조회 실패 시 기존 가격 사용
                    pass
    
    if performance_df.empty:
        return pd.DataFrame()
    
    # dt 컬럼은 이미 위에서 변환됨
    performance_df = performance_df.sort_values(['dt', 'gics_name', 'stock_name'])
    
    # 날짜별로 그룹화하여 일별 섹터별 기여도 계산 (방법 3 사용)
    dates = sorted(performance_df['dt'].unique())
    daily_contributions = []  # [{dt, gics_name, daily_contribution, cumulative_contribution}]
    sector_cumulative_contribution = {}  # {gics_name: 누적 기여도}
    
    prev_date = None
    for date in sorted(dates):
        if date < pd.to_datetime(start_date_obj):
            continue
        if date > pd.to_datetime(final_date_obj):
            break
        
        # 기준일자(start_date_obj)는 건너뛰고, 그 다음 날부터 기여도 계산
        if date.date() == start_date_obj:
            # 기준일자는 초기화만 하고 기여도 계산하지 않음 (표시하지 않음)
            current_date_data = performance_df[performance_df['dt'] == date].copy()
            current_date_data = current_date_data[current_date_data['price'].notna() & (current_date_data['price'] > 0) & current_date_data['weight'].notna()]
            
            if not current_date_data.empty:
                # 기준일자에 섹터별 초기화만 수행 (daily_contributions에 추가하지 않음)
                for gics_name in current_date_data['gics_name'].unique():
                    sector_cumulative_contribution[gics_name] = 0.0
                prev_date = date
            continue
        
        current_date_data = performance_df[performance_df['dt'] == date].copy()
        current_date_data = current_date_data[current_date_data['price'].notna() & (current_date_data['price'] > 0) & current_date_data['weight'].notna()]
        
        if current_date_data.empty:
            prev_date = date
            continue
        
        if prev_date is None:
            # 기준일자 이후 첫 날짜 (prev_date가 None이면 기준일자 데이터가 없었던 경우)
            for gics_name in current_date_data['gics_name'].unique():
                sector_cumulative_contribution[gics_name] = 0.0
                daily_contributions.append({
                    'dt': date,
                    'gics_name': gics_name,
                    'daily_contribution': 0.0,
                    'cumulative_contribution': 0.0
                })
            prev_date = date
            continue
        
        prev_date_data = performance_df[performance_df['dt'] == prev_date].copy()
        prev_date_data = prev_date_data[prev_date_data['price'].notna() & (prev_date_data['price'] > 0) & prev_date_data['weight'].notna()]
        
        if prev_date_data.empty:
            prev_date = date
            continue
        
        # 일별로 섹터별 기여도 계산 (방법 3: ret × 전날 비중)
        # 전날 비중과 가격, 당일 가격 병합
        prev_date_data = prev_date_data[['stock_name', 'gics_name', 'weight', 'price']].copy()
        prev_date_data.rename(columns={'weight': 'prev_weight', 'price': 'prev_price'}, inplace=True)
        
        current_date_data = current_date_data[['stock_name', 'price']].copy()
        current_date_data.rename(columns={'price': 'current_price'}, inplace=True)
        
        # 전날 비중과 당일 가격 병합
        contribution_df = prev_date_data.merge(
            current_date_data,
            on='stock_name',
            how='inner'
        )
        
        if not contribution_df.empty:
            # 각 종목별 수익률 계산: (당일 가격 - 전날 가격) / 전날 가격 × 100
            contribution_df['ret'] = ((contribution_df['current_price'].astype(float) - contribution_df['prev_price'].astype(float)) / contribution_df['prev_price'].astype(float)) * 100
            
            # 각 종목별 기여도 계산: ret × 전날 비중
            contribution_df['ret_contribution'] = contribution_df['ret'] * contribution_df['prev_weight'].astype(float)
            
            # 섹터별 기여도 합산
            sector_contributions = contribution_df.groupby('gics_name')['ret_contribution'].sum().to_dict()
            
            # 누적 기여도 업데이트
            for gics_name, sector_contribution_value in sector_contributions.items():
                if gics_name not in sector_cumulative_contribution:
                    sector_cumulative_contribution[gics_name] = 0.0
                
                # 일별 섹터 기여도 (%) = 섹터 ret_contribution 합
                daily_contribution = float(sector_contribution_value)
                sector_cumulative_contribution[gics_name] += daily_contribution
                
                daily_contributions.append({
                    'dt': date,
                    'gics_name': gics_name,
                    'daily_contribution': daily_contribution,
                    'cumulative_contribution': sector_cumulative_contribution[gics_name]
                })
        
        prev_date = date
    
    result_df = pd.DataFrame(daily_contributions)
    if result_df.empty:
        return pd.DataFrame()
    
    return result_df.sort_values(['dt', 'gics_name'])


@with_connection
def get_price_major_index_for_comparison(
    fetch_start_date: str,
    end_date_str: str,
    ticker_list: List[str],
    connection: Optional[Connection] = None,
) -> List[Dict[str, Any]]:
    """
    지수별 수익률 비교용 price_major_index 조회. 스키마(public/market) 자동 해석.
    """
    schema = _resolve_table_schema("price_major_index", connection)
    table_ref = f'"{schema}"."price_major_index"' if schema != "public" else "price_major_index"
    tickers_str = "', '".join(ticker_list)
    where_clause = (
        "value IS NOT NULL AND (lower(value_type) = 'price') "
        f"AND dt >= '{fetch_start_date}' AND dt <= '{end_date_str}' "
        f"AND ticker IN ('{tickers_str}')"
    )
    query = f"""
        SELECT dt, ticker as index_name, value as price
        FROM {table_ref}
        WHERE {where_clause}
        ORDER BY dt, ticker
    """
    return execute_custom_query(query, connection=connection)


@with_connection
def get_major_indices_raw_data(start_date: Optional[str] = None,
                               end_date: Optional[str] = None,
                               connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    price_major_index 테이블에서 주요 지수들의 원시 가격 데이터를 조회하는 함수
    (index_constituents에 있는 index만 대상)
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD 형식, None이면 제한 없음)
        end_date: 종료 날짜 (YYYY-MM-DD 형식, None이면 제한 없음)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 지수별 가격 데이터프레임 (dt, index_name, price)
    """
    try:
        # 먼저 index_constituents 테이블에서 고유한 index 목록 가져오기
        table_info = get_table_info("index_constituents", connection=connection)
        column_names = [col['column_name'] for col in table_info]
        
        # index 컬럼 찾기
        index_col = None
        for col in ['index', 'index_name', 'index_code', 'idx']:
            if col in column_names:
                index_col = col
                break
        
        if index_col is None:
            return pd.DataFrame()
        
        # index_constituents에서 고유한 index 목록 조회
        index_list_query = f"""
            SELECT DISTINCT {index_col} as index_name
            FROM index_constituents
            WHERE {index_col} IS NOT NULL
        """
        
        index_list_data = execute_custom_query(index_list_query, connection=connection)
        index_list_df = pd.DataFrame(index_list_data)
        
        if index_list_df.empty:
            return pd.DataFrame()
        
        # index 목록을 set으로 변환 (빠른 조회를 위해)
        available_indices_set = set(index_list_df['index_name'].astype(str).tolist())
        
        if not available_indices_set:
            return pd.DataFrame()
        
        # price_major_index 테이블에서 모든 데이터 조회 (날짜 조건만 적용)
        where_conditions = [
            "value IS NOT NULL",
            "value_type = 'price'"
        ]
        
        if start_date:
            where_conditions.append(f"dt >= '{start_date}'")
        if end_date:
            where_conditions.append(f"dt <= '{end_date}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            SELECT 
                dt,
                ticker as index_name,
                value as price
            FROM price_major_index
            WHERE {where_clause}
            ORDER BY dt, ticker
        """
        
        data = execute_custom_query(query, connection=connection)
        df = pd.DataFrame(data)
        
        if df.empty:
            return pd.DataFrame()
        
        df['dt'] = pd.to_datetime(df['dt'])
        
        # index_constituents에 있는 index만 필터링
        # index_name을 문자열로 변환하여 비교 (공백 제거)
        df['index_name_str'] = df['index_name'].astype(str).str.strip()
        available_indices_set_clean = {str(idx).strip() for idx in available_indices_set}
        
        # 필터링 전에 실제로 매칭되는 index 확인
        df_filtered = df[df['index_name_str'].isin(available_indices_set_clean)].copy()
        
        if df_filtered.empty:
            # 필터링 결과가 비어있으면, 원본 데이터의 index_name 목록과 available_indices_set을 비교
            # 디버깅을 위해 실제로 어떤 index가 있는지 확인
            actual_indices = set(df['index_name_str'].unique())
            missing_in_price = sorted(available_indices_set_clean - actual_indices)
            extra_in_price = sorted(actual_indices - available_indices_set_clean)
            
            # 만약 매칭되는 것이 하나도 없으면, 필터링 없이 반환 (임시 조치)
            if not actual_indices.intersection(available_indices_set_clean):
                print(f"WARNING: index_constituents와 price_major_index의 index가 매칭되지 않습니다.")
                print(f"  price_major_index에 있는 index: {sorted(actual_indices)}")
                print(f"  index_constituents에 있는 index: {sorted(available_indices_set_clean)}")
                # 필터링 없이 반환 (사용자가 PRICE_INDEX에 데이터가 다 있다고 했으므로)
                df_result = df.drop(columns=['index_name_str']).sort_values(['dt', 'index_name'])
                return df_result
        
        # index_name_str 컬럼 제거
        df_filtered = df_filtered.drop(columns=['index_name_str'])
        df_filtered = df_filtered.sort_values(['dt', 'index_name'])
        
        return df_filtered
    except Exception as e:
        print(f"Error in get_major_indices_raw_data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


@with_connection
def get_major_indices_returns(start_date: str,
                              end_date: str,
                              connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    price_major_index 테이블에서 주요 지수들의 누적 수익률을 계산하는 함수
    (performance_주요지수.py의 1D 로직과 동일하게 처리)
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD 형식)
        end_date: 종료 날짜 (YYYY-MM-DD 형식)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 지수별 가격 및 누적 수익률 데이터프레임 (dt, index_name, price, cumulative_return)
    """
    # 고정된 9개 지수 목록 (차트에 표시되는 지수들)
    available_indices = [
        'SPX-SPX',
        'SPHYDA-USA',
        'NDX-USA',
        'ESX-STX',
        'HSCEI-HKX',
        'NSENIF-NSE',
        'VN30-STC',
        'NIK-NKX',
        'KOSPI-KRX'
    ]
    
    # price_major_index에서 해당 기간 데이터 가져오기 (시작일 이전 데이터도 필요)
    from datetime import datetime, timedelta
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
    # 긴 기간(YTD, 1Y 등)에서 시작일 이전 가격 확보를 위해 넉넉히 조회
    extended_start_date = (start_date_obj - timedelta(days=800)).strftime('%Y-%m-%d')
    
    where_conditions = [
        "value IS NOT NULL",
        "value_type = 'price'"
    ]
    where_conditions.append(f"dt >= '{extended_start_date}'")
    where_conditions.append(f"dt <= '{end_date}'")
    
    where_clause = " AND ".join(where_conditions)
    
    query = f"""
        SELECT 
            dt,
            ticker as index_name,
            value as price
        FROM price_major_index
        WHERE {where_clause}
        ORDER BY dt, ticker
    """
    
    data = execute_custom_query(query, connection=connection)
    df = pd.DataFrame(data)
    
    if df.empty:
        return pd.DataFrame()
    
    df['dt'] = pd.to_datetime(df['dt'])
    df['index_name'] = df['index_name'].astype(str).str.strip()
    
    # index_constituents에 있는 index만 필터링
    df = df[df['index_name'].isin(available_indices)].copy()
    
    if df.empty:
        return pd.DataFrame()
    
    # 각 지수별로 누적 수익률 계산
    # YTD 통일: 1월 1일이 시작일이면 "연말(전년 12/31) 종가"부터 계산 (지수별 수익률 비교 테이블과 동일)
    start_dt = pd.to_datetime(start_date)
    if start_date_obj.month == 1 and start_date_obj.day == 1:
        effective_start = start_date_obj - timedelta(days=1)
        start_dt = pd.to_datetime(effective_start)
    end_dt = pd.to_datetime(end_date)
    results = []
    
    # 고정된 8개 지수에 대해 모두 처리
    for index_name in available_indices:
        index_data = df[df['index_name'] == index_name].copy()
        index_data = index_data.sort_values('dt')
        
        if index_data.empty:
            continue
        
        # 시작일 이하의 데이터 중 가장 가까운 데이터 찾기 (YTD면 effective_start=전일)
        start_data = index_data[index_data['dt'] <= start_dt]
        
        if start_data.empty:
            # 시작일 이후의 첫 번째 데이터를 기준으로 사용
            start_data = index_data[index_data['dt'] >= start_dt]
            if start_data.empty:
                continue
            base_price = float(start_data.iloc[0]['price'])
            base_date = start_data.iloc[0]['dt']
        else:
            base_price = float(start_data.iloc[-1]['price'])
            base_date = start_data.iloc[-1]['dt']
        
        # base_date 이후이고 end_date 이하인 데이터만 필터링
        filtered_data = index_data[
            (index_data['dt'] >= base_date) & (index_data['dt'] <= end_dt)
        ].copy()
        
        if not filtered_data.empty:
            # 누적 수익률 계산
            filtered_data['cumulative_return'] = (
                (filtered_data['price'].astype(float) - base_price) / base_price * 100
            )
            
            # 기준일의 수익률은 0으로 설정
            filtered_data.loc[filtered_data['dt'] == base_date, 'cumulative_return'] = 0.0
            
            results.append(filtered_data)
    
    if not results:
        return pd.DataFrame()
    
    result_df = pd.concat(results, ignore_index=True)
    result_df = result_df.sort_values(['dt', 'index_name'])
    
    return result_df


@with_connection
def get_sector_returns_by_region(start_date: str,
                                end_date: str,
                                region: str,
                                connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    PRICE_INDEX 테이블에서 지역별 섹터 지수들의 가격 데이터를 조회하는 함수
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD 형식)
        end_date: 종료 날짜 (YYYY-MM-DD 형식)
        region: 지역 ('US', 'Europe', 'Japan')
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 섹터별 가격 데이터프레임 (dt, sector, price)
    """
    # 지역별 섹터 지수 매핑
    region_sectors = {
        'US': ['SPX Index', 'NDX Index', 'RTY Index'],
        'Europe': ['SX5E Index', 'SXXP Index'],
        'Japan': ['NKY Index', 'TPX Index']
    }
    
    if region not in region_sectors:
        return pd.DataFrame()
    
    sector_tickers = region_sectors[region]
    ticker_list = "', '".join(sector_tickers)
    
    where_conditions = [
        "value IS NOT NULL",
        "value_type = 'price'",
        f"ticker IN ('{ticker_list}')",
        f"dt >= '{start_date}'",
        f"dt <= '{end_date}'"
    ]
    where_clause = " AND ".join(where_conditions)
    
    query = f"""
        SELECT 
            dt,
            ticker as sector,
            value as price
        FROM price_index
        WHERE {where_clause}
        ORDER BY dt, ticker
    """
    
    try:
        data = execute_custom_query(query, connection=connection)
        df = pd.DataFrame(data)
        
        if df.empty:
            return pd.DataFrame()
        
        df['dt'] = pd.to_datetime(df['dt'])
        df = df.sort_values(['dt', 'sector'])
        
        return df
    except Exception as e:
        print(f"Error in get_sector_returns_by_region: {e}")
        return pd.DataFrame()


@with_connection
def get_gics_sector_returns(start_date: str,
                           end_date: str,
                           connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    index_constituents 테이블에서 GICS 섹터별 수익률을 계산하는 함수
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD 형식)
        end_date: 종료 날짜 (YYYY-MM-DD 형식)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 섹터별 수익률 데이터프레임 (sector, return, start_price, end_price, start_date, end_date)
    """
    # 테이블 구조 확인
    table_info = get_table_info("index_constituents", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 필수 컬럼 확인
    if 'dt' not in column_names:
        raise ValueError("'dt' 컬럼을 찾을 수 없습니다.")
    
    # gics_name 컬럼 찾기
    gics_name_col = None
    for col in ['gics_name', 'gics_sector', 'sector', 'gics_sector_name', 'sector_name']:
        if col in column_names:
            gics_name_col = col
            break
    
    if gics_name_col is None:
        return pd.DataFrame()
    
    # 가격 컬럼 찾기
    price_candidates = ['local_price', 'price', 'close', 'close_price', 'value']
    price_column = None
    for col in price_candidates:
        if col in column_names:
            price_column = col
            break
    
    if price_column is None:
        return pd.DataFrame()
    
    # 데이터 조회
    query = f"""
        SELECT 
            dt,
            {gics_name_col} as sector,
            {price_column} as price
        FROM index_constituents
        WHERE {price_column} IS NOT NULL
          AND dt >= '{start_date}'
          AND dt <= '{end_date}'
        ORDER BY dt, {gics_name_col}
    """
    
    try:
        data = execute_custom_query(query, connection=connection)
        df = pd.DataFrame(data)
        
        if df.empty:
            return pd.DataFrame()
        
        df['dt'] = pd.to_datetime(df['dt'])
        df = df.sort_values(['dt', 'sector'])
        
        # 섹터별 수익률 계산
        results = []
        for sector in df['sector'].unique():
            sector_data = df[df['sector'] == sector].copy()
            sector_data = sector_data.sort_values('dt')
            
            if len(sector_data) < 2:
                continue
            
            # 시작일과 종료일의 평균 가격 사용
            start_data = sector_data[sector_data['dt'] <= pd.to_datetime(start_date)]
            end_data = sector_data[sector_data['dt'] <= pd.to_datetime(end_date)]
            
            if start_data.empty or end_data.empty:
                continue
            
            start_price = float(start_data.iloc[-1]['price'])
            end_price = float(end_data.iloc[-1]['price'])
            start_dt = start_data.iloc[-1]['dt']
            end_dt = end_data.iloc[-1]['dt']
            
            if start_price > 0:
                return_pct = ((end_price - start_price) / start_price) * 100
            else:
                return_pct = 0.0
            
            results.append({
                'sector': sector,
                'return': return_pct,
                'start_price': start_price,
                'end_price': end_price,
                'start_date': start_dt,
                'end_date': end_dt
            })
        
        result_df = pd.DataFrame(results)
        if result_df.empty:
            return pd.DataFrame()
        
        result_df = result_df.sort_values('return', ascending=False)
        return result_df
    except Exception as e:
        print(f"Error in get_gics_sector_returns: {e}")
        return pd.DataFrame()


@with_connection
def get_top_bottom_stocks(start_date: str,
                         end_date: str,
                         top_n: int = 10,
                         connection: Optional[Connection] = None) -> Dict[str, pd.DataFrame]:
    """
    stock_price 테이블에서 수익률 상위/하위 종목을 조회하는 함수
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD 형식)
        end_date: 종료 날짜 (YYYY-MM-DD 형식)
        top_n: 상위/하위 N개
        connection: 데이터베이스 연결 객체
    
    Returns:
        Dict[str, pd.DataFrame]: {'top': DataFrame, 'bottom': DataFrame}
        각 DataFrame은 stock_name, return, start_price, end_price, start_date, end_date 컬럼 포함
    """
    # stock_price 테이블 구조 확인
    table_info = get_table_info("stock_price", connection=connection)
    column_names = [col['column_name'] for col in table_info]
    
    # 필수 컬럼 확인
    if 'dt' not in column_names:
        return {'top': pd.DataFrame(), 'bottom': pd.DataFrame()}
    
    # ticker 컬럼 찾기
    ticker_col = None
    for col in ['ticker', 'stock_name', 'stock', 'symbol', 'name']:
        if col in column_names:
            ticker_col = col
            break
    
    if ticker_col is None:
        return {'top': pd.DataFrame(), 'bottom': pd.DataFrame()}
    
    # price 컬럼 찾기
    price_col = None
    for col in ['price', 'close', 'close_price', 'value']:
        if col in column_names:
            price_col = col
            break
    
    if price_col is None:
        return {'top': pd.DataFrame(), 'bottom': pd.DataFrame()}
    
    # 데이터 조회
    query = f"""
        SELECT 
            dt,
            {ticker_col} as stock_name,
            {price_col} as price
        FROM stock_price
        WHERE {price_col} IS NOT NULL
          AND dt >= '{start_date}'
          AND dt <= '{end_date}'
        ORDER BY dt, {ticker_col}
    """
    
    try:
        data = execute_custom_query(query, connection=connection)
        df = pd.DataFrame(data)
        
        if df.empty:
            return {'top': pd.DataFrame(), 'bottom': pd.DataFrame()}
        
        df['dt'] = pd.to_datetime(df['dt'])
        df = df.sort_values(['dt', 'stock_name'])
        
        # 종목별 수익률 계산
        results = []
        for stock_name in df['stock_name'].unique():
            stock_data = df[df['stock_name'] == stock_name].copy()
            stock_data = stock_data.sort_values('dt')
            
            if len(stock_data) < 2:
                continue
            
            # 시작일과 종료일의 가격
            start_data = stock_data[stock_data['dt'] <= pd.to_datetime(start_date)]
            end_data = stock_data[stock_data['dt'] <= pd.to_datetime(end_date)]
            
            if start_data.empty or end_data.empty:
                continue
            
            start_price = float(start_data.iloc[-1]['price'])
            end_price = float(end_data.iloc[-1]['price'])
            start_dt = start_data.iloc[-1]['dt']
            end_dt = end_data.iloc[-1]['dt']
            
            if start_price > 0:
                return_pct = ((end_price - start_price) / start_price) * 100
            else:
                return_pct = 0.0
            
            results.append({
                'stock_name': stock_name,
                'return': return_pct,
                'start_price': start_price,
                'end_price': end_price,
                'start_date': start_dt,
                'end_date': end_dt
            })
        
        result_df = pd.DataFrame(results)
        if result_df.empty:
            return {'top': pd.DataFrame(), 'bottom': pd.DataFrame()}
        
        # 상위/하위 정렬
        result_df = result_df.sort_values('return', ascending=False)
        top_df = result_df.head(top_n).copy()
        bottom_df = result_df.tail(top_n).copy()
        bottom_df = bottom_df.sort_values('return', ascending=True)
        
        return {'top': top_df, 'bottom': bottom_df}
    except Exception as e:
        print(f"Error in get_top_bottom_stocks: {e}")
        return {'top': pd.DataFrame(), 'bottom': pd.DataFrame()}


@with_connection
def get_52w_high_stocks(ref_date, connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    stock_price에서 기준일 기준 52주 신고가 종목 목록 반환.
    반환 컬럼: 종목코드, 종목명, 업종, 일별등락률(%).
    """
    table_info = get_table_info("stock_price", connection=connection)
    col_names = [c["column_name"] for c in table_info]
    if "dt" not in col_names:
        return pd.DataFrame()
    ticker_col = next((c for c in ["ticker", "stock_code", "code", "symbol"] if c in col_names), None)
    name_col = next((c for c in ["stock_name", "name", "종목명"] if c in col_names), None)
    price_col = next((c for c in ["price", "close", "close_price"] if c in col_names), None)
    sector_col = next((c for c in ["업종", "sector", "industry", "gics_name"] if c in col_names), None)
    if not ticker_col or not price_col:
        return pd.DataFrame()
    end_str = ref_date.strftime("%Y-%m-%d") if hasattr(ref_date, "strftime") else str(ref_date)[:10]
    # 52주 = 1년: 기준일에서 1Y 전(365일)부터 기준일까지 구간으로 신고가 산출
    start_str = (ref_date - timedelta(days=365)).strftime("%Y-%m-%d")
    query = f"""
        SELECT dt, "{ticker_col}" AS ticker, "{price_col}" AS price
    """
    if name_col:
        query += f', "{name_col}" AS name'
    else:
        query += ", NULL::text AS name"
    if sector_col:
        query += f', "{sector_col}" AS sector'
    else:
        query += ", NULL::text AS sector"
    query += f"""
        FROM stock_price
        WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0
          AND dt >= '{start_str}' AND dt <= '{end_str}'
        ORDER BY dt, "{ticker_col}"
    """
    try:
        data = execute_custom_query(query, connection=connection)
        df = pd.DataFrame(data)
        if df.empty or len(df) < 2:
            return pd.DataFrame(columns=["종목코드", "종목명", "업종", "일별등락률(%)"])
        df["dt"] = pd.to_datetime(df["dt"])
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df.dropna(subset=["price"])
        df["dt_date"] = df["dt"].dt.date
        ref_d = ref_date if hasattr(ref_date, "strftime") else pd.to_datetime(ref_date).date()
        last_dates = df.groupby("ticker")["dt_date"].max()
        tickers_at_ref = last_dates[last_dates >= ref_d - timedelta(days=5)].index.tolist()
        df = df[df["ticker"].isin(tickers_at_ref)]
        if df.empty:
            return pd.DataFrame(columns=["종목코드", "종목명", "업종", "일별등락률(%)"])
        rows = []
        for ticker in df["ticker"].unique():
            sub = df[df["ticker"] == ticker].sort_values("dt")
            sub = sub.drop_duplicates(subset=["dt_date"], keep="last")
            if len(sub) < 2:
                continue
            high_52w = sub["price"].max()
            last_row = sub.iloc[-1]
            last_price = float(last_row["price"])
            if last_price < high_52w * 0.99:
                continue
            prev_row = sub.iloc[-2]
            prev_price = float(prev_row["price"])
            daily_ret = (last_price - prev_price) / prev_price * 100.0 if prev_price and prev_price > 0 else None
            name_val = last_row.get("name") or ticker
            sector_val = last_row.get("sector") or "—"
            rows.append({"종목코드": ticker, "종목명": name_val, "업종": sector_val, "일별등락률(%)": round(daily_ret, 2) if daily_ret is not None else None})
        out = pd.DataFrame(rows)
        return out.sort_values("일별등락률(%)", ascending=False).reset_index(drop=True) if not out.empty else out
    except Exception:
        return pd.DataFrame(columns=["종목코드", "종목명", "업종", "일별등락률(%)"])


def get_52w_high_stocks_from_factset(index_name: str, ref_date) -> pd.DataFrame:
    """
    index_constituents의 bb_ticker와 price_factset 매칭으로 52주 신고가 종목 산출.
    조건: 기준일 포함 최근 7일(기준일~기준일-7일) 중 어떤 종가든 과거 52주 최고가를 초과한 종목.
    - 과거 52주 max = (기준일-7일) 이전 365일 구간의 종가 최댓값
    - 최근 7일 = (기준일 - 7일) ~ 기준일 구간
    반환: 종목코드(ticker), 종목명, 업종(gics_name), 일별등락률(%), bb_ticker(차트용).
    """
    const_df = get_constituents_for_date(index_name, ref_date)
    if const_df.empty:
        return pd.DataFrame(columns=["종목코드", "종목명", "업종", "52주최고가", "현재종가", "1개월수익률(%)", "3개월수익률(%)", "1년수익률(%)", "이격률(%)", "12M-1M", "bb_ticker"])
    ref_date = ref_date.date() if hasattr(ref_date, "date") else pd.to_datetime(ref_date).date()
    ref_str = ref_date.strftime("%Y-%m-%d")
    # 52주(365일) + 최근 7일 구간 확보
    start_str = (ref_date - timedelta(days=365 + 7)).strftime("%Y-%m-%d")
    bb_tickers = const_df["bb_ticker"].dropna().astype(str).str.strip().unique().tolist()
    if not bb_tickers:
        return pd.DataFrame(columns=["종목코드", "종목명", "업종", "52주최고가", "현재종가", "1개월수익률(%)", "3개월수익률(%)", "1년수익률(%)", "이격률(%)", "12M-1M", "bb_ticker"])
    price_df = get_price_factset(bb_tickers, start_str, ref_str)
    if price_df.empty or len(price_df) < 2:
        return pd.DataFrame(columns=["종목코드", "종목명", "업종", "52주최고가", "현재종가", "1개월수익률(%)", "3개월수익률(%)", "1년수익률(%)", "이격률(%)", "12M-1M", "bb_ticker"])
    price_df["dt_date"] = price_df["dt"].dt.date
    price_df["price"] = pd.to_numeric(price_df["price"], errors="coerce")
    price_df = price_df.dropna(subset=["price"])
    # 과거 52주 구간 끝 = 기준일 - 7일 (최근 7일 창 제외)
    end_52w = ref_date - timedelta(days=7)
    start_52w = ref_date - timedelta(days=365 + 7)
    start_7d = ref_date - timedelta(days=7)  # 최근 7일 창: start_7d ~ ref_date
    rows = []
    for bb in price_df["bb_ticker"].unique():
        sub = price_df[price_df["bb_ticker"] == bb].sort_values("dt").drop_duplicates(subset=["dt_date"], keep="last")
        if len(sub) < 2:
            continue
        # 과거 52주 최고가: (기준일-7일) 이전 365일
        in_52w = (sub["dt_date"] >= start_52w) & (sub["dt_date"] <= end_52w)
        high_52w = sub.loc[in_52w, "price"].max()
        if pd.isna(high_52w) or high_52w <= 0:
            continue
        # 최근 7일(기준일-7일 ~ 기준일) 중 최고 종가
        in_7d = (sub["dt_date"] >= start_7d) & (sub["dt_date"] <= ref_date)
        max_recent = sub.loc[in_7d, "price"].max()
        if pd.isna(max_recent) or max_recent <= high_52w:
            continue
        last_price = float(sub.iloc[-1]["price"])
        prev_price = float(sub.iloc[-2]["price"]) if len(sub) >= 2 else last_price
        daily_ret = (last_price - prev_price) / prev_price * 100.0 if prev_price and prev_price > 0 else None
        # 1개월·3개월·1년 수익률
        cutoff_1m = ref_date - timedelta(days=30)
        cutoff_3m = ref_date - timedelta(days=90)
        sub_before_1m = sub[sub["dt_date"] <= cutoff_1m].sort_values("dt")
        sub_before_3m = sub[sub["dt_date"] <= cutoff_3m].sort_values("dt")
        price_1m_ago = float(sub_before_1m.iloc[-1]["price"]) if len(sub_before_1m) and sub_before_1m.iloc[-1]["price"] else None
        price_3m_ago = float(sub_before_3m.iloc[-1]["price"]) if len(sub_before_3m) and sub_before_3m.iloc[-1]["price"] else None
        ret_1m = round((last_price - price_1m_ago) / price_1m_ago * 100.0, 2) if price_1m_ago and price_1m_ago > 0 else None
        ret_3m = round((last_price - price_3m_ago) / price_3m_ago * 100.0, 2) if price_3m_ago and price_3m_ago > 0 else None
        sub_52w = sub.loc[in_52w].sort_values("dt")
        price_1y_ago = float(sub_52w.iloc[0]["price"]) if len(sub_52w) and sub_52w.iloc[0]["price"] else None
        ret_1y = round((last_price - price_1y_ago) / price_1y_ago * 100.0, 2) if price_1y_ago and price_1y_ago > 0 else None
        # 이격률: (현재가 - 52주고가) / 52주고가 * 100. 고가 위면 +, 아래면 -
        displacement_pct = round((last_price - high_52w) / high_52w * 100.0, 2) if high_52w and high_52w > 0 else None
        # 12M-1M 모멘텀: (Price_1m / Price_12m) - 1 (%)
        mom_12m_1m = round((price_1m_ago / price_1y_ago - 1) * 100.0, 2) if price_1m_ago and price_1y_ago and price_1y_ago > 0 else None
        const_row = const_df[const_df["bb_ticker"].astype(str).str.strip() == bb].iloc[0]
        rows.append({
            "종목코드": const_row.get("ticker", bb),
            "종목명": const_row.get("name", bb),
            "업종": const_row.get("gics_name", "—"),
            "52주최고가": round(high_52w, 2),
            "현재종가": round(last_price, 2),
            "1개월수익률(%)": ret_1m,
            "3개월수익률(%)": ret_3m,
            "1년수익률(%)": ret_1y,
            "이격률(%)": displacement_pct,
            "12M-1M": mom_12m_1m,
            "일별등락률(%)": round(daily_ret, 2) if daily_ret is not None else None,
            "bb_ticker": bb,
        })
    out = pd.DataFrame(rows)
    return out.sort_values("일별등락률(%)", ascending=False).reset_index(drop=True) if not out.empty else out


def get_all_constituents_52w_summary(index_name: str, ref_date) -> pd.DataFrame:
    """
    선택 Index 구성종목 전체에 대해 52주 최고가·현재 종가·1M/3M/1Y 수익률·이격률·12M-1M(모멘텀) 반환.
    반환 컬럼: 종목코드, 종목명, 업종, 52주최고가, 현재종가, 1개월수익률(%), 3개월수익률(%), 1년수익률(%), 이격률(%), 12M-1M, bb_ticker.
    12M-1M = (Price_1m / Price_12m) - 1 (%).
    """
    const_df = get_constituents_for_date(index_name, ref_date)
    if const_df.empty:
        return pd.DataFrame(columns=["종목코드", "종목명", "업종", "52주최고가", "현재종가", "1개월수익률(%)", "3개월수익률(%)", "1년수익률(%)", "이격률(%)", "12M-1M", "bb_ticker"])
    ref_date = ref_date.date() if hasattr(ref_date, "date") else pd.to_datetime(ref_date).date()
    ref_str = ref_date.strftime("%Y-%m-%d")
    start_str = (ref_date - timedelta(days=365)).strftime("%Y-%m-%d")
    bb_tickers = const_df["bb_ticker"].dropna().astype(str).str.strip().unique().tolist()
    if not bb_tickers:
        return pd.DataFrame(columns=["종목코드", "종목명", "업종", "52주최고가", "현재종가", "1개월수익률(%)", "3개월수익률(%)", "1년수익률(%)", "이격률(%)", "12M-1M", "bb_ticker"])
    price_df = get_price_factset(bb_tickers, start_str, ref_str)
    if price_df.empty:
        return pd.DataFrame(columns=["종목코드", "종목명", "업종", "52주최고가", "현재종가", "1개월수익률(%)", "3개월수익률(%)", "1년수익률(%)", "이격률(%)", "12M-1M", "bb_ticker"])
    price_df["dt_date"] = price_df["dt"].dt.date
    price_df["price"] = pd.to_numeric(price_df["price"], errors="coerce")
    price_df = price_df.dropna(subset=["price"])
    rows = []
    for bb in bb_tickers:
        const_row = const_df[const_df["bb_ticker"].astype(str).str.strip() == bb].iloc[0]
        sub = price_df[price_df["bb_ticker"] == bb].sort_values("dt").drop_duplicates(subset=["dt_date"], keep="last")
        if sub.empty:
            rows.append({
                "종목코드": const_row.get("ticker", bb),
                "종목명": const_row.get("name", bb),
                "업종": const_row.get("gics_name", "—"),
                "52주최고가": None,
                "현재종가": None,
                "1개월수익률(%)": None,
                "3개월수익률(%)": None,
                "1년수익률(%)": None,
                "이격률(%)": None,
                "12M-1M": None,
                "bb_ticker": bb,
            })
            continue
        sub_sorted = sub.sort_values("dt")
        high_52w = float(sub_sorted["price"].max())
        current = float(sub_sorted.iloc[-1]["price"])
        cutoff_1m = ref_date - timedelta(days=30)
        cutoff_3m = ref_date - timedelta(days=90)
        sub_before_1m = sub_sorted[sub_sorted["dt_date"] <= cutoff_1m]
        sub_before_3m = sub_sorted[sub_sorted["dt_date"] <= cutoff_3m]
        price_1m_ago = float(sub_before_1m.iloc[-1]["price"]) if len(sub_before_1m) else None
        price_3m_ago = float(sub_before_3m.iloc[-1]["price"]) if len(sub_before_3m) else None
        ret_1m = round((current - price_1m_ago) / price_1m_ago * 100.0, 2) if price_1m_ago and price_1m_ago > 0 else None
        ret_3m = round((current - price_3m_ago) / price_3m_ago * 100.0, 2) if price_3m_ago and price_3m_ago > 0 else None
        price_1y_ago = float(sub_sorted.iloc[0]["price"]) if len(sub_sorted) else None
        ret_1y = round((current - price_1y_ago) / price_1y_ago * 100.0, 2) if price_1y_ago and price_1y_ago > 0 else None
        # 이격률: (현재가 - 52주고가) / 52주고가 * 100. 고가 위면 +, 아래면 -
        displacement_pct = round((current - high_52w) / high_52w * 100.0, 2) if high_52w and high_52w > 0 else None
        # 12M-1M 모멘텀: (Price_1m / Price_12m) - 1 (%)
        mom_12m_1m = round((price_1m_ago / price_1y_ago - 1) * 100.0, 2) if price_1m_ago and price_1y_ago and price_1y_ago > 0 else None
        rows.append({
            "종목코드": const_row.get("ticker", bb),
            "종목명": const_row.get("name", bb),
            "업종": const_row.get("gics_name", "—"),
            "52주최고가": round(high_52w, 2) if high_52w else None,
            "현재종가": round(current, 2) if current else None,
            "1개월수익률(%)": ret_1m,
            "3개월수익률(%)": ret_3m,
            "1년수익률(%)": ret_1y,
            "이격률(%)": displacement_pct,
            "12M-1M": mom_12m_1m,
            "bb_ticker": bb,
        })
    out = pd.DataFrame(rows)
    # 이격률 높은 순(고가 돌파 → 근접 → 하회)
    out = out.sort_values("이격률(%)", ascending=False, na_position="last").reset_index(drop=True)
    return out


@with_connection
def get_stock_price_series(ticker: str, start_date: str, end_date: str, connection: Optional[Connection] = None) -> pd.DataFrame:
    """stock_price에서 해당 종목의 기간별 가격 시계열 반환 (dt, price)."""
    table_info = get_table_info("stock_price", connection=connection)
    col_names = [c["column_name"] for c in table_info]
    ticker_col = next((c for c in ["ticker", "stock_code", "code", "symbol"] if c in col_names), None)
    price_col = next((c for c in ["price", "close", "close_price"] if c in col_names), None)
    if not ticker_col or not price_col:
        return pd.DataFrame()
    query = f"""
        SELECT dt, "{price_col}" AS price
        FROM stock_price
        WHERE "{ticker_col}" = '{ticker.replace(chr(39), chr(39)+chr(39))}'
          AND dt >= '{start_date}' AND dt <= '{end_date}'
          AND "{price_col}" IS NOT NULL
        ORDER BY dt
    """
    try:
        data = execute_custom_query(query, connection=connection)
        df = pd.DataFrame(data)
        if df.empty:
            return df
        df["dt"] = pd.to_datetime(df["dt"])
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        return df[["dt", "price"]].dropna()
    except Exception:
        return pd.DataFrame()


@with_connection
def get_index_returns_trend(start_date: str,
                           end_date: str,
                           connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    PRICE_INDEX 테이블에서 지수별 수익률 추이를 계산하는 함수
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD 형식)
        end_date: 종료 날짜 (YYYY-MM-DD 형식)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 지수별 수익률 데이터프레임 (dt, index_name, price, cumulative_return, daily_return)
    """
    # 원시 데이터 가져오기
    raw_df = get_major_indices_raw_data(
        start_date=start_date,
        end_date=end_date,
        connection=connection
    )
    
    if raw_df.empty:
        return pd.DataFrame()
    
    # 각 지수별로 일별 수익률 및 누적 수익률 계산
    results = []
    start_dt = pd.to_datetime(start_date)
    
    for index_name in raw_df['index_name'].unique():
        index_data = raw_df[raw_df['index_name'] == index_name].copy()
        index_data = index_data.sort_values('dt')
        
        if len(index_data) < 2:
            continue
        
        # 시작일 기준 가격 찾기
        start_data = index_data[index_data['dt'] <= start_dt]
        if start_data.empty:
            continue
        
        base_price = float(start_data.iloc[-1]['price'])
        base_date = start_data.iloc[-1]['dt']
        
        # 시작일 이후 데이터만 필터링
        filtered_data = index_data[index_data['dt'] >= base_date].copy()
        
        if len(filtered_data) < 2:
            continue
        
        # 전일 대비 일별 수익률 계산
        filtered_data['prev_price'] = filtered_data['price'].shift(1)
        filtered_data['daily_return'] = ((filtered_data['price'].astype(float) - filtered_data['prev_price'].astype(float)) / filtered_data['prev_price'].astype(float)) * 100
        
        # 누적 수익률 계산 (시작일 기준)
        filtered_data['cumulative_return'] = ((filtered_data['price'].astype(float) - base_price) / base_price) * 100
        
        # 첫 번째 행의 daily_return은 NaN이므로 0으로 설정
        filtered_data.loc[filtered_data['dt'] == base_date, 'daily_return'] = 0.0
        filtered_data.loc[filtered_data['dt'] == base_date, 'cumulative_return'] = 0.0
        
        # 필요한 컬럼만 선택
        result_cols = ['dt', 'index_name', 'price', 'cumulative_return', 'daily_return']
        filtered_data = filtered_data[result_cols].copy()
        
        results.append(filtered_data)
    
    if not results:
        return pd.DataFrame()
    
    result_df = pd.concat(results, ignore_index=True)
    result_df = result_df.sort_values(['dt', 'index_name'])
    
    return result_df


@with_connection
def get_mp_weight_data(start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    mp_weight 테이블에서 active_weight 데이터를 가져오는 함수
    
    Args:
        start_date: 시작일자 (YYYY-MM-DD 형식, None이면 전체)
        end_date: 종료일자 (YYYY-MM-DD 형식, None이면 전체)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 종목별 active_weight 데이터 (dt, stock_name, active_weight)
    """
    # 테이블 구조 확인
    try:
        table_info = get_table_info("mp_weight", connection=connection)
        column_names = [col['column_name'] for col in table_info]
    except Exception as e:
        # 테이블이 없을 수 있음
        return pd.DataFrame()
    
    # 필수 컬럼 확인
    if 'dt' not in column_names:
        return pd.DataFrame()
    
    # 종목명 컬럼 찾기
    stock_col = None
    for col in ['stock', 'stock_name', 'ticker', 'symbol', 'name']:
        if col in column_names:
            stock_col = col
            break
    
    if stock_col is None:
        return pd.DataFrame()
    
    # active_weight 컬럼 찾기
    active_weight_col = None
    for col in ['active_weight', 'weight', 'weight_pct', 'weight_percent', 'active_weight_pct']:
        if col in column_names:
            active_weight_col = col
            break
    
    if active_weight_col is None:
        return pd.DataFrame()
    
    # WHERE 조건 구성
    # active_weight이 NULL이어도 해당 날짜에 데이터가 있는 것으로 간주 (NULL = 비중 0)
    where_conditions = []
    
    if start_date:
        where_conditions.append(f"dt >= '{start_date}'")
    if end_date:
        where_conditions.append(f"dt <= '{end_date}'")
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    
    query = f"""
        SELECT 
            dt,
            {stock_col} as stock_name,
            {active_weight_col} as active_weight
        FROM mp_weight
        WHERE {where_clause}
        ORDER BY dt, {stock_col}
    """
    
    try:
        data = execute_custom_query(query, connection=connection)
        df = pd.DataFrame(data)
        
        if df.empty:
            return pd.DataFrame()
        
        # dt를 datetime으로 변환
        df['dt'] = pd.to_datetime(df['dt'])
        df = df.sort_values(['dt', 'stock_name'])
        
        return df
    except Exception as e:
        return pd.DataFrame()


@with_connection
def calculate_strategy_portfolio_returns(index_name: str,
                                        base_date: str,
                                        end_date: str,
                                        bm_returns_df: Optional[pd.DataFrame] = None,
                                        connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    전략 포트폴리오의 일별 누적 수익률을 계산하는 함수
    BM 비중 + active_weight를 적용한 전략 포트폴리오 수익률 계산
    
    Args:
        index_name: 지수명 (BM)
        base_date: 기준일자 (YYYY-MM-DD 형식)
        end_date: 종료일자 (YYYY-MM-DD 형식)
        bm_returns_df: BM 수익률 데이터프레임 (dt, cumulative_return) - None이면 자동 계산
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 전략 포트폴리오 누적 수익률 (dt, strategy_cumulative_return, strategy_value)
    """
    from datetime import datetime
    from utils import get_business_day_by_country, get_index_country_code
    
    # BM 구성종목 데이터 가져오기
    bm_data = get_index_constituents_data(
        index_name=index_name,
        start_date=base_date,
        end_date=end_date,
        connection=connection
    )
    
    if bm_data.empty:
        return pd.DataFrame()
    
    # mp_weight 데이터 가져오기
    mp_weight_data = get_mp_weight_data(
        start_date=base_date,
        end_date=end_date,
        connection=connection
    )
    
    # 날짜별로 그룹화
    dates = sorted(bm_data['dt'].unique())
    
    if not dates:
        return pd.DataFrame()
    
    # 기준일자 찾기
    base_date_obj = pd.to_datetime(base_date).date() if isinstance(base_date, str) else base_date
    if hasattr(base_date_obj, 'date'):
        base_date_obj = base_date_obj.date()
    
    # 기준일자 이하의 가장 가까운 날짜 찾기
    base_data = bm_data[bm_data['dt'].dt.date <= base_date_obj]
    if base_data.empty:
        return pd.DataFrame()
    
    base_actual_date = base_data['dt'].max().date()
    
    # 기준일자의 BM 비중 가져오기
    base_bm_weights = {}
    base_bm_data = bm_data[bm_data['dt'].dt.date == base_actual_date]
    for _, row in base_bm_data.iterrows():
        stock_name = row['stock_name']
        weight = row.get('weight', 0.0)
        if pd.notna(weight):
            base_bm_weights[stock_name] = float(weight)
    
    # 기준일자의 가격 가져오기 (stock_price 테이블에서)
    stock_price_table_info = get_table_info("stock_price", connection=connection)
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
    
    # LOCAL_PRICE 컬럼 찾기
    local_price_col = None
    for col in ['local_price', 'localprice', 'local_price_usd', 'local_price_local']:
        if col in stock_price_column_names:
            local_price_col = col
            break
    
    if ticker_col is None or price_col is None:
        # 디버깅: ticker_col 또는 price_col을 찾을 수 없음
        return pd.DataFrame()
    
    # 기준일자 가격 조회 (BM 종목 + mp_weight에 있는 종목 모두 포함)
    # 먼저 mp_weight에 있는 모든 종목 가져오기
    all_mp_stocks = set()
    if not mp_weight_data.empty:
        all_mp_stocks = set(mp_weight_data['stock_name'].unique())
    
    # BM 종목과 mp_weight 종목 모두 포함
    all_stock_names = set(list(base_bm_weights.keys()) + list(all_mp_stocks))
    
    if not all_stock_names:
        # 디버깅: 종목이 없음
        return pd.DataFrame()
    
    stock_names_list = list(all_stock_names)
    stock_names_str = "', '".join(stock_names_list)
    
    # LOCAL_PRICE 컬럼이 있으면 포함, 없으면 NULL로 처리
    local_price_select = f", {local_price_col} as local_price" if local_price_col else ", NULL as local_price"
    
    base_price_query = f"""
        SELECT 
            {ticker_col} as stock_name,
            {price_col} as price
            {local_price_select}
        FROM stock_price
        WHERE {ticker_col} IN ('{stock_names_str}')
        AND dt = '{base_actual_date}'
    """
    
    base_price_data = execute_custom_query(base_price_query, connection=connection)
    base_prices = {}
    base_local_prices = {}
    for row in base_price_data:
        base_prices[row['stock_name']] = float(row['price']) if pd.notna(row['price']) else None
        if local_price_col and 'local_price' in row:
            base_local_prices[row['stock_name']] = float(row['local_price']) if pd.notna(row['local_price']) else None
    
    # base_prices가 비어있어도 계속 진행
    # 기준일자는 수익률이 0%이므로 base_prices가 없어도 추가 가능
    # 이후 날짜에서 base_price가 없으면 해당 종목의 수익률 계산을 건너뛰면 됨
    
    # 날짜별 전략 포트폴리오 수익률 계산
    # 중요: active_weight은 해당 날짜의 종가에 반영되므로, 다음 날의 수익률 계산에는 전일 BM 비중만 사용해야 함
    # 예: 12-01에 MSFT BM 비중 7.5%, active_weight 0.01이면
    #     12-02의 수익률 계산 시에는 전일(12-01)의 BM 비중 7.5%를 사용 (active_weight 반영 전)
    results = []
    prev_bm_weights = {}  # 전일 BM 비중 저장 (active_weight 반영 전)
    
    for i, date in enumerate(dates):
        date_obj = date.date() if hasattr(date, 'date') else pd.to_datetime(date).date()
        
        if date_obj < base_actual_date:
            continue
        
        # 해당 날짜의 BM 비중 가져오기
        date_bm_data = bm_data[bm_data['dt'] == date]
        date_bm_weights = {}
        for _, row in date_bm_data.iterrows():
            stock_name = row['stock_name']
            weight = row.get('weight', 0.0)
            if pd.notna(weight):
                date_bm_weights[stock_name] = float(weight)
        
        # 해당 날짜의 active_weight 가져오기 (누적이 아니라 해당 날짜의 값)
        # active_weight이 NULL이면 0.0으로 처리 (비중이 없는 것이 아니라 비중이 0인 것)
        date_active_weights = {}
        if not mp_weight_data.empty and 'dt' in mp_weight_data.columns:
            date_mp_weight = mp_weight_data[mp_weight_data['dt'].dt.date == date_obj]
            for _, row in date_mp_weight.iterrows():
                stock_name = row['stock_name']
                active_weight = row.get('active_weight', 0.0)
                # NULL이면 0.0으로 처리
                if pd.isna(active_weight):
                    active_weight = 0.0
                date_active_weights[stock_name] = float(active_weight)
        
        # 기준일자가 아닌 경우: 전일 BM 비중을 사용하여 수익률 계산
        # 기준일자인 경우: 해당 날짜의 비중을 사용 (기준일자는 수익률이 0%)
        if date_obj == base_actual_date:
            # 기준일자: BM 비중 + 해당 날짜의 active_weight (전략 비중)
            strategy_weights = {}
            all_stocks = set(list(date_bm_weights.keys()) + list(date_active_weights.keys()))
            
            for stock_name in all_stocks:
                bm_weight = date_bm_weights.get(stock_name, 0.0)
                active_weight = date_active_weights.get(stock_name, 0.0)
                strategy_weights[stock_name] = bm_weight + active_weight
            
            # 기준일자의 BM 비중 저장 (다음 날 수익률 계산에 사용, active_weight 반영 전)
            prev_bm_weights = date_bm_weights.copy()
        else:
            # 기준일자가 아닌 경우: 전일 BM 비중을 사용하여 수익률 계산
            # 전일 BM 비중이 없으면 해당 날짜의 BM 비중 사용
            if not prev_bm_weights:
                # 전일 BM 비중이 없는 경우 (첫 번째 날짜가 기준일자가 아닌 경우)
                # 전일 BM 비중을 찾아서 사용
                prev_date_idx = i - 1
                if prev_date_idx >= 0:
                    prev_date = dates[prev_date_idx]
                    prev_date_obj = prev_date.date() if hasattr(prev_date, 'date') else pd.to_datetime(prev_date).date()
                    prev_bm_data = bm_data[bm_data['dt'].dt.date == prev_date_obj]
                    for _, row in prev_bm_data.iterrows():
                        stock_name = row['stock_name']
                        weight = row.get('weight', 0.0)
                        if pd.notna(weight):
                            strategy_weights[stock_name] = float(weight)
                else:
                    strategy_weights = date_bm_weights.copy()
            else:
                # 전일 BM 비중 사용 (active_weight 반영 전)
                strategy_weights = prev_bm_weights.copy()
            
            # 전일 BM 비중에 없는 종목은 현재 BM 비중 사용
            for stock_name in date_bm_weights.keys():
                if stock_name not in strategy_weights:
                    strategy_weights[stock_name] = date_bm_weights[stock_name]
            
            # 다음 날을 위해 현재 BM 비중 저장 (active_weight 반영 전)
            prev_bm_weights = date_bm_weights.copy()
        
        all_stocks = set(list(strategy_weights.keys()))
        
        # 해당 날짜의 가격 조회 (모든 종목 포함)
        # strategy_weights에 있는 모든 종목의 가격을 조회
        # LOCAL_PRICE 컬럼이 있으면 포함, 없으면 NULL로 처리
        local_price_select = f", {local_price_col} as local_price" if local_price_col else ", NULL as local_price"
        
        if all_stocks:
            date_stock_names_list = list(all_stocks)
            date_stock_names_str = "', '".join(date_stock_names_list)
            date_price_query = f"""
                SELECT 
                    {ticker_col} as stock_name,
                    {price_col} as price
                    {local_price_select}
                FROM stock_price
                WHERE {ticker_col} IN ('{date_stock_names_str}')
                AND dt = '{date_obj}'
            """
        else:
            date_price_query = f"""
                SELECT 
                    {ticker_col} as stock_name,
                    {price_col} as price
                    {local_price_select}
                FROM stock_price
                WHERE {ticker_col} IN ('{stock_names_str}')
                AND dt = '{date_obj}'
            """
        
        date_price_data = execute_custom_query(date_price_query, connection=connection)
        date_prices = {}
        date_local_prices = {}
        for row in date_price_data:
            date_prices[row['stock_name']] = float(row['price']) if pd.notna(row['price']) else None
            if local_price_col and 'local_price' in row:
                date_local_prices[row['stock_name']] = float(row['local_price']) if pd.notna(row['local_price']) else None
        
        # 전략 포트폴리오 가치 계산 (기준일자 대비)
        # BM 비중과 active_weight는 모두 소수점 형태 (0.05 = 5%, 0.01 = 1%)
        strategy_value = 0.0
        total_weight = 0.0
        
        for stock_name in all_stocks:
            base_price = base_prices.get(stock_name)
            date_price = date_prices.get(stock_name)
            strategy_weight = strategy_weights.get(stock_name, 0.0)
            
            if base_price and date_price and base_price > 0:
                # 종목별 수익률 (퍼센트)
                stock_return = (date_price / base_price - 1) * 100
                # 종목별 기여도 = 수익률 * 전략비중 (비중이 소수점 형태이므로 그대로 곱함)
                # 예: 수익률 10%, 비중 0.06 (6%) -> 기여도 = 10% * 0.06 = 0.6%
                contribution = stock_return * strategy_weight
                strategy_value += contribution
                total_weight += strategy_weight
        
        # 기준일자는 0%로 설정
        if date_obj == base_actual_date:
            strategy_value = 0.0
        
        results.append({
            'dt': date,
            'strategy_cumulative_return': strategy_value,
            'strategy_value': strategy_value
        })
    
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        # 디버깅: results가 비어있음
        # 가능한 원인:
        # 1. dates가 비어있거나 기준일자 이후 날짜가 없음
        # 2. 모든 날짜에서 base_price 또는 date_price가 없어서 strategy_value가 계산되지 않음
        return pd.DataFrame()
    
    result_df = result_df.sort_values('dt')
    
    return result_df[['dt', 'strategy_cumulative_return', 'strategy_value']]


@with_connection
def get_strategy_portfolio_weight_comparison(index_name: str,
                                            base_date: str,
                                            end_date: str,
                                            connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    전략 포트폴리오와 BM의 종목별 비중 비교 데이터를 생성하는 함수
    날짜별로 BM 비중, 전략 포트폴리오 비중, 비중 차이, 기여도를 계산
    
    Args:
        index_name: 지수명 (BM)
        base_date: 기준일자 (YYYY-MM-DD 형식)
        end_date: 종료일자 (YYYY-MM-DD 형식)
        connection: 데이터베이스 연결 객체
    
    Returns:
        pd.DataFrame: 날짜별 종목별 비중 비교 데이터
    """
    from datetime import datetime
    from utils import get_business_day_by_country, get_index_country_code
    
    # BM 구성종목 데이터 가져오기
    bm_data = get_index_constituents_data(
        index_name=index_name,
        start_date=base_date,
        end_date=end_date,
        connection=connection
    )
    
    if bm_data.empty:
        return pd.DataFrame()
    
    # mp_weight 데이터 가져오기
    mp_weight_data = get_mp_weight_data(
        start_date=base_date,
        end_date=end_date,
        connection=connection
    )
    
    # 날짜별로 그룹화
    dates = sorted(bm_data['dt'].unique())
    
    if not dates:
        return pd.DataFrame()
    
    # 기준일자 찾기
    base_date_obj = pd.to_datetime(base_date).date() if isinstance(base_date, str) else base_date
    if hasattr(base_date_obj, 'date'):
        base_date_obj = base_date_obj.date()
    
    # 기준일자 이하의 가장 가까운 날짜 찾기
    base_data = bm_data[bm_data['dt'].dt.date <= base_date_obj]
    if base_data.empty:
        return pd.DataFrame()
    
    base_actual_date = base_data['dt'].max().date()
    
    # 기준일자의 가격 가져오기
    stock_price_table_info = get_table_info("stock_price", connection=connection)
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
    
    # LOCAL_PRICE 컬럼 찾기
    local_price_col = None
    for col in ['local_price', 'localprice', 'local_price_usd', 'local_price_local']:
        if col in stock_price_column_names:
            local_price_col = col
            break
    
    if ticker_col is None or price_col is None:
        return pd.DataFrame()
    
    # 기준일자 가격 조회
    all_mp_stocks = set()
    if not mp_weight_data.empty:
        all_mp_stocks = set(mp_weight_data['stock_name'].unique())
    
    base_bm_weights = {}
    base_bm_data = bm_data[bm_data['dt'].dt.date == base_actual_date]
    for _, row in base_bm_data.iterrows():
        stock_name = row['stock_name']
        weight = row.get('weight', 0.0)
        if pd.notna(weight):
            base_bm_weights[stock_name] = float(weight)
    
    all_stock_names = set(list(base_bm_weights.keys()) + list(all_mp_stocks))
    if not all_stock_names:
        return pd.DataFrame()
    
    stock_names_list = list(all_stock_names)
    stock_names_str = "', '".join(stock_names_list)
    
    # LOCAL_PRICE 컬럼이 있으면 포함, 없으면 NULL로 처리
    local_price_select = f", {local_price_col} as local_price" if local_price_col else ", NULL as local_price"
    
    base_price_query = f"""
        SELECT 
            {ticker_col} as stock_name,
            {price_col} as price
            {local_price_select}
        FROM stock_price
        WHERE {ticker_col} IN ('{stock_names_str}')
        AND dt = '{base_actual_date}'
    """
    
    base_price_data = execute_custom_query(base_price_query, connection=connection)
    base_prices = {}
    base_local_prices = {}
    for row in base_price_data:
        base_prices[row['stock_name']] = float(row['price']) if pd.notna(row['price']) else None
        if local_price_col and 'local_price' in row:
            base_local_prices[row['stock_name']] = float(row['local_price']) if pd.notna(row['local_price']) else None
    
    # 날짜별 종목별 비중 비교 데이터 생성
    results = []
    daily_weight_summary = []  # 날짜별 비중 합계 저장
    prev_bm_weights = {}
    prev_strategy_weights = {}  # 전일 전략 비중 저장
    prev_prices = {}  # 전일 가격 저장
    prev_bm_nav = None  # 전일 BM NAV 저장
    prev_mp_nav = None  # 전일 MP NAV 저장
    
    for i, date in enumerate(dates):
        date_obj = date.date() if hasattr(date, 'date') else pd.to_datetime(date).date()
        
        if date_obj < base_actual_date:
            continue
        
        # 해당 날짜의 BM 비중 가져오기
        date_bm_data = bm_data[bm_data['dt'] == date]
        date_bm_weights = {}
        for _, row in date_bm_data.iterrows():
            stock_name = row['stock_name']
            weight = row.get('weight', 0.0)
            if pd.notna(weight):
                date_bm_weights[stock_name] = float(weight)
        
        # 해당 날짜의 active_weight 가져오기
        # active_weight이 NULL이면 0.0으로 처리 (비중이 없는 것이 아니라 비중이 0인 것)
        date_active_weights = {}
        if not mp_weight_data.empty and 'dt' in mp_weight_data.columns:
            date_mp_weight = mp_weight_data[mp_weight_data['dt'].dt.date == date_obj]
            for _, row in date_mp_weight.iterrows():
                stock_name = row['stock_name']
                active_weight = row.get('active_weight', 0.0)
                # NULL이면 0.0으로 처리
                if pd.isna(active_weight):
                    active_weight = 0.0
                date_active_weights[stock_name] = float(active_weight)
        
        # 해당 날짜의 가격 조회
        all_stocks_for_price = set(list(date_bm_weights.keys()) + list(date_active_weights.keys()))
        if not all_stocks_for_price:
            continue
            
        date_stock_names_list = list(all_stocks_for_price)
        date_stock_names_str = "', '".join(date_stock_names_list)
        
        # LOCAL_PRICE 컬럼이 있으면 포함, 없으면 NULL로 처리
        local_price_select = f", {local_price_col} as local_price" if local_price_col else ", NULL as local_price"
        
        date_price_query = f"""
            SELECT 
                {ticker_col} as stock_name,
                {price_col} as price
                {local_price_select}
            FROM stock_price
            WHERE {ticker_col} IN ('{date_stock_names_str}')
            AND dt = '{date_obj}'
        """
        
        date_price_data = execute_custom_query(date_price_query, connection=connection)
        date_prices = {}
        date_local_prices = {}
        for row in date_price_data:
            date_prices[row['stock_name']] = float(row['price']) if pd.notna(row['price']) else None
            if local_price_col and 'local_price' in row:
                date_local_prices[row['stock_name']] = float(row['local_price']) if pd.notna(row['local_price']) else None
        
        # 전략 포트폴리오 비중 계산
        if date_obj == base_actual_date:
            # 기준일자: active_weight은 종가 기준으로 단순 추가
            # ACTIVE WEIGHT이 0.01 (1%)이면 종가 기준으로 1%를 추가하는 것이므로
            # 전략 비중 = BM 비중 + active_weight (단순 덧셈)
            strategy_weights = {}
            all_stocks = set(list(date_bm_weights.keys()) + list(date_active_weights.keys()))
            
            for stock_name in all_stocks:
                bm_weight = date_bm_weights.get(stock_name, 0.0)
                active_weight = date_active_weights.get(stock_name, 0.0)
                # 단순 덧셈: BM 비중 + active_weight
                strategy_weights[stock_name] = bm_weight + active_weight
            
            prev_bm_weights = date_bm_weights.copy()
            prev_strategy_weights = strategy_weights.copy()
            prev_prices = date_prices.copy()
        else:
            # 기준일자가 아닌 경우: 전일 시장 변동을 반영하여 비중 계산
            if not prev_strategy_weights:
                # 전일 데이터가 없는 경우 전일 BM 비중 사용
                prev_date_idx = i - 1
                if prev_date_idx >= 0:
                    prev_date = dates[prev_date_idx]
                    prev_date_obj = prev_date.date() if hasattr(prev_date, 'date') else pd.to_datetime(prev_date).date()
                    prev_bm_data = bm_data[bm_data['dt'].dt.date == prev_date_obj]
                    for _, row in prev_bm_data.iterrows():
                        stock_name = row['stock_name']
                        weight = row.get('weight', 0.0)
                        if pd.notna(weight):
                            prev_bm_weights[stock_name] = float(weight)
                    prev_strategy_weights = prev_bm_weights.copy()
                else:
                    prev_bm_weights = date_bm_weights.copy()
                    prev_strategy_weights = date_bm_weights.copy()
            
            # 전일 전략 비중에 전일 수익률을 반영하여 조정
            # active_weight이 있을 때는 정확한 타겟팅 방식 적용
            all_stocks = set(list(prev_strategy_weights.keys()) + list(date_bm_weights.keys()) + list(date_active_weights.keys()))
            
            # 먼저 모든 종목의 시장 변동 반영 비중 계산
            adjusted_weights = {}
            for stock_name in all_stocks:
                prev_strategy_weight = prev_strategy_weights.get(stock_name, 0.0)
                prev_price = prev_prices.get(stock_name)
                current_price = date_prices.get(stock_name)
                
                # 전일 수익률 반영: 전일 비중 * (1 + 전일 수익률)
                if prev_price and current_price and prev_price > 0:
                    daily_return_ratio = current_price / prev_price
                    adjusted_weights[stock_name] = prev_strategy_weight * daily_return_ratio
                else:
                    adjusted_weights[stock_name] = prev_strategy_weight
            
            # active_weight이 있는 종목들 찾기 (종목명 매칭 포함)
            stocks_with_active_weight = {}
            for stock_name in all_stocks:
                current_active_weight = date_active_weights.get(stock_name, 0.0)
                if current_active_weight == 0.0 and date_active_weights:
                    # 종목명 매칭 시도
                    stock_name_clean = stock_name.replace(' ', '').upper()
                    for mp_stock_name, mp_active_weight in date_active_weights.items():
                        mp_stock_name_clean = mp_stock_name.replace(' ', '').upper()
                        if stock_name_clean == mp_stock_name_clean:
                            current_active_weight = mp_active_weight
                            break
                        stock_name_no_us = stock_name_clean.replace('US', '').strip()
                        mp_stock_name_no_us = mp_stock_name_clean.replace('US', '').strip()
                        if stock_name_no_us and mp_stock_name_no_us and stock_name_no_us == mp_stock_name_no_us:
                            current_active_weight = mp_active_weight
                            break
                
                if current_active_weight > 0:
                    adjusted_weight = adjusted_weights.get(stock_name, 0.0)
                    target_weight = adjusted_weight + current_active_weight
                    stocks_with_active_weight[stock_name] = {
                        'adjusted_weight': adjusted_weight,
                        'target_weight': target_weight,
                        'active_weight': current_active_weight
                    }
            
            # active_weight이 있는 종목이 있으면 정확한 타겟팅 방식 적용
            strategy_weights = {}
            if stocks_with_active_weight:
                # 현재 NAV (시장 변동 반영 후) - 비중의 합으로 계산
                nav_old = sum(adjusted_weights.values()) if adjusted_weights else 1.0
                if nav_old <= 0:
                    nav_old = 1.0
                
                # 각 active_weight 종목에 대해 순차적으로 정확한 타겟팅 적용
                # 여러 종목에 active_weight이 있어도 각각 독립적으로 계산
                nav_current = nav_old
                adjusted_weights_current = adjusted_weights.copy()
                
                for stock_name, info in stocks_with_active_weight.items():
                    adjusted_weight = adjusted_weights_current.get(stock_name, 0.0)
                    target_weight = info['target_weight']
                    
                    # 정확한 타겟팅 공식: x = (B_target * NAV_old - B_old) / (1 - B_target)
                    if (1 - target_weight) > 0:
                        additional_amount = (target_weight * nav_current - adjusted_weight) / (1 - target_weight)
                        # 추가 매수 후 새로운 금액
                        new_amount = adjusted_weight + additional_amount
                        # 새로운 NAV
                        nav_current = nav_current + additional_amount
                        # 새로운 비중 (목표 비중과 일치해야 함)
                        strategy_weights[stock_name] = target_weight
                        # adjusted_weights_current 업데이트 (다음 종목 계산을 위해)
                        adjusted_weights_current[stock_name] = new_amount
                    else:
                        # target_weight이 1.0 이상이면 처리 불가
                        strategy_weights[stock_name] = adjusted_weight
                
                # active_weight이 없는 종목들의 비중 스케일링 (NAV 증가로 인한 희석)
                # nav_current는 모든 active_weight 종목의 추가 매수 후 최종 NAV
                for stock_name in all_stocks:
                    if stock_name not in stocks_with_active_weight:
                        adjusted_weight = adjusted_weights.get(stock_name, 0.0)
                        # NAV 증가로 인한 희석: 원래 비중 / (새 NAV / 원래 NAV)
                        strategy_weights[stock_name] = adjusted_weight / nav_current if nav_current > 0 else adjusted_weight
            else:
                # active_weight이 없으면 시장 변동 반영 비중 그대로 사용
                strategy_weights = adjusted_weights.copy()
            
            # 다음 날을 위해 업데이트
            prev_bm_weights = date_bm_weights.copy()
            prev_strategy_weights = strategy_weights.copy()
            # prev_prices는 NAV 계산 후에 업데이트됨
        
        # 종목별 데이터 생성
        # 기준일자: 해당 날짜의 BM 구성종목과 active_weight가 있는 종목만 포함
        # 다른 날짜: 전략 비중에 있는 종목과 현재 BM 구성종목만 포함
        # (이미 strategy_weights 계산 시 필요한 종목만 포함됨)
        if date_obj == base_actual_date:
            # 기준일자: 해당 날짜의 BM 구성종목과 active_weight가 있는 종목만
            all_stocks_for_result = set(list(date_bm_weights.keys()) + list(date_active_weights.keys()))
        else:
            # 다른 날짜: 전략 비중에 있는 종목과 현재 BM 구성종목
            all_stocks_for_result = set(list(strategy_weights.keys()) + list(date_bm_weights.keys()))
        
        for stock_name in all_stocks_for_result:
            base_price = base_prices.get(stock_name)
            date_price = date_prices.get(stock_name)
            date_local_price = date_local_prices.get(stock_name) if date_local_prices else None
            bm_weight = date_bm_weights.get(stock_name, 0.0)
            strategy_weight = strategy_weights.get(stock_name, 0.0)
            # active_weight 조회: 정확한 종목명 매칭 시도
            active_weight = date_active_weights.get(stock_name, 0.0)
            # 종목명이 정확히 일치하지 않는 경우를 위한 추가 매칭 시도
            if active_weight == 0.0 and date_active_weights:
                # 종목명의 다양한 변형 시도 (예: "META US" <-> "META", "NFLX US" <-> "NFLX")
                stock_name_clean = stock_name.replace(' ', '').upper()
                for mp_stock_name, mp_active_weight in date_active_weights.items():
                    # 정확히 일치하는 경우
                    if stock_name == mp_stock_name:
                        active_weight = mp_active_weight
                        break
                    # 공백 제거 후 비교
                    mp_stock_name_clean = mp_stock_name.replace(' ', '').upper()
                    # 종목명이 포함되어 있는지 확인 (예: "META US"에 "META"가 포함)
                    if stock_name_clean == mp_stock_name_clean:
                        active_weight = mp_active_weight
                        break
                    # "US" 제거 후 비교 (예: "NVDA US" <-> "NVDA")
                    stock_name_no_us = stock_name_clean.replace('US', '').strip()
                    mp_stock_name_no_us = mp_stock_name_clean.replace('US', '').strip()
                    if stock_name_no_us and mp_stock_name_no_us and stock_name_no_us == mp_stock_name_no_us:
                        active_weight = mp_active_weight
                        break
                    # startswith 체크
                    if stock_name_clean.startswith(mp_stock_name_clean) or mp_stock_name_clean.startswith(stock_name_clean):
                        active_weight = mp_active_weight
                        break
            
            weight_diff = strategy_weight - bm_weight
            
            # 수익률 계산
            stock_return = 0.0
            if base_price and date_price and base_price > 0:
                stock_return = (date_price / base_price - 1) * 100
            
            # 기여도 계산
            bm_contribution = stock_return * bm_weight
            strategy_contribution = stock_return * strategy_weight
            excess_contribution = strategy_contribution - bm_contribution
            
            # 저장할 active_weight 값 결정
            final_active_weight = active_weight if active_weight > 0 else 0.0
            
            # NAV(금액) 기준 계산
            # 각 날짜마다 일관되게 계산: BM NAV = BM 비중 합계
            bm_nav_base = sum(date_bm_weights.values()) if date_bm_weights else 1.0
            if bm_nav_base <= 0:
                bm_nav_base = 1.0
            
            # 각 종목의 BM 금액 = BM NAV * BM 비중
            bm_amount = bm_nav_base * bm_weight
            
            # 각 종목의 MP 금액 계산
            # 기준일자: BM 금액 + (BM NAV * active_weight)
            # 기준일자가 아닌 날짜: MP NAV * 전략 비중 (정규화된 경우)
            if date_obj == base_actual_date:
                # 기준일자: active_weight은 종가 기준으로 추가 투입
                mp_amount = bm_amount + (bm_nav_base * final_active_weight)
            else:
                # 기준일자가 아닌 날짜: MP NAV 기준으로 계산
                # MP NAV = BM NAV + (BM NAV * active_weight 합계)
                active_weight_sum = sum(date_active_weights.values())
                mp_nav = bm_nav_base + (bm_nav_base * active_weight_sum)
                # 전략 비중이 정규화되어 있으면 MP NAV로 곱함
                # 전략 비중이 정규화되어 있지 않으면 그대로 사용
                if strategy_weight > 0:
                    # 전략 비중의 합이 MP NAV와 일치하는지 확인
                    strategy_weight_sum = sum(strategy_weights.values())
                    if strategy_weight_sum > 0 and abs(strategy_weight_sum - mp_nav) < 0.01:
                        # 정규화되어 있지 않음 (이미 금액 형태)
                        mp_amount = strategy_weight
                    else:
                        # 정규화되어 있음 (비중 형태)
                        mp_amount = mp_nav * strategy_weight
                else:
                    mp_amount = 0.0
            
            # 절대 Active 금액 = MP 금액 - BM 금액
            absolute_active_amount = mp_amount - bm_amount
            
            # 절대 Active 비율 = (MP 금액 - BM 금액) / BM NAV
            absolute_active_pct = absolute_active_amount / bm_nav_base if bm_nav_base > 0 else 0.0
            
            results.append({
                '날짜': date_obj.strftime('%Y-%m-%d'),
                '종목명': stock_name,
                'PRICE': date_price if date_price is not None else None,
                'LOCAL_PRICE': date_local_price if date_local_price is not None else None,
                'BM_비중': bm_weight,  # 소수점 형태 (예: 0.0896 = 8.96%)
                'active_weight': final_active_weight,  # 소수점 형태 (예: 0.01 = 1%)
                '전략_비중': strategy_weight,  # 소수점 형태 (예: 0.0996 = 9.96%)
                '비중_차이': weight_diff,  # 소수점 형태
                'BM_금액': bm_amount,  # NAV 기준 금액
                'MP_금액': mp_amount,  # NAV 기준 금액
                '절대_Active_금액': absolute_active_amount,  # NAV 기준 금액
                '절대_Active_비율': absolute_active_pct,  # 소수점 형태 (예: 0.01 = 1%)
                '기준일자_대비_수익률': stock_return / 100 if stock_return != 0 else 0.0,  # 소수점 형태로 변환 (예: 2.5% -> 0.025)
                'BM_기여도': bm_contribution / 100 if bm_contribution != 0 else 0.0,  # 소수점 형태로 변환
                '전략_기여도': strategy_contribution / 100 if strategy_contribution != 0 else 0.0,  # 소수점 형태로 변환
                '초과_기여도': excess_contribution / 100 if excess_contribution != 0 else 0.0  # 소수점 형태로 변환
            })
        
        # 날짜별 비중 합계 계산
        bm_weight_sum = sum(date_bm_weights.values())  # 소수점 형태
        strategy_weight_sum = sum(strategy_weights.values())  # 소수점 형태
        active_weight_sum = sum(date_active_weights.values())  # 소수점 형태
        weight_sum_diff = strategy_weight_sum - bm_weight_sum
        
        # NAV(금액) 합계 계산
        # 기준일자: 기준 NAV = BM 비중 합계
        if date_obj == base_actual_date:
            bm_nav_base = bm_weight_sum if bm_weight_sum > 0 else 1.0
            # MP NAV = BM NAV + (BM NAV * active_weight 합계)
            mp_nav = bm_nav_base + (bm_nav_base * active_weight_sum)
            prev_bm_nav = bm_nav_base
            prev_mp_nav = mp_nav
        else:
            # 기준일자가 아닌 날짜: 시장 가격 변동 반영
            # 일별 수익률 계산 (전일 가격 대비 오늘 가격)
            bm_daily_return = 0.0
            mp_daily_return = 0.0
            
            # BM 일별 수익률 = Σ(종목별 일별 수익률 × 전일 BM 비중)
            for stock_name, bm_weight in prev_bm_weights.items():
                prev_price = prev_prices.get(stock_name)
                current_price = date_prices.get(stock_name)
                if prev_price and current_price and prev_price > 0:
                    stock_daily_return = (current_price / prev_price - 1)
                    bm_daily_return += stock_daily_return * bm_weight
            
            # MP 일별 수익률 = Σ(종목별 일별 수익률 × 전일 전략 비중)
            for stock_name, strategy_weight in prev_strategy_weights.items():
                prev_price = prev_prices.get(stock_name)
                current_price = date_prices.get(stock_name)
                if prev_price and current_price and prev_price > 0:
                    stock_daily_return = (current_price / prev_price - 1)
                    mp_daily_return += stock_daily_return * strategy_weight
            
            # NAV 계산: 전일 NAV × (1 + 일별 수익률)
            if prev_bm_nav is not None and prev_bm_nav > 0:
                bm_nav_base = prev_bm_nav * (1 + bm_daily_return)
            else:
                bm_nav_base = bm_weight_sum if bm_weight_sum > 0 else 1.0
            
            if prev_mp_nav is not None and prev_mp_nav > 0:
                mp_nav = prev_mp_nav * (1 + mp_daily_return)
            else:
                active_weight_sum = sum(date_active_weights.values())
                mp_nav = bm_nav_base + (bm_nav_base * active_weight_sum)
            
            # 다음 날을 위해 업데이트
            prev_bm_nav = bm_nav_base
            prev_mp_nav = mp_nav
        
        # NAV 차이 = MP NAV - BM NAV
        nav_diff = mp_nav - bm_nav_base
        
        # NAV 계산 후 다음 날을 위해 업데이트
        if date_obj != base_actual_date:
            prev_prices = date_prices.copy()
            prev_bm_weights = date_bm_weights.copy()
            prev_strategy_weights = strategy_weights.copy()
        
        daily_weight_summary.append({
            '날짜': date_obj.strftime('%Y-%m-%d'),
            'BM_비중_합계': bm_weight_sum,
            '전략_비중_합계': strategy_weight_sum,
            'active_weight_합계': active_weight_sum,
            '비중_합계_차이': weight_sum_diff,
            'BM_NAV': bm_nav_base,
            'MP_NAV': mp_nav,
            'NAV_차이': nav_diff
        })
        
        # 해당 날짜의 가격 조회
        all_stocks = set(list(strategy_weights.keys()))
        if all_stocks:
            date_stock_names_list = list(all_stocks)
            date_stock_names_str = "', '".join(date_stock_names_list)
            # LOCAL_PRICE 컬럼이 있으면 포함, 없으면 NULL로 처리
            local_price_select = f", {local_price_col} as local_price" if local_price_col else ", NULL as local_price"
            
            date_price_query = f"""
                SELECT 
                    {ticker_col} as stock_name,
                    {price_col} as price
                    {local_price_select}
                FROM stock_price
                WHERE {ticker_col} IN ('{date_stock_names_str}')
                AND dt = '{date_obj}'
            """
            
            date_price_data = execute_custom_query(date_price_query, connection=connection)
            date_prices = {}
            date_local_prices = {}
            for row in date_price_data:
                date_prices[row['stock_name']] = float(row['price']) if pd.notna(row['price']) else None
                if local_price_col and 'local_price' in row:
                    date_local_prices[row['stock_name']] = float(row['local_price']) if pd.notna(row['local_price']) else None
            
            # 종목별 데이터 생성
            for stock_name in all_stocks:
                base_price = base_prices.get(stock_name)
                date_price = date_prices.get(stock_name)
                bm_weight = date_bm_weights.get(stock_name, 0.0)
                strategy_weight = strategy_weights.get(stock_name, 0.0)
                
                # active_weight 조회: 정확한 종목명 매칭 시도
                active_weight = date_active_weights.get(stock_name, 0.0)
                # 종목명이 정확히 일치하지 않는 경우를 위한 추가 매칭 시도
                if active_weight == 0.0 and date_active_weights:
                    # 종목명의 다양한 변형 시도
                    stock_name_clean = stock_name.replace(' ', '').upper()
                    for mp_stock_name, mp_active_weight in date_active_weights.items():
                        # 정확히 일치하는 경우
                        if stock_name == mp_stock_name:
                            active_weight = mp_active_weight
                            break
                        # 공백 제거 후 비교
                        mp_stock_name_clean = mp_stock_name.replace(' ', '').upper()
                        if stock_name_clean == mp_stock_name_clean:
                            active_weight = mp_active_weight
                            break
                        # "US" 제거 후 비교
                        stock_name_no_us = stock_name_clean.replace('US', '').strip()
                        mp_stock_name_no_us = mp_stock_name_clean.replace('US', '').strip()
                        if stock_name_no_us and mp_stock_name_no_us and stock_name_no_us == mp_stock_name_no_us:
                            active_weight = mp_active_weight
                            break
                        # startswith 체크
                        if stock_name_clean.startswith(mp_stock_name_clean) or mp_stock_name_clean.startswith(stock_name_clean):
                            active_weight = mp_active_weight
                            break
                
                weight_diff = strategy_weight - bm_weight
                
                # 수익률 계산
                stock_return = 0.0
                if base_price and date_price and base_price > 0:
                    stock_return = (date_price / base_price - 1) * 100
                
                # 기여도 계산
                bm_contribution = stock_return * bm_weight
                strategy_contribution = stock_return * strategy_weight
                excess_contribution = strategy_contribution - bm_contribution
                
                # 저장할 active_weight 값 결정
                final_active_weight = active_weight if active_weight > 0 else 0.0
                
                # NAV(금액) 기준 계산
                # 각 날짜마다 일관되게 계산: BM NAV = BM 비중 합계
                bm_nav_base = sum(date_bm_weights.values()) if date_bm_weights else 1.0
                if bm_nav_base <= 0:
                    bm_nav_base = 1.0
                
                # 각 종목의 BM 금액 = BM NAV * BM 비중
                bm_amount = bm_nav_base * bm_weight
                
                # 각 종목의 MP 금액 계산
                # 기준일자: BM 금액 + (BM NAV * active_weight)
                # 기준일자가 아닌 날짜: MP NAV * 전략 비중 (정규화된 경우)
                if date_obj == base_actual_date:
                    # 기준일자: active_weight은 종가 기준으로 추가 투입
                    mp_amount = bm_amount + (bm_nav_base * final_active_weight)
                else:
                    # 기준일자가 아닌 날짜: MP NAV 기준으로 계산
                    # MP NAV = BM NAV + (BM NAV * active_weight 합계)
                    active_weight_sum = sum(date_active_weights.values())
                    mp_nav = bm_nav_base + (bm_nav_base * active_weight_sum)
                    # 전략 비중이 정규화되어 있으면 MP NAV로 곱함
                    # 전략 비중이 정규화되어 있지 않으면 그대로 사용
                    if strategy_weight > 0:
                        # 전략 비중의 합이 MP NAV와 일치하는지 확인
                        strategy_weight_sum = sum(strategy_weights.values())
                        if strategy_weight_sum > 0 and abs(strategy_weight_sum - mp_nav) < 0.01:
                            # 정규화되어 있지 않음 (이미 금액 형태)
                            mp_amount = strategy_weight
                        else:
                            # 정규화되어 있음 (비중 형태)
                            mp_amount = mp_nav * strategy_weight
                    else:
                        mp_amount = 0.0
                
                # 절대 Active 금액 = MP 금액 - BM 금액
                absolute_active_amount = mp_amount - bm_amount
                
                # 절대 Active 비율 = (MP 금액 - BM 금액) / BM NAV
                absolute_active_pct = absolute_active_amount / bm_nav_base if bm_nav_base > 0 else 0.0
                
                # LOCAL_PRICE 가져오기
                date_local_price = date_local_prices.get(stock_name) if date_local_prices else None
                
                results.append({
                    '날짜': date_obj.strftime('%Y-%m-%d'),
                    '종목명': stock_name,
                    'PRICE': date_price if date_price is not None else None,
                    'LOCAL_PRICE': date_local_price if date_local_price is not None else None,
                    'BM_비중': bm_weight,  # 소수점 형태
                    'active_weight': final_active_weight,  # 소수점 형태
                    '전략_비중': strategy_weight,  # 소수점 형태
                    '비중_차이': weight_diff,  # 소수점 형태
                    'BM_금액': bm_amount,  # NAV 기준 금액
                    'MP_금액': mp_amount,  # NAV 기준 금액
                    '절대_Active_금액': absolute_active_amount,  # NAV 기준 금액
                    '절대_Active_비율': absolute_active_pct,  # 소수점 형태 (예: 0.01 = 1%)
                    '기준일자_대비_수익률': stock_return / 100 if stock_return != 0 else 0.0,  # 소수점 형태로 변환
                    'BM_기여도': bm_contribution / 100 if bm_contribution != 0 else 0.0,  # 소수점 형태로 변환
                    '전략_기여도': strategy_contribution / 100 if strategy_contribution != 0 else 0.0,  # 소수점 형태로 변환
                    '초과_기여도': excess_contribution / 100 if excess_contribution != 0 else 0.0  # 소수점 형태로 변환
                })
    
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        return pd.DataFrame()
    
    # 중복 제거: 같은 날짜, 같은 종목이 여러 번 나오는 경우 제거
    result_df = result_df.drop_duplicates(subset=['날짜', '종목명'], keep='last')
    
    # 날짜, 종목명 순으로 정렬
    result_df = result_df.sort_values(['날짜', '종목명'])
    
    # 날짜별 비중 합계 데이터프레임 생성
    daily_weight_summary_df = pd.DataFrame(daily_weight_summary)
    if not daily_weight_summary_df.empty:
        daily_weight_summary_df = daily_weight_summary_df.sort_values('날짜')
        # 결과에 메타데이터로 포함 (사용자가 접근할 수 있도록)
        result_df.attrs['daily_weight_summary'] = daily_weight_summary_df
    
    return result_df


def _index_constituents_index_column(connection: Optional[Connection]) -> Optional[str]:
    """index_constituents 테이블에서 지수명 컬럼 이름 반환 (DB 실제 컬럼명)."""
    info = get_table_info("index_constituents", connection=connection)
    if not info:
        return None
    col_names = [c.get("column_name") for c in info if c.get("column_name")]
    for cand in ("index", "index_name", "Index", "INDEX", "index_nm"):
        if cand in col_names:
            return cand
    for c in col_names:
        if "index" in c.lower():
            return c
    return None


@with_connection
def get_constituents_for_date(index_name: str, ref_date, connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    index_constituents에서 특정 지수·특정 일자의 구성종목 조회.
    반환: dt, index_name, ticker, bb_ticker, name, gics_name, index_weight 등.
    지수명 컬럼은 DB 실제 컬럼명(index / index_name 등)으로 자동 매칭.
    """
    ref_str = ref_date.strftime("%Y-%m-%d") if hasattr(ref_date, "strftime") else str(ref_date)[:10]
    index_col = _index_constituents_index_column(connection)
    if not index_col:
        index_col = "index"  # 기본 시도
    idx_quoted = f'"{index_col}"' if index_col in ("index", "Index", "INDEX") or index_col != index_col.lower() else index_col
    query = f"""
        SELECT
            dt,
            {idx_quoted} AS index_name,
            ticker,
            bb_ticker,
            name,
            gics_name,
            index_weight,
            local_price,
            index_market_cap
        FROM index_constituents
        WHERE {idx_quoted} = '{index_name.replace(chr(39), chr(39)+chr(39))}'
          AND dt::date = '{ref_str}'
          AND index_weight IS NOT NULL
        ORDER BY gics_name, index_weight DESC
    """
    data = execute_custom_query(query, connection=connection)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["dt"] = pd.to_datetime(df["dt"])
    return df


@with_connection
def get_earnings_calendar_closest_dates(
    ref_date,
    factset_tickers: List[str],
    connection: Optional[Connection] = None,
) -> pd.DataFrame:
    """
    market.earnings_calendar에서 종목별 기준일 기준 가장 가까운 지난 실적일·다가오는 실적일 조회.
    SPX/NDX 구성종목의 factset_ticker 리스트를 넘기면, 각각에 대해
    - closest_past_dt: 기준일 이하 중 가장 최근 dt
    - closest_future_dt: 기준일 초과 중 가장 가까운 dt
    를 반환한다.
    """
    if not factset_tickers:
        return pd.DataFrame(columns=["factset_ticker", "closest_past_dt", "closest_future_dt"])
    ref_str = ref_date.strftime("%Y-%m-%d") if hasattr(ref_date, "strftime") else str(ref_date)[:10]
    tickers_escaped = [t.replace("'", "''") for t in factset_tickers]
    tickers_in = "', '".join(tickers_escaped)
    query = f"""
        SELECT
            factset_ticker,
            MAX(CASE WHEN dt::date <= '{ref_str}' THEN dt END) AS closest_past_dt,
            MIN(CASE WHEN dt::date > '{ref_str}' THEN dt END) AS closest_future_dt
        FROM market.earnings_calendar
        WHERE factset_ticker IN ('{tickers_in}')
        GROUP BY factset_ticker
        ORDER BY factset_ticker
    """
    try:
        data = execute_custom_query(query, connection=connection)
    except Exception:
        return pd.DataFrame(columns=["factset_ticker", "closest_past_dt", "closest_future_dt"])
    df = pd.DataFrame(data)
    if df.empty:
        return df
    if "closest_past_dt" in df.columns:
        df["closest_past_dt"] = pd.to_datetime(df["closest_past_dt"], errors="coerce")
    if "closest_future_dt" in df.columns:
        df["closest_future_dt"] = pd.to_datetime(df["closest_future_dt"], errors="coerce")
    return df


@with_connection
def get_earnings_calendar_by_date_range(
    ref_date,
    factset_tickers: List[str],
    days_before: int = 7,
    days_after: int = 7,
    connection: Optional[Connection] = None,
) -> pd.DataFrame:
    """
    market.earnings_calendar에서 기준일 전후 기간 내 실적 발표 일정 조회.
    반환: dt, factset_ticker (None인 행 제외).
    """
    if not factset_tickers:
        return pd.DataFrame(columns=["dt", "factset_ticker"])
    try:
        from datetime import timedelta
        ref_d = ref_date.date() if hasattr(ref_date, "date") and callable(getattr(ref_date, "date")) else ref_date
        start_d = (ref_d - timedelta(days=days_before)).strftime("%Y-%m-%d")
        end_d = (ref_d + timedelta(days=days_after)).strftime("%Y-%m-%d")
    except Exception:
        return pd.DataFrame(columns=["dt", "factset_ticker"])
    tickers_escaped = [t.replace("'", "''") for t in factset_tickers]
    tickers_in = "', '".join(tickers_escaped)
    query = f"""
        SELECT dt, factset_ticker
        FROM market.earnings_calendar
        WHERE factset_ticker IN ('{tickers_in}')
          AND dt::date >= '{start_d}'
          AND dt::date <= '{end_d}'
        ORDER BY dt, factset_ticker
    """
    try:
        data = execute_custom_query(query, connection=connection)
    except Exception:
        return pd.DataFrame(columns=["dt", "factset_ticker"])
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df = df.dropna(subset=["dt", "factset_ticker"])
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    df = df.dropna(subset=["dt"])
    return df


@with_connection
def get_price_factset(bb_tickers: List[str], start_date: str, end_date: str, connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    price_factset(또는 PRICE_FACTSET)에서 bb_ticker 목록에 해당하는 가격 시계열 조회.
    public 뿐 아니라 market 스키마도 자동 탐색. 컬럼: dt, bb_ticker, price 등 자동 감지.
    """
    if not bb_tickers:
        return pd.DataFrame()
    # 스키마 자동 탐색 (public → market). price_factset이 market 스키마에 있는 경우 대비
    schema = _resolve_table_schema("price_factset", connection)
    table_info = get_table_info("price_factset", connection=connection)
    table_name = "price_factset"
    if not table_info:
        schema = _resolve_table_schema("PRICE_FACTSET", connection)
        table_info = get_table_info("PRICE_FACTSET", connection=connection)
        table_name = "PRICE_FACTSET"
    if not table_info:
        return pd.DataFrame(columns=["dt", "bb_ticker", "price"])

    col_names = [c["column_name"] for c in table_info]
    # 날짜 컬럼: dt, date, trade_dt 등
    date_candidates = ["dt", "date", "trade_dt", "trade_date", "d"]
    date_col = None
    for c in date_candidates:
        if c in col_names:
            date_col = c
            break
    if not date_col:
        date_like = [c["column_name"] for c in table_info if c.get("data_type") in ("date", "timestamp with time zone", "timestamp without time zone")]
        date_col = date_like[0] if date_like else "dt"

    ticker_col = "bb_ticker" if "bb_ticker" in col_names else ("ticker" if "ticker" in col_names else None)
    if not ticker_col and "id" in col_names:
        ticker_col = "id"
    price_candidates = ["price", "close", "value", "local_price", "px_last", "last_price", "adj_price"]
    price_col = None
    for c in price_candidates:
        if c in col_names:
            price_col = c
            break
    if not price_col:
        numeric = [c["column_name"] for c in table_info if c.get("data_type") in ("numeric", "double precision", "real", "bigint", "integer")]
        price_col = next((x for x in numeric if x not in (date_col, "dt_date", ticker_col)), None)
    if not ticker_col or not price_col:
        return pd.DataFrame(columns=["dt", "bb_ticker", "price"])

    # 스키마가 public이 아니면 "schema"."table" 형태로 조회
    if schema and schema != "public":
        from_clause = f'"{schema}"."{table_name}"'
    else:
        from_clause = f'"{table_name}"' if table_name == "PRICE_FACTSET" else table_name
    placeholders = ",".join([f"'{str(t).strip()}'" for t in bb_tickers if t and str(t).strip()])
    if not placeholders:
        return pd.DataFrame()
    # 날짜 비교: dt::date 사용 시 타임스탬프도 해당 일자로 비교
    query = f"""
        SELECT "{date_col}" AS dt, "{ticker_col}" AS bb_ticker, "{price_col}" AS price
        FROM {from_clause}
        WHERE "{ticker_col}" IN ({placeholders})
          AND "{date_col}"::date >= '{start_date}'
          AND "{date_col}"::date <= '{end_date}'
          AND "{price_col}" IS NOT NULL
          AND "{price_col}" > 0
        ORDER BY "{date_col}", "{ticker_col}"
    """
    data = execute_custom_query(query, connection=connection)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["dt"] = pd.to_datetime(df["dt"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype(float)
    return df


@with_connection
def get_op_factset_ticker_list(connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    market.op_factset에 있는 factset_ticker 목록 조회 (검색용).
    name/종목명 컬럼이 있으면 함께 반환.
    """
    schema = _resolve_table_schema("op_factset", connection)
    table_ref = f'"{schema}"."op_factset"' if schema != "public" else "op_factset"
    info = get_table_info("op_factset", connection=connection, schema=schema)
    if not info:
        return pd.DataFrame(columns=["factset_ticker"])
    col_names = [c["column_name"] for c in info]
    ticker_col = "factset_ticker" if "factset_ticker" in col_names else ("ticker" if "ticker" in col_names else None)
    if not ticker_col:
        return pd.DataFrame(columns=["factset_ticker"])
    name_col = None
    for n in ["name", "stock_name", "종목명", "company_name"]:
        if n in col_names:
            name_col = n
            break
    select_list = [f'"{ticker_col}" AS factset_ticker']
    if name_col:
        select_list.append(f'"{name_col}" AS name')
    try:
        query = f"""
            SELECT DISTINCT {", ".join(select_list)}
            FROM {table_ref}
            WHERE "{ticker_col}" IS NOT NULL AND TRIM("{ticker_col}") <> ''
            ORDER BY factset_ticker
        """
        data = execute_custom_query(query, connection=connection)
    except Exception:
        return pd.DataFrame(columns=["factset_ticker"])
    return pd.DataFrame(data)


@with_connection
def get_op_factset_by_ticker(factset_ticker: str, connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    market.op_factset에서 해당 factset_ticker의 전체 재무 데이터 조회.
    """
    if not factset_ticker or not str(factset_ticker).strip():
        return pd.DataFrame()
    schema = _resolve_table_schema("op_factset", connection)
    table_ref = f'"{schema}"."op_factset"' if schema != "public" else "op_factset"
    info = get_table_info("op_factset", connection=connection, schema=schema)
    if not info:
        return pd.DataFrame()
    col_names = [c["column_name"] for c in info]
    ticker_col = "factset_ticker" if "factset_ticker" in col_names else "ticker"
    if ticker_col not in col_names:
        return pd.DataFrame()
    cols_escaped = ", ".join([f'"{c}"' for c in col_names])
    ticker_escaped = str(factset_ticker).strip().replace("'", "''")
    order_clause = ' ORDER BY "dt" DESC NULLS LAST' if "dt" in col_names else ""
    try:
        query = f"""
            SELECT {cols_escaped}
            FROM {table_ref}
            WHERE "{ticker_col}" = '{ticker_escaped}'{order_clause}
        """
        data = execute_custom_query(query, connection=connection)
    except Exception:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df


@with_connection
def get_sales_factset_ticker_list(connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    market.sales_factset에 있는 factset_ticker 목록 조회 (검색용).
    name/종목명 컬럼이 있으면 함께 반환.
    """
    schema = _resolve_table_schema("sales_factset", connection)
    table_ref = f'"{schema}"."sales_factset"' if schema != "public" else "sales_factset"
    info = get_table_info("sales_factset", connection=connection, schema=schema)
    if not info:
        return pd.DataFrame(columns=["factset_ticker"])
    col_names = [c["column_name"] for c in info]
    ticker_col = "factset_ticker" if "factset_ticker" in col_names else ("ticker" if "ticker" in col_names else None)
    if not ticker_col:
        return pd.DataFrame(columns=["factset_ticker"])
    name_col = None
    for n in ["name", "stock_name", "종목명", "company_name"]:
        if n in col_names:
            name_col = n
            break
    select_list = [f'"{ticker_col}" AS factset_ticker']
    if name_col:
        select_list.append(f'"{name_col}" AS name')
    try:
        query = f"""
            SELECT DISTINCT {", ".join(select_list)}
            FROM {table_ref}
            WHERE "{ticker_col}" IS NOT NULL AND TRIM("{ticker_col}") <> ''
            ORDER BY factset_ticker
        """
        data = execute_custom_query(query, connection=connection)
    except Exception:
        return pd.DataFrame(columns=["factset_ticker"])
    return pd.DataFrame(data)


@with_connection
def get_sales_factset_by_ticker(factset_ticker: str, connection: Optional[Connection] = None) -> pd.DataFrame:
    """
    market.sales_factset에서 해당 factset_ticker의 전체 재무 데이터 조회.
    """
    if not factset_ticker or not str(factset_ticker).strip():
        return pd.DataFrame()
    schema = _resolve_table_schema("sales_factset", connection)
    table_ref = f'"{schema}"."sales_factset"' if schema != "public" else "sales_factset"
    info = get_table_info("sales_factset", connection=connection, schema=schema)
    if not info:
        return pd.DataFrame()
    col_names = [c["column_name"] for c in info]
    ticker_col = "factset_ticker" if "factset_ticker" in col_names else "ticker"
    if ticker_col not in col_names:
        return pd.DataFrame()
    cols_escaped = ", ".join([f'"{c}"' for c in col_names])
    ticker_escaped = str(factset_ticker).strip().replace("'", "''")
    order_clause = ' ORDER BY "dt" DESC NULLS LAST' if "dt" in col_names else ""
    try:
        query = f"""
            SELECT {cols_escaped}
            FROM {table_ref}
            WHERE "{ticker_col}" = '{ticker_escaped}'{order_clause}
        """
        data = execute_custom_query(query, connection=connection)
    except Exception:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    return df
