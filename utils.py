"""
공통 유틸리티 함수 모음
"""
from datetime import datetime, timedelta
from typing import Optional
from call import execute_custom_query, with_connection
from psycopg2.extensions import connection as Connection


def get_index_country_code(index_name: str) -> str:
    """
    지수명에서 국가 코드를 반환
    
    Args:
        index_name: 지수명 (예: 'SPX Index', 'NDX Index', 'HSCEI Index')
    
    Returns:
        str: 국가 코드 ('US', 'HK', 'IN', 'JP', 'VN', 'EU', 'KR' 등)
    """
    index_name_upper = index_name.upper()
    
    # 미국 지수
    if any(x in index_name_upper for x in ['SPX', 'NDX', 'RUT', 'DJX', 'OEX']):
        return 'US'
    
    # 홍콩 지수
    if any(x in index_name_upper for x in ['HSCEI', 'HSTECH', 'HSI', 'HANG']):
        return 'HK'
    
    # 인도 지수
    if 'NIFTY' in index_name_upper:
        return 'IN'
    
    # 일본 지수
    if any(x in index_name_upper for x in ['NKY', 'NIKKEI', 'TOPIX']):
        return 'JP'
    
    # 베트남 지수
    if 'VN30' in index_name_upper or 'VN' in index_name_upper:
        return 'VN'
    
    # 유럽 지수
    if any(x in index_name_upper for x in ['SX5E', 'SXX', 'STOXX', 'DAX', 'CAC', 'FTSE']):
        return 'EU'
    
    # 한국 지수
    if any(x in index_name_upper for x in ['KOSPI', 'KOSDAQ', 'KRW']):
        return 'KR'
    
    # 기본값: US (대부분의 주요 지수가 미국)
    return 'US'


@with_connection
def get_business_day_by_country(date, days_back: int, country_code: str, connection: Optional[Connection] = None) -> datetime.date:
    """
    주어진 날짜에서 지정된 영업일 수만큼 이전 날짜를 반환 (국가별 영업일 기준)
    business_day 테이블을 참고하여 해당 국가의 영업일만 카운트
    
    Args:
        date: 기준 날짜
        days_back: 이전 영업일 수 (1 = 1영업일 전, 2 = 2영업일 전)
        country_code: 국가 코드 ('US', 'HK', 'IN', 'JP', 'VN', 'EU', 'KR' 등)
        connection: 데이터베이스 연결 객체
    
    Returns:
        datetime.date: 이전 영업일 날짜
    """
    if days_back <= 0:
        return date
    
    # business_day 테이블에서 해당 국가의 영업일 조회
    # 충분한 범위의 날짜를 조회 (days_back * 2 정도의 여유)
    start_date = date - timedelta(days=days_back * 3)
    end_date = date
    
    query = f"""
        SELECT dt
        FROM business_day
        WHERE dt >= '{start_date.strftime('%Y-%m-%d')}'
          AND dt <= '{end_date.strftime('%Y-%m-%d')}'
          AND "{country_code}" = 1
        ORDER BY dt DESC
    """
    
    try:
        data = execute_custom_query(query, connection=connection)
        if not data:
            # business_day 테이블에 데이터가 없으면 기존 로직 사용 (주말만 체크)
            return get_business_day(date, days_back)
        
        # 영업일 리스트 생성
        business_dates = [row[0] for row in data if row[0] < date]
        
        if len(business_dates) < days_back:
            # 충분한 영업일이 없으면 기존 로직 사용
            return get_business_day(date, days_back)
        
        # days_back번째 영업일 반환
        return business_dates[days_back - 1]
    except Exception as e:
        # 에러 발생 시 기존 로직 사용
        return get_business_day(date, days_back)


def get_business_day(date, days_back):
    """
    주어진 날짜에서 지정된 영업일 수만큼 이전 날짜를 반환 (기본 로직: 주말만 체크)
    국가별 영업일이 필요한 경우 get_business_day_by_country 사용
    
    Args:
        date: 기준 날짜
        days_back: 이전 영업일 수 (1 = 어제, 2 = 그 전 영업일)
    
    Returns:
        datetime.date: 이전 영업일 날짜
    """
    current_date = date
    business_days_count = 0
    
    while business_days_count < days_back:
        current_date = current_date - timedelta(days=1)
        # 주말이 아니면 영업일
        if current_date.weekday() < 5:  # 0=월요일, 4=금요일
            business_days_count += 1
    
    return current_date


def get_period_dates(today):
    """
    각 기간에 대한 날짜 계산
    시차 고려: 해외 시장 데이터는 한국 시간 기준 하루 전까지 있을 수 있음
    """
    # 1D 기간의 경우: 한국 시간 기준 -1 영업일(어제)과 -2 영업일(그 전 영업일)을 비교
    # 시작 날짜: -2 영업일 (그 전 영업일, 예: 12/05)
    # 종료 날짜: -1 영업일 (어제, 예: 12/08)
    prev_business = get_business_day(today, 2)  # -2 영업일 (시작 날짜)
    yesterday_business = get_business_day(today, 1)  # -1 영업일 (종료 날짜)
    
    # MTD: 이번 달 1일부터 어제까지
    mtd_start = today.replace(day=1)
    mtd_end = today - timedelta(days=1)
    
    period_dates = {
        '1D': (prev_business, yesterday_business),  # 시작: -2 영업일, 종료: -1 영업일
        '1W': (today - timedelta(days=7), today - timedelta(days=1)),  # 시차 고려
        'MTD': (mtd_start, mtd_end),  # 이번 달 1일부터 어제까지
        '1M': (today - timedelta(days=30), today - timedelta(days=1)),  # 시차 고려
        '3M': (today - timedelta(days=90), today - timedelta(days=1)),  # 시차 고려
        '6M': (today - timedelta(days=180), today - timedelta(days=1)),  # 시차 고려
        'YTD': (today.replace(month=1, day=1), today - timedelta(days=1)),  # 시차 고려
        '1Y': (today - timedelta(days=365), today - timedelta(days=1))  # 시차 고려
    }
    
    return period_dates


def get_period_options():
    """
    기간 선택 옵션 및 라벨 반환
    순서: 1D / 1W / MTD / 1M / 3M / 6M / YTD / 1Y
    """
    period_options = ['1D', '1W', 'MTD', '1M', '3M', '6M', 'YTD', '1Y']
    period_labels = {
        '1D': '1일',
        '1W': '1주',
        'MTD': '이번 달',
        '1M': '1개월',
        '3M': '3개월',
        '6M': '6개월',
        'YTD': '올해',
        '1Y': '1년'
    }
    return period_options, period_labels


def get_period_dates_from_base_date(base_date):
    """
    기준일자를 기준으로 각 기간에 대한 시작일과 종료일 계산
    차트의 기간 계산과 동일한 로직 사용
    
    Args:
        base_date: 기준일자 (예: 2025-12-08)
    
    Returns:
        dict: 각 기간별 (시작일, 종료일) 튜플
    """
    # 1D: 기준일자와 1영업일 전 비교
    # 예: 12/08 기준 -> 시작: 12/05 (1영업일 전), 종료: 12/08
    start_1d = get_business_day(base_date, 1)
    end_1d = base_date
    
    # 1W: 기준일자와 1주일 전 비교 (7일 전)
    # 예: 12/08 기준 -> 시작: 12/02 (7일 전), 종료: 12/08
    start_1w = base_date - timedelta(days=7)
    end_1w = base_date
    
    # MTD: 기준일자의 월 시작일과 기준일자 비교
    # 예: 12/08 기준 -> 시작: 12/01 (월 시작일), 종료: 12/08
    start_mtd = base_date.replace(day=1)
    end_mtd = base_date
    
    # 1M: 기준일자와 30일 전 비교
    start_1m = base_date - timedelta(days=30)
    end_1m = base_date
    
    # 3M: 기준일자와 90일 전 비교
    start_3m = base_date - timedelta(days=90)
    end_3m = base_date
    
    # 6M: 기준일자와 180일 전 비교
    start_6m = base_date - timedelta(days=180)
    end_6m = base_date
    
    # YTD: 기준일자의 연도 시작일과 기준일자 비교
    start_ytd = base_date.replace(month=1, day=1)
    end_ytd = base_date
    
    # 1Y: 기준일자와 365일 전 비교
    start_1y = base_date - timedelta(days=365)
    end_1y = base_date
    
    period_dates = {
        '1D': (start_1d, end_1d),
        '1W': (start_1w, end_1w),
        'MTD': (start_mtd, end_mtd),
        '1M': (start_1m, end_1m),
        '3M': (start_3m, end_3m),
        '6M': (start_6m, end_6m),
        'YTD': (start_ytd, end_ytd),
        '1Y': (start_1y, end_1y)
    }
    
    return period_dates