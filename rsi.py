import os
import pandas as pd
from typing import List, Optional

def _rsi_from_prices(closes: List[float], period: int = 14) -> List[Optional[float]]:
    """가격 리스트로 RSI(period) 계산. 앞 period개는 None, 이후 RSI 값."""
    n = len(closes)
    result = [None] * n
    if n < period + 1:
        return result
    vals = []
    for i in range(n):
        v = closes[i]
        if v is None or (isinstance(v, float) and pd.isna(v)):
            v = vals[-1] if vals else None
        vals.append(v)
    if None in vals:
        return result
    price_changes = [vals[i] - vals[i - 1] for i in range(1, n)]
    up = [c if c > 0 else 0 for c in price_changes]
    down = [abs(c) if c < 0 else 0 for c in price_changes]
    avg_gain = sum(up[:period]) / period
    avg_loss = sum(down[:period]) / period
    for i in range(period, n):
        avg_gain = (avg_gain * (period - 1) + up[i - 1]) / period
        avg_loss = (avg_loss * (period - 1) + down[i - 1]) / period
        result[i] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1 + avg_gain / avg_loss))
    return result


def calculate_twoweeks_rsi(file_path: str, sheet_name: str = "raw_price", period: int = 14) -> pd.DataFrame:
    """
    raw_price 시트 구조: 1열=날짜, 나머지 열=종목 티커별 가격.
    종목별로 RSI(period)를 계산해 RSI_{period}_티커 컬럼으로 추가한 DataFrame 반환.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
    except Exception as e:
        raise Exception(f"Excel 파일을 읽는 중 오류 발생: {e}")
    if df.empty or len(df.columns) < 2:
        raise ValueError("raw_price 시트에 날짜 열과 최소 1개 이상의 종목 가격 열이 필요합니다.")
    ticker_cols = [c for c in df.columns[1:] if pd.api.types.is_numeric_dtype(df[c])]
    if not ticker_cols:
        ticker_cols = [c for c in df.columns[1:]]
    for ticker in ticker_cols:
        series = pd.to_numeric(df[ticker], errors="coerce")
        closes = series.tolist()
        rsi_list = _rsi_from_prices(closes, period=period)
        df[f"RSI_{period}_{ticker}"] = rsi_list
    return df


def plot_rsi(df: pd.DataFrame, period: int = 14) -> None:
    """DataFrame의 날짜 열과 RSI_{period}_* 열로 RSI 차트 출력."""
    import matplotlib.pyplot as plt
    date_col = df.columns[0]
    prefix = f"RSI_{period}_"
    rsi_cols = [c for c in df.columns if c.startswith(prefix)]
    if not rsi_cols:
        return
    x = pd.to_datetime(df[date_col])
    plt.figure(figsize=(12, 6))
    for col in rsi_cols:
        plt.plot(x, df[col], label=col.replace(prefix, ""), alpha=0.8)
    plt.axhline(y=70, color="red", linestyle="--", alpha=0.5, label="과매수(70)")
    plt.axhline(y=30, color="green", linestyle="--", alpha=0.5, label="과매도(30)")
    plt.ylim(0, 100)
    plt.ylabel(f"RSI({period})")
    plt.legend(loc="best", fontsize=8)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # ========== 설정 (여기서 period 변경) ==========
    RSI_PERIOD = 14  # RSI 기간 (일)
    # ==============================================
    base_folder = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_folder, "index.xlsx")
    sheet_name = "raw_price"
    try:
        print(f"Excel 읽기: {file_path}, RSI 기간: {RSI_PERIOD}")
        df = calculate_twoweeks_rsi(file_path, sheet_name, period=RSI_PERIOD)
        prefix = f"RSI_{RSI_PERIOD}_"
        rsi_cols = [c for c in df.columns if c.startswith(prefix)]
        print(f"총 {len(df)}행, RSI 컬럼: {', '.join(rsi_cols)}")
        plot_rsi(df, period=RSI_PERIOD)
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()