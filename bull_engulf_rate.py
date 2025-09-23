# bull_engulf_rate.py
# 看涨吞没形态频率因子实现
# 使用polars进行高效的滚动窗口计算

# coin=["BTCUSDT","ETHUSDT","XRPUSDT"]
# timeframe="15m"
# window=["550","20","850"]

import polars as pl

EPS = 1e-9


def bull_engulf_rate(window: int) -> pl.Expr:
    """
    看涨吞没频率（单窗函数规范）：
    仅接收 window，返回 pl.Expr，并正确 alias(f"bull_engulf_rate_{window}")
    依赖列：open, high, low, close（来自你的K线输入）

    计算逻辑：
    1. 当前K线为阳线 (close > open)
    2. 当前K线的实体完全吞没前一根K线的实体
    3. 统计在滚动窗口内的看涨吞没发生频率
    """
    # 当前K线实体边界
    body_low  = pl.min_horizontal(pl.col("open"),  pl.col("close"))
    body_high = pl.max_horizontal(pl.col("open"),  pl.col("close"))

    # 前一根K线实体边界
    prev_low  = pl.min_horizontal(pl.col("open").shift(1),  pl.col("close").shift(1))
    prev_high = pl.max_horizontal(pl.col("open").shift(1),  pl.col("close").shift(1))

    # 看涨吞没条件：
    # 1. 当前为阳线 (close > open)
    # 2. 当前实体下沿 <= 前实体下沿
    # 3. 当前实体上沿 >= 前实体上沿
    bull = (pl.col("close") > pl.col("open")) & (body_low <= prev_low) & (body_high >= prev_high)

    # 计算滚动窗口内的看涨吞没频率
    return (pl.when(bull).then(1.0).otherwise(0.0)
              .rolling_mean(window_size=window, min_periods=window)
              .alias(f"bull_engulf_rate_{window}"))


# 使用示例:
# df = pl.DataFrame({
#     "open":  [100, 102, 101, 103, 102],
#     "high":  [105, 106, 104, 107, 105],
#     "low":   [98,  99,  100, 101, 100],
#     "close": [104, 103, 105, 106, 101]
# })
#
# result = df.with_columns(bull_engulf_rate(3))
# print(result)
