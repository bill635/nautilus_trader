# factors_ar3.py
# AR(3)反转因子实现
# 使用polars进行高效的滚动窗口计算

# coin=["DOGEUSDT","SOLUSDT"]
# timeframe="15m"
# window=["30","30"]

import polars as pl

EPS   = 1e-12   # 防除零
RIDGE = 1e-12   # 对角小岭


def ar3_reversal(window: int) -> pl.Expr:
    """
    AR(3) 反转因子（单窗函数）：
    在滚动窗口内对 r_t ~ r_{t-1}, r_{t-2}, r_{t-3} 做"中心化"OLS，
    用 {r_t, r_{t-1}, r_{t-2}} 预测 ŷ_{t+1}，返回 -ŷ_{t+1}。
    依赖列：close
    返回列名：ar3_reversal_{window}
    """
    if window < 6:
        raise ValueError("window 必须 >= 6（建议 ≥ 10）")

    c = pl.col("close").cast(pl.Float64)
    r = c / c.shift(1) - 1.0

    x1, x2, x3 = r.shift(1), r.shift(2), r.shift(3)
    y = r

    roll_sum  = lambda e: e.rolling_sum(window_size=window, min_periods=window)
    roll_mean = lambda e: e.rolling_mean(window_size=window, min_periods=window)

    # 中心化
    m1, m2, m3, my = roll_mean(x1), roll_mean(x2), roll_mean(x3), roll_mean(y)
    x1c, x2c, x3c, yc = x1 - m1, x2 - m2, x3 - m3, y - my

    # Sxx（加岭）与 Sxy
    s11 = roll_sum(x1c * x1c) + pl.lit(RIDGE)
    s22 = roll_sum(x2c * x2c) + pl.lit(RIDGE)
    s33 = roll_sum(x3c * x3c) + pl.lit(RIDGE)
    s12 = roll_sum(x1c * x2c)
    s13 = roll_sum(x1c * x3c)
    s23 = roll_sum(x2c * x3c)

    t1 = roll_sum(x1c * yc)
    t2 = roll_sum(x2c * yc)
    t3 = roll_sum(x3c * yc)

    # 3x3 逆
    det = (
        s11 * (s22 * s33 - s23 * s23)
        - s12 * (s12 * s33 - s13 * s23)
        + s13 * (s12 * s23 - s13 * s22)
    )
    det = pl.when(det.abs() < pl.lit(EPS)).then(pl.lit(EPS)).otherwise(det)

    inv11 = (s22 * s33 - s23 * s23) / det
    inv12 = (s13 * s23 - s12 * s33) / det
    inv13 = (s12 * s23 - s13 * s22) / det
    inv22 = (s11 * s33 - s13 * s13) / det
    inv23 = (s12 * s13 - s11 * s23) / det
    inv33 = (s11 * s22 - s12 * s12) / det

    # 斜率与截距
    b1 = inv11 * t1 + inv12 * t2 + inv13 * t3
    b2 = inv12 * t1 + inv22 * t2 + inv23 * t3
    b3 = inv13 * t1 + inv23 * t2 + inv33 * t3
    b0 = my - (b1 * m1 + b2 * m2 + b3 * m3)

    # ŷ_{t+1} = b0 + b1*r_t + b2*r_{t-1} + b3*r_{t-2}; 因子取反转
    yhat_next = b0 + b1 * r + b2 * x1 + b3 * x2
    return (-yhat_next).alias(f"ar3_reversal_{window}")


# 使用示例:
# df = pl.DataFrame({
#     "close": [100, 101, 102, 103, 102, 101, 100, 99, 98, 99, 100, 101]
# })
#
# result = df.with_columns(ar3_reversal(10))
# print(result)
