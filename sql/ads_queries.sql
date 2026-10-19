-- 三张 ADS 表的生成逻辑（等价 SQL，用于口径说明与结果核对）
-- 实际计算在 src/dws_metrics.py 中用 Spark SQL / 窗口函数执行后写入 MySQL。
-- 这里的 SQL 既可在 MySQL 里直接运行核对，也便于向面试官解释口径。

-- ---------------------------------------------------------------------------
-- ADS 1：行业 × 报告期毛利率/净利率趋势
-- 从 DWD 公司明细出发，按行业聚合，避免直接拿 ODS 算（口径统一在 DWD）。
-- ---------------------------------------------------------------------------
SELECT
    industry,
    report_date,
    ROUND(AVG((revenue - cost) / NULLIF(revenue, 0)), 6) AS gross_margin,
    ROUND(AVG(net_profit / NULLIF(revenue, 0)), 6)        AS net_margin
FROM dwd_fin_report
WHERE revenue > 0 AND cost IS NOT NULL
GROUP BY industry, report_date
ORDER BY industry, report_date;

-- ---------------------------------------------------------------------------
-- ADS 2：连续 3 期毛利率下滑的公司（窗口函数：LAG 取前 3 期，逐期下降）
-- ---------------------------------------------------------------------------
WITH m AS (
    SELECT
        stock_code, stock_name, industry, report_date,
        (revenue - cost) / NULLIF(revenue, 0) AS gross_margin
    FROM dwd_fin_report
    WHERE revenue > 0 AND cost IS NOT NULL
),
lagged AS (
    SELECT *,
        LAG(gross_margin, 1) OVER (PARTITION BY stock_code ORDER BY report_date) AS m1,
        LAG(gross_margin, 2) OVER (PARTITION BY stock_code ORDER BY report_date) AS m2,
        LAG(gross_margin, 3) OVER (PARTITION BY stock_code ORDER BY report_date) AS m3
    FROM m
)
SELECT stock_code, stock_name, industry, report_date, gross_margin
FROM lagged
WHERE m1 IS NOT NULL AND m2 IS NOT NULL AND m3 IS NOT NULL
  AND m3 > m2 AND m2 > m1 AND m1 > gross_margin;   -- 逐期下滑

-- ---------------------------------------------------------------------------
-- ADS 3：连续 2 期盈利质量 < 0.5 的公司（ocf / net_profit，< 1 即需警惕）
-- ---------------------------------------------------------------------------
WITH q AS (
    SELECT
        stock_code, stock_name, industry, report_date,
        ocf / NULLIF(net_profit, 0) AS profit_quality
    FROM dwd_fin_report
    WHERE net_profit IS NOT NULL AND ocf IS NOT NULL
),
lagged AS (
    SELECT *,
        LAG(profit_quality, 1) OVER (PARTITION BY stock_code ORDER BY report_date) AS pq1
    FROM q
)
SELECT stock_code, stock_name, industry, report_date, profit_quality
FROM lagged
WHERE pq1 IS NOT NULL AND pq1 < 0.5 AND profit_quality < 0.5;
