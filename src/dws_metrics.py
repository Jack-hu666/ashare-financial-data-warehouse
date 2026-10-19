"""Step 3：Spark SQL 指标计算 → DWS / ADS，写 MySQL。

核心用 Spark SQL + 窗口函数实现：
  - 同比          LAG(gross_margin, 4) OVER (PARTITION BY stock_code ORDER BY report_date)
  - 行业排名      RANK() OVER (PARTITION BY industry, report_date ORDER BY gross_margin DESC)
  - 连续下滑标记  LAG 取前 3 期，判断逐期下降

产出 5 张表（见 sql/ddl.sql）：
  dws_company_metrics / dws_industry_metrics / ads_industry_margin_trend
  / ads_consecutive_decline / ads_low_quality_profit

写库方式：结果表都很小（公司×报告期级别），用 pandas + SQLAlchemy + pymysql
写入，避免额外维护 JDBC driver；建表结构统一由 sql/ddl.sql 控制。

用法：
  python -m src.dws_metrics
"""
import sys

import pandas as pd
from sqlalchemy import create_engine, text

from . import config


# ---------------------------------------------------------------------------
# 指标计算（Spark SQL）
# ---------------------------------------------------------------------------
METRIC_SQL = """
CREATE OR REPLACE TEMP VIEW company_metrics AS
SELECT
    stock_code, stock_name, industry, report_date,
    revenue, cost, net_profit, total_assets, total_liab, ocf, ar,
    (revenue - cost) / NULLIF(revenue, 0)                             AS gross_margin,
    net_profit / NULLIF(revenue, 0)                                   AS net_margin,
    net_profit / NULLIF(total_assets - total_liab, 0)                 AS roe,
    total_liab / NULLIF(total_assets, 0)                              AS debt_ratio,
    ocf / NULLIF(net_profit, 0)                                       AS profit_quality,
    revenue / NULLIF(AVG(ar) OVER (PARTITION BY stock_code
                                   ORDER BY report_date
                                   ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 0)
                                                                      AS ar_turnover
FROM dwd_view
"""

WINDOW_SQL = """
CREATE OR REPLACE TEMP VIEW company_metrics_ranked AS
SELECT *,
    LAG(gross_margin, 4) OVER (PARTITION BY stock_code ORDER BY report_date)
        AS margin_yoy,
    RANK() OVER (PARTITION BY industry, report_date ORDER BY gross_margin DESC)
        AS industry_rank
FROM company_metrics
"""

ADS_DECLINE_SQL = """
CREATE OR REPLACE TEMP VIEW ads_consecutive_decline AS
SELECT stock_code, stock_name, industry, report_date, gross_margin
FROM (
    SELECT *,
        LAG(gross_margin, 1) OVER (PARTITION BY stock_code ORDER BY report_date) AS m1,
        LAG(gross_margin, 2) OVER (PARTITION BY stock_code ORDER BY report_date) AS m2,
        LAG(gross_margin, 3) OVER (PARTITION BY stock_code ORDER BY report_date) AS m3
    FROM company_metrics
)
WHERE m3 > m2 AND m2 > m1 AND m1 > gross_margin
"""

ADS_LOW_QUALITY_SQL = """
CREATE OR REPLACE TEMP VIEW ads_low_quality_profit AS
SELECT stock_code, stock_name, industry, report_date, profit_quality
FROM (
    SELECT *,
        LAG(profit_quality, 1) OVER (PARTITION BY stock_code ORDER BY report_date) AS pq1
    FROM company_metrics
)
WHERE pq1 < 0.5 AND profit_quality < 0.5
"""


def compute_metrics(spark):
    """读 DWD，用 Spark SQL 计算 5 张表，返回 {表名: Spark DataFrame}。"""
    spark.read.parquet(str(config.DWD_DIR)).createOrReplaceTempView("dwd_view")
    spark.sql(METRIC_SQL)
    spark.sql(WINDOW_SQL)

    company = spark.sql("SELECT * FROM company_metrics_ranked")

    industry = spark.sql(
        """
        SELECT
            industry, report_date,
            COUNT(DISTINCT stock_code)                                   AS company_cnt,
            AVG(gross_margin)                                            AS gross_margin,
            AVG(net_margin)                                              AS net_margin,
            AVG(roe)                                                     AS roe,
            AVG(debt_ratio)                                              AS debt_ratio,
            AVG(profit_quality)                                          AS profit_quality,
            AVG(ar_turnover)                                             AS ar_turnover
        FROM company_metrics
        GROUP BY industry, report_date
        """
    )

    trend = spark.sql(
        """
        SELECT industry, report_date, gross_margin, net_margin
        FROM company_metrics
        """
    ).groupBy("industry", "report_date").agg(
        {"gross_margin": "avg", "net_margin": "avg"}
    ).selectExpr(
        "industry", "report_date", "`avg(gross_margin)` AS gross_margin",
        "`avg(net_margin)` AS net_margin",
    )

    spark.sql(ADS_DECLINE_SQL)
    spark.sql(ADS_LOW_QUALITY_SQL)
    decline = spark.sql("SELECT * FROM ads_consecutive_decline")
    low_quality = spark.sql("SELECT * FROM ads_low_quality_profit")

    return {
        "dws_company_metrics": company,
        "dws_industry_metrics": industry,
        "ads_industry_margin_trend": trend,
        "ads_consecutive_decline": decline,
        "ads_low_quality_profit": low_quality,
    }


# ---------------------------------------------------------------------------
# 写 MySQL
# ---------------------------------------------------------------------------
def _engine():
    c = config.MYSQL
    url = (
        f"mysql+pymysql://{c['user']}:{c['password']}@{c['host']}:{c['port']}"
        f"/{c['database']}?charset=utf8mb4"
    )
    return create_engine(url)


def _sync_schema(engine) -> None:
    """按 sql/ddl.sql 建表（IF NOT EXISTS），保证 DECIMAL 等类型正确。"""
    ddl = (config.BASE_DIR / "sql" / "ddl.sql").read_text(encoding="utf-8")
    with engine.begin() as conn:
        for stmt in ddl.split(";"):
            s = stmt.strip()
            if s and not s.startswith("--"):
                conn.execute(text(s))


def _to_pandas(sdf) -> pd.DataFrame:
    """Spark DataFrame → pandas，统一 dtype 以匹配 MySQL 表结构。"""
    pdf = sdf.toPandas()
    if "report_date" in pdf.columns:
        pdf["report_date"] = pd.to_datetime(pdf["report_date"]).dt.date
    for c in pdf.columns:
        if pdf[c].dtype == "object":
            pdf[c] = pdf[c].astype(str)
    return pdf


def write_mysql(tables: dict) -> dict:
    """把 5 张表写入 MySQL（先清空再插入，保证可重跑）。"""
    engine = _engine()
    _sync_schema(engine)
    counts = {}
    with engine.begin() as conn:
        for name, sdf in tables.items():
            conn.execute(text(f"DELETE FROM {name}"))
        # 逐张表 to_sql（append 到已清空的表，类型由 ddl.sql 决定）
    for name, sdf in tables.items():
        pdf = _to_pandas(sdf)
        pdf.to_sql(name, engine, if_exists="append", index=False, chunksize=500)
        counts[name] = len(pdf)
    engine.dispose()
    return counts


def main() -> int:
    spark = config.get_spark_session("finance-dw-dws")
    tables = compute_metrics(spark)

    # 先打印结果规模（即便 MySQL 未就绪也能看到计算是否正确）
    for name, sdf in tables.items():
        print(f"{name}: rows = {sdf.count()}")

    counts = write_mysql(tables)
    print("已写入 MySQL：", counts)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
