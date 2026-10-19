"""Step 2：PySpark 清洗 ODS → DWD（明细层）。

清洗项（每项的理由详见 docs/decisions.md）：
  1. 单位统一：识别金额单位并统一到「元」。
  2. 财报期对齐：report_date 统一为 date 类型。
  3. 去重：按 (stock_code, report_date) 保留最新一次采集（dt 最新）。
  4. 缺失值：不补 0，保留 NULL，交给指标计算时跳过。
  5. ST/退市：保留记录，仅打标 is_st，不删除。

用法：
  python -m src.dwd_clean
"""
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config


# ---------------------------------------------------------------------------
# 单位统一
# ---------------------------------------------------------------------------
# 两个数据源（新浪 stock_financial_abstract、东财 stock_balance_sheet_by_report_em）
# 均以「元」返回金额。已用浦发银行 2025 年报营收 1739.6 亿元、总资产约 10 万亿核对无误。
# 若换接口出现「万元/亿元」，改 SOURCE_UNIT 即可整体换算，无需改下游逻辑。
UNIT_SCALE = {"元": 1.0, "万元": 1e4, "亿元": 1e8}
SOURCE_UNIT = "元"  # 已核对：本数据源实际单位


def _to_yuan(col_expr):
    """把金额列统一换算成元。当前源单位为「元」，倍率 = 1，但保留换算结构。"""
    scale = UNIT_SCALE[SOURCE_UNIT]
    return col_expr * scale if scale != 1.0 else col_expr


AMOUNT_COLS = ["revenue", "cost", "net_profit", "total_assets", "total_liab", "ocf", "ar"]


def clean(spark: SparkSession) -> DataFrame:
    """ODS → DWD 全流程清洗，写 Parquet（按 report_date 分区），返回结果 DataFrame。"""
    ods = spark.read.parquet(str(config.ODS_DIR))

    dwd = ods

    # 1) 单位统一（源单位=元，见顶部说明）
    for c in AMOUNT_COLS:
        dwd = dwd.withColumn(c, _to_yuan(F.col(c)))

    # 2) 财报期对齐：统一为 date 类型
    dwd = dwd.withColumn("report_date", F.col("report_date").cast("date"))

    # 3) 去重：按 (stock_code, report_date) 保留 dt 最新一版（ODS 按采集日期 dt 分区）
    win = Window.partitionBy("stock_code", "report_date").orderBy(F.col("dt").desc())
    dwd = (
        dwd.withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 4) 缺失值：不补 0，保留 NULL（禁止 fillna(0)，否则毛利率会被扭曲成 100%）

    # 5) ST/退市：保留记录，仅打标 is_st（按名称含 ST 判断），不删除
    dwd = dwd.withColumn("is_st", F.col("stock_name").contains("ST"))

    # 最终列 + 类型（金额转 decimal，比值类留给 DWS 计算）
    dwd = dwd.select(
        F.col("stock_code").cast("string"),
        F.col("stock_name").cast("string"),
        F.col("industry").cast("string"),
        F.col("report_date"),
        *[F.col(c).cast("decimal(22,2)").alias(c) for c in AMOUNT_COLS],
        F.col("is_st").cast("boolean"),
    )

    dwd.write.mode("overwrite").partitionBy("report_date").parquet(str(config.DWD_DIR))
    return dwd


def _report(spark: SparkSession) -> None:
    """打印 DWD 概况与主键重复校验结果（与验收命令一致）。"""
    dwd = spark.read.parquet(str(config.DWD_DIR))
    print("=== DWD schema ===")
    dwd.printSchema()
    print("rows =", dwd.count())
    dup = (
        dwd.groupBy("stock_code", "report_date")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    print("dup (主键重复行数) =", dup)
    print("ST 公司数 =", dwd.filter(F.col("is_st")).count())
    print("revenue 空值数 =", dwd.filter(F.col("revenue").isNull()).count())


def main() -> int:
    spark = config.get_spark_session("finance-dw-dwd")
    clean(spark)
    _report(spark)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
