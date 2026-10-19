"""Step 4：3 条数据质量校验，每条输出明确的 PASS/FAIL。

规则：
  1. 主键唯一   —— DWD 层 (stock_code, report_date) 无重复
  2. 空值率     —— DWD 层 revenue 空值率 < 5%，超过则 FAIL
  3. 环比波动   —— 任一行业平均毛利率环比变化绝对值 > 0.3 则告警

--inject-bad-data：注入一条重复主键，验证规则 1 确实会 FAIL（证明校验有效）。

用法：
  python -m src.quality_check
  python -m src.quality_check --inject-bad-data
"""
import argparse
import sys

from pyspark.sql import functions as F
from pyspark.sql import Window

from . import config


def _dup_count(dwd):
    return (
        dwd.groupBy("stock_code", "report_date")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def run_checks(spark, inject_bad: bool = False) -> list[tuple[str, str, str]]:
    dwd = spark.read.parquet(str(config.DWD_DIR))

    if inject_bad:
        # 故意造错：复制第一条记录，制造重复主键（仅内存中验证，不回写 DWD）
        dwd = dwd.union(dwd.limit(1))

    results = []

    # 规则 1：主键唯一
    dup = _dup_count(dwd)
    results.append(("主键唯一", "PASS" if dup == 0 else "FAIL", f"重复主键行数 = {dup}"))

    # 规则 2：revenue 空值率 < 5%
    total = dwd.count()
    null_rev = dwd.filter(F.col("revenue").isNull()).count()
    rate = null_rev / total if total else 0.0
    results.append(("revenue空值率", "PASS" if rate < 0.05 else "FAIL", f"空值率 = {rate:.4f}"))

    # 规则 3：行业平均毛利率环比波动（abs > 0.3 告警）
    gm = dwd.withColumn(
        "gross_margin",
        (F.col("revenue") - F.col("cost")) / F.nullif(F.col("revenue"), F.lit(0)),
    )
    ind = gm.groupBy("industry", "report_date").agg(
        F.avg("gross_margin").alias("avg_gm")
    )
    win = Window.partitionBy("industry").orderBy("report_date")
    ind = ind.withColumn("prev_gm", F.lag("avg_gm", 1).over(win))
    ind = ind.withColumn("momo", F.abs(F.col("avg_gm") - F.col("prev_gm")))
    max_momo = ind.agg(F.max("momo")).collect()[0][0]
    max_momo = float(max_momo) if max_momo is not None else 0.0
    results.append(("行业毛利率环比波动", "PASS" if max_momo <= 0.3 else "FAIL", f"最大环比变化 = {max_momo:.4f}"))

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="数据质量校验")
    parser.add_argument("--inject-bad-data", action="store_true", help="注入重复主键验证规则1")
    args = parser.parse_args()

    spark = config.get_spark_session("finance-dw-qc")
    results = run_checks(spark, inject_bad=args.inject_bad_data)

    print("\n" + "=" * 60)
    print("数据质量校验结果" + ("（已注入坏数据）" if args.inject_bad_data else ""))
    print("=" * 60)
    all_pass = True
    for name, status, detail in results:
        print(f"[{status}] {name} —— {detail}")
        all_pass &= (status == "PASS")
    print("=" * 60)
    print("总结：", "全部 PASS" if all_pass else "存在 FAIL")
    spark.stop()
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
