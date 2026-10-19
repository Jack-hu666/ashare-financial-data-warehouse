"""Step 1：AkShare 采集 A 股财务数据 → ODS（Parquet，按 dt 分区）。

数据源（4 个 AkShare 接口，按公司维度各调一次）：
  1. stock_profit_sheet_by_report_em(symbol)    —— 东财利润表：营业总收入/营业成本/净利润
  2. stock_balance_sheet_by_report_em(symbol)   —— 东财资产负债表：总资产/总负债/应收账款
  3. stock_cash_flow_sheet_by_report_em(symbol) —— 东财现金流量表：经营活动现金流量净额
  4. stock_profile_cninfo(symbol)               —— 巨潮公司档案：所属行业

为什么不用新浪 stock_financial_abstract：
  其「营业成本」字段实际是「营业总成本」（已用贵州茅台核对：新浪给出 545 亿，
  而东财 OPERATE_COST 真营业成本为 138 亿，算得毛利率 92.1% 与新浪自带 91.93% 吻合），
  会导致毛利率严重偏低，故统一改用东财三大报表，口径一致、字段正确。

原则（ODS 层）：
  - 金额保持 AkShare 原始数值（元），不做任何换算/补 0/去重/过滤（仅按年份范围裁剪）
  - 单公司拉取失败记录到失败清单，不补假数据
  - 每次运行落一个 dt=YYYY-MM-DD 分区，并写采集日志

用法：
  python -m src.ingest                        # 全量
  python -m src.ingest --limit 3              # 先跑 3 家验证
  python -m src.ingest --codes 600030,000333  # 补采指定代码
"""
import argparse
import logging
import sys
import time
from datetime import datetime

import pandas as pd

from . import config


# ---------------------------------------------------------------------------
# AkShare 接口封装
# ---------------------------------------------------------------------------
def em_symbol(code: str) -> str:
    """东财接口需要带交易所前缀：6 开头沪市 SH，其余深市 SZ。"""
    return ("SH" if code.startswith("6") else "SZ") + code


def _fetch_em_profit(em_code: str) -> pd.DataFrame:
    """东财利润表：营业收入 / 营业成本 / 净利润。

    注意：金融企业（银行/券商/保险）的利润表没有 TOTAL_OPERATE_INCOME、OPERATE_COST
    两列，但有 OPERATE_INCOME（营业收入）。为统一口径，revenue 取「营业收入 OPERATE_INCOME」，
    缺失时才回退到 TOTAL_OPERATE_INCOME；cost 取 OPERATE_COST（金融企业缺失→NULL）。
    """
    import akshare as ak

    df = ak.stock_profit_sheet_by_report_em(symbol=em_code)
    out = pd.DataFrame()
    out["report_date"] = pd.to_datetime(df["REPORT_DATE"]).dt.normalize()

    rev = pd.to_numeric(df["OPERATE_INCOME"], errors="coerce") if "OPERATE_INCOME" in df.columns else pd.NA
    if "OPERATE_INCOME" not in df.columns and "TOTAL_OPERATE_INCOME" in df.columns:
        rev = pd.to_numeric(df["TOTAL_OPERATE_INCOME"], errors="coerce")
    out["revenue"] = rev
    out["cost"] = (
        pd.to_numeric(df["OPERATE_COST"], errors="coerce")
        if "OPERATE_COST" in df.columns
        else pd.NA
    )
    out["net_profit"] = (
        pd.to_numeric(df["NETPROFIT"], errors="coerce")
        if "NETPROFIT" in df.columns
        else pd.NA
    )
    return out


def _fetch_em_balance(em_code: str) -> pd.DataFrame:
    """东财资产负债表：总资产 / 总负债 / 应收账款。"""
    import akshare as ak

    df = ak.stock_balance_sheet_by_report_em(symbol=em_code)
    out = pd.DataFrame()
    out["report_date"] = pd.to_datetime(df["REPORT_DATE"]).dt.normalize()
    # 券商等金融企业的资产负债表可能缺少 ACCOUNTS_RECE 列，缺失按 NULL 处理
    for src, dst in (
        ("TOTAL_ASSETS", "total_assets"),
        ("TOTAL_LIABILITIES", "total_liab"),
        ("ACCOUNTS_RECE", "ar"),
    ):
        out[dst] = pd.to_numeric(df[src], errors="coerce") if src in df.columns else pd.NA
    return out


def _fetch_em_cashflow(em_code: str) -> pd.DataFrame:
    """东财现金流量表：经营活动现金流量净额。"""
    import akshare as ak

    df = ak.stock_cash_flow_sheet_by_report_em(symbol=em_code)
    out = pd.DataFrame()
    out["report_date"] = pd.to_datetime(df["REPORT_DATE"]).dt.normalize()
    out["ocf"] = (
        pd.to_numeric(df["NETCASH_OPERATE"], errors="coerce")
        if "NETCASH_OPERATE" in df.columns
        else pd.NA
    )
    return out


def _fetch_industry(code: str) -> str:
    """巨潮公司档案：取证监会行业分类（所属行业）。"""
    import akshare as ak

    profile = ak.stock_profile_cninfo(symbol=code)
    return str(profile["所属行业"].iloc[0])


def _with_retry(fn, *args, retries=None, **kwargs):
    """对单公司拉取做指数退避重试，最终失败则抛出。"""
    retries = retries or config.MAX_RETRY
    last = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 —— 任何接口异常都按失败重试
            last = exc
            time.sleep(config.SLEEP_SECONDS * (i + 1))
    raise last


# ---------------------------------------------------------------------------
# 单公司采集
# ---------------------------------------------------------------------------
ODS_COLS = [
    "stock_code", "stock_name", "industry", "report_date",
    "revenue", "cost", "net_profit", "total_assets", "total_liab", "ocf", "ar",
]


def fetch_company(code: str, name: str) -> pd.DataFrame:
    """拉取一家公司的三张报表 + 行业，合并成 (report_date × 字段) 长表。"""
    em = em_symbol(code)
    profit = _with_retry(_fetch_em_profit, em)
    time.sleep(config.SLEEP_SECONDS)
    balance = _with_retry(_fetch_em_balance, em)
    time.sleep(config.SLEEP_SECONDS)
    cashflow = _with_retry(_fetch_em_cashflow, em)
    time.sleep(config.SLEEP_SECONDS)
    industry = _with_retry(_fetch_industry, code)

    merged = profit.merge(balance, on="report_date", how="inner").merge(
        cashflow, on="report_date", how="inner"
    )
    merged["stock_code"] = code
    merged["stock_name"] = name
    merged["industry"] = industry

    merged = merged[
        (merged["report_date"].dt.year >= config.START_YEAR)
        & (merged["report_date"].dt.year <= config.END_YEAR)
    ]
    return merged[ODS_COLS].sort_values("report_date")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def run(limit: int | None, sleep_seconds: float | None, codes: str | None = None) -> dict:
    """采集全部（或前 limit 家 / 指定 codes 家）公司，返回统计信息。"""
    if sleep_seconds is not None:
        config.SLEEP_SECONDS = sleep_seconds

    frames, failed = [], []
    if codes:
        code_set = {c.strip() for c in codes.split(",")}
        companies = [(c, n) for c, n in config.COMPANIES if c in code_set]
    elif limit:
        companies = config.COMPANIES[:limit]
    else:
        companies = config.COMPANIES

    for i, (code, name) in enumerate(companies, 1):
        try:
            df = fetch_company(code, name)
            frames.append(df)
            logging.info("OK %s %s rows=%d", code, name, len(df))
        except Exception as exc:  # noqa: BLE001
            failed.append((code, name, type(exc).__name__, str(exc)[:120]))
            logging.warning("FAIL %s %s %s: %s", code, name, type(exc).__name__, str(exc)[:120])
        finally:
            time.sleep(config.SLEEP_SECONDS)
        if i % 10 == 0:
            logging.info("进度 %d/%d", i, len(companies))

    if not frames:
        logging.error("全部公司采集失败，未产出数据，停止。")
        return {"ok": 0, "failed": len(failed), "rows": 0, "partition": None}

    combined = pd.concat(frames, ignore_index=True)
    # report_date 本就是日期，转成 date 再落盘（避免 pyarrow 写成 TIMESTAMP(NANOS)，
    # Spark 的 Parquet 读取器不支持纳秒精度时间戳）
    combined["report_date"] = combined["report_date"].dt.date
    today = datetime.now().strftime("%Y-%m-%d")
    part_dir = config.ODS_DIR / f"dt={today}"
    part_dir.mkdir(parents=True, exist_ok=True)
    # 同一 dt 分区内多次运行用递增 part 序号，避免覆盖前一次结果
    part_idx = len(list(part_dir.glob("part-*.parquet")))
    combined.to_parquet(part_dir / f"part-{part_idx:05d}.parquet", index=False)

    return {
        "ok": len(frames),
        "failed": len(failed),
        "rows": int(len(combined)),
        "partition": str(part_dir),
        "failed_list": failed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="AkShare 采集 A 股财务数据 → ODS")
    parser.add_argument("--limit", type=int, default=None, help="只跑前 N 家（调试用）")
    parser.add_argument("--codes", type=str, default=None, help="只跑指定代码，逗号分隔，如 600030,000333")
    parser.add_argument("--sleep", type=float, default=None, help="覆盖接口限频间隔（秒）")
    args = parser.parse_args()

    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOG_DIR / f"ingest_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")],
    )

    stats = run(args.limit, args.sleep, args.codes)
    logging.info("采集完成：成功 %d 家，失败 %d 家，共 %d 行，分区 %s",
                 stats["ok"], stats["failed"], stats["rows"], stats["partition"])
    for code, name, err_type, err_msg in stats.get("failed_list", []):
        logging.warning("失败清单 -> %s %s [%s] %s", code, name, err_type, err_msg)
    return 0 if stats["ok"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
