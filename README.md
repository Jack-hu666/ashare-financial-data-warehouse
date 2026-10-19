# A 股财务指标离线数仓

## 项目简介

基于 AkShare 采集 A 股 64 家上市公司近 3 年财报，按 ODS → DWD → DWS → ADS 四层搭建离线数仓；用 PySpark 完成金额单位核对、财报期对齐、主键去重；用 Spark SQL 窗口函数计算 6 个财务指标并识别「连续 3 期毛利率下滑」「盈利质量偏低」的公司；结果落 MySQL 8.0（Docker），Streamlit 看板展示。

## 架构图

```
数据源 (AkShare: 东财利润表/资产负债表/现金流量表 + 巨潮行业)
   ↓ 采集脚本 src/ingest.py
ODS   原始层：按 dt 分区存 Parquet，字段原样保留，一行不删
   ↓ PySpark src/dwd_clean.py
DWD   明细层：单位统一(元)、期对齐(date)、主键去重、NULL 保留、ST 打标
   ↓ Spark SQL src/dws_metrics.py（窗口函数）
DWS   汇总层：公司级/行业级指标宽表（6 指标 + 同比 + 行业排名）
   ↓ 派生
ADS   应用层：毛利率趋势 / 连续下滑公司 / 低盈利质量公司
   ↓
MySQL 8.0 (Docker) → Streamlit 看板
```

## 数据来源

| 接口 | 用途 |
| --- | --- |
| `ak.stock_profit_sheet_by_report_em` | 营业收入、营业成本、净利润 |
| `ak.stock_balance_sheet_by_report_em` | 总资产、总负债、应收账款 |
| `ak.stock_cash_flow_sheet_by_report_em` | 经营活动现金流量净额 |
| `ak.stock_profile_cninfo` | 行业分类（证监会口径） |

- **数据规模**：64 家公司 × 3 年（12 个季度）= **768 行**，覆盖 **29 个行业**。
- 金额单位：东财接口原始即为「元」，已用浦发银行、贵州茅台年报核对。

## 分层设计

| 层 | 职责 | 存储 |
| --- | --- | --- |
| ODS | 原始数据，不清理，可回溯 | `data/ods/fin_report/dt=YYYY-MM-DD/` |
| DWD | 清洗后的明细宽表 | `data/dwd/fin_report/`（按 report_date 分区） |
| DWS | 指标宽表（公司级 + 行业级） | MySQL `dws_company_metrics` / `dws_industry_metrics` |
| ADS | 面向业务问题的结果表 | MySQL `ads_*` 三张表 |

**DWD 明细层字段**：`stock_code, stock_name, industry, report_date(date), revenue, cost, net_profit, total_assets, total_liab, ocf, ar`（金额统一 decimal(22,2) 元）`is_st(boolean)`。

## 关键指标口径

| 指标 | 公式 |
| --- | --- |
| 毛利率 gross_margin | (revenue - cost) / revenue |
| 净利率 net_margin | net_profit / revenue |
| ROE | net_profit / (total_assets - total_liab) |
| 资产负债率 debt_ratio | total_liab / total_assets |
| 盈利质量 profit_quality | ocf / net_profit |
| 应收账款周转率 ar_turnover | revenue / avg(ar)（近 4 期均值） |

所有分母均用 `NULLIF(x, 0)` 保护，NULL 不参与聚合。

## 快速开始

前置：JDK 17（Temurin，含 `jdk.incubator.vector`）、`HADOOP_HOME/bin` 下的 winutils.exe + hadoop.dll（Windows 必需，见 `docs/decisions.md` 决策 6）。

```bash
# 1. 装依赖
python -m venv --system-site-packages .venv && .venv/Scripts/activate
pip install -r requirements.txt

# 2. 起 MySQL（Docker）
docker compose up -d

# 3. 一条命令从采集重跑到 ADS
python -m src.run_all

# 4. 看板
streamlit run dashboard/app.py
```

## 数据质量校验

`python -m src.quality_check` 三条规则：

1. **主键唯一**：DWD `(stock_code, report_date)` 无重复
2. **空值率**：DWD `revenue` 空值率 < 5%
3. **环比波动**：任一行业平均毛利率环比变化绝对值 > 0.3 则告警

验证校验有效性：`python -m src.quality_check --inject-bad-data`（注入重复主键，规则 1 应报 FAIL）。

## 关键设计决策

详见 `docs/decisions.md`，要点：
- 为什么弃用新浪「营业成本」（其字段实为「营业总成本」，会导致毛利率偏低）
- 为什么缺失值不补 0
- 为什么 ST 股保留而打标不删除
- 为什么需要 DWD 层
- 为什么 Spark 用 local[*] + Java 17 + winutils

## 遇到的问题与解决

1. **PyCharm 精简 JBR 缺 `jdk.incubator.vector` 模块** → Spark 4.x 起不来；改装完整 Temurin JDK 17。
2. **Windows 跑 Spark 报 `UnsatisfiedLinkError`** → 需 winutils.exe + hadoop.dll，且 `-Djava.library.path` 必须用正斜杠。
3. **pyarrow 把日期写成 TIMESTAMP(NANOS)** → Spark 读不了纳秒精度；ODS 落盘前把 `report_date` 转成 date。
4. **新浪「营业成本」口径错误** → 实为营业总成本；改用东财利润表 `OPERATE_COST`。
5. **金融企业（银行/券商/保险）报表结构不同** → 无 `TOTAL_OPERATE_INCOME`/`OPERATE_COST`；统一用 `OPERATE_INCOME`（营业收入），金融企业营业成本为 NULL 属正常。

（MySQL 落库行数、看板截图等运行期数字见验证输出，【待实测】补充）
