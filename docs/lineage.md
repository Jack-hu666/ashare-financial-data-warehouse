# 分层血缘说明

## 整体分层

```
                    ┌──────────────────────────────────────────────────┐
                    │  数据源：AkShare（免费开源，无需 token）           │
                    │  ① stock_profit_sheet_by_report_em   东财利润表    │
                    │  ② stock_balance_sheet_by_report_em  东财资产负债表 │
                    │  ③ stock_cash_flow_sheet_by_report_em 东财现金流量表 │
                    │  ④ stock_profile_cninfo              巨潮公司档案   │
                    └───────────────┬──────────────────────────────────┘
                                    │ src/ingest.py（合并 + 落盘）
                                    ▼
                    ┌──────────────────────────────────────────────────┐
                    │  ODS  原始层  data/ods/fin_report/dt=YYYY-MM-DD    │
                    │  原则：金额原样保留（元），一行不删，不清洗          │
                    │  粒度：(stock_code, report_date)                   │
                    └───────────────┬──────────────────────────────────┘
                                    │ src/dwd_clean.py（PySpark）
                                    ▼
                    ┌──────────────────────────────────────────────────┐
                    │  DWD  明细层  data/dwd/fin_report/                 │
                    │  单位统一(元) / 期对齐(date) / 主键去重 /           │
                    │  NULL 保留 / ST 打标                               │
                    └───────────────┬──────────────────────────────────┘
                                    │ src/dws_metrics.py（Spark SQL + 窗口函数）
                                    ▼
                    ┌──────────────────────────────────────────────────┐
                    │  DWS  汇总层  dws_company_metrics /                │
                    │               dws_industry_metrics                │
                    │  6 个指标 + 同比(LAG4) + 行业排名(RANK)             │
                    └───────────────┬──────────────────────────────────┘
                                    │ 派生
                                    ▼
                    ┌──────────────────────────────────────────────────┐
                    │  ADS  应用层  ads_industry_margin_trend /           │
                    │               ads_consecutive_decline /            │
                    │               ads_low_quality_profit               │
                    └───────────────┬──────────────────────────────────┘
                                    │ 结果写入 MySQL 8.0（Docker）
                                    ▼
                    ┌──────────────────────────────────────────────────┐
                    │  可视化：Streamlit 看板（dashboard/app.py）         │
                    └──────────────────────────────────────────────────┘
```

## 字段级血缘

| ODS 字段（原始） | AkShare 来源 | DWD 字段（清洗后） | 清洗动作 |
| --- | --- | --- | --- |
| stock_code | 采集清单 | stock_code (string) | 去重键之一 |
| stock_name | 采集清单 | stock_name (string) | 用于 ST 判断 |
| industry | 巨潮 ④「所属行业」 | industry (string) | 保留 |
| report_date | ①②③ 的 REPORT_DATE | report_date (date) | 类型对齐为 date |
| revenue（营业总收入） | ① 东财 TOTAL_OPERATE_INCOME | revenue (decimal, 元) | 单位核对（源=元） |
| cost（营业成本） | ① 东财 OPERATE_COST | cost (decimal, 元) | 同上 |
| net_profit（净利润） | ① 东财 NETPROFIT | net_profit (decimal, 元) | 同上 |
| total_assets（总资产） | ② 东财 TOTAL_ASSETS | total_assets (decimal, 元) | 同上 |
| total_liab（总负债） | ② 东财 TOTAL_LIABILITIES | total_liab (decimal, 元) | 同上 |
| ocf（经营现金流净额） | ③ 东财 NETCASH_OPERATE | ocf (decimal, 元) | 同上 |
| ar（应收账款） | ② 东财 ACCOUNTS_RECE | ar (decimal, 元) | 同上（金融业为 NULL） |
| — | — | is_st (boolean) | 由 stock_name 是否含 ST 派生 |

## 指标口径（DWS 层）

| 指标 | 公式 | 分母保护 |
| --- | --- | --- |
| gross_margin 毛利率 | (revenue - cost) / revenue | NULLIF(revenue,0) |
| net_margin 净利率 | net_profit / revenue | NULLIF(revenue,0) |
| roe 净资产收益率 | net_profit / (total_assets - total_liab) | NULLIF(净资产,0) |
| debt_ratio 资产负债率 | total_liab / total_assets | NULLIF(total_assets,0) |
| profit_quality 盈利质量 | ocf / net_profit | NULLIF(net_profit,0) |
| ar_turnover 应收账款周转率 | revenue / avg(ar)（近 4 期窗口均值） | NULLIF(avg_ar,0) |

## 数据流转的「为什么」

- **ODS 不做清洗**：原始值可回溯，重跑 DWD 无需重新采集。
- **DWD 统一口径**：单位、日期、主键、ST 标记都在这一层一次做对，DWS/ADS 直接复用，口径不散落。
- **DWS 与 ADS 分离**：DWS 是可复用的指标宽表，ADS 是面向具体问题的结果表，ADS 只从 DWS 派生。
