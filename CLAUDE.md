# 任务指令：A 股财务指标离线数仓

## 你的角色

你是这个项目的唯一实现者。目标是在 **2 周内**从零建出一个可运行、可演示、可讲解的离线数仓项目。

这个项目的用途是：候选人投递「数据开发实习生」岗位时的作品。因此**重点不是功能多，而是分层设计规范、代码可读、能解释每个技术选择**。

---

## 硬约束（违反任何一条都算失败）

1. **规模上限**：最多 100 家公司 × 3 年财报数据。总行数超过 20000 行就停止扩展，不要"顺便多拉一点"。
2. **只做批处理**。不要引入 Flink、Kafka 或任何流式组件。
3. **数据源只用 AkShare**。不要付费 API，不要写爬虫，不要自己造假数据文件。
4. **不得编造任何数字**。README 和报告里出现的所有数值，必须是你在本次实现中实际跑出来的。写完 README 后逐个核对。
5. **Spark 用 local[*] 模式**。不要搭真集群，不要用 YARN/K8s。
6. **不做 Web 前端**。可视化只用 Streamlit。
7. **结果库用 MySQL 8.0**，通过 Docker 启动。
8. **每完成一个 Step 就 `git commit` 一次**，message 用中文，说明这一步做了什么。
9. **遇到阻塞先停下报告**：如果 AkShare 接口不通、字段与预期不符、或依赖装不上，停止执行并向用户说明，不要绕过或造数据。

---

## 目标仓库结构

```
finance-dw/
├── CLAUDE.md
├── README.md
├── requirements.txt
├── docker-compose.yml          # 只起 MySQL 8.0
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── config.py               # 路径、库连接、公司清单、年份范围
│   ├── ingest.py               # Step1: AkShare → ODS (Parquet 分区)
│   ├── dwd_clean.py            # Step2: PySpark 清洗 → DWD
│   ├── dws_metrics.py          # Step3: 指标计算 → DWS/ADS
│   ├── quality_check.py        # Step4: 3 条数据质量校验
│   └── run_all.py              # Step4: 顺序编排 + 失败重试 + 日志
├── sql/
│   ├── ddl.sql                 # MySQL 建表语句
│   └── ads_queries.sql         # ADS 层的查询示例
├── dashboard/
│   └── app.py                  # Streamlit 看板
└── docs/
    ├── lineage.md              # 分层血缘说明
    └── decisions.md            # 关键决策记录（见 Step5）
```

---

## 分层数据规范

### ODS（原始层）
- 路径：`data/ods/fin_report/dt=YYYY-MM-DD/`
- 格式：Parquet
- 原则：**字段原样保留，一行不删，不做任何清洗**

### DWD（明细层）
字段清单（清洗后）：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `stock_code` | string | 股票代码，**主键之一** |
| `report_date` | date | 报告期，统一 `yyyy-mm-dd`，**主键之一** |
| `industry` | string | 行业分类 |
| `revenue` | decimal | 营业收入，统一为**元** |
| `cost` | decimal | 营业成本，元 |
| `net_profit` | decimal | 净利润，元 |
| `total_assets` | decimal | 总资产，元 |
| `total_liab` | decimal | 总负债，元 |
| `ocf` | decimal | 经营活动现金流净额，元 |
| `ar` | decimal | 应收账款，元 |
| `is_st` | boolean | 是否 ST |

### DWS（汇总层）
按 `industry + report_date` 聚合，指标：

| 指标 | 公式 |
| --- | --- |
| `gross_margin` | (revenue - cost) / revenue |
| `net_margin` | net_profit / revenue |
| `roe` | net_profit / (total_assets - total_liab) |
| `debt_ratio` | total_liab / total_assets |
| `profit_quality` | ocf / net_profit |
| `ar_turnover` | revenue / avg(ar) over 窗口 |

### ADS（应用层）
实现这三张表：

1. `ads_industry_margin_trend` —— 行业 × 报告期的毛利率与净利率趋势
2. `ads_consecutive_decline` —— 连续 3 期毛利率下滑的公司清单
3. `ads_low_quality_profit` —— 连续 2 期 `profit_quality < 0.5` 的公司清单

---

## 执行步骤

### Step 1：采集 → ODS

**做**：
- `src/config.py` 里定义公司清单（50–100 家，写死一个列表即可）、年份范围（3 年）、路径常量
- `src/ingest.py`：调 AkShare 拉财务数据 → 写 Parquet，按 `dt` 分区
- 加 `data/logs/ingest_YYYYMMDD.log`，记录每次拉到多少行

**注意**：
- AkShare 字段名可能和预期不一致，**先打印 schema 再写映射**，不要凭猜
- 接口可能需要 `time.sleep` 控制频率
- 某些公司可能拉不到数据，记录到失败清单，**不要补假数据**

**验收**：
```bash
python -m src.ingest
ls data/ods/fin_report/
# 期望：至少一个 dt=* 分区目录，里面有 parquet 文件
python -c "import pandas as pd,glob; \
d=pd.concat([pd.read_parquet(f) for f in glob.glob('data/ods/fin_report/**/*.parquet',recursive=True)]); \
print(d.shape, d.columns.tolist())"
```

---

### Step 2：PySpark 清洗 → DWD（本项目核心）

**做**：
- 读 ODS 全部分区
- 逐项清洗，每一项都要在 `docs/decisions.md` 里记录理由：
  1. **单位统一**：识别金额字段的单位（元 / 万元 / 亿元），统一换算成元
  2. **财报期对齐**：统一成 `report_date`（date 类型）；处理非自然季度的公司
  3. **去重**：按 `(stock_code, report_date)` 去重，保留最新采集的一版
  4. **缺失值**：**不补 0**，保留 NULL；在指标计算时跳过
  5. **ST/退市**：保留记录但打标 `is_st`，**不删除**（理由写进 decisions.md）
- 结果写 `data/dwd/fin_report/`（Parquet，按 report_date 分区）

**禁止**：不要用 `.fillna(0)`。用 0 填充会让毛利率变成 100%，属于严重错误。

**验收**：
```bash
python -m src.dwd_clean
python -c "
import pyspark.sql as ps
from pyspark.sql import SparkSession
s=SparkSession.builder.master('local[*]').getOrCreate()
d=s.read.parquet('data/dwd/fin_report')
d.printSchema(); print('rows=', d.count())
print('dup=', d.groupBy('stock_code','report_date').count().filter('count>1').count())
"
# 期望：dup=0
```

---

### Step 3：指标计算 → DWS / ADS

**做**：
- `src/dws_metrics.py`：用 Spark SQL 算 DWS 的 6 个指标
- **必须用窗口函数**实现下面三件事：
  - 同比：`LAG(gross_margin, 4) OVER (PARTITION BY stock_code ORDER BY report_date)`
  - 行业排名：`RANK() OVER (PARTITION BY industry, report_date ORDER BY gross_margin DESC)`
  - 连续下滑标记：用 `LAG` 取前 3 期，判断是否逐期下降
- 结果写 MySQL（表结构见 `sql/ddl.sql`）
- `sql/ads_queries.sql` 里写清楚三张 ADS 表的查询语句

**验收**：
```bash
python -m src.dws_metrics
docker compose exec mysql mysql -uroot -p$MYSQL_ROOT_PASSWORD finance_dw -e "
SELECT COUNT(*) FROM dws_industry_metrics;
SELECT * FROM ads_consecutive_decline LIMIT 5;
SELECT * FROM ads_low_quality_profit LIMIT 5;"
# 期望：三张表都有数据，且 dws 行数 > 0
```

---

### Step 4：数据质量校验 + 编排

**做**：
- `src/quality_check.py` 实现 3 条规则，每条**输出明确的 PASS/FAIL**：
  1. **主键唯一**：DWD 层 `(stock_code, report_date)` 无重复
  2. **空值率**：DWD 层 `revenue` 空值率 < 5%，超过则 FAIL
  3. **环比波动**：任一行业平均毛利率环比变化绝对值 > 0.3 则告警
- 每条规则要能**故意造错并触发 FAIL**：写一个 `--inject-bad-data` 参数，注入一条重复主键，验证规则确实会报错
- `src/run_all.py`：顺序调用 ingest → dwd_clean → dws_metrics → quality_check，失败重试 2 次，记录耗时

**验收**：
```bash
python -m src.run_all
# 期望：四步全绿，末尾打印汇总表
python -m src.quality_check --inject-bad-data
# 期望：规则 1 报 FAIL，说明校验有效
```

---

### Step 5：可视化 + 文档

**做**：
- `dashboard/app.py`：Streamlit，3–4 张图（行业毛利率趋势折线、连续下滑公司条形图、盈利质量分布直方图）
- `docs/decisions.md`：**这一项很重要，必须逐条写清楚**
  - 为什么 ST 股保留而不剔除
  - 为什么缺失值不补 0
  - 为什么需要 DWD 层，不能直接从 ODS 算 DWS
  - 单位不统一是怎么发现和处理的
- `docs/lineage.md`：画分层血缘图（ASCII 即可），说明每层输入输出
- `README.md`：见下方要求

**验收**：
```bash
streamlit run dashboard/app.py
# 期望：浏览器能打开，图表有数据
```

---

## README 必须包含的章节

```markdown
# A 股财务指标离线数仓

## 项目简介          (3 句话说清做什么)
## 架构图            (ASCII 分层图)
## 数据来源          (AkShare + 具体接口名 + 数据规模)
## 分层设计          (ODS/DWD/DWS/ADS 各层职责，附 schema)
## 关键指标口径       (6 个指标的公式)
## 快速开始          (docker compose up + 4 条命令，能照抄跑通)
## 数据质量校验       (3 条规则说明)
## 关键设计决策       (引用 docs/decisions.md 的要点)
## 遇到的问题与解决   (至少 3 条真实问题)
```

**README 里的所有数字必须是实际跑出来的。**写完后执行一遍统计命令，逐个核对。

---

## 完成定义（DoD）

全部满足才算完成：

- [ ] `git log` 至少 5 次提交，对应 5 个 Step
- [ ] 一条命令 `python -m src.run_all` 能从 ODS 重跑到 ADS
- [ ] DWD 层主键无重复
- [ ] 3 条质量校验都能跑，且 `--inject-bad-data` 能触发 FAIL
- [ ] 三张 ADS 表都有数据
- [ ] Streamlit 看板能打开且有图
- [ ] `docs/decisions.md` 至少 4 条决策，每条有理由
- [ ] README 完整，且数字与实际一致
- [ ] `.gitignore` 排除了 `data/`、`.venv/`、`__pycache__/`、`.env`

---

## 工作方式

1. **先输出实现计划**（文件清单 + 顺序），等用户确认后再动手
2. **一个 Step 做完立刻验证**，用上面给的验收命令，通过后再进下一步
3. **不要一次性写大量代码**，每个 Step 单独提交
4. **遇到数据问题如实报告**，不要用模拟数据糊过去
5. 全部完成后，输出一份「实际结果汇总」：数据规模、表行数、遇到的问题，供用户填简历用
