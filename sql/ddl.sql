-- finance-dw 结果库建表语句（MySQL 8.0）
-- 说明：指标由 PySpark 计算后写入；金额统一为「元」，比率保留 6 位小数。

-- DWS 公司级指标宽表：窗口函数（同比/行业排名/连续下滑）都在这张表基础上计算
CREATE TABLE IF NOT EXISTS dws_company_metrics (
    stock_code     VARCHAR(16)   NOT NULL COMMENT '股票代码',
    stock_name     VARCHAR(64)            COMMENT '股票名称',
    industry       VARCHAR(64)            COMMENT '行业分类',
    report_date    DATE          NOT NULL COMMENT '报告期',
    revenue        DECIMAL(20,2)          COMMENT '营业收入(元)',
    cost           DECIMAL(20,2)          COMMENT '营业成本(元)',
    net_profit     DECIMAL(20,2)          COMMENT '净利润(元)',
    total_assets   DECIMAL(20,2)          COMMENT '总资产(元)',
    total_liab     DECIMAL(20,2)          COMMENT '总负债(元)',
    ocf            DECIMAL(20,2)          COMMENT '经营活动现金流净额(元)',
    ar             DECIMAL(20,2)          COMMENT '应收账款(元)',
    gross_margin   DECIMAL(12,6)          COMMENT '毛利率',
    net_margin     DECIMAL(12,6)          COMMENT '净利率',
    roe            DECIMAL(12,6)          COMMENT '净资产收益率',
    debt_ratio     DECIMAL(12,6)          COMMENT '资产负债率',
    profit_quality DECIMAL(12,6)          COMMENT '盈利质量 ocf/net_profit',
    ar_turnover    DECIMAL(12,6)          COMMENT '应收账款周转率',
    margin_yoy     DECIMAL(12,6)          COMMENT '毛利率同比(LAG 4期)',
    industry_rank  INT                     COMMENT '行业毛利率排名',
    PRIMARY KEY (stock_code, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公司级指标宽表';

-- DWS 行业级指标：按 industry + report_date 聚合
CREATE TABLE IF NOT EXISTS dws_industry_metrics (
    industry       VARCHAR(64)   NOT NULL COMMENT '行业',
    report_date    DATE          NOT NULL COMMENT '报告期',
    company_cnt    INT                    COMMENT '行业内公司数',
    gross_margin   DECIMAL(12,6)          COMMENT '平均毛利率',
    net_margin     DECIMAL(12,6)          COMMENT '平均净利率',
    roe            DECIMAL(12,6)          COMMENT '平均ROE',
    debt_ratio     DECIMAL(12,6)          COMMENT '平均资产负债率',
    profit_quality DECIMAL(12,6)          COMMENT '平均盈利质量',
    ar_turnover    DECIMAL(12,6)          COMMENT '平均应收账款周转率',
    PRIMARY KEY (industry, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='行业级指标汇总';

-- ADS 1：行业 × 报告期毛利率与净利率趋势
CREATE TABLE IF NOT EXISTS ads_industry_margin_trend (
    industry     VARCHAR(64)   NOT NULL,
    report_date  DATE          NOT NULL,
    gross_margin DECIMAL(12,6)          COMMENT '毛利率',
    net_margin   DECIMAL(12,6)          COMMENT '净利率',
    PRIMARY KEY (industry, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='行业毛利率/净利率趋势';

-- ADS 2：连续 3 期毛利率下滑的公司清单
CREATE TABLE IF NOT EXISTS ads_consecutive_decline (
    stock_code   VARCHAR(16)   NOT NULL,
    stock_name   VARCHAR(64),
    industry     VARCHAR(64),
    report_date  DATE          NOT NULL COMMENT '当期报告期',
    gross_margin DECIMAL(12,6)          COMMENT '当期毛利率',
    PRIMARY KEY (stock_code, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='连续3期毛利率下滑公司';

-- ADS 3：连续 2 期盈利质量 < 0.5 的公司清单
CREATE TABLE IF NOT EXISTS ads_low_quality_profit (
    stock_code     VARCHAR(16)   NOT NULL,
    stock_name     VARCHAR(64),
    industry       VARCHAR(64),
    report_date    DATE          NOT NULL COMMENT '当期报告期',
    profit_quality DECIMAL(12,6)          COMMENT '当期盈利质量',
    PRIMARY KEY (stock_code, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='连续2期盈利质量偏低公司';
