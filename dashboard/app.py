"""A 股财务指标离线数仓 —— Streamlit 看板。

读取 MySQL 结果表（DWS/ADS），展示：
  1. 行业毛利率 / 净利率趋势折线
  2. 连续 3 期毛利率下滑公司条形图
  3. 盈利质量（ocf/net_profit）分布直方图

运行：streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402

# dataviz 参考配色（已验证的色板）
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE = "#2a78d6"
INK = "#0b0b0b"
MUTED = "#898781"
GRID = "#e1e0d9"


def _engine():
    c = config.MYSQL
    url = (f"mysql+pymysql://{c['user']}:{c['password']}@{c['host']}:{c['port']}"
           f"/{c['database']}?charset=utf8mb4")
    return create_engine(url)


@st.cache_data(ttl=60, show_spinner=False)
def load_table(name: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {name}", _engine())


def _style_fig(fig):
    fig.update_layout(
        font=dict(family="system-ui, Segoe UI, sans-serif", color=INK),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=40, b=10),
        hoverlabel=dict(font_color="#0b0b0b"),
    )
    fig.update_xaxes(gridcolor=GRID, linecolor="#c3c2b7", tickfont_color=MUTED)
    fig.update_yaxes(gridcolor=GRID, linecolor="#c3c2b7", tickfont_color=MUTED, zeroline=False)
    return fig


st.set_page_config(page_title="A股财务指标数仓看板", layout="wide")
st.title("A 股财务指标离线数仓看板")
st.caption("数据源：AkShare · 分层：ODS → DWD → DWS → ADS · 结果库：MySQL 8.0")

try:
    company = load_table("dws_company_metrics")
    industry = load_table("dws_industry_metrics")
    decline = load_table("ads_consecutive_decline")
    low_quality = load_table("ads_low_quality_profit")
except Exception as exc:  # noqa: BLE001 —— MySQL 未就绪时给明确提示
    st.error(f"无法连接 MySQL（{config.MYSQL['host']}:{config.MYSQL['port']}）。"
             f"请先 `docker compose up -d` 并运行 `python -m src.run_all`。\n\n{exc}")
    st.stop()

# ---- 顶部指标卡 ----
c1, c2, c3, c4 = st.columns(4)
c1.metric("覆盖公司", int(company["stock_code"].nunique()))
c2.metric("报告期数", int(company["report_date"].nunique()))
c3.metric("连续下滑公司", int(decline["stock_code"].nunique()))
c4.metric("低盈利质量公司", int(low_quality["stock_code"].nunique()))

# ---- 图 1：行业毛利率 / 净利率趋势 ----
st.subheader("行业毛利率 / 净利率趋势")
top_inds = (company.groupby("industry")["stock_code"].nunique()
            .sort_values(ascending=False).head(8).index.tolist())
sel_inds = st.multiselect("选择行业（默认展示公司数最多的 8 个）", sorted(industry["industry"].unique()),
                          default=top_inds)
trend = industry[industry["industry"].isin(sel_inds)].sort_values("report_date")
fig1 = px.line(trend, x="report_date", y="gross_margin", color="industry",
               color_discrete_sequence=CAT[: len(sel_inds)],
               labels={"report_date": "报告期", "gross_margin": "毛利率", "industry": "行业"})
fig1.update_traces(line=dict(width=2), mode="lines+markers", marker=dict(size=4))
fig1.update_yaxes(tickformat=".0%")
st.plotly_chart(_style_fig(fig1), use_container_width=True)

# ---- 图 2：连续下滑公司 ----
st.subheader("连续 3 期毛利率下滑的公司")
decline_latest = (decline.sort_values("report_date")
                  .groupby("stock_code", as_index=False).last()
                  .sort_values("gross_margin"))
fig2 = go.Figure(go.Bar(
    x=decline_latest["gross_margin"], y=decline_latest["stock_name"],
    orientation="h", marker_color=BLUE,
    text=[f"{v:.1%}" for v in decline_latest["gross_margin"]],
    textposition="outside",
))
fig2.update_layout(xaxis_title="当期毛利率", yaxis_title=None)
fig2.update_xaxes(tickformat=".0%")
st.plotly_chart(_style_fig(fig2), use_container_width=True)

# ---- 图 3：盈利质量分布 ----
st.subheader("盈利质量（经营现金流 / 净利润）分布")
pq = company.dropna(subset=["profit_quality"])
pq = pq[(pq["profit_quality"] > -2) & (pq["profit_quality"] < 3)]  # 裁剪极端值便于观察
fig3 = px.histogram(pq, x="profit_quality", nbins=40, color_discrete_sequence=[BLUE],
                    labels={"profit_quality": "盈利质量 ocf/net_profit"})
fig3.add_vline(x=0.5, line_dash="dash", line_color="#e34948",
               annotation_text="警戒线 0.5", annotation_font_color="#e34948")
st.plotly_chart(_style_fig(fig3), use_container_width=True)

st.caption("注：金融企业（银行/券商）应收账款为 NULL，其应收账款周转率不参与行业均值。")
