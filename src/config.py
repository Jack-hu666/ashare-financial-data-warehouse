"""全局配置：路径、连接、公司清单、年份范围。

所有可调参数集中在这里，方便重跑时只改一处。
"""
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# 路径
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
ODS_DIR = DATA_DIR / "ods" / "fin_report"   # 原始层，按 dt=YYYY-MM-DD 分区
DWD_DIR = DATA_DIR / "dwd" / "fin_report"   # 明细层，按 report_date 分区
LOG_DIR = DATA_DIR / "logs"                 # 采集日志

# ---------------------------------------------------------------------------
# 采集范围
# ---------------------------------------------------------------------------
# 沪深两市代表性上市公司（覆盖主要行业）。代码为 6 位股票代码。
# 行业分类不写死在这里，采集时通过 AkShare 单独拉取，保证口径一致。
COMPANIES = [
    # 金融
    ("600000", "浦发银行"), ("600036", "招商银行"), ("601166", "兴业银行"),
    ("601318", "中国平安"), ("600030", "中信证券"), ("300059", "东方财富"),
    # 食品饮料
    ("600519", "贵州茅台"), ("000858", "五粮液"), ("000568", "泸州老窖"),
    ("600887", "伊利股份"), ("603288", "海天味业"), ("000895", "双汇发展"),
    # 家电
    ("000333", "美的集团"), ("000651", "格力电器"), ("600690", "海尔智家"),
    # 医药
    ("600276", "恒瑞医药"), ("000538", "云南白药"), ("300760", "迈瑞医疗"),
    ("600196", "复星医药"), ("603259", "药明康德"),
    # 电子 / 半导体
    ("000725", "京东方A"), ("002475", "立讯精密"), ("002415", "海康威视"),
    ("600745", "闻泰科技"), ("002371", "北方华创"),
    # 新能源 / 电池
    ("300750", "宁德时代"), ("601012", "隆基绿能"), ("002594", "比亚迪"),
    ("300274", "阳光电源"),
    # 汽车
    ("600104", "上汽集团"), ("601633", "长城汽车"), ("000625", "长安汽车"),
    # 房地产
    ("000002", "万科A"), ("600048", "保利发展"), ("001979", "招商蛇口"),
    # 能源 / 公用
    ("601857", "中国石油"), ("600028", "中国石化"), ("601088", "中国神华"),
    ("600900", "长江电力"),
    # 基建 / 建筑
    ("601668", "中国建筑"), ("601390", "中国中铁"), ("601186", "中国铁建"),
    # 通信
    ("600050", "中国联通"), ("000063", "中兴通讯"),
    # 计算机
    ("002230", "科大讯飞"), ("600570", "恒生电子"), ("002410", "广联达"),
    ("300454", "深信服"),
    # 钢铁 / 有色 / 建材
    ("600019", "宝钢股份"), ("601899", "紫金矿业"), ("600585", "海螺水泥"),
    ("603993", "洛阳钼业"),
    # 交运
    ("601006", "大秦铁路"), ("600009", "上海机场"), ("601111", "中国国航"),
    # 农业
    ("000876", "新希望"), ("300498", "温氏股份"), ("002714", "牧原股份"),
    # 化工
    ("600309", "万华化学"), ("600426", "华鲁恒升"),
    # 军工
    ("600760", "中航沈飞"), ("002179", "中航光电"),
    # 零售 / 消费服务
    ("601888", "中国中免"), ("600754", "锦江酒店"),
]

# 拉取近 3 年财报（含季报）。采集时按报告期过滤到该区间。
START_YEAR = 2023
END_YEAR = 2025

# ---------------------------------------------------------------------------
# 采集控制
# ---------------------------------------------------------------------------
SLEEP_SECONDS = 1.0          # 接口限频，每次调用间隔（秒）
MAX_RETRY = 3                # 单公司失败重试次数

# ---------------------------------------------------------------------------
# MySQL（Docker 启动，供 Step3 写结果与看板读取）
# ---------------------------------------------------------------------------
# 注意：本地演示用明文，生产环境应从环境变量 / 密钥系统读取。
MYSQL = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3307")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "finance_dw"),
}

# ---------------------------------------------------------------------------
# Spark 运行时环境（Windows 本地模式必需）
# ---------------------------------------------------------------------------
# Spark 在 Windows 上跑 local[*] 需要三样东西，缺一不可：
#   1. JDK 17（Spark 4.x 要求 Java 17，且必须含 jdk.incubator.vector 模块，
#      所以不能用 PyCharm 精简版 JBR，需完整 Temurin JDK）
#   2. HADOOP_HOME/bin 下的 winutils.exe + hadoop.dll（Hadoop 本地文件系统依赖）
#   3. JVM 参数 -Djava.library.path 指向该 bin 目录（加载 hadoop.dll）
# 这些默认值指向本机安装路径，可用环境变量覆盖。
JAVA_HOME = os.getenv("JAVA_HOME", r"D:\software\jdk\jdk-17.0.20.1+1")
HADOOP_HOME = os.getenv("HADOOP_HOME", r"D:\software\hadoop")


def _ensure_runtime_env() -> None:
    """确保 Spark 启动前 JAVA_HOME / HADOOP_HOME / PATH 就绪（幂等）。"""
    if not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = JAVA_HOME
    if not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = HADOOP_HOME
    bin_dir = os.path.join(HADOOP_HOME, "bin")
    if bin_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def get_spark_session(app_name: str = "finance-dw"):
    """创建 local[*] 模式的 SparkSession，并注入 Windows 所需的 native 库路径。

    所有需要 Spark 的脚本（dwd_clean / dws_metrics / quality_check）统一走这里，
    避免各自散落一份 Spark 配置。
    """
    _ensure_runtime_env()
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    # JVM 的 -Djava.library.path 必须用正斜杠（反斜杠会被 Java 解析错乱导致
    # 加载 hadoop.dll 失败：UnsatisfiedLinkError: NativeIO$Windows.access0）
    lib_dir = os.path.join(HADOOP_HOME, "bin").replace("\\", "/")
    conf = SparkConf().setAppName(app_name).setMaster("local[*]")
    conf.set("spark.driver.extraJavaOptions", f"-Djava.library.path={lib_dir}")
    conf.set("spark.executor.extraJavaOptions", f"-Djava.library.path={lib_dir}")
    return SparkSession.builder.config(conf=conf).getOrCreate()
