"""Step 4：顺序编排 ingest → dwd_clean → dws_metrics → quality_check。

每个步骤失败重试 2 次，记录耗时，末尾打印汇总表。

用法：
  python -m src.run_all
"""
import subprocess
import sys
import time
from datetime import datetime

from . import config

STEPS = [
    ("ingest",        ["-m", "src.ingest"]),
    ("dwd_clean",     ["-m", "src.dwd_clean"]),
    ("dws_metrics",   ["-m", "src.dws_metrics"]),
    ("quality_check", ["-m", "src.quality_check"]),
]

MAX_RETRY = 2  # 失败后额外重试次数（共 1+2=3 次尝试）


def _run_step(name: str, args: list[str]) -> tuple[str, float, int]:
    for attempt in range(MAX_RETRY + 1):
        start = time.time()
        proc = subprocess.run([sys.executable, *args], capture_output=True, text=True)
        elapsed = time.time() - start
        if proc.returncode == 0:
            return "PASS", elapsed, attempt + 1
        print(f"[{name}] 第 {attempt + 1} 次尝试失败（耗时 {elapsed:.1f}s）："
              f"{proc.stderr.strip()[-200:]}")
        if attempt < MAX_RETRY:
            time.sleep(5)
    return "FAIL", elapsed, MAX_RETRY + 1


def main() -> int:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    print(f"=== run_all 开始 {datetime.now():%Y-%m-%d %H:%M:%S} ===")
    summary = []
    for name, args in STEPS:
        status, elapsed, attempts = _run_step(name, args)
        summary.append((name, status, elapsed, attempts))
        print(f"  {name:<14} {status:<4} 耗时 {elapsed:6.1f}s  尝试 {attempts} 次")

    print("\n=== 汇总 ===")
    ok = True
    for name, status, elapsed, attempts in summary:
        print(f"  {name:<14} {status}")
        ok &= (status == "PASS")
    print(f"=== 结果：{'全部成功' if ok else '存在失败'} ===")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
