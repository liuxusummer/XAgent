from __future__ import annotations

import time
import argparse
from datetime import datetime
from pathlib import Path

from reflect.run_reflect import run_reflect


DEFAULT_INTERVAL_HOURS = 6


def schedule_loop(interval_hours: float, config_path: str | None = None) -> None:
    interval_seconds = interval_hours * 3600
    print(f"[Schedule] 记忆结算定时任务启动，间隔 {interval_hours} 小时")
    while True:
        try:
            print(f"[Schedule] {datetime.now().isoformat()} 触发记忆结算...")
            result = run_reflect(config_path=config_path)
            print(
                f"[Schedule] 结算完成: exit_reason={result.get('exit_reason')}, "
                f"turns={result.get('turns')}"
            )
        except Exception as exc:
            print(f"[Schedule] 结算异常: {exc}")
        print(f"[Schedule] 下次结算: {interval_hours} 小时后")
        time.sleep(interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="XAgent Memory Reflect Scheduler")
    parser.add_argument("--config", type=str, default=None, help="Path to config JSON file")
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_HOURS,
        help=f"Interval in hours (default: {DEFAULT_INTERVAL_HOURS})",
    )
    args = parser.parse_args()
    schedule_loop(interval_hours=args.interval, config_path=args.config)


if __name__ == "__main__":
    main()
