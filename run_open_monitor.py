from __future__ import annotations

from ashare.open_monitor import MA5MA20OpenMonitorRunner
from ashare.schema_manager import ensure_schema


def main() -> None:
    ensure_schema()
    MA5MA20OpenMonitorRunner().run(force=True)


if __name__ == "__main__":
    main()
