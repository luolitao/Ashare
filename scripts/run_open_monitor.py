from __future__ import annotations

import sys
import os

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ashare.monitor.open_monitor import MA5MA20OpenMonitorRunner
from ashare.core.schema_manager import ensure_schema


def main() -> None:
    ensure_schema()
    MA5MA20OpenMonitorRunner().run(force=True)


if __name__ == "__main__":
    main()
