"""日志工具."""

import logging
from pathlib import Path


def setup_logger(log_dir: Path | None = None) -> logging.Logger:
    """
    配置基础日志记录器，输出到文件和控制台。

    默认：把日志写到“项目根目录”的 ashare.log。
    如果传入 log_dir，则写到 log_dir/ashare.log。
    """
    if log_dir is None:
        # logger.py -> utils -> ashare -> 项目根目录
        log_dir = Path(__file__).resolve().parents[2]
    else:
        log_dir = Path(log_dir)

    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "ashare.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("ashare")