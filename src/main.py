import luigi
from src.config import LuigiConfig
from loguru import logger

from src.pipelines import main_pipeline


logger.add("logs.log", level="INFO")


if __name__ == "__main__":
    luigi_config = LuigiConfig()

    luigi.build(
        tasks=list(task() for task in main_pipeline),
        workers=luigi_config.max_workers,
        local_scheduler=luigi_config.local_scheduler,
        no_lock=luigi_config.no_lock,
    )
