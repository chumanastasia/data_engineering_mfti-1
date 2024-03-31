from enum import Enum, StrEnum
from pathlib import Path


class Datasets(StrEnum):
    gse_68849 = "GSE68849"


class DataPaths(Enum):
    base: Path = Path("data")
    output: Path = Path("data/output")
    tsv: Path = Path("data/tsv")


class DatasetsPaths(StrEnum):
    full = "FULL"
    partial = "PARTIAL"
