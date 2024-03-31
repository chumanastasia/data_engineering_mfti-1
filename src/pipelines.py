from src.tasks import (
    TaskArchiveLoader,
    TaskDatasetGetterDownloadURL,
    TaskExtractTarFile,
    TaskDataSetsProcessing,
)


main_pipeline = (
    TaskArchiveLoader,
    TaskDatasetGetterDownloadURL,
    TaskExtractTarFile,
    TaskDataSetsProcessing,
)
