import gzip
import io
import os
import tarfile
from collections import defaultdict
from datetime import datetime, timezone

import luigi
import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
from yarl import URL

from src.config import DSNConfig
from src.enums import DataPaths, Datasets, DatasetsPaths


class TaskDatasetGetterDownloadURL(luigi.Task):

    parser = "html.parser"
    data_source = DSNConfig().ncbi_dsn
    dataset_name = luigi.Parameter(default=Datasets.gse_68849)

    def run(self) -> None:
        """Запуск задачи."""

        logger.info(f"Запуск задачи [{self.__class__.__name__}]...")

        ncbi_page = self._get_ncbi_download_page()
        download_url = self._parse_download_url(ncbi_page)

        logger.success("Получена ссылка на скачивание датасета!")

        with open(self.output().path, "w") as file:
            file.write(str(download_url))
            logger.success("Сохранена информация о ссылке на скачивание!")

    def output(self) -> luigi.LocalTarget:
        """Путь к файлу с ссылкой на скачивание датасета."""
        return luigi.LocalTarget(f"{DataPaths.output.value}/{self.dataset_name}_download_url.txt")

    def complete(self):
        """Проверка завершения задачи."""
        return self.output().exists()

    def _parse_download_url(self, page: BeautifulSoup) -> URL:
        """Парсинг ссылки на скачивание датасета."""
        all_a_tags = page.find_all("a")
        ncbi_set_download_url = None
        for a_tag in all_a_tags:
            if a_tag.text == "(http)":
                ncbi_set_download_url = a_tag["href"]
                break

        if ncbi_set_download_url is None:
            raise ValueError("Не удалось найти ссылку на скачивание датасета.")

        return URL(
            f"{self.data_source.scheme}://"
            f"{self.data_source.host}"
            f"{ncbi_set_download_url}"
        )

    def _get_ncbi_download_page(self) -> BeautifulSoup:
        """Получение страницы с ссылкой на скачивание датасета."""
        page_url = self.data_source % {"acc": self.dataset_name}
        response = requests.get(page_url)
        response.raise_for_status()
        return BeautifulSoup(response.text, features=self.parser)


class TaskArchiveLoader(luigi.Task):

    def requires(self) -> TaskDatasetGetterDownloadURL:
        """Требование к задаче."""
        return TaskDatasetGetterDownloadURL()

    def run(self) -> None:
        """Запуск задачи."""
        logger.info(f"Запуск задачи [{self.__class__.__name__}] загрузки архива...")

        with self.input().open("r") as f:
            download_url = URL(f.read())

        logger.info(f"Скачивание архива из {download_url}...")

        response = requests.get(url=download_url, allow_redirects=True)
        response.raise_for_status()

        logger.success(f"Архив загружен из {download_url}!")

        with open(self.output().path, "wb") as file:
            file.write(response.content)
            logger.success(f"Архив сохранен в {self.output().path}!")

    def output(self) -> luigi.LocalTarget:
        """Путь к архиву с датасетом."""
        archive_file_path = f"{DataPaths.output.value}/NCBI_GEO_Dataset_TEMP.tar"
        return luigi.LocalTarget(archive_file_path)


class TaskExtractTarFile(luigi.Task):

    def requires(self) -> TaskArchiveLoader:
        """Требование к задаче."""
        return TaskArchiveLoader()

    def run(self) -> None:
        """Запуск задачи."""
        logger.info(f"Запуск задачи [{self.__class__.__name__}] распаковки архива...")

        with tarfile.open(self.input().path, "r") as tar:
            tar.extractall(path=DataPaths.output.value)

            logger.success(f"Архив {self.input().path} распакован в {DataPaths.output.value}!")

        datasets_gz_files = self._get_txt_gx_files_names(directory_path=DataPaths.output.value)

        unzip_files_paths = self._unzip_gz_files(gz_files=datasets_gz_files)
        self._drop_gz_files(gz_files=datasets_gz_files)

        with open(self.output().path, "w") as file:
            file.write("\n".join(unzip_files_paths))

            logger.success(f"Сохранен список распакованных файлов в {self.output().path}!")

    def output(self):
        """Путь к файлу с именами распакованных файлов."""
        return luigi.LocalTarget(f"{DataPaths.output.value}/extracted_files.txt")

    @classmethod
    def _get_txt_gx_files_names(cls, directory_path: str) -> tuple[str, ...]:
        """Получение имен файлов с расширением .txt.gz."""
        return tuple(
            file_name
            for file_name in os.listdir(directory_path)
            if file_name.endswith(".txt.gz")
        )

    @classmethod
    def _unzip_gz_files(cls, gz_files: tuple[str, ...]) -> list[str]:
        """Распаковка файлов с расширением .gz."""
        unzip_files = []
        for gz_file in gz_files:
            file_path = f"{DataPaths.output.value}/{gz_file}"

            with gzip.open(file_path, "rb") as file_input:
                unzip_file = gz_file.replace(".gz", "")
                unzip_file_path = f"{DataPaths.base.value}/{unzip_file}"
                with open(unzip_file_path, "wb") as file_output:
                    file_output.write(file_input.read())

                unzip_files.append(unzip_file_path)

        return unzip_files

    @classmethod
    def _drop_gz_files(cls, gz_files: tuple[str, ...]):
        """Удаление файлов с расширением .gz."""
        for gz_file in gz_files:
            os.remove(f"{DataPaths.output.value}/{gz_file}")


class TaskDataSetsProcessing(luigi.Task):

    def requires(self) -> TaskExtractTarFile:
        """Требование к задаче."""
        return TaskExtractTarFile()

    def run(self) -> None:
        """Запуск задачи."""
        logger.info(f"Запуск задачи [{self.__class__.__name__}] обработки датасетов...")

        raw_datasets = self._get_raw_datasets()
        datasets = defaultdict(list)
        for raw_dataset in raw_datasets:
            raw_dataset_file_path = f"{DataPaths.base.value}/{raw_dataset}"
            dfs = self._process_raw_datasets(raw_dataset_file_path)

            for key, df in dfs.items():
                datasets[key].append(df)

        processed_datasets = {}
        for key, dfs in datasets.items():
            df = pd.concat(dfs)
            processed_datasets[key] = df

        self._save_processed_datasets(processed_datasets=processed_datasets)
        self._special_processing(processed_datasets=processed_datasets)
        self._drop_unnecessary_files()

    def output(self) -> tuple[luigi.LocalTarget, ...]:
        """Пути к обработанным датасетам."""
        return tuple(
            luigi.LocalTarget(f"{DataPaths.tsv.value}/{file_name}")
            for file_name in os.listdir(DataPaths.tsv.value)
            if file_name.endswith(".tsv")
        )

    @classmethod
    def _get_raw_datasets(cls) -> tuple[str, ...]:
        """Получение имен файлов сырых датасетов."""
        return tuple(
            file_name
            for file_name in os.listdir(DataPaths.base.value)
            if file_name.endswith(".txt")
        )

    @classmethod
    def _process_raw_datasets(cls, raw_dataset_file_path: str) -> dict[str, pd.DataFrame]:
        """Обработка сырых датасетов."""
        dfs = {}
        write_key = None

        with open(raw_dataset_file_path) as file:
            fio = io.StringIO()

            for raw_line in file.readlines():
                if raw_line.startswith("["):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == "Heading" else "infer"
                        dfs[write_key] = pd.read_csv(fio, sep="\t", header=header)
                    fio = io.StringIO()
                    write_key = raw_line.strip("[]\n")
                    continue

                if write_key:
                    fio.write(raw_line)
            fio.seek(0)

            dfs[write_key] = pd.read_csv(fio, sep="\t")
        return dfs

    @classmethod
    def _save_processed_datasets(
        cls,
        processed_datasets: dict[str, pd.DataFrame],
        type_: DatasetsPaths = DatasetsPaths.full,
    ) -> None:
        """Сохранение обработанных датасетов."""
        for key, df in processed_datasets.items():
            file_name = f"{key}_{type_}.tsv"
            file_name = file_name.replace(" ", "_")
            file_path = f"{DataPaths.tsv.value}/{file_name}"
            df.to_csv(file_path, sep="\t", index=False)
            logger.success(f"Сохранен обработанный датасет в {file_path}!")

    @classmethod
    def _special_processing(cls, processed_datasets: dict[str, pd.DataFrame]) -> None:
        """Специальная обработка датасетов."""
        special_key = "Probes"
        special_df = processed_datasets[special_key]

        columns4drop = [
            "Definition",
            "Ontology_Component",
            "Ontology_Process",
            "Obsolete_Probe_Id",
            "Probe_Sequence",
            "Synonyms",
            "Ontology_Function",
        ]

        special_df.drop(
            columns=columns4drop,
            inplace=True,
        )

        cls._save_processed_datasets(
            processed_datasets={special_key: special_df},
            type_=DatasetsPaths.partial,
        )

    @classmethod
    def _drop_unnecessary_files(cls) -> None:
        """Удаление ненужных файлов."""
        for file_name in os.listdir(DataPaths.base.value):
            file_path = f"{DataPaths.base.value}/{file_name}"
            if file_name.endswith(".txt"):
                os.remove(file_path)

                logger.warning(f"Удален файл {file_path}")
