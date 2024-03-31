# Инжиниринг данных. Домашнее задание 1

Установка зависимостей:
```bash
poetry install
```

Запуск пайплайна:
```bash
poetry run python -m src.main
```


Результат работы пайплайна:
```log
2024-03-31 18:13:24.366 | INFO     | src.tasks:run:28 - Запуск задачи [TaskDatasetGetterDownloadURL]...
2024-03-31 18:13:25.010 | SUCCESS  | src.tasks:run:33 - Получена ссылка на скачивание датасета!
2024-03-31 18:13:25.011 | SUCCESS  | src.tasks:run:37 - Сохранена информация о ссылке на скачивание!
2024-03-31 18:13:25.017 | INFO     | src.tasks:run:81 - Запуск задачи [TaskArchiveLoader] загрузки архива...
2024-03-31 18:13:25.018 | INFO     | src.tasks:run:86 - Скачивание архива из https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE68849&format=file...
2024-03-31 18:13:34.093 | SUCCESS  | src.tasks:run:91 - Архив загружен из https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE68849&format=file!
2024-03-31 18:13:34.098 | SUCCESS  | src.tasks:run:95 - Архив сохранен в data/output/NCBI_GEO_Dataset_TEMP.tar!
2024-03-31 18:13:34.099 | INFO     | src.tasks:run:111 - Запуск задачи [TaskExtractTarFile] распаковки архива...
2024-03-31 18:13:34.117 | SUCCESS  | src.tasks:run:116 - Архив data/output/NCBI_GEO_Dataset_TEMP.tar распакован в data/output!
2024-03-31 18:13:34.452 | SUCCESS  | src.tasks:run:126 - Сохранен список распакованных файлов в data/output/extracted_files.txt!
2024-03-31 18:13:34.454 | INFO     | src.tasks:run:173 - Запуск задачи [TaskDataSetsProcessing] обработки датасетов...
2024-03-31 18:13:35.816 | SUCCESS  | src.tasks:_save_processed_datasets:248 - Сохранен обработанный датасет в data/tsv/Heading_FULL.tsv!
2024-03-31 18:13:37.389 | SUCCESS  | src.tasks:_save_processed_datasets:248 - Сохранен обработанный датасет в data/tsv/Probes_FULL.tsv!
2024-03-31 18:13:37.392 | SUCCESS  | src.tasks:_save_processed_datasets:248 - Сохранен обработанный датасет в data/tsv/Controls_FULL.tsv!
2024-03-31 18:13:37.393 | SUCCESS  | src.tasks:_save_processed_datasets:248 - Сохранен обработанный датасет в data/tsv/Columns_FULL.tsv!
2024-03-31 18:13:37.771 | SUCCESS  | src.tasks:_save_processed_datasets:248 - Сохранен обработанный датасет в data/tsv/Probes_PARTIAL.tsv!
2024-03-31 18:13:37.772 | WARNING  | src.tasks:_drop_unnecessary_files:284 - Удален файл data/GPL10558_HumanHT-12_V4_0_R1_15002873_B.txt
2024-03-31 18:13:37.773 | WARNING  | src.tasks:_drop_unnecessary_files:284 - Удален файл data/GPL10558_HumanHT-12_V4_0_R2_15002873_B.txt
```

Директории полученные в результате работы пайплайна:
```bash
data
├── output
│   ├── GSE68849_download_url.txt
│   ├── NCBI_GEO_Dataset_TEMP.tar
│   └── extracted_files.txt
└── tsv
    ├── Columns_FULL.tsv
    ├── Controls_FULL.tsv
    ├── Heading_FULL.tsv
    ├── Probes_FULL.tsv
    └── Probes_PARTIAL.tsv
```
