# EXON parser

Скрипт собирает данные по актам из Exon через Playwright, сохраняет результат в SQLite и формирует CSV для просмотра в Excel.

## Возможности

- ручной вход в Exon с последующим автоматическим сбором
- проход по списку актов в `/itd/registry/work-sections`
- прокрутка виртуализированного списка, а не только видимых строк
- сохранение в SQLite
- CSV-экспорт с полями `act_id`, `doc_type`, `act_number`, `act_date`, `title`, `author`, `status`, `remark`, `category`, `url`, `collected_at`

## Установка

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

## Запуск

Базовый запуск:

```powershell
.\.venv\Scripts\python.exe .\exon_remarks_collector.py
```

Пример с параметрами:

```powershell
.\.venv\Scripts\python.exe .\exon_remarks_collector.py --headless --limit 50 --db-path .\data\exon.db --csv-path .\data\exon.csv
```

## Параметры

- `--db-path` путь к SQLite-базе
- `--csv-path` путь к CSV-файлу
- `--start-url` стартовый URL Exon
- `--headless` запуск браузера без окна
- `--limit` ограничение на количество актов
- `--scroll-pause-ms` пауза после прокрутки списка
- `--scroll-step-ratio` размер шага прокрутки
- `--max-stagnant-scrolls` сколько пустых прокруток подряд допустимо
- `--no-export-csv` не создавать CSV после выполнения

## Переменные окружения

Этим параметрам соответствуют переменные окружения:

- `EXON_DB_PATH`
- `EXON_CSV_PATH`
- `EXON_START_URL`
- `EXON_HEADLESS`
- `EXON_LIMIT`
- `EXON_SCROLL_PAUSE_MS`
- `EXON_SCROLL_STEP_RATIO`
- `EXON_MAX_STAGNANT_SCROLLS`

## Что сохраняется

Таблица `remarks` содержит:

- тип документа
- номер акта
- дату акта
- название акта
- автора
- статус
- текст замечания
- категорию
- URL акта
- время сбора

## Ограничения

Извлечение статуса, автора и текста замечания пока основано на эвристиках по DOM Exon. Если интерфейс конкретного проекта отличается, селекторы и правила разбора нужно подстроить под реальную разметку.
