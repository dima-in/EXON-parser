# EXON parser

Скрипт для сбора данных по актам из Exon через Playwright.

## Запуск

```powershell
.\.venv\Scripts\python.exe .\exon_remarks_collector.py
```

## Что делает

- открывает Exon и ждёт ручной логин
- проходит список актов в `/itd/registry/work-sections`
- собирает данные по акту и замечаниям
- сохраняет результат в `SQLite`
- формирует `CSV`-экспорт
