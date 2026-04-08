import argparse
import asyncio
import csv
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

STATUS_VALUES = {
    "Нет замечаний",
    "Комментарии",
    "Помощь",
    "На проверке",
    "Согласовано",
    "Отклонено",
    "Подписан",
}

SELECTORS = {
    "rows": [
        "div[role='row'][data-rowindex]",
        ".MuiDataGrid-row",
    ],
    "act_link": [
        "a[href*='selectedActId=']",
        "a[href*='work-sections']",
    ],
    "remark_tab": [
        "text=Замечания",
        "text=Комментарии",
    ],
    "virtual_scroller": [
        ".MuiDataGrid-virtualScroller",
        "[class*='MuiDataGrid-virtualScroller']",
        ".MuiDataGrid-main",
    ],
    "detail_text_candidates": [
        "textarea",
        "[contenteditable='true']",
        "[data-field*='comment']",
        "[data-field*='remark']",
        "[class*='comment']",
        "[class*='remark']",
        ".MuiAlert-message",
        ".MuiTypography-root",
    ],
}

CATEGORY_RULES = [
    ("комплектность", [r"комплект", r"нет схем", r"сформир"]),
    ("реквизиты", [r"адрес", r"наименован"]),
    ("схема", [r"схем", r"привяз", r"оси"]),
    ("оформление", [r"оформлен", r"подпись"]),
    ("AutoCAD/КМ", [r"\bкм\b", r"dwg", r"чертеж"]),
]


@dataclass
class AppConfig:
    db_path: Path
    csv_path: Path
    start_url: str
    headless: bool
    scroll_pause_ms: int
    scroll_step_ratio: float
    max_stagnant_scrolls: int
    limit: int | None
    export_csv: bool


def configure_output() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Сбор данных по актам Exon через Playwright."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("EXON_DB_PATH", "exon_remarks.db"),
        help="Путь к SQLite-базе.",
    )
    parser.add_argument(
        "--csv-path",
        default=os.getenv("EXON_CSV_PATH", "exon_remarks_export.csv"),
        help="Путь к CSV-экспорту.",
    )
    parser.add_argument(
        "--start-url",
        default=os.getenv("EXON_START_URL", "https://exon.exonproject.ru/"),
        help="Стартовый URL Exon.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=env_bool("EXON_HEADLESS", False),
        help="Запускать браузер без окна.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("EXON_LIMIT", "0")) or None,
        help="Ограничение на количество актов. По умолчанию без лимита.",
    )
    parser.add_argument(
        "--scroll-pause-ms",
        type=int,
        default=int(os.getenv("EXON_SCROLL_PAUSE_MS", "1200")),
        help="Пауза после прокрутки списка в миллисекундах.",
    )
    parser.add_argument(
        "--scroll-step-ratio",
        type=float,
        default=float(os.getenv("EXON_SCROLL_STEP_RATIO", "0.85")),
        help="Доля высоты видимой области для одного шага прокрутки.",
    )
    parser.add_argument(
        "--max-stagnant-scrolls",
        type=int,
        default=int(os.getenv("EXON_MAX_STAGNANT_SCROLLS", "4")),
        help="Сколько раз подряд можно не получить новых строк перед остановкой.",
    )
    parser.add_argument(
        "--no-export-csv",
        action="store_true",
        help="Не формировать CSV после сбора.",
    )
    return parser


def get_config() -> AppConfig:
    args = build_parser().parse_args()
    return AppConfig(
        db_path=Path(args.db_path),
        csv_path=Path(args.csv_path),
        start_url=args.start_url,
        headless=args.headless,
        scroll_pause_ms=args.scroll_pause_ms,
        scroll_step_ratio=args.scroll_step_ratio,
        max_stagnant_scrolls=args.max_stagnant_scrolls,
        limit=args.limit,
        export_csv=not args.no_export_csv,
    )


def classify(text: str) -> str:
    normalized = (text or "").lower()
    for name, patterns in CATEGORY_RULES:
        if any(re.search(pattern, normalized) for pattern in patterns):
            return name
    return "прочее"


def ensure_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS remarks (
            id INTEGER PRIMARY KEY,
            act TEXT,
            remark TEXT,
            category TEXT
        )
        """
    )

    existing_columns = {row[1] for row in cur.execute("PRAGMA table_info(remarks)")}
    required_columns = {
        "act_id": "TEXT",
        "source_row_index": "INTEGER",
        "doc_type": "TEXT",
        "act_number": "TEXT",
        "act_date": "TEXT",
        "title": "TEXT",
        "author": "TEXT",
        "status": "TEXT",
        "url": "TEXT",
        "raw_row_text": "TEXT",
        "collected_at": "TEXT",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            cur.execute(f"ALTER TABLE remarks ADD COLUMN {column_name} {column_type}")

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_remarks_act_id ON remarks(act_id)"
    )
    conn.commit()
    conn.close()


def normalize_lines(text: str) -> list[str]:
    lines = []
    for line in (text or "").splitlines():
        cleaned = re.sub(r"\s+", " ", line).strip()
        if cleaned and cleaned != "—":
            lines.append(cleaned)
    return lines


def extract_act_id(href: str | None) -> str:
    if not href:
        return ""
    query = parse_qs(urlparse(href).query)
    values = query.get("selectedActId") or query.get("actId") or []
    return values[0] if values else ""


def parse_row_metadata(row_text: str) -> dict[str, str]:
    lines = normalize_lines(row_text)
    metadata = {
        "doc_type": "",
        "act_number": "",
        "act_date": "",
        "title": "",
        "author": "",
        "status": "",
    }
    if not lines:
        return metadata

    metadata["doc_type"] = lines[0]

    date_index = next(
        (index for index, line in enumerate(lines) if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", line)),
        -1,
    )
    if date_index > 0:
        metadata["act_number"] = lines[date_index - 1]
        metadata["act_date"] = lines[date_index]
        tail = lines[date_index + 1 :]
    else:
        tail = lines[1:]

    title_parts = []
    for line in tail:
        if line in STATUS_VALUES:
            metadata["status"] = line
            continue
        if " из " in line and len(line) <= 120 and not metadata["author"]:
            metadata["author"] = line
            continue
        title_parts.append(line)

    metadata["title"] = " ".join(title_parts).strip()
    return metadata


async def wait_login(page) -> None:
    print("Войди в Exon вручную и нажми Enter")
    input()
    await page.wait_for_load_state("domcontentloaded")


async def wait_registry_page(page) -> None:
    await page.wait_for_url(re.compile(r".*/itd/registry/work-sections.*"), timeout=20000)
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(3000)


async def get_rows_locator(page):
    await wait_registry_page(page)
    for selector in SELECTORS["rows"]:
        locator = page.locator(selector)
        count = await locator.count()
        print(f"Пробую селектор строк: {selector} -> {count}")
        if count > 0:
            return locator, selector

    html = await page.content()
    print("Не удалось найти строки. Первые 3000 символов HTML:\n")
    print(html[:3000])
    return None, None


async def get_virtual_scroller(page):
    for selector in SELECTORS["virtual_scroller"]:
        locator = page.locator(selector).first
        if await locator.count():
            return locator, selector
    return None, None


async def get_row_snapshot(row, start_url: str) -> dict[str, str] | None:
    row_text = (await row.inner_text()).strip()
    if not row_text:
        return None

    href = ""
    for selector in SELECTORS["act_link"]:
        link_locator = row.locator(selector).first
        if await link_locator.count():
            href = await link_locator.get_attribute("href") or ""
            if href:
                break

    href = urljoin(start_url, href) if href else ""
    act_id = extract_act_id(href)
    row_index = await row.get_attribute("data-rowindex") or ""

    return {
        "row_index": row_index,
        "href": href,
        "act_id": act_id,
        "row_text": row_text,
    }


def choose_remark(lines: list[str]) -> str:
    if not lines:
        return ""

    banned = {
        "Замечания",
        "Комментарии",
        "Помощь",
        "Нет замечаний",
        "Сохранить",
        "Отмена",
    }

    candidates = []
    for line in lines:
        if line in banned or line in STATUS_VALUES:
            continue
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", line):
            continue
        if len(line) < 5:
            continue
        candidates.append(line)

    return max(candidates, key=len, default="")


async def extract_detail_payload(page, row_meta: dict[str, str]) -> dict[str, str]:
    status = row_meta.get("status", "")
    texts: list[str] = []

    for selector in SELECTORS["remark_tab"]:
        try:
            await page.locator(selector).first.click(timeout=2500)
            await page.wait_for_timeout(1000)
            break
        except Exception:
            continue

    body_lines = normalize_lines(await page.locator("body").inner_text())
    for line in body_lines:
        if line in STATUS_VALUES:
            status = line
            break

    for selector in SELECTORS["detail_text_candidates"]:
        locator = page.locator(selector)
        count = min(await locator.count(), 20)
        for index in range(count):
            try:
                text = (await locator.nth(index).inner_text()).strip()
            except Exception:
                continue
            normalized = re.sub(r"\s+", " ", text).strip()
            if normalized and normalized not in texts:
                texts.append(normalized)

    if not row_meta.get("author"):
        row_meta["author"] = next(
            (
                line
                for line in body_lines
                if " из " in line and len(line) <= 120 and not re.search(r"\d{2}\.\d{2}\.\d{4}", line)
            ),
            "",
        )

    remark = choose_remark(texts + body_lines)
    return {
        "status": status,
        "remark": remark,
        "category": classify(remark),
        "author": row_meta.get("author", ""),
    }


def upsert_record(
    db_path: Path,
    snapshot: dict[str, str],
    row_meta: dict[str, str],
    detail: dict[str, str],
) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    collected_at = datetime.now().isoformat(timespec="seconds")
    act_label = " | ".join(
        part
        for part in [row_meta.get("doc_type"), row_meta.get("act_number"), row_meta.get("title")]
        if part
    )

    cur.execute(
        """
        INSERT INTO remarks (
            act,
            remark,
            category,
            act_id,
            source_row_index,
            doc_type,
            act_number,
            act_date,
            title,
            author,
            status,
            url,
            raw_row_text,
            collected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(act_id) DO UPDATE SET
            act=excluded.act,
            remark=excluded.remark,
            category=excluded.category,
            source_row_index=excluded.source_row_index,
            doc_type=excluded.doc_type,
            act_number=excluded.act_number,
            act_date=excluded.act_date,
            title=excluded.title,
            author=excluded.author,
            status=excluded.status,
            url=excluded.url,
            raw_row_text=excluded.raw_row_text,
            collected_at=excluded.collected_at
        """,
        (
            act_label,
            detail["remark"],
            detail["category"],
            snapshot["act_id"],
            int(snapshot["row_index"]) if snapshot["row_index"].isdigit() else None,
            row_meta.get("doc_type", ""),
            row_meta.get("act_number", ""),
            row_meta.get("act_date", ""),
            row_meta.get("title", ""),
            detail.get("author", ""),
            detail.get("status", ""),
            snapshot["href"],
            snapshot["row_text"],
            collected_at,
        ),
    )
    conn.commit()
    conn.close()


def export_csv(db_path: Path, csv_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            act_id,
            doc_type,
            act_number,
            act_date,
            title,
            author,
            status,
            remark,
            category,
            url,
            collected_at
        FROM remarks
        ORDER BY
            CASE
                WHEN act_date GLOB '__.__.____'
                    THEN substr(act_date, 7, 4) || '-' || substr(act_date, 4, 2) || '-' || substr(act_date, 1, 2)
                ELSE ''
            END DESC,
            id DESC
        """
    ).fetchall()

    with csv_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        if rows:
            writer = csv.DictWriter(csv_file, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(dict(row) for row in rows)
        else:
            csv_file.write("")

    conn.close()


async def open_act_page(context, href: str):
    page = await context.new_page()
    await page.goto(href)
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(1500)
    return page


async def process_visible_rows(
    page,
    config: AppConfig,
    selector_used: str,
    seen_ids: set[str],
) -> int:
    rows = page.locator(selector_used)
    visible_count = await rows.count()
    processed_now = 0

    for index in range(visible_count):
        if config.limit is not None and len(seen_ids) >= config.limit:
            break

        row = rows.nth(index)
        try:
            snapshot = await get_row_snapshot(row, config.start_url)
        except Exception as exc:
            print(f"Не удалось прочитать строку {index + 1}: {exc}")
            continue

        if not snapshot:
            continue

        row_marker = snapshot["act_id"] or f"row-{snapshot['row_index']}"
        if row_marker in seen_ids:
            continue

        if not snapshot["href"]:
            print(f"Строка {snapshot['row_index'] or index + 1}: нет ссылки на акт, пропускаю")
            seen_ids.add(row_marker)
            continue

        row_meta = parse_row_metadata(snapshot["row_text"])
        print(
            f"Обрабатываю строку {snapshot['row_index'] or index + 1}: "
            f"{row_meta.get('doc_type', '')} {row_meta.get('act_number', '')} | "
            f"{row_meta.get('status', '') or 'статус не найден'}"
        )

        act_page = None
        try:
            act_page = await open_act_page(page.context, snapshot["href"])
            detail = await extract_detail_payload(act_page, row_meta)
            upsert_record(config.db_path, snapshot, row_meta, detail)
            print(
                f"  -> {detail['status'] or 'без статуса'} | "
                f"{detail['category']} | "
                f"{(detail['remark'] or 'без текста замечания')[:140]}"
            )
            processed_now += 1
        except Exception as exc:
            print(f"Ошибка при обработке акта {snapshot['href']}: {exc}")
        finally:
            seen_ids.add(row_marker)
            if act_page:
                await act_page.close()

    return processed_now


async def scroll_next_chunk(page, scroller, config: AppConfig) -> tuple[bool, int, int]:
    metrics_before = await scroller.evaluate(
        """node => ({
            top: Math.round(node.scrollTop),
            height: Math.round(node.scrollHeight),
            client: Math.round(node.clientHeight)
        })"""
    )
    step = max(int(metrics_before["client"] * config.scroll_step_ratio), 300)
    await scroller.evaluate("(node, delta) => { node.scrollTop = node.scrollTop + delta; }", step)
    await page.wait_for_timeout(config.scroll_pause_ms)
    metrics_after = await scroller.evaluate(
        """node => ({
            top: Math.round(node.scrollTop),
            height: Math.round(node.scrollHeight),
            client: Math.round(node.clientHeight)
        })"""
    )
    moved = metrics_after["top"] > metrics_before["top"]
    return moved, metrics_after["top"], metrics_after["height"]


async def process_list(page, config: AppConfig) -> None:
    rows, selector_used = await get_rows_locator(page)
    if not rows or not selector_used:
        print("Строки не найдены. Проверь, что открыт именно список актов.")
        return

    visible_count = await rows.count()
    print(f"Найдено видимых строк: {visible_count} | селектор: {selector_used}")

    scroller, scroller_selector = await get_virtual_scroller(page)
    if scroller_selector:
        print(f"Скроллер списка: {scroller_selector}")
    else:
        print("Скроллер не найден, обработаю только текущие видимые строки.")

    seen_ids: set[str] = set()
    processed_total = 0
    stagnant_scrolls = 0

    while True:
        processed_before = len(seen_ids)
        processed_total += await process_visible_rows(page, config, selector_used, seen_ids)
        new_items = len(seen_ids) - processed_before

        if config.limit is not None and len(seen_ids) >= config.limit:
            break
        if not scroller:
            break

        moved, top, height = await scroll_next_chunk(page, scroller, config)
        print(f"Прокрутка списка: top={top}, height={height}, новых элементов={new_items}")

        if new_items == 0:
            stagnant_scrolls += 1
        else:
            stagnant_scrolls = 0

        if not moved or stagnant_scrolls >= config.max_stagnant_scrolls:
            break

    print(f"Обработано актов: {processed_total}")


async def main() -> None:
    configure_output()
    config = get_config()
    ensure_db(config.db_path)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=config.headless)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(config.start_url)
        await wait_login(page)

        print("Открой список актов по адресу /itd/registry/work-sections и дождись полной загрузки, затем Enter")
        input()

        try:
            await wait_registry_page(page)
        except PlaywrightTimeoutError:
            print(f"Текущий URL не похож на список актов: {page.url}")
            print("Перейди на страницу списка и запусти скрипт повторно.")
            await browser.close()
            return

        print(f"Текущий URL: {page.url}")
        await process_list(page, config)

        if config.export_csv:
            export_csv(config.db_path, config.csv_path)
            print(f"Экспорт сохранён: {config.csv_path.resolve()}")

        print("Готово. Нажми Enter для закрытия браузера")
        input()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
