import asyncio
import csv
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

DB_PATH = Path("exon_remarks.db")
CSV_PATH = Path("exon_remarks_export.csv")
START_URL = "https://exon.exonproject.ru/"
HEADLESS = False
SCROLL_STEP_RATIO = 0.85
SCROLL_PAUSE_MS = 1200
MAX_STAGNANT_SCROLLS = 4

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


def configure_output() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")


def classify(text: str) -> str:
    normalized = (text or "").lower()
    for name, patterns in CATEGORY_RULES:
        if any(re.search(pattern, normalized) for pattern in patterns):
            return name
    return "прочее"


def ensure_db() -> None:
    conn = sqlite3.connect(DB_PATH)
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


async def get_row_snapshot(row) -> dict[str, str] | None:
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

    href = urljoin(START_URL, href) if href else ""
    act_id = extract_act_id(href)
    row_index = await row.get_attribute("data-rowindex") or ""

    if not act_id and not href:
        return {
            "row_index": row_index,
            "href": "",
            "act_id": "",
            "row_text": row_text,
        }

    return {
        "row_index": row_index,
        "href": href,
        "act_id": act_id,
        "row_text": row_text,
    }


def choose_remark(lines: list[str], fallback_status: str) -> str:
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

    if not candidates:
        return "" if fallback_status == "Нет замечаний" else ""

    return max(candidates, key=len)


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

    remark = choose_remark(texts + body_lines, status)
    return {
        "status": status,
        "remark": remark,
        "category": classify(remark),
        "author": row_meta.get("author", ""),
    }


def upsert_record(snapshot: dict[str, str], row_meta: dict[str, str], detail: dict[str, str]) -> None:
    conn = sqlite3.connect(DB_PATH)
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


def export_csv() -> None:
    conn = sqlite3.connect(DB_PATH)
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

    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=rows[0].keys() if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(dict(row) for row in rows)

    conn.close()


async def open_act_page(context, href: str):
    page = await context.new_page()
    await page.goto(href)
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(1500)
    return page


async def process_visible_rows(page, selector_used: str, seen_ids: set[str]) -> int:
    rows = page.locator(selector_used)
    visible_count = await rows.count()
    processed_now = 0

    for index in range(visible_count):
        row = rows.nth(index)
        try:
            snapshot = await get_row_snapshot(row)
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
            upsert_record(snapshot, row_meta, detail)
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


async def scroll_next_chunk(scroller) -> tuple[bool, int, int]:
    metrics_before = await scroller.evaluate(
        """node => ({
            top: Math.round(node.scrollTop),
            height: Math.round(node.scrollHeight),
            client: Math.round(node.clientHeight)
        })"""
    )
    step = max(int(metrics_before["client"] * SCROLL_STEP_RATIO), 300)
    await scroller.evaluate("(node, delta) => { node.scrollTop = node.scrollTop + delta; }", step)
    await scroller.page.wait_for_timeout(SCROLL_PAUSE_MS)
    metrics_after = await scroller.evaluate(
        """node => ({
            top: Math.round(node.scrollTop),
            height: Math.round(node.scrollHeight),
            client: Math.round(node.clientHeight)
        })"""
    )
    moved = metrics_after["top"] > metrics_before["top"]
    reached_end = metrics_after["top"] + metrics_after["client"] >= metrics_after["height"] - 5
    return moved, metrics_after["top"], metrics_after["height"]


async def process_list(page) -> None:
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
        processed_total += await process_visible_rows(page, selector_used, seen_ids)
        new_items = len(seen_ids) - processed_before

        if not scroller:
            break

        moved, top, height = await scroll_next_chunk(scroller)
        print(f"Прокрутка списка: top={top}, height={height}, новых элементов={new_items}")

        if new_items == 0:
            stagnant_scrolls += 1
        else:
            stagnant_scrolls = 0

        if not moved or stagnant_scrolls >= MAX_STAGNANT_SCROLLS:
            break

    print(f"Обработано актов: {processed_total}")


async def main() -> None:
    configure_output()
    ensure_db()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=HEADLESS)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(START_URL)
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
        await process_list(page)
        export_csv()
        print(f"Экспорт сохранён: {CSV_PATH.resolve()}")

        print("Готово. Нажми Enter для закрытия браузера")
        input()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
