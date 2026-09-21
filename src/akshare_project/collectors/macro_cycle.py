"""Official monthly macro releases; resumable, append-only, no synthetic observations."""

import asyncio
import base64
from io import BytesIO
import json
import re
import sys
import time
from datetime import date, datetime, timedelta
from urllib.parse import urljoin

import requests
import aiomysql
from bs4 import BeautifulSoup

from akshare_project.collectors.macro_cycle_parsing import (
    PARSER_VERSION, article_body, compact, discover_page,
    parse_money_supply_table, parse_release, parse_tsf_table,
)
from akshare_project.db.db_tool import DbTools
from akshare_project.db.macro_cycle import MacroCycleStore


INDEXES = (
    "https://www.pbc.gov.cn/diaochatongjisi/116219/116225/index.html",
    "https://www.stats.gov.cn/sj/zxfb/",
    "https://www.stats.gov.cn/sj/sjjd/",
)


class OfficialClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "zh-CN,zh;q=0.9"})
        self.last_request = 0.0

    def get(self, url, *, binary=False):
        for attempt in range(3):
            time.sleep(max(0, 1.0 - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            try:
                response = self.session.get(url, timeout=35)
                response.raise_for_status()
                if binary:
                    return response.content
                try:
                    return response.content.decode("utf-8")
                except UnicodeDecodeError:
                    return response.content.decode("gb18030")
            except (requests.RequestException, UnicodeError):
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)

    def get_article(self, url):
        html = self.get(url)
        soup = BeautifulSoup(html, "lxml")
        try:
            article_body(soup)
            return html
        except ValueError:
            links = {urljoin(url, a["href"]) for a in soup.select("a[href]")
                     if a["href"].lower().endswith(".pdf")}
            if not links:
                raise
        from pypdf import PdfReader
        attachments = []
        for link in sorted(links):
            if not link.startswith("https://www.pbc.gov.cn/"):
                raise ValueError("Unexpected official attachment host")
            raw = self.get(link, binary=True)
            content = "\n".join(p.extract_text() or "" for p in PdfReader(BytesIO(raw)).pages)
            if len(compact(content)) < 80:
                raise ValueError("PDF contains no extractable official text")
            attachments.append({"url": link, "base64": base64.b64encode(raw).decode(), "text": content})
        return json.dumps({"official_html": html, "attachments": attachments}, ensure_ascii=False)


async def sync_releases(start_date, end_date, *, backfill=False, max_pages=None):
    start_date, end_date = date.fromisoformat(str(start_date)), date.fromisoformat(str(end_date))
    if start_date > end_date:
        raise ValueError("start_date must not exceed end_date")
    db = DbTools()
    client = OfficialClient()
    store = MacroCycleStore(db)
    errors, discovered = [], {}
    summary = {"status": "ok", "target_date": end_date.isoformat(), "new_observations": 0,
               "checked_releases": 0, "unchanged_releases": 0, "source_count": len(INDEXES)}
    try:
        await store.ensure_tables()
        done = await store.completed_urls(PARSER_VERSION) if backfill else set()
        for root in INDEXES:
            queue, visited = [root], set()
            while queue and (max_pages is None or len(visited) < max_pages):
                url = queue.pop(0)
                if url in visited:
                    continue
                visited.add(url)
                try:
                    html = await asyncio.to_thread(client.get, url)
                    releases, pages = discover_page(html, url)
                    if not releases and not pages:
                        # An official page can contain only unrelated releases. A maintenance
                        # page, unlike a listing, has neither pagination nor article links.
                        soup = BeautifulSoup(html, "lxml")
                        if not ("总记录数" in soup.get_text() or any("t20" in a["href"] for a in soup.select("a[href]"))):
                            raise ValueError("Official release listing is unavailable or changed")
                    for release in releases:
                        period = date.fromisoformat(release["period_end"])
                        if start_date <= period <= end_date:
                            discovered[release["url"]] = release
                    oldest = min((date.fromisoformat(r["period_end"]) for r in releases), default=end_date)
                    if oldest >= start_date:
                        queue.extend(p for p in pages if p not in visited)
                except Exception as exc:
                    errors.append({"url": url, "error": str(exc)})
                    break
        for url, entry in discovered.items():
            if url in done:
                summary["unchanged_releases"] += 1
                continue
            try:
                html = await asyncio.to_thread(client.get_article, url)
                release, observations = parse_release(html, url)
                count = await store.save(release, observations)
                await store.checkpoint(url, "success", parser_version=PARSER_VERSION)
                summary["new_observations"] += count
                summary["checked_releases"] += 1
                summary["unchanged_releases"] += int(count == 0)
                print(json.dumps({"period": entry["period_end"], "title": release["title"], "saved": count}, ensure_ascii=False), flush=True)
            except Exception as exc:
                await store.checkpoint(url, "failed", str(exc), PARSER_VERSION)
                errors.append({"url": url, "error": str(exc)})
        summary["coverage"] = await store.coverage()
        summary["errors"] = errors
        if errors:
            summary["status"] = "failed"
        summary["note"] = "月度所属期不等于发布日期；没有新公告属于正常。历史归档仅供展示，严格回测不得早于实际留存版本时间。"
        return summary
    finally:
        client.session.close()
        await db.close()


async def sync_tsf_tables(start_year=2005, end_year=None):
    """Follow official year directories and HTML attachments; keep unknown releases explicit."""
    end_year = end_year or date.today().year
    client, db = OfficialClient(), DbTools()
    store = MacroCycleStore(db)
    errors, saved = [], 0
    root = "https://www.pbc.gov.cn/diaochatongjisi/116219/116319/index.html"
    try:
        await store.ensure_tables()
        soup = BeautifulSoup(await asyncio.to_thread(client.get, root), "lxml")
        years = {}
        for a in soup.select("a[href]"):
            m = re.fullmatch(r"(20\d{2})年统计数据", compact(a.get_text()))
            if m and start_year <= int(m[1]) <= end_year:
                years[int(m[1])] = urljoin(root, a["href"])
        if not years:
            raise ValueError("Official PBC year directory is empty")
        for year, year_url in sorted(years.items(), reverse=True):
            try:
                soup = BeautifulSoup(await asyncio.to_thread(client.get, year_url), "lxml")
                sections = {urljoin(year_url, a["href"]) for a in soup.select("a[href]") if "社会融资规模" in a.get_text()}
                for section in sections:
                    page = BeautifulSoup(await asyncio.to_thread(client.get, section), "lxml")
                    attachments = {urljoin(section, a["href"]) for a in page.select("a[href]") if a["href"].lower().endswith((".htm", ".html")) and "attach" in a["href"].lower()}
                    for url in sorted(attachments):
                        html = await asyncio.to_thread(client.get, url)
                        if "社会融资规模增量统计表" not in compact(BeautifulSoup(html, "lxml").get_text()):
                            continue
                        release, rows = parse_tsf_table(html, url)
                        saved += await store.save(release, rows)
                        await store.checkpoint(url, "success", parser_version=PARSER_VERSION)
            except Exception as exc:
                errors.append({"url": year_url, "year": year, "error": str(exc)})
        return {"status": "failed" if errors else "ok", "new_observations": saved, "errors": errors}
    finally:
        client.session.close()
        await db.close()


async def sync_money_supply_tables(start_year=2005, end_year=None):
    """Read the official annual money-supply tables, including comparable M1 rows."""
    end_year = end_year or date.today().year
    client, db = OfficialClient(), DbTools()
    store = MacroCycleStore(db)
    errors, saved = [], 0
    root = "https://www.pbc.gov.cn/diaochatongjisi/116219/116319/index.html"
    try:
        await store.ensure_tables()
        soup = BeautifulSoup(await asyncio.to_thread(client.get, root), "lxml")
        years = {}
        for a in soup.select("a[href]"):
            match = re.fullmatch(r"(20\d{2})年统计数据", compact(a.get_text()))
            if match and start_year <= int(match[1]) <= end_year:
                years[int(match[1])] = urljoin(root, a["href"])
        if not years:
            raise ValueError("Official PBC year directory is empty")
        for year, year_url in sorted(years.items(), reverse=True):
            try:
                page = BeautifulSoup(await asyncio.to_thread(client.get, year_url), "lxml")
                candidates = {urljoin(year_url, a["href"]) for a in page.select("a[href]")
                              if "货币供应量" in compact(a.get_text()) and a["href"].lower().endswith((".htm", ".html"))}
                sections = {urljoin(year_url, a["href"]) for a in page.select("a[href]")
                            if "货币统计概览" in compact(a.get_text())}
                for section in sections:
                    section_page = BeautifulSoup(await asyncio.to_thread(client.get, section), "lxml")
                    matching_rows = [row for row in section_page.select("tr")
                                     if "货币供应量" in compact(row.get_text())]
                    if matching_rows:
                        # Legacy PBC pages use nested layout tables. Ancestor rows contain
                        # every attachment on the page, so use the most specific row only.
                        row = min(matching_rows, key=lambda item: len(compact(item.get_text())))
                        candidates.update(urljoin(section, a["href"]) for a in row.select("a[href]")
                                          if a["href"].lower().endswith((".htm", ".html")))
                if not candidates:
                    raise ValueError("Official money-supply table link is missing")
                for url in sorted(candidates):
                    html = await asyncio.to_thread(client.get, url)
                    release, rows = parse_money_supply_table(html, url)
                    saved += await store.save(release, rows)
                    await store.checkpoint(url, "success", parser_version=PARSER_VERSION)
            except Exception as exc:
                errors.append({"url": year_url, "year": year, "error": str(exc)})
        return {"status": "failed" if errors else "ok", "new_observations": saved, "errors": errors}
    finally:
        client.session.close()
        await db.close()


async def load_coverage():
    db = DbTools()
    try:
        store = MacroCycleStore(db)
        await store.ensure_tables()
        return await store.coverage()
    finally:
        await db.close()


async def sync_daily(target_date=None):
    target = date.fromisoformat(str(target_date)) if target_date else date.today()
    # Revisit recent releases even when previously collected, retaining revisions.
    result = await sync_releases(target - timedelta(days=120), target)
    tables = await sync_tsf_tables(target.year - 1, target.year)
    money = await sync_money_supply_tables(target.year - 1, target.year)
    result["new_observations"] += tables["new_observations"] + money["new_observations"]
    result["errors"].extend(tables["errors"] + money["errors"])
    result["status"] = "failed" if result["errors"] else "ok"
    result["coverage"] = await load_coverage()
    return result


async def repair_cached():
    """Reparse retained originals and retry failed article URLs without rescanning history."""
    db, client = DbTools(), OfficialClient()
    store = MacroCycleStore(db)
    result = {"status": "ok", "new_observations": 0, "checked_releases": 0, "errors": []}
    try:
        await store.ensure_tables()
        async with db.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""SELECT r.* FROM cn_macro_release r WHERE NOT EXISTS
                    (SELECT 1 FROM cn_macro_release n WHERE n.source_key=r.source_key AND n.revision_number>r.revision_number)""")
                cached = list(await cursor.fetchall())
                await cursor.execute("SELECT source_url FROM cn_macro_collection_checkpoint WHERE status='failed'")
                failures = [r["source_url"] for r in await cursor.fetchall()]
        inputs = [(r["source_url"], r["raw_response"], r["category"]) for r in cached]
        inputs += [(url, None, None) for url in failures]
        for url, html, category in inputs:
            try:
                html = html if html is not None else await asyncio.to_thread(client.get_article, url)
                parser = (parse_tsf_table if category == "tsf_monthly_table" else
                          parse_money_supply_table if category == "money_supply_table" else parse_release)
                release, rows = parser(html, url)
                result["new_observations"] += await store.save(release, rows)
                result["checked_releases"] += 1
                await store.checkpoint(url, "success", parser_version=PARSER_VERSION)
            except Exception as exc:
                result["errors"].append({"url": url, "error": str(exc)})
                await store.checkpoint(url, "failed", str(exc), PARSER_VERSION)
        result["status"] = "failed" if result["errors"] else "ok"
        result["coverage"] = await store.coverage()
        return result
    finally:
        client.session.close()
        await db.close()


async def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "daily"
    if command == "daily":
        result = await sync_daily(sys.argv[2] if len(sys.argv) > 2 else None)
    elif command == "repair":
        result = await repair_cached()
    elif command == "backfill":
        start = sys.argv[2] if len(sys.argv) > 2 else "2005-01-01"
        end = sys.argv[3] if len(sys.argv) > 3 else date.today().isoformat()
        result = await sync_releases(start, end, backfill=True)
        tables = await sync_tsf_tables(date.fromisoformat(start).year, date.fromisoformat(end).year)
        money = await sync_money_supply_tables(date.fromisoformat(start).year, date.fromisoformat(end).year)
        result["new_observations"] += tables["new_observations"] + money["new_observations"]
        result["errors"].extend(tables["errors"] + money["errors"])
        result["status"] = "failed" if result["errors"] else "ok"
        result["coverage"] = await load_coverage()
    else:
        raise ValueError("macro-cycle daily [DATE] | backfill START [END]")
    print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
    if result["status"] == "failed":
        raise RuntimeError(f"Official macro collection failed for {len(result['errors'])} sources; checkpoints retained")
