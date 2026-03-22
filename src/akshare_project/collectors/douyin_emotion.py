import asyncio
import os
import re
import sys
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.paths import get_artifacts_dir, get_cache_dir
from akshare_project.core.progress import ProgressStore
from akshare_project.db.db_tool import DbTools

ACCOUNT_ID = "1368194981"
SEARCH_URL = f"https://www.douyin.com/search/{ACCOUNT_ID}?type=user"
LOGGER = get_logger("douyin_emotion")
PROGRESS_STORE = ProgressStore("douyin_emotion")
USER_DATA_DIR = get_cache_dir("douyin_playwright_profile")
FRAME_ARTIFACT_DIR = get_artifacts_dir("douyin_emotion_frames")
HEADLESS = False
WAIT_TIMEOUT_MS = 15000
SCROLL_LIMIT = 30
VIEWPORT = {"width": 1600, "height": 1000}
SHOW_CLICK_MARKERS = True

AI_PROMPT = (
    "请读取当前视频中的日期和四个指数情绪指标，只按下面固定格式输出，不要补充解释，不要代码块：\n"
    "DATE=YYYY-MM-DD\n"
    "SZ50=数字或NA\n"
    "HS300=数字或NA\n"
    "ZZ500=数字或NA\n"
    "ZZ1000=数字或NA"
)


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_date_text(text):
    match = re.search(r"(\d{4})[^\d]?(\d{1,2})[^\d]?(\d{1,2})", clean_text(text))
    if not match:
        return ""
    return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"


def parse_number(text):
    if not text or text.upper() == "NA":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_ai_response(text):
    cleaned = clean_text(text)

    def last_value(pattern):
        matches = re.findall(pattern, cleaned, re.IGNORECASE)
        return matches[-1] if matches else None

    date_value = last_value(r"DATE\s*[:=：]\s*([0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}|NA)")
    sz50_value = last_value(r"SZ50\s*[:=：]\s*(-?\d+(?:\.\d+)?|NA)")
    hs300_value = last_value(r"HS300\s*[:=：]\s*(-?\d+(?:\.\d+)?|NA)")
    zz500_value = last_value(r"ZZ500\s*[:=：]\s*(-?\d+(?:\.\d+)?|NA)")
    zz1000_value = last_value(r"ZZ1000\s*[:=：]\s*(-?\d+(?:\.\d+)?|NA)")

    if not date_value:
        date_value = normalize_date_text(cleaned)

    return {
        "emotion_date": str(date_value).replace("/", "-") if date_value else "",
        "sz50_emotion": parse_number(sz50_value),
        "hs300_emotion": parse_number(hs300_value),
        "zz500_emotion": parse_number(zz500_value),
        "zz1000_emotion": parse_number(zz1000_value),
    }


def build_status(parsed_row):
    values = [
        parsed_row.get("sz50_emotion"),
        parsed_row.get("hs300_emotion"),
        parsed_row.get("zz500_emotion"),
        parsed_row.get("zz1000_emotion"),
    ]
    matched = sum(1 for value in values if value is not None)
    if matched == 4:
        return "SUCCESS"
    if matched > 0:
        return "PARTIAL"
    return "FAILED"


def load_progress():
    return PROGRESS_STORE.load()


def save_progress(progress_key):
    PROGRESS_STORE.append(progress_key)


def log_error(video_url, error_message):
    LOGGER.error("%s,%s", video_url, error_message)


async def wait_for_enter(message):
    print(message)
    await asyncio.to_thread(input)


async def launch_browser_context(playwright):
    return await playwright.chromium.launch_persistent_context(
        str(USER_DATA_DIR),
        channel="chrome",
        headless=HEADLESS,
        viewport=VIEWPORT,
        args=["--autoplay-policy=no-user-gesture-required"],
    )


async def wait_for_manual_verification(page):
    for _ in range(120):
        body_text = clean_text(await page.locator("body").inner_text())
        if "请完成下列验证后继续" in body_text or "按住左边按钮拖动完成上方拼图" in body_text:
            await wait_for_enter("检测到抖音验证，请在浏览器中手工完成，完成后回终端按回车继续...")
            await page.wait_for_timeout(2000)
            continue
        return


async def close_login_popup(page):
    for _ in range(6):
        body_text = clean_text(await page.locator("body").inner_text())
        if "登录后免费畅享高清视频" not in body_text and "验证码登录" not in body_text:
            return

        dialogs = page.locator("div[role='dialog']")
        count = await dialogs.count()
        clicked = False
        for index in range(count):
            dialog = dialogs.nth(index)
            try:
                box = await dialog.bounding_box()
                if not box:
                    continue
                await page.mouse.click(box["x"] + box["width"] - 28, box["y"] + 28)
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass

        await page.wait_for_timeout(1000)


async def open_account_page(page):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=WAIT_TIMEOUT_MS)
    await page.wait_for_timeout(4000)
    await close_login_popup(page)
    await wait_for_manual_verification(page)

    user_links = page.locator("a[href*='/user/']")
    count = await user_links.count()
    for index in range(count):
        candidate = user_links.nth(index)
        try:
            href = await candidate.get_attribute("href")
            text = clean_text(await candidate.inner_text())
        except Exception:
            continue

        if not href:
            continue
        if ACCOUNT_ID not in href and ACCOUNT_ID not in text:
            continue

        full_url = href if href.startswith("http") else urljoin("https://www.douyin.com", href)
        await page.goto(full_url, wait_until="domcontentloaded", timeout=WAIT_TIMEOUT_MS)
        await page.wait_for_timeout(3000)
        await close_login_popup(page)
        await wait_for_manual_verification(page)
        return

    await wait_for_enter("没有自动定位到目标账号。请在浏览器中手工进入抖音号 1368194981 的主页，完成后回终端按回车继续...")


async def collect_video_cards(page):
    video_cards = {}
    stable_rounds = 0
    last_count = 0

    for _ in range(SCROLL_LIMIT):
        link_handles = await page.locator("a[href*='/video/']").element_handles()
        for handle in link_handles:
            try:
                href = await handle.get_attribute("href")
            except Exception:
                continue

            if not href:
                continue
            full_url = href if href.startswith("http") else urljoin("https://www.douyin.com", href)
            match = re.search(r"/video/(\d+)", full_url)
            if not match:
                continue

            video_id = match.group(1)
            if video_id in video_cards:
                continue

            try:
                title = clean_text(await handle.inner_text())
            except Exception:
                title = ""

            video_cards[video_id] = {
                "video_id": video_id,
                "video_url": full_url,
                "video_title": title or None,
                "emotion_date": normalize_date_text(title),
            }

        await page.mouse.wheel(0, 5000)
        await page.wait_for_timeout(1500)
        if len(video_cards) == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = len(video_cards)
        if stable_rounds >= 3:
            break

    return list(video_cards.values())


async def open_video_from_account_page(page, account_page_url, video_card):
    await page.goto(account_page_url, wait_until="domcontentloaded", timeout=WAIT_TIMEOUT_MS)
    await page.wait_for_timeout(3000)
    await close_login_popup(page)
    await wait_for_manual_verification(page)

    target_selector = f"a[href*='/video/{video_card['video_id']}']"
    for _ in range(10):
        locator = page.locator(target_selector)
        if await locator.count() > 0:
            try:
                await locator.first.click()
                await page.wait_for_timeout(3000)
                return
            except Exception:
                pass
        await page.mouse.wheel(0, 4000)
        await page.wait_for_timeout(1200)

    await page.goto(video_card["video_url"], wait_until="domcontentloaded", timeout=WAIT_TIMEOUT_MS)
    await page.wait_for_timeout(3000)


async def wait_for_video_ready(page):
    for _ in range(30):
        await close_login_popup(page)
        await wait_for_manual_verification(page)

        body_text = clean_text(await page.locator("body").inner_text())
        if "不支持的音频/视频格式" in body_text or "设备无网络" in body_text:
            await wait_for_enter("视频未正常加载。请在浏览器中手工点击“刷新”或“打开声音”，确认视频可正常显示后回终端按回车继续...")
            await page.wait_for_timeout(2000)
            continue

        video = page.locator("video")
        if await video.count() > 0:
            try:
                duration = await page.evaluate(
                    """() => {
                        const video = document.querySelector('video');
                        return video ? Number(video.duration || 0) : 0;
                    }"""
                )
                if duration and duration > 0:
                    return
            except Exception:
                pass

        await page.wait_for_timeout(1000)

    raise ValueError("video did not become ready")


async def is_video_paused(page):
    try:
        return await page.evaluate(
            """() => {
                const video = document.querySelector('video');
                return video ? Boolean(video.paused) : false;
            }"""
        )
    except Exception:
        return False


async def pause_video(page):
    video = page.locator("video")
    if await video.count() == 0:
        raise ValueError("video element not found")

    for _ in range(6):
        try:
            await video.first.hover()
            await page.wait_for_timeout(200)
            await page.evaluate(
                """() => {
                    const video = document.querySelector('video');
                    if (video) video.pause();
                }"""
            )
            await page.wait_for_timeout(600)
            if await is_video_paused(page):
                return

            await video.first.click(force=True)
            await page.wait_for_timeout(400)
            await page.evaluate(
                """() => {
                    const video = document.querySelector('video');
                    if (video) video.pause();
                }"""
            )
            await page.wait_for_timeout(600)
            if await is_video_paused(page):
                return
        except Exception:
            continue

    raise ValueError("failed to pause video")


async def panel_markers_visible(page):
    summary_button = page.get_by_text("总结当前视频内容", exact=False)
    if await summary_button.count() > 0:
        for index in range(await summary_button.count()):
            try:
                if await summary_button.nth(index).is_visible():
                    return True
            except Exception:
                continue

    body_text = clean_text(await page.locator("body").inner_text())
    if "视频主要内容总结如下" in body_text:
        return True
    if "DATE=" in body_text and "SZ50=" in body_text and "HS300=" in body_text:
        return True
    return False


async def show_click_marker(page, x, y, label):
    if not SHOW_CLICK_MARKERS:
        return

    await page.evaluate(
        """([marker_x, marker_y, marker_label]) => {
            const id = `codex-click-marker-${marker_label}`;
            const old = document.getElementById(id);
            if (old) old.remove();

            const marker = document.createElement('div');
            marker.id = id;
            marker.style.position = 'fixed';
            marker.style.left = `${marker_x - 18}px`;
            marker.style.top = `${marker_y - 18}px`;
            marker.style.width = '36px';
            marker.style.height = '36px';
            marker.style.borderRadius = '50%';
            marker.style.background = 'rgba(255, 59, 48, 0.35)';
            marker.style.border = '3px solid #ff3b30';
            marker.style.zIndex = '2147483647';
            marker.style.pointerEvents = 'none';
            marker.style.display = 'flex';
            marker.style.alignItems = 'center';
            marker.style.justifyContent = 'center';
            marker.style.color = '#fff';
            marker.style.fontSize = '12px';
            marker.style.fontWeight = '700';
            marker.textContent = marker_label;
            document.body.appendChild(marker);
        }""",
        [x, y, label],
    )


async def click_recognize_button(page):
    viewport = page.viewport_size or VIEWPORT
    center_x = int(viewport["width"] * 0.868)
    center_y = int(viewport["height"] * 0.443)
    offsets = [
        (0, 0),
        (-12, 0),
        (12, 0),
        (0, -12),
        (0, 12),
        (-20, 0),
        (20, 0),
        (0, -20),
        (0, 20),
        (-20, -20),
        (20, -20),
        (-20, 20),
        (20, 20),
        (-35, 0),
        (35, 0),
        (0, -35),
        (0, 35),
    ]

    print(f"recognize debug center: ({center_x}, {center_y})")
    await show_click_marker(page, center_x, center_y, "C")

    for index, (dx, dy) in enumerate(offsets, start=1):
        await pause_video(page)
        x = center_x + dx
        y = center_y + dy
        print(f"recognize attempt {index}: click at ({x}, {y})")
        await show_click_marker(page, x, y, str(index))
        await page.mouse.move(x, y)
        await page.wait_for_timeout(500)
        await page.mouse.click(x, y)
        await page.wait_for_timeout(1500)
        if await panel_markers_visible(page):
            print(f"recognize attempt {index}: panel opened")
            return
        if not await is_video_paused(page):
            print(f"recognize attempt {index}: video resumed, pausing again")
            await pause_video(page)

    await wait_for_enter("脚本没有自动点中“识别画面”。请在浏览器中手工点击该按钮，面板打开后回终端按回车继续...")


async def ensure_ai_panel_open(page):
    already_open = await panel_markers_visible(page)
    print(f"ensure_ai_panel_open: panel already open = {already_open}")
    if already_open:
        return

    print("ensure_ai_panel_open: trying recognize button flow")
    await click_recognize_button(page)
    await page.wait_for_timeout(1500)
    if await panel_markers_visible(page):
        print("ensure_ai_panel_open: panel opened after click")
        return
    raise ValueError("failed to open AI panel")


async def find_ai_input(page):
    selectors = [
        "textarea[placeholder*='问AI']",
        "textarea[placeholder*='问']",
        "input[placeholder*='问AI']",
        "input[placeholder*='问']",
        "[contenteditable='true']",
        "textarea",
        "input",
    ]
    for selector in selectors:
        locator = page.locator(selector)
        count = await locator.count()
        for index in range(count - 1, -1, -1):
            candidate = locator.nth(index)
            try:
                if await candidate.is_visible():
                    return candidate, selector
            except Exception:
                continue
    return None, ""


async def submit_ai_prompt(page, prompt):
    await ensure_ai_panel_open(page)

    prompt_button = page.get_by_text("总结当前视频内容", exact=False)
    if await prompt_button.count() > 0:
        try:
            await prompt_button.first.click()
            await page.wait_for_timeout(1000)
        except Exception:
            pass

    input_locator, selector = await find_ai_input(page)
    if input_locator is None:
        await wait_for_enter("脚本没有自动定位到 AI 输入框。请手工在右侧输入框粘贴提示词并发送，发送后回终端按回车继续...")
        return

    await input_locator.click()
    await page.keyboard.press("Control+A")
    await page.keyboard.press("Backspace")

    if selector == "[contenteditable='true']":
        await page.keyboard.insert_text(prompt)
    else:
        try:
            await input_locator.fill(prompt)
        except Exception:
            await page.keyboard.insert_text(prompt)

    await page.wait_for_timeout(300)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(800)


async def wait_for_ai_response(page):
    for _ in range(40):
        body_text = clean_text(await page.locator("body").inner_text())
        if "DATE=" in body_text and "SZ50=" in body_text and "HS300=" in body_text:
            return body_text
        await page.wait_for_timeout(1000)

    await wait_for_enter("脚本没有等到标准格式回复。请确认右侧已经返回标准结果，再回终端按回车继续...")
    return clean_text(await page.locator("body").inner_text())


async def extract_video_row(page, account_page_url, video_card):
    await open_video_from_account_page(page, account_page_url, video_card)
    await wait_for_video_ready(page)
    await pause_video(page)
    await submit_ai_prompt(page, AI_PROMPT)
    response_text = await wait_for_ai_response(page)

    parsed = parse_ai_response(response_text)
    row = {
        "emotion_date": parsed["emotion_date"] or video_card.get("emotion_date"),
        "video_id": video_card["video_id"],
        "account_id": ACCOUNT_ID,
        "account_name": "立伟",
        "video_title": video_card.get("video_title"),
        "video_url": video_card["video_url"],
        "hs300_emotion": parsed["hs300_emotion"],
        "zz500_emotion": parsed["zz500_emotion"],
        "zz1000_emotion": parsed["zz1000_emotion"],
        "sz50_emotion": parsed["sz50_emotion"],
        "raw_ocr_text": f"PROMPT:\n{AI_PROMPT}\n\nRESPONSE:\n{response_text}"[:60000],
        "extraction_method": "playwright+ai_prompt",
        "extraction_status": build_status(parsed),
    }

    if not row["emotion_date"]:
        row["emotion_date"] = normalize_date_text(response_text)

    return row


async def process_videos(page, account_page_url, video_cards, date_filter=None):
    processed = load_progress()
    db_tools = DbTools()
    await db_tools.init_pool()
    affected_rows = 0

    try:
        for video_card in video_cards:
            progress_key = video_card["video_id"]
            if progress_key in processed:
                continue

            try:
                row = await extract_video_row(page, account_page_url, video_card)
                if date_filter and row.get("emotion_date") and row["emotion_date"] < date_filter:
                    save_progress(progress_key)
                    processed.add(progress_key)
                    continue

                if not row.get("emotion_date"):
                    raise ValueError("failed to determine emotion date")

                affected_rows += await db_tools.upsert_douyin_emotion_daily([row])
                save_progress(progress_key)
                processed.add(progress_key)
                print(
                    f"saved {row['emotion_date']} "
                    f"SZ50={row['sz50_emotion']} HS300={row['hs300_emotion']} "
                    f"ZZ500={row['zz500_emotion']} ZZ1000={row['zz1000_emotion']} "
                    f"status={row['extraction_status']}"
                )
            except Exception as exc:
                print(f"failed to process {video_card['video_url']}: {exc}")
                log_error(video_card["video_url"], str(exc))
    finally:
        await db_tools.close()

    return affected_rows


async def get_latest_emotion_date():
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        return await db_tools.get_douyin_latest_emotion_date(ACCOUNT_ID)
    finally:
        await db_tools.close()


async def run_pipeline(date_filter=None, reverse=False):
    async with async_playwright() as playwright:
        context = await launch_browser_context(playwright)
        page = await context.new_page()
        try:
            await open_account_page(page)
            account_page_url = page.url
            video_cards = await collect_video_cards(page)
            video_cards.sort(key=lambda item: item.get("emotion_date") or "", reverse=reverse)
            return await process_videos(page, account_page_url, video_cards, date_filter=date_filter)
        finally:
            await context.close()


async def backfill_history():
    return await run_pipeline(date_filter=None, reverse=False)


async def sync_daily():
    latest_date = await get_latest_emotion_date()
    return await run_pipeline(date_filter=latest_date, reverse=True)


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "daily"

    if command == "backfill":
        affected_rows = await backfill_history()
        print(f"douyin backfill finished, affected rows: {affected_rows}")
        return

    if command == "daily":
        affected_rows = await sync_daily()
        print(f"douyin daily sync finished, affected rows: {affected_rows}")
        return

    raise ValueError("supported commands: backfill, daily")


if __name__ == "__main__":
    asyncio.run(main())
