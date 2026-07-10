import asyncio
import fcntl
import json
import os
import re
import sys
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import cv2
import numpy as np
from playwright.async_api import async_playwright
from rapidocr_onnxruntime import RapidOCR

from akshare_project.collectors import quant_index
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.paths import get_cache_dir, get_state_path
from akshare_project.db.db_tool import DbTools


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
ACCOUNT_ID = "1368194981"
ACCOUNT_NAME = "立伟"
ACCOUNT_URL = (
    "https://www.douyin.com/user/"
    "MS4wLjABAAAArV-i8PGGGYk9tlP1JGrFxL5BR1VF01lb0Kn5MUOoNcM"
    "?from_tab_name=main"
)
COZE_AGENT_ID = "7493181057945092105"
COZE_AGENT_URL = (
    "https://www.coze.cn/store/agent/7493181057945092105"
    "?bot_id=true&bid=6kg9s3m48101i"
)
USER_DATA_DIR = get_cache_dir("douyin_coze_playwright_profile")
LOGIN_STATE_PATH = get_state_path("douyin_coze_login", "json")
STORAGE_STATE_PATH = get_state_path("douyin_coze_storage", "json")
LOCK_PATH = get_state_path("douyin_coze_daily", "lock")
VIDEO_INDEX_PATH = get_state_path("douyin_video_index", "json")
LOGGER = get_logger("douyin_emotion")
PAGE_TIMEOUT_MS = 60_000
COZE_RESPONSE_TIMEOUT_SECONDS = 300
MAX_VIDEO_CARDS = 16
DOUYIN_CONTENT_PATH_PATTERN = re.compile(r"/(?P<content_type>video|note)/(?P<content_id>\d+)")
NOTE_IMAGE_URL_MARKER = "biz_tag=aweme_images"
NOTE_VALUE_CROP_START_RATIO = 0.82
NOTE_VALUE_MIN_X_RATIO = 0.65
_NOTE_OCR_ENGINE = None
DOUYIN_SESSION_COOKIE_NAMES = {
    "sessionid",
    "sessionid_ss",
    "sid_guard",
}

INDEX_SPECS = {
    "sz50_emotion": ("上证50", "上证五零", "SZ50"),
    "hs300_emotion": ("沪深300", "沪深三百", "HS300"),
    "zz500_emotion": ("中证500", "中证五百", "ZZ500"),
    "zz1000_emotion": ("中证1000", "中证一千", "ZZ1000"),
}
NUMBER_TOKEN_PATTERN = r"(?:-?\d{1,3}(?:\.\d+)?|负?[零〇一二两三四五六七八九十百点]+)"


class AuthenticationRequiredError(RuntimeError):
    pass


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def clean_text(value):
    return re.sub(r"[ \t\u00a0]+", " ", str(value or "")).strip()


def normalize_date_text(value):
    text = clean_text(value)
    match = re.search(r"(\d{4})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})", text)
    if not match:
        return ""
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
    except ValueError:
        return ""


def decode_video_published_at(video_id):
    try:
        timestamp = int(str(video_id).strip()) >> 32
        parsed = datetime.fromtimestamp(timestamp, tz=SHANGHAI_TZ)
    except (TypeError, ValueError, OverflowError, OSError):
        return None

    now = datetime.now(SHANGHAI_TZ)
    if parsed.year < 2016 or parsed > now + timedelta(days=2):
        return None
    return parsed


def parse_douyin_content_url(raw_url):
    href = str(raw_url or "").strip()
    match = DOUYIN_CONTENT_PATH_PATTERN.search(href)
    if not match:
        return None
    content_type = match.group("content_type")
    content_id = match.group("content_id")
    return {
        "content_type": content_type,
        "content_id": content_id,
        "content_url": (
            href
            if href.startswith("http")
            else urljoin("https://www.douyin.com", href)
        ),
    }


def _get_note_ocr_engine():
    global _NOTE_OCR_ENGINE
    if _NOTE_OCR_ENGINE is None:
        _NOTE_OCR_ENGINE = RapidOCR()
    return _NOTE_OCR_ENGINE


def _ocr_image(image):
    result, _ = _get_note_ocr_engine()(image)
    return result or []


def _ocr_line_text(item):
    return clean_text(item[1]) if len(item) > 1 else ""


def _ocr_line_center_y(item):
    box = item[0] if item else []
    points = [point for point in box if isinstance(point, (list, tuple)) and len(point) >= 2]
    return sum(float(point[1]) for point in points) / len(points) if points else 0.0


def extract_note_chart_slots(ocr_result, image_height):
    slots = {}
    for item in ocr_result:
        text = _ocr_line_text(item).replace(" ", "")
        if not text:
            continue
        for field_name, aliases in INDEX_SPECS.items():
            if not any(alias.upper() in text.upper() for alias in aliases):
                continue
            slot = "top" if _ocr_line_center_y(item) < image_height / 2 else "bottom"
            slots[slot] = field_name
            break
    return slots


def extract_note_chart_regions(ocr_result, image_height):
    title_positions = {}
    for item in ocr_result:
        text = _ocr_line_text(item).replace(" ", "")
        if not text or ("情绪" not in text and "指标" not in text):
            continue
        for field_name, aliases in INDEX_SPECS.items():
            if not any(alias.upper() in text.upper() for alias in aliases):
                continue
            title_positions.setdefault(field_name, _ocr_line_center_y(item))
            break

    ordered_titles = sorted(
        ((center_y, field_name) for field_name, center_y in title_positions.items()),
        key=lambda item: item[0],
    )
    regions = {}
    for index, (center_y, field_name) in enumerate(ordered_titles):
        previous_y = ordered_titles[index - 1][0] if index else 0
        next_y = (
            ordered_titles[index + 1][0]
            if index + 1 < len(ordered_titles)
            else image_height
        )
        start_y = 0 if index == 0 else int((previous_y + center_y) / 2)
        end_y = (
            image_height
            if index + 1 == len(ordered_titles)
            else int((center_y + next_y) / 2)
        )
        regions[field_name] = (start_y, end_y)
    return regions


def select_note_right_edge_value(ocr_result, crop_width):
    candidates = []
    for item in ocr_result:
        text = _ocr_line_text(item)
        if not re.fullmatch(r"-?\d{1,3}(?:\.\d+)?", text):
            continue
        value = float(text)
        if value < 0 or value > 100:
            continue
        box = item[0] if item else []
        points = [point for point in box if isinstance(point, (list, tuple)) and len(point) >= 2]
        if not points:
            continue
        right_x = max(float(point[0]) for point in points)
        if right_x < crop_width * NOTE_VALUE_MIN_X_RATIO:
            continue
        candidates.append((right_x, value))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def extract_note_emotions_from_image(image):
    image_height, image_width = image.shape[:2]
    full_ocr_result = _ocr_image(image)
    chart_regions = extract_note_chart_regions(full_ocr_result, image_height)
    values = {}
    if not chart_regions:
        return values, [_ocr_line_text(item) for item in full_ocr_result if _ocr_line_text(item)]

    crop_start = int(image_width * NOTE_VALUE_CROP_START_RATIO)
    for field_name, (start_y, end_y) in chart_regions.items():
        crop = image[start_y:end_y, crop_start:image_width]
        enlarged = cv2.resize(crop, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
        crop_ocr_result = _ocr_image(enlarged)
        value = select_note_right_edge_value(crop_ocr_result, enlarged.shape[1])
        if value is not None:
            values[field_name] = value

    raw_lines = [_ocr_line_text(item) for item in full_ocr_result if _ocr_line_text(item)]
    return values, raw_lines


async def request_note_ocr_transcript(page, note_url):
    await page.goto(note_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    image_urls = []
    stable_rounds = 0
    for attempt in range(10):
        await page.wait_for_timeout(1000)
        try:
            current_urls = await page.locator("img").evaluate_all(
                """
                (elements, marker) => [...new Set(
                    elements
                        .map(element => element.currentSrc || element.src || '')
                        .filter(url => url.includes(marker))
                )]
                """,
                NOTE_IMAGE_URL_MARKER,
            )
        except Exception:
            stable_rounds = 0
            continue
        if current_urls == image_urls and image_urls:
            stable_rounds += 1
        else:
            image_urls = current_urls
            stable_rounds = 0
        # Douyin loads the last carousel image lazily. Do not start OCR as soon
        # as the first image appears; wait for the image list to settle.
        if attempt >= 5 and stable_rounds >= 2:
            break
    if not image_urls:
        raise RuntimeError("抖音图文页未找到作品图片")

    values = {}
    raw_sections = []
    for image_index, image_url in enumerate(image_urls, start=1):
        response = await page.request.get(image_url)
        if not response.ok:
            raise RuntimeError(f"抖音图文第 {image_index} 张图片下载失败：HTTP {response.status}")
        image_bytes = await response.body()
        image = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            raise RuntimeError(f"抖音图文第 {image_index} 张图片无法解码")
        image_values, raw_lines = await asyncio.to_thread(extract_note_emotions_from_image, image)
        values.update(image_values)
        if raw_lines:
            raw_sections.append(f"[图片{image_index}]\n" + "\n".join(raw_lines))

    missing_fields = [field_name for field_name in INDEX_SPECS if field_name not in values]
    if missing_fields:
        missing_labels = "、".join(INDEX_SPECS[field_name][0] for field_name in missing_fields)
        raise ValueError(f"图文 OCR 缺少情绪指标：{missing_labels}")

    normalized_lines = [
        f"{INDEX_SPECS[field_name][0]}情绪指标：{values[field_name]:g}"
        for field_name in INDEX_SPECS
    ]
    return "\n".join(normalized_lines + ["", *raw_sections]), values


def select_latest_video(video_cards, target_date=None):
    valid_cards = [
        card
        for card in video_cards
        if isinstance(card.get("published_at"), datetime)
    ]
    if not valid_cards:
        raise ValueError("无法确定抖音视频发布时间")

    latest = max(valid_cards, key=lambda item: item["published_at"])
    if target_date is None:
        return latest, latest

    target_cards = [
        card
        for card in valid_cards
        if card["published_at"].astimezone(SHANGHAI_TZ).date() == target_date
    ]
    if not target_cards:
        return None, latest
    return max(target_cards, key=lambda item: item["published_at"]), latest


def video_card_from_existing_record(record):
    if not record:
        return None
    video_id = str(record.get("video_id") or "").strip()
    raw_date = record.get("emotion_date")
    if not video_id or not raw_date:
        return None
    emotion_date = raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date))
    return {
        "video_id": video_id,
        "video_url": str(record.get("video_url") or "").strip()
        or f"https://www.douyin.com/video/{video_id}",
        "content_type": (
            parse_douyin_content_url(record.get("video_url") or "") or {}
        ).get("content_type", "video"),
        "video_title": clean_text(record.get("video_title")) or None,
        "published_at": datetime.combine(
            emotion_date,
            datetime.min.time(),
            tzinfo=SHANGHAI_TZ,
        ),
    }


def load_cached_video_card(target_date):
    if not VIDEO_INDEX_PATH.exists():
        return None
    try:
        payload = json.loads(VIDEO_INDEX_PATH.read_text(encoding="utf-8"))
        record = payload.get(target_date.isoformat()) if isinstance(payload, dict) else None
        if not isinstance(record, dict):
            return None
        return video_card_from_existing_record(
            {
                **record,
                "emotion_date": target_date.isoformat(),
            }
        )
    except (OSError, ValueError, TypeError):
        return None


def cache_video_cards(video_cards):
    payload = {}
    if VIDEO_INDEX_PATH.exists():
        try:
            existing = json.loads(VIDEO_INDEX_PATH.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                payload.update(existing)
        except (OSError, ValueError, TypeError):
            pass
    for card in video_cards:
        published_at = card.get("published_at")
        if not isinstance(published_at, datetime):
            continue
        card_date = published_at.astimezone(SHANGHAI_TZ).date().isoformat()
        current = payload.get(card_date)
        current_id = str(current.get("video_id") or "") if isinstance(current, dict) else ""
        if current_id and int(current_id) >= int(card["video_id"]):
            continue
        payload[card_date] = {
            "video_id": card["video_id"],
            "video_url": card["video_url"],
            "content_type": card.get("content_type", "video"),
            "video_title": card.get("video_title"),
        }
    VIDEO_INDEX_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


async def _existing_video_card_for_date(target_date):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        record = await db_tools.get_douyin_emotion_by_date(target_date.isoformat())
        return video_card_from_existing_record(record)
    finally:
        await db_tools.close()


def extract_labeled_transcript_date(transcript):
    text = clean_text(transcript)
    pattern = (
        r"(?:日期|时间|今天|今日)\s*(?:是|为|[:：=])?\s*"
        r"(\d{4}\D{0,3}\d{1,2}\D{0,3}\d{1,2})"
    )
    match = re.search(pattern, text)
    return normalize_date_text(match.group(1)) if match else ""


def parse_number_token(raw_value):
    text = clean_text(raw_value)
    try:
        return float(text)
    except ValueError:
        pass

    negative = text.startswith("负")
    if negative:
        text = text[1:]
    if not text:
        raise ValueError(f"无法解析数值：{raw_value}")

    digit_map = {
        "零": 0,
        "〇": 0,
        "一": 1,
        "二": 2,
        "两": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
    }
    integer_text, dot, fraction_text = text.partition("点")
    if any(unit in integer_text for unit in ("十", "百")):
        integer_value = 0
        pending_digit = 0
        for char in integer_text:
            if char in digit_map:
                pending_digit = digit_map[char]
            elif char == "百":
                integer_value += (pending_digit or 1) * 100
                pending_digit = 0
            elif char == "十":
                integer_value += (pending_digit or 1) * 10
                pending_digit = 0
            else:
                raise ValueError(f"无法解析数值：{raw_value}")
        integer_value += pending_digit
    else:
        if not integer_text or any(char not in digit_map for char in integer_text):
            raise ValueError(f"无法解析数值：{raw_value}")
        integer_value = int("".join(str(digit_map[char]) for char in integer_text))

    fraction_value = 0.0
    if dot:
        if not fraction_text or any(char not in digit_map for char in fraction_text):
            raise ValueError(f"无法解析数值：{raw_value}")
        fraction_digits = "".join(str(digit_map[char]) for char in fraction_text)
        fraction_value = int(fraction_digits) / (10 ** len(fraction_digits))

    value = integer_value + fraction_value
    return -value if negative else value


def _validate_metric_value(value, label):
    if value < 0 or value > 100:
        raise ValueError(f"{label}情绪指标超出 0~100：{value}")
    return value


def _extract_pair_metrics(text, first_aliases, second_aliases, first_label, second_label):
    first_pattern = "|".join(re.escape(alias) for alias in first_aliases)
    second_pattern = "|".join(re.escape(alias) for alias in second_aliases)
    pattern = (
        rf"(?:{first_pattern}).{{0,30}}?(?:{second_pattern}).{{0,50}}?"
        rf"(?:分别|依次)\s*(?:是|为|[:：=])?\s*"
        rf"({NUMBER_TOKEN_PATTERN})\s*(?:和|与|、|以及|,|，)\s*"
        rf"({NUMBER_TOKEN_PATTERN})"
    )
    matches = re.findall(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    parsed_pairs = {
        (
            _validate_metric_value(parse_number_token(first_value), first_label),
            _validate_metric_value(parse_number_token(second_value), second_label),
        )
        for first_value, second_value in matches
    }
    same_value_pattern = (
        rf"(?:{first_pattern})[^，。；\n]{{0,30}}?(?:{second_pattern})"
        rf"[^，。；\n]{{0,50}}?"
        rf"(?:的)?(?:情绪|恐贪|恐慌|孔摊)?(?:指标|值|数值)?\s*"
        rf"(?:都是|均为|均是)\s*({NUMBER_TOKEN_PATTERN})"
    )
    for raw_value in re.findall(
        same_value_pattern,
        text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        value = _validate_metric_value(parse_number_token(raw_value), first_label)
        parsed_pairs.add((value, value))
    if not parsed_pairs:
        return None
    if len(parsed_pairs) > 1:
        raise ValueError(f"文案中{first_label}/{second_label}情绪指标存在歧义：{sorted(parsed_pairs)}")
    return next(iter(parsed_pairs))


def _extract_unique_metric(text, aliases, label):
    alias_pattern = "|".join(re.escape(alias) for alias in aliases)
    pattern = (
        rf"(?:{alias_pattern})(?:指数)?(?:的)?\s*"
        rf"(?:(?:今日|当日)?(?:情绪|恐贪|孔摊)(?:指标|值|数值)?"
        rf"\s*(?:为|是|达|[:：=])?\s*|数值\s*(?:为|是|达|[:：=])?\s*|[:：=]\s*)"
        rf"({NUMBER_TOKEN_PATTERN})"
    )
    values = []
    for raw_value in re.findall(pattern, text, flags=re.IGNORECASE):
        values.append(_validate_metric_value(parse_number_token(raw_value), label))

    distinct_values = sorted(set(values))
    if not distinct_values:
        raise ValueError(f"文案缺少{label}情绪指标")
    if len(distinct_values) > 1:
        raise ValueError(f"文案中{label}情绪指标存在歧义：{distinct_values}")
    return distinct_values[0]


def parse_transcript_emotions(transcript):
    text = clean_text(transcript)
    text = re.sub(
        r"(上证|沪深|中证)\s+(50|300|500|1000)",
        r"\1\2",
        text,
    )
    if not text:
        raise ValueError("Coze 未返回可解析文案")

    result = {}
    sz50_hs300 = _extract_pair_metrics(
        text,
        INDEX_SPECS["sz50_emotion"],
        INDEX_SPECS["hs300_emotion"],
        "上证50",
        "沪深300",
    )
    if sz50_hs300 is not None:
        result["sz50_emotion"], result["hs300_emotion"] = sz50_hs300

    zz500_zz1000 = _extract_pair_metrics(
        text,
        INDEX_SPECS["zz500_emotion"],
        INDEX_SPECS["zz1000_emotion"],
        "中证500",
        "中证1000",
    )
    if zz500_zz1000 is not None:
        result["zz500_emotion"], result["zz1000_emotion"] = zz500_zz1000

    for field_name, aliases in INDEX_SPECS.items():
        if field_name in result:
            continue
        result[field_name] = _extract_unique_metric(text, aliases, aliases[0])
    return result


def build_extraction_status(values):
    matched = sum(values.get(field_name) is not None for field_name in INDEX_SPECS)
    if matched == len(INDEX_SPECS):
        return "SUCCESS"
    if matched:
        return "PARTIAL"
    return "FAILED"


def has_douyin_session_cookie(cookies):
    return any(
        str(cookie.get("name") or "").strip().lower() in DOUYIN_SESSION_COOKIE_NAMES
        and bool(cookie.get("value"))
        for cookie in cookies or []
    )


@contextmanager
def single_instance_lock():
    lock_file = open(LOCK_PATH, "a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("抖音情绪采集已有实例正在运行") from exc
        yield
    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()


async def launch_browser_context(playwright, *, headless):
    context = await playwright.chromium.launch_persistent_context(
        str(USER_DATA_DIR),
        headless=headless,
        viewport={"width": 1440, "height": 960},
        args=[
            "--autoplay-policy=no-user-gesture-required",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    if STORAGE_STATE_PATH.exists():
        try:
            storage_state = json.loads(STORAGE_STATE_PATH.read_text(encoding="utf-8"))
            cookies = storage_state.get("cookies") if isinstance(storage_state, dict) else None
            if isinstance(cookies, list) and cookies:
                await context.add_cookies(cookies)
        except (OSError, ValueError, TypeError):
            pass
    return context


async def persist_browser_storage_state(context):
    await context.storage_state(path=str(STORAGE_STATE_PATH))
    os.chmod(STORAGE_STATE_PATH, 0o600)


async def _visible_exact_button(page, name):
    locator = page.get_by_role("button", name=name, exact=True)
    count = await locator.count()
    for index in range(count):
        try:
            if await locator.nth(index).is_visible():
                return locator.nth(index)
        except Exception:
            continue
    return None


async def is_douyin_authenticated(page):
    try:
        if "/login" in page.url:
            return False
        login_button = await _visible_exact_button(page, "登录")
        if login_button is not None:
            return False
        cookies = await page.context.cookies([ACCOUNT_URL])
        return has_douyin_session_cookie(cookies)
    except Exception:
        return False


async def _find_visible_chat_input(page):
    selectors = [
        "textarea[placeholder*='输入']",
        "textarea[placeholder*='发送']",
        "textarea",
        "[contenteditable='true']",
    ]
    for selector in selectors:
        locator = page.locator(selector)
        count = await locator.count()
        for index in range(count):
            candidate = locator.nth(index)
            try:
                if await candidate.is_visible() and await candidate.is_enabled():
                    return candidate, selector
            except Exception:
                continue
    return None, ""


async def is_coze_authenticated(page):
    try:
        if "/sign" in page.url or "/login" in page.url:
            return False
        body_text = clean_text(await page.locator("body").inner_text())
        if "立即注册扣子" in body_text:
            return False
        chat_input, _ = await _find_visible_chat_input(page)
        if chat_input is not None:
            return True
        cookies = await page.context.cookies([COZE_AGENT_URL])
        return any(
            cookie.get("value")
            and any(token in str(cookie.get("name", "")).lower() for token in ("session", "passport", "token"))
            for cookie in cookies
        )
    except Exception:
        return False


async def initialize_login(timeout_seconds=900):
    async with async_playwright() as playwright:
        context = await launch_browser_context(playwright, headless=False)
        douyin_page = await context.new_page()
        coze_page = await context.new_page()
        try:
            await douyin_page.goto(ACCOUNT_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            await coze_page.goto(COZE_AGENT_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            print("已打开抖音和 Coze，请在两个页面完成登录。脚本会自动检测登录状态。")

            deadline = asyncio.get_running_loop().time() + timeout_seconds
            last_status = None
            while asyncio.get_running_loop().time() < deadline:
                douyin_ready = await is_douyin_authenticated(douyin_page)
                coze_ready = await is_coze_authenticated(coze_page)
                status = (douyin_ready, coze_ready)
                if status != last_status:
                    print(f"登录状态：抖音={'已登录' if douyin_ready else '待登录'}，Coze={'已登录' if coze_ready else '待登录'}")
                    last_status = status
                if douyin_ready and coze_ready:
                    await persist_browser_storage_state(context)
                    LOGIN_STATE_PATH.write_text(
                        json.dumps(
                            {
                                "ready": True,
                                "verified_at": datetime.now(SHANGHAI_TZ).isoformat(),
                                "account_url": ACCOUNT_URL,
                                "coze_agent_url": COZE_AGENT_URL,
                            },
                            ensure_ascii=False,
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                    os.chmod(LOGIN_STATE_PATH, 0o600)
                    print("抖音和 Coze 登录已确认，持久化会话已保存。")
                    return {"status": "SUCCESS", "verified_at": datetime.now(SHANGHAI_TZ).isoformat()}
                await asyncio.sleep(2)
        finally:
            await context.close()

    raise TimeoutError("等待抖音和 Coze 登录超时")


async def _open_douyin_account(page):
    await page.goto(ACCOUNT_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    await page.wait_for_timeout(3000)
    if not await is_douyin_authenticated(page):
        raise AuthenticationRequiredError("抖音登录已失效，请运行 python run.py douyin login")


async def collect_video_cards(page, *, max_cards=MAX_VIDEO_CARDS, stop_before_date=None):
    video_cards = {}
    stable_rounds = 0
    last_count = 0
    max_scroll_rounds = 60 if stop_before_date else 10

    for _ in range(max_scroll_rounds):
        raw_cards = await page.locator(
            "a[href*='/video/'], a[href*='/note/']"
        ).evaluate_all(
            """
            elements => elements.map(element => ({
                href: element.href || element.getAttribute('href') || '',
                title: (
                    element.getAttribute('aria-label')
                    || element.getAttribute('title')
                    || element.innerText
                    || ''
                ).trim()
            }))
            """
        )
        for raw_card in raw_cards:
            href = str(raw_card.get("href") or "").strip()
            if "source=baiduspider" in href.lower():
                continue
            content = parse_douyin_content_url(href)
            if content is None:
                continue
            video_id = content["content_id"]
            video_cards[video_id] = {
                "video_id": video_id,
                "video_url": content["content_url"],
                "content_type": content["content_type"],
                "video_title": clean_text(raw_card.get("title")) or None,
                "published_at": decode_video_published_at(video_id),
            }

        valid_publish_dates = [
            card["published_at"].astimezone(SHANGHAI_TZ).date()
            for card in video_cards.values()
            if isinstance(card.get("published_at"), datetime)
        ]
        if stop_before_date and valid_publish_dates:
            if stop_before_date in valid_publish_dates:
                break
            older_card_count = sum(
                publish_date < stop_before_date
                for publish_date in valid_publish_dates
            )
            # A few old pinned videos can appear before the chronological feed.
            if older_card_count >= 4:
                break
        if len(video_cards) >= max_cards:
            break
        if len(video_cards) == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = len(video_cards)
        if stop_before_date is None and stable_rounds >= 3 and video_cards:
            break
        await page.evaluate("window.scrollBy(0, Math.max(window.innerHeight * 1.5, 1200))")
        await page.wait_for_timeout(1200)

    if not video_cards:
        raise RuntimeError("未在抖音主页找到视频或图文作品，请检查登录状态或页面结构")
    return list(video_cards.values())


async def _open_coze_chat(page):
    await page.goto(COZE_AGENT_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    await page.wait_for_timeout(2500)
    if not await is_coze_authenticated(page):
        raise AuthenticationRequiredError("Coze 登录已失效，请运行 python run.py douyin login")

    for _ in range(10):
        chat_input, selector = await _find_visible_chat_input(page)
        if chat_input is not None:
            return chat_input, selector
        await page.wait_for_timeout(1500)

    start_button = await _visible_exact_button(page, "开始使用")
    if start_button is not None:
        await start_button.click()
        await page.wait_for_timeout(2000)
        if "/sign" in page.url or "/login" in page.url:
            raise AuthenticationRequiredError("Coze 登录已失效，请运行 python run.py douyin login")

    for _ in range(10):
        chat_input, selector = await _find_visible_chat_input(page)
        if chat_input is not None:
            return chat_input, selector
        await page.wait_for_timeout(1500)
    raise RuntimeError("无法定位 Coze 对话输入框，页面结构可能已变化")


async def _body_lines(page):
    body_text = await page.locator("body").inner_text()
    return [clean_text(line) for line in str(body_text or "").splitlines() if clean_text(line)]


def _new_lines(before_lines, after_lines, prompt):
    remaining = Counter(before_lines)
    result = []
    for line in after_lines:
        if remaining[line] > 0:
            remaining[line] -= 1
            continue
        if line == prompt or line in {"发送", "停止生成", "停止响应", "重新生成"}:
            continue
        result.append(line)
    return result


def coze_response_is_pending(lines):
    pending_markers = ("正在调用", "正在生成", "停止响应")
    return any(
        any(marker in line for marker in pending_markers)
        for line in lines
    )


def clean_coze_response_lines(lines):
    ignored_markers = ("正在调用", "正在生成", "停止响应")
    ignored_lines = {"wenan_tiqu"}
    return [
        line
        for line in lines
        if line not in ignored_lines
        and not any(marker in line for marker in ignored_markers)
    ]


def extract_existing_coze_transcript(body_text, video_url):
    text = str(body_text or "")
    marker_index = text.rfind(str(video_url or ""))
    if marker_index < 0:
        return ""
    candidate = text[marker_index + len(str(video_url)) :].strip()
    for marker in (
        "\n单视频文案提取",
        "\n批量视频文案提取",
        "\n内容由AI生成",
    ):
        marker_offset = candidate.find(marker)
        if marker_offset >= 0:
            candidate = candidate[:marker_offset].strip()
    if candidate.startswith("短视频提取文案"):
        candidate = candidate[len("短视频提取文案") :].strip()
    return candidate


def extract_douyin_short_url(payload):
    if not isinstance(payload, dict) or payload.get("code") != 0:
        return ""
    short_url = str(payload.get("data") or "").strip()
    return short_url if short_url.startswith("https://v.douyin.com/") else ""


async def resolve_douyin_share_url(page, video_url):
    for attempt in range(2):
        try:
            await page.goto(video_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            await page.wait_for_timeout(8000)
            share_icon = page.locator("[data-e2e='video-share-icon-container']")
            if await share_icon.count() != 1 or not await share_icon.is_visible():
                continue
            async with page.expect_response(
                lambda response: "/aweme/v1/web/web_shorten/" in response.url,
                timeout=15000,
            ) as response_info:
                await share_icon.hover(force=True)
            response = await response_info.value
            short_url = extract_douyin_short_url(await response.json())
            if short_url:
                return short_url
        except Exception:
            if attempt == 0:
                await page.wait_for_timeout(1000)
    return video_url


async def request_coze_transcript(page, video_url):
    chat_input, selector = await _open_coze_chat(page)
    existing_transcript = extract_existing_coze_transcript(
        await page.locator("body").inner_text(),
        video_url,
    )
    if existing_transcript:
        try:
            parse_transcript_emotions(existing_transcript)
            return existing_transcript
        except ValueError:
            pass

    clear_context_button = page.get_by_test_id("chat-input-clear-context-button")
    if await clear_context_button.count() == 1 and await clear_context_button.is_visible():
        await clear_context_button.click()
        await page.wait_for_timeout(1000)
        chat_input, selector = await _find_visible_chat_input(page)
        if chat_input is None:
            raise RuntimeError("Coze 新对话已打开，但无法定位输入框")

    before_lines = await _body_lines(page)
    prompt = video_url
    before_prompt_count = "\n".join(before_lines).count(prompt)

    await chat_input.click()
    if selector == "[contenteditable='true']":
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
        await page.keyboard.insert_text(prompt)
    else:
        await chat_input.fill(prompt)

    send_button = page.get_by_test_id("bot-home-chart-send-button")
    if await send_button.count() == 1 and await send_button.is_enabled():
        await send_button.click()
    else:
        await chat_input.press("Enter")

    submitted = False
    for _ in range(10):
        await page.wait_for_timeout(500)
        current_body = await page.locator("body").inner_text()
        if current_body.count(prompt) > before_prompt_count:
            submitted = True
            break
    if not submitted:
        raise RuntimeError("Coze 视频链接未成功提交")

    stable_rounds = 0
    previous_response = ""
    deadline = asyncio.get_running_loop().time() + COZE_RESPONSE_TIMEOUT_SECONDS
    while asyncio.get_running_loop().time() < deadline:
        await page.wait_for_timeout(2000)
        after_lines = await _body_lines(page)
        response_lines = clean_coze_response_lines(
            _new_lines(before_lines, after_lines, prompt)
        )
        response_text = "\n".join(response_lines).strip()
        if "HTTP 504" in response_text or "Gateway Timeout" in response_text:
            raise RuntimeError("Coze 提取视频文案失败：HTTP 504 Gateway Timeout")
        if "请求超时" in response_text and "文案" in response_text:
            raise RuntimeError("Coze 提取视频文案失败：请求超时")
        if len(response_text) < 80:
            stable_rounds = 0
            continue
        if response_text == previous_response:
            stable_rounds += 1
        else:
            stable_rounds = 0
            previous_response = response_text
        if stable_rounds >= 3:
            return response_text

    raise TimeoutError("等待 Coze 视频文案超时")


def _build_raw_row(video_card, values, transcript, status):
    return {
        "emotion_date": video_card["published_at"].astimezone(SHANGHAI_TZ).date().isoformat(),
        "video_id": video_card["video_id"],
        "account_id": ACCOUNT_ID,
        "account_name": ACCOUNT_NAME,
        "video_title": video_card.get("video_title"),
        "video_url": video_card["video_url"],
        "hs300_emotion": values.get("hs300_emotion"),
        "zz500_emotion": values.get("zz500_emotion"),
        "zz1000_emotion": values.get("zz1000_emotion"),
        "sz50_emotion": values.get("sz50_emotion"),
        "raw_ocr_text": str(transcript or "")[:60000],
        "extraction_method": (
            "note_rapidocr+local_parser"
            if video_card.get("content_type", "video") == "note"
            else "coze_transcript+local_parser"
        ),
        "extraction_status": status,
    }


async def _persist_success(video_card, values, transcript):
    emotion_date = video_card["published_at"].astimezone(SHANGHAI_TZ).date().isoformat()
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        await db_tools.upsert_douyin_emotion_daily(
            [_build_raw_row(video_card, values, transcript, "SUCCESS")]
        )
        normalized = await db_tools.batch_douyin_emotion_to_excel(
            emotion_date,
            video_card["video_id"],
            values,
        )
        if normalized["available_rows"] != 4:
            raise RuntimeError(
                f"正式情绪表仅有 {normalized['available_rows']} 个指数，要求 4 个"
            )
    finally:
        await db_tools.close()

    dashboard_rows = await quant_index.sync_daily(target_date=emotion_date)
    return normalized, dashboard_rows


async def _persist_failed(video_card, values, transcript, error):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        raw_text = f"{transcript or ''}\n\nERROR:\n{error}".strip()
        await db_tools.upsert_douyin_emotion_daily(
            [_build_raw_row(video_card, values, raw_text, build_extraction_status(values))]
        )
    finally:
        await db_tools.close()


async def _already_processed(video_card):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        existing = await db_tools.get_douyin_emotion_by_video_id(video_card["video_id"])
        if not existing or str(existing.get("extraction_status") or "").upper() != "SUCCESS":
            return None
        values = {field_name: existing.get(field_name) for field_name in INDEX_SPECS}
        if any(value is None for value in values.values()):
            return None
        normalized = await db_tools.batch_douyin_emotion_to_excel(
            video_card["published_at"].astimezone(SHANGHAI_TZ).date().isoformat(),
            video_card["video_id"],
            values,
        )
        return values, normalized
    finally:
        await db_tools.close()


async def _recover_existing_transcript(video_card):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        existing = await db_tools.get_douyin_emotion_by_video_id(video_card["video_id"])
    finally:
        await db_tools.close()
    transcript = str((existing or {}).get("raw_ocr_text") or "").strip()
    if not transcript:
        return None
    try:
        return transcript, parse_transcript_emotions(transcript)
    except ValueError:
        return None


async def _process_video_card(coze_page, selected, douyin_page=None):
    emotion_date = selected["published_at"].astimezone(SHANGHAI_TZ).date().isoformat()
    existing = await _already_processed(selected)
    if existing is not None:
        values, normalized = existing
        dashboard_rows = await quant_index.sync_daily(target_date=emotion_date)
        return {
            "status": "ALREADY_PROCESSED",
            "target_date": emotion_date,
            "video_id": selected["video_id"],
            "video_url": selected["video_url"],
            "values": values,
            "normalized": normalized,
            "dashboard_rows": dashboard_rows,
        }

    recovered = await _recover_existing_transcript(selected)
    if recovered is not None:
        transcript, values = recovered
        normalized, dashboard_rows = await _persist_success(selected, values, transcript)
        return {
            "status": "RECOVERED",
            "target_date": emotion_date,
            "video_id": selected["video_id"],
            "video_url": selected["video_url"],
            "values": values,
            "normalized": normalized,
            "dashboard_rows": dashboard_rows,
        }

    transcript = ""
    values = {}
    try:
        content_type = selected.get("content_type", "video")
        if content_type == "note":
            if douyin_page is None:
                raise RuntimeError("图文作品处理缺少抖音页面")
            transcript, values = await request_note_ocr_transcript(
                douyin_page,
                selected["video_url"],
            )
        else:
            coze_video_url = selected["video_url"]
            if douyin_page is not None:
                coze_video_url = await resolve_douyin_share_url(
                    douyin_page,
                    selected["video_url"],
                )
            transcript = await request_coze_transcript(coze_page, coze_video_url)
        transcript_date = extract_labeled_transcript_date(transcript)
        if transcript_date and transcript_date != emotion_date:
            raise ValueError(
                f"文案日期 {transcript_date} 与作品发布日期 {emotion_date} 不一致"
            )
        if content_type != "note":
            values = parse_transcript_emotions(transcript)
        normalized, dashboard_rows = await _persist_success(selected, values, transcript)
    except Exception as exc:
        await _persist_failed(selected, values, transcript, str(exc))
        raise

    return {
        "status": "SUCCESS",
        "target_date": emotion_date,
        "video_id": selected["video_id"],
        "video_url": selected["video_url"],
        "values": values,
        "normalized": normalized,
        "dashboard_rows": dashboard_rows,
    }


async def run_pipeline(target_date=None):
    if not LOGIN_STATE_PATH.exists() or not STORAGE_STATE_PATH.exists():
        raise AuthenticationRequiredError("尚未初始化抖音和 Coze 登录，请运行 python run.py douyin login")

    with single_instance_lock():
        async with async_playwright() as playwright:
            context = await launch_browser_context(
                playwright,
                headless=os.getenv("DOUYIN_COZE_HEADLESS", "0").strip() == "1",
            )
            douyin_page = await context.new_page()
            coze_page = await context.new_page()
            try:
                await _open_douyin_account(douyin_page)
                await persist_browser_storage_state(context)
                selected = (
                    await _existing_video_card_for_date(target_date)
                    if target_date is not None
                    else None
                )
                if selected is None and target_date is not None:
                    selected = load_cached_video_card(target_date)
                if selected is not None:
                    latest = selected
                else:
                    today = datetime.now(SHANGHAI_TZ).date()
                    is_historical_run = target_date is not None and target_date < today
                    video_cards = await collect_video_cards(
                        douyin_page,
                        max_cards=100 if is_historical_run else MAX_VIDEO_CARDS,
                        stop_before_date=target_date if is_historical_run else None,
                    )
                    cache_video_cards(video_cards)
                    selected, latest = select_latest_video(video_cards, target_date=target_date)
                latest_date = latest["published_at"].astimezone(SHANGHAI_TZ).date().isoformat()
                if selected is None:
                    return {
                        "status": "NO_UPDATE",
                        "target_date": target_date.isoformat(),
                        "latest_video_date": latest_date,
                        "latest_video_id": latest["video_id"],
                    }
                result = await _process_video_card(coze_page, selected, douyin_page=douyin_page)
                await persist_browser_storage_state(context)
                return result
            finally:
                await context.close()


async def sync_daily(target_date=None):
    normalized_target_date = (
        date.fromisoformat(str(target_date))
        if target_date
        else datetime.now(SHANGHAI_TZ).date()
    )
    return await run_pipeline(target_date=normalized_target_date)


async def backfill_history(start_date=None, end_date=None):
    if not LOGIN_STATE_PATH.exists() or not STORAGE_STATE_PATH.exists():
        raise AuthenticationRequiredError("尚未初始化抖音和 Coze 登录，请运行 python run.py douyin login")

    today = datetime.now(SHANGHAI_TZ).date()
    normalized_start = date.fromisoformat(str(start_date)) if start_date else today.replace(day=1)
    normalized_end = date.fromisoformat(str(end_date)) if end_date else today
    if normalized_start > normalized_end:
        raise ValueError("backfill start_date must not be later than end_date")

    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        trading_dates = set(
            await db_tools.get_quant_index_dashboard_trade_dates(
                ["上证50"],
                start_date=normalized_start.isoformat(),
                end_date=normalized_end.isoformat(),
            )
        )
        complete_dates = set(
            await db_tools.get_complete_excel_emotion_dates(
                normalized_start.isoformat(),
                normalized_end.isoformat(),
            )
        )
    finally:
        await db_tools.close()

    missing_dates = sorted(trading_dates - complete_dates)
    if not missing_dates:
        return {
            "status": "SUCCESS",
            "start_date": normalized_start.isoformat(),
            "end_date": normalized_end.isoformat(),
            "processed": [],
            "missing_video_dates": [],
            "failed": [],
            "message": "目标区间四大指数情绪数据已完整",
        }

    with single_instance_lock():
        async with async_playwright() as playwright:
            context = await launch_browser_context(
                playwright,
                headless=os.getenv("DOUYIN_COZE_HEADLESS", "0").strip() == "1",
            )
            douyin_page = await context.new_page()
            coze_page = await context.new_page()
            try:
                await _open_douyin_account(douyin_page)
                await persist_browser_storage_state(context)
                video_cards = await collect_video_cards(
                    douyin_page,
                    max_cards=100,
                    stop_before_date=normalized_start,
                )
                cards_by_date = {}
                for card in video_cards:
                    published_at = card.get("published_at")
                    if not isinstance(published_at, datetime):
                        continue
                    card_date = published_at.astimezone(SHANGHAI_TZ).date().isoformat()
                    current = cards_by_date.get(card_date)
                    if current is None or card["published_at"] > current["published_at"]:
                        cards_by_date[card_date] = card

                processed = []
                missing_video_dates = []
                failed = []
                for trade_date in missing_dates:
                    card = cards_by_date.get(trade_date)
                    if card is None:
                        missing_video_dates.append(trade_date)
                        print(f"douyin backfill {trade_date}: no video found")
                        continue
                    print(f"douyin backfill {trade_date}: processing video {card['video_id']}")
                    try:
                        result = await _process_video_card(
                            coze_page,
                            card,
                            douyin_page=douyin_page,
                        )
                        processed.append(result)
                        print(
                            f"douyin backfill {trade_date}: {result['status']} "
                            f"SZ50={result['values'].get('sz50_emotion')} "
                            f"HS300={result['values'].get('hs300_emotion')} "
                            f"ZZ500={result['values'].get('zz500_emotion')} "
                            f"ZZ1000={result['values'].get('zz1000_emotion')}"
                        )
                    except Exception as exc:
                        failed.append(
                            {
                                "trade_date": trade_date,
                                "video_id": card["video_id"],
                                "error": str(exc),
                            }
                        )
                        print(f"douyin backfill {trade_date}: failed: {exc}")
                await persist_browser_storage_state(context)
            finally:
                await context.close()

    return {
        "status": "SUCCESS" if not failed and not missing_video_dates else "PARTIAL",
        "start_date": normalized_start.isoformat(),
        "end_date": normalized_end.isoformat(),
        "processed": processed,
        "missing_video_dates": missing_video_dates,
        "failed": failed,
    }


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "daily"
    if command == "login":
        await initialize_login()
        return
    if command == "daily":
        result = await sync_daily()
        print(json.dumps(result, ensure_ascii=False, default=str))
        return
    if command == "backfill":
        start_date = sys.argv[2] if len(sys.argv) > 2 else None
        end_date = sys.argv[3] if len(sys.argv) > 3 else None
        result = await backfill_history(start_date=start_date, end_date=end_date)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return
    raise ValueError("supported commands: login, daily, backfill")


if __name__ == "__main__":
    asyncio.run(main())
