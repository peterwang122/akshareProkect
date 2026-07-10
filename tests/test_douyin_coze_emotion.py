from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from akshare_project.collectors.douyin_emotion import (
    coze_response_is_pending,
    clean_coze_response_lines,
    decode_video_published_at,
    extract_existing_coze_transcript,
    extract_douyin_short_url,
    extract_note_chart_regions,
    extract_note_chart_slots,
    extract_labeled_transcript_date,
    has_douyin_session_cookie,
    parse_douyin_content_url,
    parse_transcript_emotions,
    select_note_right_edge_value,
    select_latest_video,
    video_card_from_existing_record,
)
from akshare_project.services.stock_temp_service import build_daily_routes


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def test_parse_transcript_emotions_supports_chinese_names():
    result = parse_transcript_emotions(
        """
        日期：2026-06-28
        上证50指数的情绪指标为 61.5
        沪深300情绪值：62
        中证500当日情绪指标达63.25
        中证1000：64
        """
    )

    assert result == {
        "sz50_emotion": 61.5,
        "hs300_emotion": 62.0,
        "zz500_emotion": 63.25,
        "zz1000_emotion": 64.0,
    }


def test_parse_transcript_emotions_supports_spaces_in_index_names():
    result = parse_transcript_emotions(
        "上证 50 和沪深 300 的恐慌数值分别是十七和三十四，"
        "中证 500 和中证 1000 的数值分别是三十五和三十七。"
    )

    assert result == {
        "sz50_emotion": 17,
        "hs300_emotion": 34,
        "zz500_emotion": 35,
        "zz1000_emotion": 37,
    }


def test_parse_transcript_emotions_supports_equal_pair_value():
    result = parse_transcript_emotions(
        "上证50和沪深300的恐慌数值分别是67和68，"
        "中证500和中证1000的数值都是79。"
    )

    assert result == {
        "sz50_emotion": 67,
        "hs300_emotion": 68,
        "zz500_emotion": 79,
        "zz1000_emotion": 79,
    }


def test_parse_transcript_emotions_supports_short_names():
    result = parse_transcript_emotions(
        "SZ50=51 HS300:52 ZZ500情绪指标53 ZZ1000情绪值为54"
    )

    assert result["sz50_emotion"] == 51
    assert result["hs300_emotion"] == 52
    assert result["zz500_emotion"] == 53
    assert result["zz1000_emotion"] == 54


def test_parse_transcript_emotions_supports_chinese_number_pairs_from_real_transcript():
    result = parse_transcript_emotions(
        "最后再看一下情绪指标，今天上证五零和沪深三百的孔摊数值分别是二十和四十五，"
        "中证五百和中证一千的数值分别是二十八和三十九。"
    )

    assert result == {
        "sz50_emotion": 20,
        "hs300_emotion": 45,
        "zz500_emotion": 28,
        "zz1000_emotion": 39,
    }


def test_parse_transcript_emotions_rejects_missing_ambiguous_and_out_of_range():
    with pytest.raises(ValueError, match="缺少中证1000"):
        parse_transcript_emotions("上证50:51 沪深300:52 中证500:53")

    with pytest.raises(ValueError, match="存在歧义"):
        parse_transcript_emotions(
            "上证50:51 上证50:52 沪深300:53 中证500:54 中证1000:55"
        )

    with pytest.raises(ValueError, match="超出"):
        parse_transcript_emotions(
            "上证50:101 沪深300:53 中证500:54 中证1000:55"
        )


def test_extract_labeled_transcript_date_only_uses_explicit_date_context():
    assert extract_labeled_transcript_date("今日日期为2026年6月28日") == "2026-06-28"
    assert extract_labeled_transcript_date("回顾2026-06-27走势") == ""


def test_decode_video_timestamp_and_select_latest_by_publish_time():
    first_time = datetime(2026, 6, 27, 20, 0, tzinfo=SHANGHAI_TZ)
    latest_time = datetime(2026, 6, 28, 20, 30, tzinfo=SHANGHAI_TZ)
    first_id = str((int(first_time.timestamp()) << 32) + 1)
    latest_id = str((int(latest_time.timestamp()) << 32) + 2)

    assert decode_video_published_at(latest_id) == latest_time

    selected, latest = select_latest_video(
        [
            {"video_id": latest_id, "published_at": latest_time},
            {"video_id": first_id, "published_at": first_time},
        ],
        target_date=date(2026, 6, 28),
    )
    assert selected["video_id"] == latest_id
    assert latest["video_id"] == latest_id


def test_select_latest_video_returns_no_update_when_latest_is_not_target_date():
    published_at = datetime(2026, 6, 27, 20, 0, tzinfo=SHANGHAI_TZ)
    selected, latest = select_latest_video(
        [{"video_id": "1", "published_at": published_at}],
        target_date=date(2026, 6, 28),
    )

    assert selected is None
    assert latest["video_id"] == "1"


def test_select_latest_video_uses_target_date_for_historical_runs():
    historical_time = datetime(2026, 6, 2, 20, 0, tzinfo=SHANGHAI_TZ)
    latest_time = datetime(2026, 6, 28, 20, 0, tzinfo=SHANGHAI_TZ)

    selected, latest = select_latest_video(
        [
            {"video_id": "historical", "published_at": historical_time},
            {"video_id": "latest", "published_at": latest_time},
        ],
        target_date=date(2026, 6, 2),
    )

    assert selected["video_id"] == "historical"
    assert latest["video_id"] == "latest"


def test_video_card_from_existing_record_reuses_failed_video():
    card = video_card_from_existing_record(
        {
            "emotion_date": date(2026, 6, 2),
            "video_id": "7646763522840294490",
            "video_url": "",
            "video_title": "历史视频",
        }
    )

    assert card["video_id"] == "7646763522840294490"
    assert card["video_url"] == "https://www.douyin.com/video/7646763522840294490"
    assert card["published_at"].date() == date(2026, 6, 2)


def test_stock_temp_service_registers_douyin_coze_route():
    route = build_daily_routes()["/collect-douyin-coze-emotion-daily"]
    assert route.task_name == "douyin_coze_emotion_daily"
    assert route.direct_network is True


def test_coze_pending_markers_do_not_count_as_completed_response():
    assert coze_response_is_pending(["正在调用", "wenan_tiqu", "停止响应"]) is True
    assert coze_response_is_pending(["完整视频文案已经生成"]) is False
    assert clean_coze_response_lines(
        ["正在调用", "wenan_tiqu", "停止响应", "完整视频文案已经生成"]
    ) == ["完整视频文案已经生成"]


def test_extract_existing_coze_transcript_reuses_completed_answer():
    video_url = "https://www.douyin.com/video/7646763522840294490"
    transcript = extract_existing_coze_transcript(
        f"""
        {video_url}
        短视频提取文案
        上证50:20 沪深300:45 中证500:28 中证1000:39
        单视频文案提取
        批量视频文案提取
        """,
        video_url,
    )

    assert parse_transcript_emotions(transcript) == {
        "sz50_emotion": 20,
        "hs300_emotion": 45,
        "zz500_emotion": 28,
        "zz1000_emotion": 39,
    }


def test_extract_douyin_short_url_accepts_only_successful_official_url():
    assert extract_douyin_short_url(
        {"code": 0, "data": "https://v.douyin.com/5R4_oI03-wo/"}
    ) == "https://v.douyin.com/5R4_oI03-wo/"
    assert extract_douyin_short_url({"code": 1, "data": "https://v.douyin.com/bad/"}) == ""
    assert extract_douyin_short_url({"code": 0, "data": "https://example.com/video"}) == ""


def test_parse_douyin_content_url_supports_video_and_note():
    assert parse_douyin_content_url(
        "https://www.douyin.com/video/7646763522840294490?from=web"
    ) == {
        "content_type": "video",
        "content_id": "7646763522840294490",
        "content_url": "https://www.douyin.com/video/7646763522840294490?from=web",
    }
    assert parse_douyin_content_url("/note/7656789012345678901") == {
        "content_type": "note",
        "content_id": "7656789012345678901",
        "content_url": "https://www.douyin.com/note/7656789012345678901",
    }
    assert parse_douyin_content_url("https://www.douyin.com/user/example") is None


def test_douyin_login_requires_real_session_cookie():
    assert has_douyin_session_cookie(
        [{"name": "sessionid_ss", "value": "active-session"}]
    ) is True
    assert has_douyin_session_cookie(
        [
            {"name": "uid_tt", "value": "stale-identity"},
            {"name": "ttwid", "value": "visitor-cookie"},
        ]
    ) is False


def test_extract_note_chart_slots_uses_chart_title_position():
    ocr_result = [
        ([[10, 10], [200, 10], [200, 40], [10, 40]], "上证50情绪指标", 0.99),
        ([[10, 610], [240, 610], [240, 650], [10, 650]], "沪深300情绪指标", 0.99),
    ]

    assert extract_note_chart_slots(ocr_result, image_height=1000) == {
        "top": "sz50_emotion",
        "bottom": "hs300_emotion",
    }


def test_extract_note_chart_regions_supports_four_stacked_charts():
    ocr_result = [
        ([[10, 80], [200, 80], [200, 120], [10, 120]], "上证50情绪指标", 0.99),
        ([[10, 280], [240, 280], [240, 320], [10, 320]], "沪深300情绪指标", 0.99),
        ([[10, 480], [240, 480], [240, 520], [10, 520]], "中证500情绪指标", 0.99),
        ([[10, 680], [260, 680], [260, 720], [10, 720]], "中证1000情绪指标", 0.99),
    ]

    assert extract_note_chart_regions(ocr_result, image_height=800) == {
        "sz50_emotion": (0, 200),
        "hs300_emotion": (200, 400),
        "zz500_emotion": (400, 600),
        "zz1000_emotion": (600, 800),
    }


def test_extract_note_chart_regions_ignores_non_emotion_mentions():
    ocr_result = [
        ([[10, 80], [200, 80], [200, 120], [10, 120]], "沪深300", 0.99),
    ]

    assert extract_note_chart_regions(ocr_result, image_height=800) == {}


def test_select_note_right_edge_value_ignores_axis_labels():
    ocr_result = [
        ([[510, 300], [590, 300], [590, 360], [510, 360]], "73", 0.99),
        ([[470, 1080], [525, 1080], [525, 1140], [470, 1140]], "625", 0.99),
        ([[420, 1080], [465, 1080], [465, 1130], [420, 1130]], "18", 0.99),
    ]

    assert select_note_right_edge_value(ocr_result, crop_width=630) == 73
