import asyncio
import json
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from akshare_project.collectors import douyin_asr_worker as worker
from akshare_project.collectors import douyin_emotion as collector
from akshare_project.collectors import douyin_local_asr as asr
from akshare_project.db.db_tool import DbTools


VIDEO_ID = "7694264298039014619"
TRANSCRIPT = "上证50:23 沪深300:13 中证500:8 中证1000:9"


@pytest.fixture
def card():
    return {"video_id": VIDEO_ID, "video_url": f"https://www.douyin.com/video/{VIDEO_ID}",
            "published_at": datetime(2026, 10, 8, 19, 55, tzinfo=collector.SHANGHAI_TZ),
            "content_type": "video"}


@pytest.fixture
def snapshot(card):
    return {"page_url": card["video_url"], "page_title": "20261008 立伟 - 抖音",
            "body_text": "立伟 发布时间：2026-10-08 19:55",
            "detail_video_id": VIDEO_ID, "published_text": "发布时间：2026-10-08 19:55",
            "author_name": "立伟", "author_links": [collector.ACCOUNT_URL.split("?", 1)[0]],
            "players": [{"current_src": "https://v11-weba.douyinvod.com/video.mp4?private-signature=secret", "duration": 137.485}]}


def validated(snapshot, card):
    return asr.validate_player_snapshot(snapshot, card, account_url=collector.ACCOUNT_URL, account_name=collector.ACCOUNT_NAME)


def test_official_source_is_bound_to_author_id_date_type_and_duration(snapshot, card):
    source = validated(snapshot, card)
    assert source["source_date"] == "2026-10-08"
    assert source["video_id"] == VIDEO_ID
    assert source["source_host"] == "v11-weba.douyinvod.com"
    assert source["duration_seconds"] == 137.485


@pytest.mark.parametrize("mutation,match", [
    ({"page_url": "https://www.douyin.com/video/123"}, "目标视频"),
    ({"detail_video_id": "123"}, "视频 ID"),
    ({"author_links": ["https://www.douyin.com/user/other"]}, "作者"),
    ({"author_links": ["https://evil.test" + collector.ACCOUNT_URL.split("?", 1)[0].split("douyin.com", 1)[1]]}, "作者"),
    ({"author_name": "其他作者", "body_text": "推荐视频立伟"}, "作者"),
    ({"published_text": "发布时间：2026-10-07 19:55"}, "发布时间"),
    ({"published_text": "发布时间：2026-10-08 20:55"}, "发布时间"),
    ({"published_text": ""}, "发布日期"),
    ({"page_title": "20261007 立伟"}, "标题日期"),
    ({"players": []}, "播放器"),
    ({"players": [{"current_src": "https://v11-weba.douyinvod.com/a", "duration": 0}]}, "时长"),
    ({"players": [{"current_src": "https://v11-weba.douyinvod.com/a", "duration": float("inf")}]}, "时长"),
    ({"players": [{"current_src": "https://v11-weba.douyinvod.com/a", "duration": 901}]}, "时长"),
])
def test_source_snapshot_rejects_wrong_or_ambiguous_identity(snapshot, card, mutation, match):
    snapshot.update(mutation)
    with pytest.raises(ValueError, match=match):
        validated(snapshot, card)


def test_source_rejects_note_ambiguous_player_and_pre15(snapshot, card):
    card["content_type"] = "note"
    with pytest.raises(ValueError, match="目标视频"):
        validated(snapshot, card)
    card["content_type"] = "video"
    snapshot["players"] *= 2
    with pytest.raises(ValueError, match="歧义"):
        validated(snapshot, card)
    snapshot["players"] = snapshot["players"][:1]
    card["published_at"] = card["published_at"].replace(hour=14)
    snapshot["published_text"] = "发布时间：2026-10-08 14:55"
    with pytest.raises(ValueError, match="15:00"):
        validated(snapshot, card)


@pytest.mark.parametrize("url", [
    "http://v11-weba.douyinvod.com/a", "https://douyinvod.com.evil.test/a",
    "https://v11-weba.douyinvod.com.evil.test/a", "https://127.0.0.1/a",
    "https://evil.test/a", "https://user:password@v11-weba.douyinvod.com/a",
    "https://v11-weba.douyinvod.com:8443/a", "blob:https://www.douyin.com/a",
])
def test_media_source_and_redirect_allow_only_official_tls_hosts(url):
    with pytest.raises(ValueError):
        asr.validate_media_url(url)
    with pytest.raises(ValueError):
        asr._OfficialRedirects().redirect_request(None, None, 302, "", {}, url)


def test_download_is_bounded_private_and_sends_no_cookies(monkeypatch, tmp_path):
    payload = b"verified-video"
    captured = {}

    class Response:
        status = 200
        headers = {"Content-Type": "video/mp4", "Content-Length": str(len(payload))}
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def geturl(self): return "https://v11-weba.douyinvod.com/a?signature=secret"
        def read(self, _):
            if captured.get("read"): return b""
            captured["read"] = True
            return payload

    class Opener:
        def open(self, request, timeout):
            captured["headers"] = dict(request.header_items())
            captured["timeout"] = timeout
            return Response()

    monkeypatch.setattr(asr, "build_opener", lambda *_: Opener())
    output = tmp_path / "source.mp4"
    meta = asr.download_official_video("https://v11-weba.douyinvod.com/a?signature=secret", output, "https://www.douyin.com/video/123")
    assert output.read_bytes() == payload
    assert output.stat().st_mode & 0o777 == 0o600
    assert "Cookie" not in captured["headers"]
    assert meta["media_bytes"] == len(payload)
    assert "secret" not in json.dumps(meta)


@pytest.mark.parametrize("content_type,length,payload,match", [
    ("text/html", 1, b"x", "媒体类型"),
    ("video/mp4", 1, b"", "为空"),
    ("video/mp4", 4, b"x", "不完整"),
    ("video/mp4", asr.MAX_MEDIA_BYTES + 1, b"x", "大小"),
])
def test_download_rejects_wrong_media_empty_truncated_or_too_large(monkeypatch, tmp_path, content_type, length, payload, match):
    class Response:
        status = 200
        headers = {"Content-Type": content_type, "Content-Length": str(length)}
        count = 0
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def geturl(self): return "https://v11-weba.douyinvod.com/a"
        def read(self, _):
            self.count += 1
            return payload if self.count == 1 else b""
    monkeypatch.setattr(asr, "build_opener", lambda *_: SimpleNamespace(open=lambda *args, **kwargs: Response()))
    output = tmp_path / "source.mp4"
    with pytest.raises(ValueError, match=match):
        asr.download_official_video("https://v11-weba.douyinvod.com/a", output, "https://www.douyin.com/video/123")
    assert not output.exists()
    assert not output.with_suffix(".partial").exists()


def test_media_decode_refuses_missing_audio_and_mismatched_duration():
    class Media:
        streams = SimpleNamespace(audio=[])
        duration = 137_000_000
        def __enter__(self): return self
        def __exit__(self, *_): return False
    av = SimpleNamespace(open=lambda *_: Media(), time_base=1_000_000)
    with pytest.raises(ValueError, match="没有音轨"):
        worker.inspect_media("source.mp4", 137, av)
    Media.streams = SimpleNamespace(audio=[object()])
    with pytest.raises(ValueError, match="时长"):
        worker.inspect_media("source.mp4", 100, av)
    assert worker.inspect_media("source.mp4", 137, av) == 137


def test_worker_refuses_empty_transcript():
    args = SimpleNamespace()
    with pytest.raises(ValueError, match="为空"):
        worker.build_transcript_result([], SimpleNamespace(language="zh"), args, 137)


def test_asr_subprocess_timeout_kills_worker_and_keeps_cache_offline(monkeypatch, tmp_path):
    root = tmp_path / "asr"
    python = root / "venv/bin/python"
    python.parent.mkdir(parents=True)
    python.touch()
    captured = {}
    class Process:
        returncode = None
        async def communicate(self):
            if self.returncode is None:
                await asyncio.sleep(1)
            return b"", b""
        def kill(self):
            captured["killed"] = True
            self.returncode = -9
    async def create(*args, **kwargs):
        captured.update(kwargs)
        return Process()
    monkeypatch.setattr(asr, "asr_root", lambda: root)
    monkeypatch.setattr(asr.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(asr, "ASR_TIMEOUT_SECONDS", 0.001)
    source = {"source_url": "https://www.douyin.com/video/123", "source_date": "2026-10-08", "video_id": "123", "duration_seconds": 137}
    with pytest.raises(TimeoutError, match="已终止"):
        asyncio.run(asr.run_asr_worker(tmp_path / "source.mp4", tmp_path / "out.json", source))
    assert captured["killed"] is True
    assert captured["env"]["HF_HUB_OFFLINE"] == "1"
    assert captured["env"]["OMP_NUM_THREADS"] == "2"


def test_trace_artifact_binds_source_and_never_persists_signed_url(monkeypatch, tmp_path, card, snapshot):
    source = validated(snapshot, card)
    monkeypatch.setattr(asr, "asr_root", lambda: tmp_path)
    async def download(_url, output, _source):
        output.write_bytes(b"x")
        return {"media_sha256": "sha", "media_bytes": 1, "source_host": source["source_host"], "content_type": "video/mp4"}
    async def transcribe(*_):
        return {"text": TRANSCRIPT, "segments": [], "model": "turbo"}
    async def inspect(paths, _source):
        return {"source_kind":"muxed_official_video", "asr_input":str(paths[0]),
                "media":[{"path":str(paths[0]),"has_audio":True,"has_video":True}]}
    monkeypatch.setattr(asr, "run_media_worker", download)
    monkeypatch.setattr(asr, "inspect_downloaded_sources", inspect)
    monkeypatch.setattr(asr, "run_asr_worker", transcribe)
    text, meta = asyncio.run(asr.transcribe_official_source(source))
    assert text == TRANSCRIPT
    artifact = json.loads((tmp_path / "artifacts" / f"2026-10-08-{VIDEO_ID}" / "transcript.json").read_text())
    assert artifact["source"]["video_id"] == VIDEO_ID
    assert artifact["media_sha256"] == "sha"
    assert "private-signature" not in json.dumps(artifact)
    assert "secret" not in json.dumps(artifact)
    assert meta["method"] == asr.ASR_METHOD


def test_video_processing_uses_local_asr_and_existing_strict_parser(monkeypatch, card):
    captured = {}
    async def none(*_): return None
    async def local(_page, _card): return TRANSCRIPT, {"model": "turbo"}
    async def persist(_card, values, text):
        captured.update(values=values, text=text, method=_card["extraction_method"])
        return {"available_rows": 4}, 4
    monkeypatch.setattr(collector, "_already_processed", none)
    monkeypatch.setattr(collector, "_recover_existing_transcript", none)
    monkeypatch.setattr(collector, "assert_transcript_is_new", none)
    monkeypatch.setattr(collector, "request_local_video_transcript", local)
    monkeypatch.setattr(collector, "_persist_success", persist)
    result = asyncio.run(collector._process_video_card(None, card, douyin_page=object()))
    assert result["status"] == "SUCCESS"
    assert captured["values"] == {"sz50_emotion": 23, "hs300_emotion": 13, "zz500_emotion": 8, "zz1000_emotion": 9}
    assert captured["method"] == asr.ASR_METHOD


def test_video_ambiguous_metrics_fail_without_writing_default_values(monkeypatch, card):
    captured = {}
    async def none(*_): return None
    async def local(*_): return TRANSCRIPT + " 上证50:24", {}
    async def failed(_card, values, text, error, **kwargs): captured.update(values=values, error=error)
    monkeypatch.setattr(collector, "_already_processed", none)
    monkeypatch.setattr(collector, "_recover_existing_transcript", none)
    monkeypatch.setattr(collector, "assert_transcript_is_new", none)
    monkeypatch.setattr(collector, "request_local_video_transcript", local)
    monkeypatch.setattr(collector, "_persist_failed", failed)
    with pytest.raises(ValueError, match="歧义"):
        asyncio.run(collector._process_video_card(None, card, douyin_page=object()))
    assert captured["values"] == {}


def test_note_processing_retains_ocr_and_does_not_call_asr(monkeypatch, card):
    card["content_type"] = "note"
    async def none(*_): return None
    async def note(*_): return TRANSCRIPT, collector.parse_transcript_emotions(TRANSCRIPT)
    async def persist(*_): return {"available_rows": 4}, 4
    async def fail(*_): raise AssertionError("图文不应调用 ASR")
    for name in ("_already_processed", "_recover_existing_transcript"):
        monkeypatch.setattr(collector, name, none)
    monkeypatch.setattr(collector, "request_note_ocr_transcript", note)
    monkeypatch.setattr(collector, "request_local_video_transcript", fail)
    monkeypatch.setattr(collector, "_persist_success", persist)
    assert asyncio.run(collector._process_video_card(None, card, douyin_page=object()))["status"] == "SUCCESS"


def test_successful_duplicate_reuses_db_and_recomputes_without_downloading(monkeypatch, card):
    async def existing(*_): return collector.parse_transcript_emotions(TRANSCRIPT), {"available_rows": 4}
    async def sync(**_): return 4
    async def fail(*_): raise AssertionError("成功作品不应重复下载")
    monkeypatch.setattr(collector, "_already_processed", existing)
    monkeypatch.setattr(collector, "request_local_video_transcript", fail)
    monkeypatch.setattr(collector.quant_index, "sync_daily", sync)
    assert asyncio.run(collector._process_video_card(None, card))["status"] == "ALREADY_PROCESSED"


@pytest.mark.parametrize("method,source_date,text,expected", [
    ("coze_transcript+local_parser", date(2026, 10, 8), TRANSCRIPT, False),
    (asr.ASR_METHOD, date(2026, 10, 7), TRANSCRIPT, False),
    (asr.ASR_METHOD, date(2026, 10, 8), "日期：2026-10-07 " + TRANSCRIPT, False),
    (asr.ASR_METHOD, date(2026, 10, 8), TRANSCRIPT, True),
])
def test_recovery_refuses_unbound_coze_or_wrong_date(monkeypatch, card, method, source_date, text, expected):
    class Database:
        async def init_pool(self): pass
        async def close(self): pass
        async def get_douyin_emotion_by_video_id(self, _):
            return {"extraction_method": method, "emotion_date": source_date, "raw_ocr_text": text, "account_id": collector.ACCOUNT_ID}
    async def noop(*_): pass
    monkeypatch.setattr(collector, "DbTools", Database)
    monkeypatch.setattr(collector, "assert_transcript_is_new", noop)
    assert (asyncio.run(collector._recover_existing_transcript(card)) is not None) == expected


def test_no_new_work_and_expired_login_do_not_create_coze_tab(monkeypatch, card):
    class Context:
        async def new_page(self): raise AssertionError("不应新建扣子标签页")
    async def account(_): pass
    async def state(_): pass
    async def existing(_): return None
    async def collect(*_, **__): return [dict(card, published_at=card["published_at"].replace(day=7))]
    monkeypatch.setattr(collector, "_open_douyin_account", account)
    monkeypatch.setattr(collector, "persist_browser_storage_state", state)
    monkeypatch.setattr(collector, "_existing_video_card_for_date", existing)
    monkeypatch.setattr(collector, "load_cached_video_card", lambda _: None)
    monkeypatch.setattr(collector, "collect_video_cards", collect)
    monkeypatch.setattr(collector, "cache_video_cards", lambda _: None)
    result = asyncio.run(collector.run_pipeline_with_context(Context(), date(2026, 10, 8), douyin_page=object(), show_coze=True))
    assert result["status"] == "NO_UPDATE"
    async def expired(_): raise collector.AuthenticationRequiredError("抖音登录已失效")
    monkeypatch.setattr(collector, "_open_douyin_account", expired)
    with pytest.raises(collector.AuthenticationRequiredError):
        asyncio.run(collector.run_pipeline_with_context(Context(), date(2026, 10, 8), douyin_page=object()))


def test_single_instance_lock_and_pinned_selection(tmp_path, monkeypatch, card):
    monkeypatch.setattr(collector, "LOCK_PATH", tmp_path / "collector.lock")
    with collector.single_instance_lock():
        with pytest.raises(RuntimeError, match="已有实例"):
            with collector.single_instance_lock(): pass
    selected, _ = collector.select_latest_video([dict(card, is_pinned=True), dict(card, video_id="other")], date(2026, 10, 8))
    assert selected["video_id"] == "other"


def test_runtime_readiness_is_read_only_when_missing(monkeypatch, tmp_path):
    root = tmp_path / "missing"
    monkeypatch.setattr(asr, "asr_root", lambda: root)
    assert asr.runtime_readiness()["asr_ready"] is False
    assert not root.exists()


def test_normalization_labels_asr_and_preserves_excel_and_manual_rows():
    captured = {}
    class Cursor:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def execute(self, *_): pass
        async def fetchall(self):
            return [{"index_name": "上证50", "data_source": "excel"}, {"index_name": "沪深300", "data_source": "manual_task_center"}]
        async def executemany(self, _sql, rows): captured["rows"] = rows
        async def fetchone(self): return {"row_count": 4}
    class Connection:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        def cursor(self, *_): return Cursor()
        async def commit(self): pass
    database = DbTools()
    database.pool = SimpleNamespace(acquire=lambda: Connection())
    result = asyncio.run(database.batch_douyin_emotion_to_excel("2026-10-08", VIDEO_ID, collector.parse_transcript_emotions(TRANSCRIPT), data_source="douyin_local_asr"))
    assert result["protected_names"] == ["上证50", "沪深300"]
    assert len(captured["rows"]) == 2
    assert all(row[-1] == "douyin_local_asr" for row in captured["rows"])


def test_download_subprocess_hard_deadline_kills_and_cleans_partial(monkeypatch, tmp_path):
    captured = {}
    output = tmp_path / "source.mp4"
    output.with_suffix(".partial").write_bytes(b"incomplete")
    class Process:
        returncode = None
        async def communicate(self, payload=None):
            if payload is not None:
                captured["input"] = payload
            if self.returncode is None:
                await asyncio.sleep(10)
            return b"", b""
        def kill(self):
            self.returncode = -9
            captured["killed"] = True
    async def create(*args, **kwargs):
        captured["argv"] = args
        return Process()
    monkeypatch.setattr(asr.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(asr, "DOWNLOAD_TIMEOUT_SECONDS", 0.001)
    url = "https://v11-weba.douyinvod.com/a?signature=secret"
    with pytest.raises(TimeoutError, match="已终止"):
        asyncio.run(asr.run_media_worker(url, output, "https://www.douyin.com/video/123"))
    assert captured["killed"]
    assert "secret" not in str(captured["argv"])
    assert b"signature=secret" in captured["input"]
    assert not output.with_suffix(".partial").exists()


def test_download_cancellation_kills_before_returning(monkeypatch, tmp_path):
    captured = {}
    started = None
    class Process:
        returncode = None
        async def communicate(self, _payload=None):
            if self.returncode is None:
                started.set()
                await asyncio.sleep(10)
            return b"", b""
        def kill(self):
            self.returncode = -9
            captured["killed"] = True
    async def create(*_, **__): return Process()
    monkeypatch.setattr(asr.asyncio, "create_subprocess_exec", create)
    async def scenario():
        nonlocal started
        started = asyncio.Event()
        task = asyncio.create_task(asr.run_media_worker("https://v11-weba.douyinvod.com/a", tmp_path / "source.mp4", "https://www.douyin.com/video/123"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
    asyncio.run(scenario())
    assert captured["killed"]


@pytest.mark.parametrize("source_date,account_id", [(date(2026, 10, 7), collector.ACCOUNT_ID), (date(2026, 10, 8), "other")])
def test_successful_cache_refuses_mismatched_author_or_date(monkeypatch, card, source_date, account_id):
    class Database:
        async def init_pool(self): pass
        async def close(self): pass
        async def get_douyin_emotion_by_video_id(self, _):
            return {"extraction_status": "SUCCESS", "emotion_date": source_date, "account_id": account_id, **collector.parse_transcript_emotions(TRANSCRIPT)}
    monkeypatch.setattr(collector, "DbTools", Database)
    with pytest.raises(ValueError):
        asyncio.run(collector._already_processed(card))


def test_download_network_retry_is_finite_and_asr_timeout_not_retried(monkeypatch, card):
    captured = {"downloads": 0, "sources": 0}
    async def source(*_):
        captured["sources"] += 1
        return {}
    async def download(_):
        captured["downloads"] += 1
        raise ConnectionError("连接失败")
    class Page:
        async def wait_for_timeout(self, _): pass
    monkeypatch.setattr(collector, "read_official_video_source", source)
    monkeypatch.setattr(collector, "transcribe_official_source", download)
    with pytest.raises(ConnectionError):
        asyncio.run(collector.request_local_video_transcript(Page(), card))
    assert captured["downloads"] == captured["sources"] == 2
    async def timedout(_):
        captured["downloads"] += 1
        raise TimeoutError("ASR 子进程超时")
    monkeypatch.setattr(collector, "transcribe_official_source", timedout)
    with pytest.raises(TimeoutError):
        asyncio.run(collector.request_local_video_transcript(Page(), card))
    assert captured["downloads"] == 3


def test_session_cancellation_closes_browser_and_releases_lock(monkeypatch):
    captured = {}
    session = collector.PersistentDouyinBrowserSession()
    async def start(_): pass
    async def close(): captured["closed"] = True
    async def cancelled(*_, **__): raise asyncio.CancelledError()
    monkeypatch.setattr(session, "_ensure_started", start)
    monkeypatch.setattr(session, "_close_browser", close)
    monkeypatch.setattr(collector, "run_pipeline_with_context", cancelled)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(session.run(date(2026, 10, 8)))
    assert captured["closed"]


def test_asr_method_fits_existing_raw_table_column():
    assert asr.ASR_METHOD == "local_turbo_asr+local_parser"
    assert len(asr.ASR_METHOD) <= 32



def test_blob_player_requires_bounded_observed_official_mp4_sources(snapshot, card):
    snapshot["players"][0]["current_src"] = "blob:https://www.douyin.com/local-player"
    media_url = "https://v11-weba.douyinvod.com/target.mp4?temporary-signature=private"
    snapshot["official_media_urls"] = [media_url, media_url]
    source = asr.validate_player_snapshot(snapshot, card, account_url=collector.ACCOUNT_URL, account_name=collector.ACCOUNT_NAME)
    assert source["media_url"] == media_url
    assert source["source_resolution"] == "visible_player_blob_official_mp4_response"
    snapshot["players"][0].update(video_width=1280, video_height=720)
    snapshot["official_media_urls"] = [media_url, "https://v3.douyinvod.com/audio.mp4"]
    assert len(validated(snapshot, card)["media_urls"]) == 2
    for candidates in ([], [media_url, "https://v3.douyinvod.com/audio.mp4", "https://v3.douyinvod.com/ambiguous.mp4"]):
        snapshot["official_media_urls"] = candidates
        with pytest.raises(ValueError, match="来源缺失或存在歧义"):
            asr.validate_player_snapshot(snapshot, card, account_url=collector.ACCOUNT_URL, account_name=collector.ACCOUNT_NAME)
    snapshot["official_media_urls"] = ["https://evil.example/target.mp4"]
    with pytest.raises(ValueError, match="官方 HTTPS"):
        asr.validate_player_snapshot(snapshot, card, account_url=collector.ACCOUNT_URL, account_name=collector.ACCOUNT_NAME)



def fake_av_sources(stream_specs):
    class Media:
        def __init__(self, spec):
            duration,audio,video,width,height = spec
            self.duration = duration
            self.streams = SimpleNamespace(audio=[object()] if audio else [],
                video=[SimpleNamespace(codec_context=SimpleNamespace(width=width,height=height))] if video else [])
        def __enter__(self): return self
        def __exit__(self,*_): pass
    return SimpleNamespace(time_base=1,open=lambda path:Media(stream_specs[str(path)]))


def test_media_inspector_accepts_exact_split_player_stream_roles():
    from akshare_project.collectors.douyin_media_inspector import inspect_sources
    av = fake_av_sources({"video":(137.49,False,True,1280,720),"audio":(137.51,True,False,0,0)})
    result = inspect_sources(["video","audio"],137.49,av,1280,720)
    assert result["source_kind"] == "official_player_separate_video_audio"
    assert result["asr_input"] == "audio"
    assert all(abs(r["duration_seconds"]-137.49)<2 for r in result["media"])


@pytest.mark.parametrize("specs,match",[
    ({"video":(137.49,False,True,1280,720)},"没有音轨"),
    ({"video":(137.49,False,True,1280,720),"audio":(140,True,False,0,0)},"时长"),
    ({"video":(137.49,False,True,1920,1080),"audio":(137.49,True,False,0,0)},"分辨率"),
    ({"video":(137.49,True,True,1280,720),"other":(137.49,True,True,1280,720)},"存在歧义"),
])
def test_media_inspector_rejects_missing_audio_wrong_duration_resolution_or_duplicate_roles(specs,match):
    from akshare_project.collectors.douyin_media_inspector import inspect_sources
    with pytest.raises(ValueError,match=match):
        inspect_sources(list(specs),137.49,fake_av_sources(specs),1280,720)


def test_media_inspector_accepts_single_muxed_video():
    from akshare_project.collectors.douyin_media_inspector import inspect_sources
    result = inspect_sources(["full"],137.49,fake_av_sources({"full":(137.49,True,True,1280,720)}),1280,720)
    assert result["source_kind"] == "muxed_official_video"
    assert result["asr_input"] == "full"


def test_player_network_sources_are_scoped_to_target_main_frame(monkeypatch,snapshot,card):
    snapshot["players"][0].update(current_src="blob:https://www.douyin.com/player",video_width=1280,video_height=720)
    class Page:
        main_frame = object()
        callback = None
        removed = False
        def on(self,event,callback): self.callback=callback
        def remove_listener(self,event,callback):
            assert callback == self.callback
            self.removed=True
        async def goto(self,*_,**__):
            for url,frame in [("https://v11.douyinvod.com/video.mp4",self.main_frame),("https://v11.douyinvod.com/audio.mp4",self.main_frame),("https://v11.douyinvod.com/other-frame.mp4",object()),("https://evil.example/media.mp4",self.main_frame)]:
                self.callback(SimpleNamespace(url=url,status=206,headers={"content-type":"video/mp4"},request=SimpleNamespace(frame=frame,resource_type="fetch")))
        async def wait_for_timeout(self,*_): pass
        async def evaluate(self,*_): return dict(snapshot)
    async def authenticated(*_): return True
    monkeypatch.setattr(collector,"is_douyin_authenticated",authenticated)
    page=Page()
    source=asyncio.run(collector.read_official_video_source(page,card))
    assert set(source["media_urls"]) == {"https://v11.douyinvod.com/video.mp4","https://v11.douyinvod.com/audio.mp4"}
    assert page.removed



def test_partial_video_transcript_preserves_only_confirmed_metrics():
    text = "中午中证五百和中证一千的情绪数值在四到五之间。现在中证五百和中证一千的情绪分别是十和十三。"
    with pytest.raises(collector.IncompleteEmotionTranscriptError) as raised:
        collector._parse_video_transcript(text)
    assert raised.value.values == {"zz500_emotion": 10, "zz1000_emotion": 13}
    assert "上证50、沪深300" in str(raised.value)
    assert "文案缺少" not in str(raised.value)
    with pytest.raises(ValueError, match="缺少上证50"):
        collector.parse_transcript_emotions(text)


def test_partial_video_transcript_does_not_suppress_ambiguity():
    with pytest.raises(ValueError, match="歧义"):
        collector._parse_video_transcript("中证500:10 中证500:11 中证1000:13")


def test_fresh_partial_video_returns_saved_subset_without_full_recompute(monkeypatch, card):
    text = "现在中证五百和中证一千的情绪分别是十和十三。"
    captured = {}
    async def none(*_): return None
    async def local(*_): return text, {}
    async def partial(_card, error):
        captured.update(values=error.values, transcript=error.transcript)
        return {"status":"PARTIAL", "values":error.values}
    async def forbidden(*_args, **_kwargs): raise AssertionError("Partial source must not normalize")
    for name in ("_already_processed", "_recover_existing_transcript", "assert_transcript_is_new"):
        monkeypatch.setattr(collector, name, none)
    monkeypatch.setattr(collector, "request_local_video_transcript", local)
    monkeypatch.setattr(collector, "_persist_partial", partial)
    monkeypatch.setattr(collector, "_persist_success", forbidden)
    result = asyncio.run(collector._process_video_card(None, card, douyin_page=object()))
    assert result["status"] == "PARTIAL"
    assert captured["values"] == {"zz500_emotion": 10, "zz1000_emotion": 13}
    assert captured["transcript"] == text
    assert collector.build_extraction_status(captured["values"]) == "PARTIAL"


def test_cached_partial_video_is_not_retranscribed(monkeypatch, card):
    text = "现在中证五百和中证一千的情绪分别是十和十三。"
    captured = {}
    class Database:
        async def init_pool(self): pass
        async def close(self): pass
        async def get_douyin_emotion_by_video_id(self, _):
            return {"emotion_date":date(2026,10,8), "account_id":collector.ACCOUNT_ID,
                    "extraction_method":asr.ASR_METHOD,"raw_ocr_text":text + "\n\nERROR:\n文案缺少上证50情绪指标"}
    async def none(*_): return None
    async def partial(_card, error):
        captured.update(values=error.values,transcript=error.transcript)
        return {"status":"PARTIAL"}
    async def forbidden(*_args, **_kwargs): raise AssertionError("Same partial transcript must not be retranscribed")
    monkeypatch.setattr(collector,"DbTools",Database)
    monkeypatch.setattr(collector,"_already_processed",none)
    monkeypatch.setattr(collector,"assert_transcript_is_new",none)
    monkeypatch.setattr(collector,"_persist_partial",partial)
    monkeypatch.setattr(collector,"request_local_video_transcript",forbidden)
    assert asyncio.run(collector._process_video_card(None,card,douyin_page=object()))["status"] == "PARTIAL"
    assert captured == {"values":{"zz500_emotion":10,"zz1000_emotion":13},"transcript":text}


def test_partial_persistence_uses_explicit_subset_and_skips_full_dashboard_recompute(monkeypatch, card):
    captured = {}
    class Database:
        async def init_pool(self): pass
        async def close(self): captured["closed"] = True
        async def batch_douyin_emotion_to_excel(self, target_date, video_id, values, **kwargs):
            captured.update(values=values, kwargs=kwargs)
            return {"available_rows":2,"upserted_rows":2,"protected_names":[]}
        async def upsert_douyin_emotion_daily(self, rows): captured["raw"] = rows[0]
    async def forbidden(**_): raise AssertionError("Partial input cannot trigger full risk recomputation")
    monkeypatch.setattr(collector,"DbTools",Database)
    monkeypatch.setattr(collector.quant_index,"sync_daily",forbidden)
    error=collector.IncompleteEmotionTranscriptError("现在中证五百和中证一千的情绪分别是十和十三。", {"zz500_emotion":10,"zz1000_emotion":13})
    result=asyncio.run(collector._persist_partial(card,error))
    assert result["status"] == "PARTIAL"
    assert result["missing_fields"] == ["sz50_emotion","hs300_emotion"]
    assert captured["kwargs"] == {"data_source":"douyin_local_asr","allow_partial":True}
    assert captured["raw"]["extraction_status"] == "PARTIAL"
    assert captured["raw"]["sz50_emotion"] is None and captured["closed"]


def test_partial_normalization_never_supplies_missing_values_and_updates_only_known_dashboard_rows():
    captured = {"queries":[]}
    class Cursor:
        async def __aenter__(self): return self
        async def __aexit__(self,*_): pass
        async def execute(self,sql,args): captured["queries"].append((sql,args))
        async def fetchall(self): return [{"index_name":"中证500","data_source":"manual_task_center"}]
        async def executemany(self,sql,rows): captured["rows"]=rows
        async def fetchone(self): return {"row_count":2}
    class Connection:
        async def __aenter__(self): return self
        async def __aexit__(self,*_): pass
        def cursor(self,*_): return Cursor()
        async def commit(self): pass
    database=DbTools()
    database.pool=SimpleNamespace(acquire=lambda:Connection())
    values={"zz500_emotion":10,"zz1000_emotion":13}
    with pytest.raises(ValueError,match="four values"):
        asyncio.run(database.batch_douyin_emotion_to_excel("2026-10-08",VIDEO_ID,values))
    result=asyncio.run(database.batch_douyin_emotion_to_excel("2026-10-08",VIDEO_ID,values,data_source="douyin_local_asr",allow_partial=True))
    assert result["protected_names"] == ["中证500"]
    assert len(captured["rows"]) == 1 and captured["rows"][0][1:3] == ("中证1000",13)
    dashboard=[(sql,args) for sql,args in captured["queries"] if "UPDATE quant_index_dashboard_daily" in sql]
    assert len(dashboard) == 1 and dashboard[0][1] == ["2026-10-08","中证500","中证1000"]
    assert "risk" not in dashboard[0][0]
