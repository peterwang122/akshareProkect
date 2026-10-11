"""Official Douyin media download and an isolated, offline local ASR worker."""

import asyncio
import hashlib
import json
import math
import os
import re
import sys
import time
from zoneinfo import ZoneInfo
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from akshare_project.core.paths import REPO_ROOT


ASR_METHOD = "local_turbo_asr+local_parser"
MAX_MEDIA_BYTES = 200 * 1024 * 1024
MAX_DURATION_SECONDS = 15 * 60
DOWNLOAD_TIMEOUT_SECONDS = 90
ASR_TIMEOUT_SECONDS = 480
MAX_ATTEMPTS = 2
WORKER_PATH = Path(__file__).with_name("douyin_asr_worker.py")
MEDIA_WORKER_PATH = Path(__file__).with_name("douyin_media_worker.py")
INSPECTOR_PATH = Path(__file__).with_name("douyin_media_inspector.py")


def asr_root():
    override = os.getenv("DOUYIN_ASR_ROOT", "").strip()
    return Path(override).expanduser() if override else REPO_ROOT.parent / "FIT" / "runtime" / "douyin_asr"


def runtime_readiness():
    root = asr_root()
    interpreter_ready = (root / "venv" / "bin" / "python").is_file()
    worker_ready = all(path.is_file() for path in (WORKER_PATH, MEDIA_WORKER_PATH, INSPECTOR_PATH))
    model_cache = root / "model-cache" / "hub" / "models--mobiuslabsgmbh--faster-whisper-large-v3-turbo"
    model_ready = any(model_cache.glob("snapshots/*/model.bin"))
    return {
        "extraction_engine": "local_turbo_asr",
        "asr_ready": interpreter_ready and worker_ready and model_ready,
        "interpreter_ready": interpreter_ready,
        "worker_ready": worker_ready,
        "model_ready": model_ready,
    }


def validate_media_url(value):
    """No arbitrary hosts, credentials, fragments, or non-TLS media redirects."""
    parsed = urlparse(str(value or ""))
    host = (parsed.hostname or "").lower()
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("抖音播放器媒体地址端口无效") from None
    if (
        parsed.scheme != "https"
        or not host.endswith(".douyinvod.com")
        or not re.fullmatch(r"[a-z0-9.-]+", host)
        or parsed.username
        or parsed.password
        or port not in (None, 443)
        or parsed.fragment
    ):
        raise ValueError(f"抖音播放器媒体地址不是允许的官方 HTTPS 视频源（协议={parsed.scheme or chr(45)}，主机={host or chr(45)}）")
    return host


def validate_player_snapshot(snapshot, selected, *, account_url, account_name):
    video_id = str(selected.get("video_id") or "")
    page_url = urlparse(str(snapshot.get("page_url") or ""))
    if (
        selected.get("content_type", "video") != "video"
        or not re.fullmatch(r"\d+", video_id)
        or page_url.scheme != "https"
        or page_url.hostname != "www.douyin.com"
        or page_url.path != f"/video/{video_id}"
    ):
        raise ValueError("抖音播放器页面与目标视频不一致")
    expected_author_path = urlparse(account_url).path
    author_links = snapshot.get("author_links") or []
    if not any(
        urlparse(str(link)).hostname == "www.douyin.com"
        and urlparse(str(link)).path == expected_author_path
        for link in author_links
    ) or account_name != str(snapshot.get("author_name") or "").strip():
        raise ValueError("抖音视频作者未通过指定账号校验")
    local_publish = selected["published_at"].astimezone(ZoneInfo("Asia/Shanghai"))
    emotion_date = local_publish.date().isoformat()
    if str(snapshot.get("detail_video_id") or "") != video_id:
        raise ValueError("抖音作品详情与目标视频 ID 不一致")
    published_match = re.search(r"(20\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2})", str(snapshot.get("published_text") or ""))
    if not published_match:
        raise ValueError("抖音视频页面未确认目标发布日期")
    page_published = datetime.strptime(" ".join(published_match.groups()), "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    if page_published.strftime("%Y-%m-%d %H:%M") != local_publish.strftime("%Y-%m-%d %H:%M"):
        raise ValueError("抖音视频页面发布时间与目标作品不一致")
    if page_published.hour < 15:
        raise ValueError("抖音视频发布时间早于15:00")
    title = str(snapshot.get("page_title") or "")
    # The observed Liwei work title has YYYYMMDD; date chips may also include
    # recommendation dates, so require the target date rather than all chips.
    title_dates = set(re.findall(r"(?<!\d)(20\d{6})(?!\d)", title))
    if title_dates and emotion_date.replace("-", "") not in title_dates:
        raise ValueError("抖音视频标题日期与目标日期不一致")
    players = snapshot.get("players") or []
    if len(players) != 1:
        raise ValueError("抖音页面可见播放器缺失或存在歧义")
    player = players[0]
    duration = float(player.get("duration") or 0)
    if not math.isfinite(duration) or not 0 < duration <= MAX_DURATION_SECONDS:
        raise ValueError("抖音视频时长无效或超过本地转写上限")
    media_url = str(player.get("current_src") or "")
    source_resolution = "visible_player_current_src"
    if media_url.startswith("blob:https://www.douyin.com/"):
        # MSE hides the download URL behind a blob. The caller records only
        # successful MP4 responses from this page's main frame after navigation.
        candidates = sorted(set(snapshot.get("official_media_urls") or []))
        if not 1 <= len(candidates) <= 2:
            raise ValueError(f"抖音 blob 播放器的官方 MP4 来源缺失或存在歧义（候选={len(candidates)}）")
        for candidate in candidates:
            validate_media_url(candidate)
        if len(candidates) == 2 and not (player.get("video_width") and player.get("video_height")):
            raise ValueError("抖音 blob 播放器视频分辨率未确认")
        media_url = candidates[0]
        source_resolution = "visible_player_blob_official_mp4_response"
    host = validate_media_url(media_url)
    return {
        "media_url": media_url,
        "media_urls": candidates if source_resolution.startswith("visible_player_blob") else [media_url],
        "source_resolution": source_resolution,
        "video_width": int(player.get("video_width") or 0),
        "video_height": int(player.get("video_height") or 0),
        "source_host": host,
        "source_url": f"https://www.douyin.com/video/{video_id}",
        "source_date": emotion_date,
        "video_id": video_id,
        "account_name": account_name,
        "account_url": account_url.split("?", 1)[0],
        "duration_seconds": duration,
        "page_title": title,
        "published_at": selected["published_at"].isoformat(),
        "page_published_at": page_published.isoformat(),
        "content_type": "video",
    }


class _OfficialRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, new_url):
        validate_media_url(new_url)
        return super().redirect_request(request, fp, code, msg, headers, new_url)


def download_official_video(media_url, destination, source_url):
    validate_media_url(media_url)
    destination = Path(destination)
    partial = destination.with_suffix(".partial")
    opener = build_opener(_OfficialRedirects())
    request = Request(media_url, headers={
        "User-Agent": "Mozilla/5.0",
        "Referer": source_url,
    })
    started = time.monotonic()
    digest = hashlib.sha256()
    total = 0
    try:
        with opener.open(request, timeout=20) as response:
            host = validate_media_url(response.geturl())
            if response.status != 200:
                raise RuntimeError(f"官方视频下载失败：HTTP {response.status}")
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type != "video/mp4":
                raise ValueError("官方视频下载返回的媒体类型不是 video/mp4")
            raw_length = response.headers.get("Content-Length")
            expected_length = int(raw_length) if raw_length else None
            if expected_length is not None and not 0 < expected_length <= MAX_MEDIA_BYTES:
                raise ValueError("官方视频大小无效或超过下载上限")
            with partial.open("wb") as output:
                os.chmod(partial, 0o600)
                while True:
                    if time.monotonic() - started > DOWNLOAD_TIMEOUT_SECONDS:
                        raise TimeoutError("官方视频下载超时")
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_MEDIA_BYTES:
                        raise ValueError("官方视频超过下载大小上限")
                    digest.update(chunk)
                    output.write(chunk)
            if not total or (expected_length is not None and total != expected_length):
                raise ValueError("官方视频下载为空或长度不完整")
        partial.replace(destination)
        return {"media_sha256": digest.hexdigest(), "media_bytes": total, "source_host": host, "content_type": content_type}
    except HTTPError as exc:
        # urllib exceptions contain the signed URL: expose only status.
        raise ConnectionError(f"官方视频下载失败：HTTP {exc.code}") from None
    except (URLError, OSError) as exc:
        raise ConnectionError(f"官方视频下载连接失败：{type(exc).__name__}") from None
    finally:
        partial.unlink(missing_ok=True)


async def run_media_worker(media_url, destination, source_url):
    validate_media_url(media_url)
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(MEDIA_WORKER_PATH),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    # Signed currentSrc never enters argv, the environment, persisted files,
    # service logs, or subprocess output. Parent owns the hard wall deadline.
    payload = json.dumps({"media_url": media_url, "destination": str(destination), "source_url": source_url}).encode("utf-8")
    try:
        stdout, _stderr = await asyncio.wait_for(process.communicate(payload), timeout=DOWNLOAD_TIMEOUT_SECONDS)
    except (asyncio.TimeoutError, asyncio.CancelledError) as exc:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.communicate()
        Path(destination).with_suffix(".partial").unlink(missing_ok=True)
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise TimeoutError("官方视频下载子进程超时，已终止") from None
    try:
        result = json.loads(stdout.decode("utf-8"))
    except (ValueError, UnicodeError):
        raise ConnectionError("官方视频下载子进程返回无效结果") from None
    if process.returncode != 0 or result.get("status") != "SUCCESS":
        raise ConnectionError(f"官方视频下载失败：{result.get('error') or '下载子进程失败'}")
    return result["metadata"]


def _write_private_json(path, payload):
    path = Path(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(path, 0o600)


async def run_asr_worker(video_path, output_path, source):
    root = asr_root()
    python = root / "venv" / "bin" / "python"
    if not python.is_file():
        raise RuntimeError("持久隔离 ASR 环境未安装")
    Path(output_path).unlink(missing_ok=True)
    env = os.environ.copy()
    env.update({
        "HF_HOME": str(root / "model-cache"),
        "HF_HUB_OFFLINE": "1",
        "OMP_NUM_THREADS": "2",
        "OPENBLAS_NUM_THREADS": "2",
        "MKL_NUM_THREADS": "2",
        "VECLIB_MAXIMUM_THREADS": "2",
        "TOKENIZERS_PARALLELISM": "false",
    })
    process = await asyncio.create_subprocess_exec(
        str(python), str(WORKER_PATH), str(video_path),
        "--source-url", source["source_url"],
        "--source-date", source["source_date"],
        "--video-id", source["video_id"],
        "--expected-duration", str(source["duration_seconds"]),
        "--cache-dir", str(root / "model-cache"),
        "--output", str(output_path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=ASR_TIMEOUT_SECONDS)
    except (asyncio.TimeoutError, asyncio.CancelledError) as exc:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.communicate()
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise TimeoutError("本地 turbo ASR 子进程超时，已终止") from None
    if process.returncode != 0:
        # Worker emits a short JSON error without media URLs or credentials.
        try:
            message = json.loads(stdout.decode("utf-8")).get("error") or "转写失败"
        except (ValueError, UnicodeError):
            message = "转写失败"
        raise ValueError(f"本地 turbo ASR：{message}")
    try:
        result = json.loads(Path(output_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ValueError("本地 turbo ASR 输出文件无效") from None
    if (
        result.get("source_url") != source["source_url"]
        or result.get("source_date") != source["source_date"]
        or result.get("video_id") != source["video_id"]
        or result.get("model") != "turbo"
        or not str(result.get("text") or "").strip()
    ):
        raise ValueError("本地 turbo ASR 输出为空或来源不一致")
    return result


async def inspect_downloaded_sources(paths, source):
    process = await asyncio.create_subprocess_exec(
        str(asr_root() / "venv" / "bin" / "python"), str(INSPECTOR_PATH),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "OMP_NUM_THREADS":"2", "OPENBLAS_NUM_THREADS":"2", "VECLIB_MAXIMUM_THREADS":"2"},
    )
    payload = json.dumps({"paths":[str(p) for p in paths], "expected_duration":source["duration_seconds"],
                          "expected_width":source.get("video_width",0), "expected_height":source.get("video_height",0)}).encode()
    try:
        stdout, _stderr = await asyncio.wait_for(process.communicate(payload), timeout=60)
    except (asyncio.TimeoutError, asyncio.CancelledError) as exc:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.communicate()
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise TimeoutError("官方音视频来源核验子进程超时，已终止") from None
    try:
        result = json.loads(stdout.decode())
    except (ValueError, UnicodeError):
        raise ValueError("官方音视频来源核验输出无效") from None
    if process.returncode != 0 or result.get("status") != "SUCCESS":
        raise ValueError(result.get("error") or "官方音视频来源核验失败")
    return result["result"]


async def transcribe_official_source(source):
    root = asr_root()
    artifact_dir = root / "artifacts" / f"{source['source_date']}-{source['video_id']}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(artifact_dir, 0o700)
    metadata_path = artifact_dir / "source.json"
    output_path = artifact_dir / "transcript.json"
    # Both MSE parts are observed on the authenticated target page. Only
    # public work identity, CDN hosts and byte hashes survive this function.
    metadata = {key:value for key,value in source.items() if key not in {"media_url", "media_urls"}}
    urls = source.get("media_urls") or [source["media_url"]]
    if not 1 <= len(urls) <= 2:
        raise ValueError("官方播放器媒体来源存在歧义")
    paths, downloads = [], []
    for index, url in enumerate(urls):
        path = artifact_dir / ("source.mp4" if len(urls)==1 else f"source-part-{index+1}.mp4")
        download = await run_media_worker(url, path, source["source_url"])
        paths.append(path)
        downloads.append({**download,"file_name":path.name})
        if sum(item["media_bytes"] for item in downloads) > MAX_MEDIA_BYTES:
            raise ValueError("官方播放器媒体合计超过下载大小上限")
    inspected = await inspect_downloaded_sources(paths, source)
    metadata["media_files"] = downloads
    metadata["source_kind"] = inspected["source_kind"]
    metadata["media_streams"] = [{**row,"file_name":Path(row["path"]).name} for row in inspected["media"]]
    for row in metadata["media_streams"]:
        row.pop("path",None)
    media_sha256 = downloads[0]["media_sha256"] if len(downloads)==1 else hashlib.sha256(
        "".join(item["media_sha256"] for item in downloads).encode()).hexdigest()
    metadata["media_sha256"] = media_sha256
    metadata["media_bytes"] = sum(item["media_bytes"] for item in downloads)
    _write_private_json(metadata_path, metadata)
    result = await run_asr_worker(inspected["asr_input"], output_path, source)
    result["source"] = metadata
    result["media_sha256"] = media_sha256
    _write_private_json(output_path, result)
    text_path = artifact_dir / "transcript.txt"
    text_path.write_text(result["text"] + "\n", encoding="utf-8")
    os.chmod(text_path, 0o600)
    return result["text"], {"method":ASR_METHOD,"artifact_path":str(output_path),"model":"turbo",
                             "engine":"faster-whisper","media_sha256":media_sha256}
