"""Inspect downloaded player media inside the isolated PyAV environment."""
import json
import math
import os
import sys
from pathlib import Path


def inspect_sources(paths, expected_duration, av_module, expected_width=0, expected_height=0):
    rows = []
    for path in paths:
        with av_module.open(str(path)) as media:
            if media.duration is None:
                raise ValueError("官方媒体时长无法验证")
            duration = media.duration / av_module.time_base
            if not math.isfinite(duration) or abs(duration - expected_duration) > 2:
                raise ValueError("官方媒体时长与目标播放器不一致")
            audio = list(media.streams.audio)
            video = list(media.streams.video)
            if len(audio) > 1 or len(video) > 1 or not (audio or video):
                raise ValueError("官方媒体音视频流缺失或存在歧义")
            width = video[0].codec_context.width if video else 0
            height = video[0].codec_context.height if video else 0
            if video and expected_width and expected_height and (width, height) != (expected_width, expected_height):
                raise ValueError("官方视频分辨率与可见播放器不一致")
            rows.append({"path":str(path), "duration_seconds":duration,
                         "has_audio":bool(audio), "has_video":bool(video),
                         "width":width, "height":height})
    if len(rows) == 1 and rows[0]["has_audio"] and rows[0]["has_video"]:
        return {"source_kind":"muxed_official_video", "asr_input":rows[0]["path"], "media":rows}
    if len(rows) == 2:
        video_only = [r for r in rows if r["has_video"] and not r["has_audio"]]
        audio_only = [r for r in rows if r["has_audio"] and not r["has_video"]]
        if len(video_only) == len(audio_only) == 1:
            return {"source_kind":"official_player_separate_video_audio", "asr_input":audio_only[0]["path"], "media":rows}
    if not any(r["has_audio"] for r in rows):
        raise ValueError("官方视频没有音轨或对应的播放器音频源")
    raise ValueError("官方播放器音视频来源缺失或存在歧义")


def main():
    try:
        os.nice(10)
        payload = json.loads(sys.stdin.read())
        import av
        result = inspect_sources(payload["paths"], payload["expected_duration"], av,
                                 payload.get("expected_width",0), payload.get("expected_height",0))
        print(json.dumps({"status":"SUCCESS", "result":result},ensure_ascii=False))
        return 0
    except Exception as exc:
        message = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        print(json.dumps({"status":"FAILED", "error":message},ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
