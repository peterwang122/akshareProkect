"""Standalone worker: runs only inside the persistent isolated ASR venv."""

import argparse
import importlib.metadata
import json
import os
from datetime import datetime
from pathlib import Path


def inspect_media(video, expected_duration, av_module):
    with av_module.open(str(video)) as media:
        if not media.streams.audio:
            raise ValueError("视频没有音轨")
        if media.duration is None:
            raise ValueError("视频时长无法验证")
        duration = media.duration / av_module.time_base
    if duration <= 0 or abs(duration - expected_duration) > 2:
        raise ValueError("下载视频时长与播放器时长不一致")
    return duration


def build_transcript_result(segments, info, args, duration):
    rows = [{"start": segment.start, "end": segment.end, "text": segment.text.strip()}
            for segment in segments if segment.text.strip()]
    text = "\n".join(row["text"] for row in rows)
    if not text:
        raise ValueError("转写文案为空")
    return {
        "source_url": args.source_url, "source_date": args.source_date,
        "video_id": args.video_id, "duration_seconds": duration,
        "engine": "faster-whisper",
        "engine_version": importlib.metadata.version("faster-whisper"),
        "model": "turbo", "device": "cpu", "compute_type": "int8",
        "cpu_threads": 2, "num_workers": 1,
        "language": info.language, "segments": rows, "text": text,
        "transcribed_at": datetime.now().astimezone().isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("video", type=Path)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--source-date", required=True)
    parser.add_argument("--video-id", required=True)
    parser.add_argument("--expected-duration", required=True, type=float)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        os.nice(10)
        datetime.strptime(args.source_date, "%Y-%m-%d")
        if not args.video.is_file():
            raise ValueError("待转写视频不存在")
        import av
        from faster_whisper import WhisperModel

        duration = inspect_media(args.video, args.expected_duration, av)
        model = WhisperModel(
            "turbo", device="cpu", compute_type="int8", cpu_threads=2,
            num_workers=1, local_files_only=True,
            download_root=str(args.cache_dir / "hub"),
        )
        segments, info = model.transcribe(
            str(args.video), language="zh", beam_size=5, temperature=0,
            vad_filter=True,
            initial_prompt="中信、国泰、上证50、沪深300、中证500、中证1000、情绪指标。",
            hotwords="上证五十 沪深三百 中证五百 中证一千 恐贪数值",
        )
        result = build_transcript_result(segments, info, args, duration)
        revision_file = args.cache_dir / "hub" / "models--mobiuslabsgmbh--faster-whisper-large-v3-turbo" / "refs" / "main"
        result["model_repository"] = "mobiuslabsgmbh/faster-whisper-large-v3-turbo"
        result["model_revision"] = revision_file.read_text(encoding="utf-8").strip()
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        os.chmod(args.output, 0o600)
        print(json.dumps({"status": "SUCCESS", "characters": len(result["text"]), "segments": len(result["segments"])}))
        return 0
    except Exception as exc:
        # No traceback/path/signature-bearing messages go to service logs.
        allowed = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        print(json.dumps({"status": "FAILED", "error": allowed}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
