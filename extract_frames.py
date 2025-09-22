#!/usr/bin/env python3
# save as video_to_frames_audio.py

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path

def run_cmd(cmd):
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return proc.stdout

def ffprobe_size(input_path):
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "json", str(input_path)
    ]
    out = run_cmd(cmd)
    data = json.loads(out)
    w = data["streams"][0]["width"]
    h = data["streams"][0]["height"]
    return int(w), int(h)

def even(n):
    # 保证为偶数，避免部分滤镜/编码器报错
    return int(math.floor(n / 2) * 2)

def main():
    parser = argparse.ArgumentParser(description="Extract frames (JPEG) at target FPS and optional size, plus audio.mp3")
    parser.add_argument("--input", help="Input video path")
    parser.add_argument("--output", "-o", required=True, help="Output folder")
    parser.add_argument("--width", type=int, default=None, help="Target width (if only width given, height is proportional)")
    parser.add_argument("--height", type=int, default=None, help="Target height (if only height given, width is proportional)")
    parser.add_argument("--fps", type=float, default=20, help="Target FPS for frames (default: 20)")
    parser.add_argument("--clean", action="store_true", help="Clean existing jpgs in output before writing")
    args = parser.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.clean:
        for p in out_dir.glob("*.jpg"):
            p.unlink()

    # 计算目标尺寸
    scale_filter = None
    target_w, target_h = args.width, args.height
    if (args.width is None) ^ (args.height is None):
        # 只给了一个，按比例算另一个
        in_w, in_h = ffprobe_size(in_path)
        if args.width is not None:
            target_h = round(in_h * (args.width / in_w))
        else:
            target_w = round(in_w * (args.height / in_h))
        # 为兼容性将尺寸取偶数
        target_w = even(target_w)
        target_h = even(target_h)

    if target_w and target_h:
        scale_filter = f"scale={target_w}:{target_h}"

    # 组合 -vf（先 fps 再 scale / 或者只 fps）
    vf_parts = [f"fps={args.fps}"]
    if scale_filter:
        vf_parts.append(scale_filter)
    vf = ",".join(vf_parts)

    # 1) 抽帧：output/00000.jpg, 00001.jpg ...
    frame_tpl = str(out_dir / "%05d.jpg")
    extract_cmd = [
        "ffmpeg",
        "-y",
        "-i", str(in_path),
        "-vf", vf,
        "-q:v", "2",          # JPEG 质量（1-31，越小越好），可按需调整
        frame_tpl
    ]
    print("Extracting frames...")
    run_cmd(extract_cmd)
    print("Frames saved to:", out_dir)

    # # 2) 导出音频：output/audio.mp3
    # audio_out = out_dir / "audio.mp3"
    # audio_cmd = [
    #     "ffmpeg",
    #     "-y",
    #     "-i", str(in_path),
    #     "-vn",
    #     "-acodec", "libmp3lame",
    #     "-b:a", "192k",
    #     str(audio_out)
    # ]
    # try:
    #     print("Extracting audio...")
    #     run_cmd(audio_cmd)
    #     print("Audio saved to:", audio_out)
    # except Exception as e:
    #     # 可能无音轨，做友好提示
    #     print("Audio extraction failed (maybe no audio track). Detail:", e)

    print("Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
