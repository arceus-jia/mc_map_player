import os
import sys
import cv2
import argparse
import numpy as np
import time
import json
import zlib
import struct
from typing import Tuple, List, Optional

# ---------------- 4x4 Bayer for ordered dithering ----------------
BAYER4 = np.array(
    [
        [0, 8, 2, 10],
        [12, 4, 14, 6],
        [3, 11, 1, 9],
        [15, 7, 13, 5],
    ],
    dtype=np.float32,
)


def apply_ordered4_dither(img: np.ndarray, amount: float) -> np.ndarray:
    if img.ndim != 3 or img.shape[2] < 3:
        return img
    h, w = img.shape[:2]
    t = BAYER4 / 16.0 - 0.5
    tile = np.tile(t, (int(np.ceil(h / 4)), int(np.ceil(w / 4))))[:h, :w]
    off = (amount * tile).astype(np.float32)
    out = img.astype(np.float32).copy()
    for c in range(3):  # B,G,R
        out[:, :, c] = np.clip(np.rint(out[:, :, c] + off), 0, 255)
    return out.astype(np.uint8)


# ---------------- size helpers ----------------
def snap128(n: float) -> int:
    return max(128, int(round(n / 128.0)) * 128)


def decide_target_size_from_first(
    orig_w: int, orig_h: int, w: Optional[int], h: Optional[int]
):
    """
    用“第一张图”的原始尺寸 + 用户传参，确定整个批次的目标尺寸（128 的倍数）。
    """
    if w is None and h is None:
        return snap128(orig_w), snap128(orig_h)
    if w is not None and h is None:
        scale = w / orig_w
        return snap128(w), snap128(orig_h * scale)
    if w is None and h is not None:
        scale = h / orig_h
        return snap128(orig_w * scale), snap128(h)
    # both provided
    return snap128(w), snap128(h)


def resize(img: np.ndarray, w: int, h: int, pixelate: bool = False) -> np.ndarray:
    if pixelate:
        tmp_w = max(1, w // 3)
        tmp_h = max(1, h // 3)
        tmp = cv2.resize(img, (tmp_w, tmp_h), interpolation=cv2.INTER_LINEAR)
        return cv2.resize(tmp, (w, h), interpolation=cv2.INTER_NEAREST)
    return cv2.resize(img, (w, h), interpolation=cv2.INTER_LINEAR)


# ---------------- LUT quantization ----------------
def quantize_with_lut_rgb(rgb: np.ndarray, lut: np.ndarray) -> np.ndarray:
    r = rgb[:, :, 0]
    g = rgb[:, :, 1]
    b = rgb[:, :, 2]
    return lut[r, g, b]


# ---------------- SMRF pack (big-endian) ----------------
def pack_smrf_big_endian(
    pixels_bytes: bytes,
    w: int,
    h: int,
    cols: int,
    rows: int,
    xMin: int,
    yFix: int,
    zMin: int,
    compress: bool,
) -> bytes:
    assert len(pixels_bytes) == w * h, "payload size mismatch"
    magic = b"SMRF"
    version = 1
    flags = 0
    payload = pixels_bytes
    if compress:
        payload = zlib.compress(payload)
        flags |= 0x01
    header = struct.pack(
        ">4sBBBBHHiii", magic, version, flags, cols, rows, w, h, xMin, yFix, zMin
    )
    return header + payload


# ---------------- core: one image -> array/smrf ----------------
def process_one_image(
    input_img: str,
    output_path: str,
    lut: np.ndarray,
    tw: int,
    th: int,
    dither: str,
    dither_amount: float,
    pixelate: bool,
    out_format: str,
    compress: bool,
    xMin: int,
    yFix: int,
    zMin: int,
):
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    img = cv2.imread(input_img, cv2.IMREAD_UNCHANGED)
    if img is None:
        raise FileNotFoundError(f"cannot read image: {input_img}")

    img = resize(img, tw, th, pixelate=pixelate)
    cv2.imwrite("resized.png", img)
    if dither == "ordered4":
        img = apply_ordered4_dither(img, float(dither_amount))
        cv2.imwrite("dither.png", img)

    if img.ndim == 2:
        rgb = np.stack([img, img, img], axis=-1)
    else:
        if img.shape[2] >= 3:
            bgr = img[:, :, :3]
            rgb = bgr[:, :, ::-1]
        else:
            gray = img[:, :, 0]
            rgb = np.stack([gray, gray, gray], axis=-1)

    idx = quantize_with_lut_rgb(rgb, lut).astype(np.uint8)  # [H,W] 0..255
    H, W = idx.shape
    cols, rows = W // 128, H // 128

    if out_format == "json":
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                idx.astype(int).tolist(), f, ensure_ascii=False, separators=(",", ":")
            )
    elif out_format == "smrf":
        # 简单版 SMRF v1: 行优先（row-major），每像素1字节调色板索引
        with open(output_path, "wb") as f:
            f.write(idx.astype(np.uint8).tobytes(order="C"))
    else:
        raise ValueError("unknown format: " + out_format)


# ---------------- driver ----------------
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp"}


def infer_output_path_for_folder(
    output_dir: str, input_file: str, out_format: str
) -> str:
    base = os.path.splitext(os.path.basename(input_file))[0]
    ext = ".json" if out_format == "json" else ".smrf"
    return os.path.join(output_dir, base + ext)


def main():
    parser = argparse.ArgumentParser(
        description="Generate palette-index frames (single file or whole folder) → JSON/SMRF"
    )
    parser.add_argument(
        "--input", type=str, required=True, help="input image or folder"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="output file (.json/.smrf) or output folder (when input is a dir)",
    )
    parser.add_argument(
        "--lut",
        type=str,
        default="colormap_oklab.npy",
        help="LUT .npy path (shape 256x256x256)",
    )
    parser.add_argument(
        "--width",
        "-w",
        type=int,
        default=None,
        help="target width (snapped to 128x). For folder mode, applied to all frames.",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=None,
        help="target height (snapped to 128x). For folder mode, applied to all frames.",
    )
    parser.add_argument(
        "--dither",
        type=str,
        choices=["none", "ordered4"],
        default="ordered4",
        help="dithering mode",
    )
    parser.add_argument(
        "--dither-amount",
        type=float,
        default=12.0,
        help="dither strength for ordered4 (8~16 typical)",
    )
    parser.add_argument(
        "--pixelate", action="store_true", help="pixelation style scaling"
    )
    parser.add_argument(
        "--format",
        "-f",
        type=str,
        choices=["smrf", "json"],
        default="json",
        help="output format",
    )
    parser.add_argument(
        "--no-compress", action="store_true", help="disable zlib for smrf"
    )
    parser.add_argument("--x", type=int, default=0, help="world xMin for SMRF header")
    parser.add_argument("--y", type=int, default=64, help="world yFix for SMRF header")
    parser.add_argument("--z", type=int, default=0, help="world zMin for SMRF header")
    args = parser.parse_args()

    t0 = time.time()
    lut = np.load(args.lut)
    if lut.shape != (256, 256, 256):
        raise ValueError(f"LUT shape must be (256,256,256), got {lut.shape}")

    in_path = args.input
    out_path = args.output
    out_format = args.format
    compress = not args.no_compress

    if os.path.isdir(in_path):
        # -------- folder mode --------
        files = [os.path.join(in_path, name) for name in sorted(os.listdir(in_path))]
        files = [
            f
            for f in files
            if os.path.splitext(f)[1].lower() in IMAGE_EXTS and os.path.isfile(f)
        ]
        if not files:
            print(f"[WARN] no images in folder: {in_path}")
            sys.exit(0)

        # 用第一张图确定整个批次的目标尺寸
        img0 = cv2.imread(files[0], cv2.IMREAD_UNCHANGED)
        if img0 is None:
            raise FileNotFoundError(f"cannot read first image: {files[0]}")
        ih0, iw0 = img0.shape[:2]
        tw, th = decide_target_size_from_first(iw0, ih0, args.width, args.height)
        # 保证 128 倍数（再保险）
        tw, th = snap128(tw), snap128(th)
        print(
            f"[INFO] folder mode: {len(files)} files; target size = {tw}x{th} (cols={tw//128}, rows={th//128})"
        )

        os.makedirs(out_path, exist_ok=True)
        ok = 0
        for i, f in enumerate(files, 1):
            out_file = infer_output_path_for_folder(out_path, f, out_format)
            try:
                process_one_image(
                    input_img=f,
                    output_path=out_file,
                    lut=lut,
                    tw=tw,
                    th=th,
                    dither=args.dither,
                    dither_amount=args.dither_amount,
                    pixelate=args.pixelate,
                    out_format=out_format,
                    compress=compress,
                    xMin=args.x,
                    yFix=args.y,
                    zMin=args.z,
                )
                ok += 1
                if i % 10 == 0 or i == len(files):
                    print(f"[{i}/{len(files)}] last -> {os.path.basename(out_file)}")
            except Exception as e:
                print(f"[ERR] {os.path.basename(f)}: {e}")
        print(f"[DONE] {ok}/{len(files)} frames written in {time.time() - t0:.2f}s")

    else:
        # -------- single file mode --------
        img0 = cv2.imread(in_path, cv2.IMREAD_UNCHANGED)
        if img0 is None:
            raise FileNotFoundError(f"cannot read image: {in_path}")
        ih0, iw0 = img0.shape[:2]
        tw, th = decide_target_size_from_first(iw0, ih0, args.width, args.height)
        tw, th = snap128(tw), snap128(th)
        print(
            f"[INFO] single file: target size = {tw}x{th} (cols={tw//128}, rows={th//128})"
        )

        # 如果输出是目录，自动用输入文件名推导输出文件
        if os.path.isdir(out_path) or out_path.endswith(os.sep):
            os.makedirs(out_path, exist_ok=True)
            out_path = infer_output_path_for_folder(out_path, in_path, out_format)

        process_one_image(
            input_img=in_path,
            output_path=out_path,
            lut=lut,
            tw=tw,
            th=th,
            dither=args.dither,
            dither_amount=args.dither_amount,
            pixelate=args.pixelate,
            out_format=out_format,
            compress=compress,
            xMin=args.x,
            yFix=args.y,
            zMin=args.z,
        )
        print(f"[DONE] wrote {out_path} in {time.time() - t0:.2f}s")


if __name__ == "__main__":
    main()

# python gen.py --input ../output/mickey --output ~/Desktop/mc/paper_1121/frames/pokemon --height 256 --width 384 --input ../output/pokemon/ --format smrf
#  python gen.py --input ../output/mickey --output ~/Desktop/mc/paper_1121/frames/pokemon2 --height 256 --width 384 --input ../output/pokemon/ --format json