#!/usr/bin/env python3
import os
import time
import csv
from typing import Tuple, List, Optional

import numpy as np
import gzip


# ---------- sRGB -> OKLab ----------
# Ref: https://bottosson.github.io/posts/oklab/
def _srgb_to_linear(c: np.ndarray) -> np.ndarray:
    c = c / 255.0
    return np.where(c <= 0.04045, c / 12.92, ((c + 0.055) / 1.055) ** 2.4)

def rgb_to_oklab_array(r: np.ndarray, g: np.ndarray, b: np.ndarray) :
    r = _srgb_to_linear(r)
    g = _srgb_to_linear(g)
    b = _srgb_to_linear(b)

    l = 0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b
    m = 0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b
    s = 0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b

    l_ = np.cbrt(l)
    m_ = np.cbrt(m)
    s_ = np.cbrt(s)

    L = 0.2104542553 * l_ + 0.7936177850 * m_ - 0.0040720468 * s_
    a = 1.9779984951 * l_ - 2.4285922050 * m_ + 0.4505937099 * s_
    b2 = 0.0259040371 * l_ + 0.7827717662 * m_ - 0.8086757660 * s_
    return L, a, b2

# ---------- palette loader ----------
def load_palette_csv(path: str, ignore: Optional[List[int]] = None) :
    """
    读取 palette.csv（index,r,g,b）
    返回：
      pal_rgb  [P_use,3] float32  —— 参与匹配的 RGB（已过滤 ignore）
      pal_idx  [P_use]   int32    —— 与 pal_rgb 一一对应的“原始调色板索引”
    """
    idx_to_rgb = {}
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            i, r, g, b = map(int, row[:4])
            idx_to_rgb[i] = (r, g, b)
    if not idx_to_rgb:
        raise ValueError(f"empty palette: {path}")

    ig = set(ignore or [])
    pairs = [(i, rgb) for i, rgb in idx_to_rgb.items() if i not in ig]
    pairs.sort(key=lambda x: x[0])  # 按原始 index 排个序（非必须，只为稳定）

    pal_idx = np.array([p[0] for p in pairs], dtype=np.int32)                # 原始调色板索引
    pal_rgb = np.array([p[1] for p in pairs], dtype=np.float32)              # 对应 RGB
    if pal_idx.size == 0:
        raise ValueError("all palette indices are ignored; nothing to build")

    return pal_rgb, pal_idx

# ---------- build LUT (single file; values = ORIGINAL index) ----------
def build_oklab_lut_write_original_index(
    palette_rgb255: np.ndarray,
    palette_orig_idx: np.ndarray,
    output_path: str,
    dtype_uint16: bool = False,
):
    """
    用 OKLab 距离构建 3D LUT（[256,256,256]），最终值写入“原始调色板索引”。
    可选 dtype：uint8(默认) 或 uint16。
    """
    # 预计算调色板的 OKLab
    pr, pg, pb = palette_rgb255[:, 0], palette_rgb255[:, 1], palette_rgb255[:, 2]
    pL, pA, pB = rgb_to_oklab_array(pr, pg, pb)
    palette_oklab = np.stack([pL, pA, pB], axis=1).astype(np.float32)  # [P,3]

    out_dtype = np.uint16 if dtype_uint16 else np.uint8
    result = np.empty((256, 256, 256), dtype=out_dtype)

    g_grid, b_grid = np.mgrid[0:256, 0:256].astype(np.float32)

    t0 = time.time()
    for r in range(256):
        if r % 8 == 0:
            print(f"slice {r}/255 elapsed={time.time() - t0:.1f}s")
        r_plane = np.full_like(g_grid, float(r))
        L, A, B = rgb_to_oklab_array(r_plane, g_grid, b_grid)  # [256,256] each
        img = np.stack([L, A, B], axis=0).astype(np.float32)   # [3,256,256]

        # 距离：广播 [P,3,256,256] -> sum over axis=1 -> [P,256,256]
        diff = img[None, ...] - palette_oklab[:, :, None, None]
        dist2 = (diff ** 2).sum(axis=1)

        # 找到最近色在“候选集合”的下标（0..P_use-1）
        nearest_idx_in_candidates = dist2.argmin(axis=0)  # [256,256] int64
        # 映射成“原始调色板索引”
        nearest_raw_index = palette_orig_idx[nearest_idx_in_candidates]  # int32 -> 0..255

        # 写入结果（强转到目标 dtype）
        result[r] = nearest_raw_index.astype(out_dtype, copy=False)

    # np.save(output_path, result)
    print(f"saved LUT (values=original indices) to {output_path}; time={time.time()-t0:.1f}s; dtype={out_dtype}")
    return result

def save_lut_by_ext(arr: np.ndarray, output_path: str):
    """
    根据扩展名保存 LUT：
      - .npy         -> np.save(arr)
      - .lut/.bin    -> 裸字节（一维 C-order）
      - .lut.gz/.bin.gz -> 裸字节 gzip 压缩
    对于 uint16，会用 little-endian 写出（Java 侧读取时按 LE 解析）。
    """
    path = output_path
    lower = path.lower()

    # .npy：直接保存
    if lower.endswith(".npy"):
        np.save(path, arr)
        print(f"saved numpy array to {path} shape={arr.shape} dtype={arr.dtype}")
        return

    # 准备扁平字节
    if arr.dtype == np.uint16:
        # 明确 little-endian，便于 Java ByteBuffer.order(LITTLE_ENDIAN) 读取
        blob = arr.astype("<u2", copy=False).tobytes(order="C")
    elif arr.dtype == np.uint8:
        blob = arr.astype(np.uint8, copy=False).tobytes(order="C")
    else:
        raise ValueError(f"unsupported dtype for LUT: {arr.dtype}")

    # .lut.gz / .bin.gz
    if lower.endswith(".lut.gz") or lower.endswith(".bin.gz"):
        with gzip.open(path, "wb") as f:
            f.write(blob)
        print(f"saved gzip-compressed LUT to {path} bytes(uncompressed)={len(blob)}")
        return

    # .lut / .bin
    if lower.endswith(".lut") or lower.endswith(".bin"):
        with open(path, "wb") as f:
            f.write(blob)
        print(f"saved raw LUT to {path} bytes={len(blob)}")
        return

    # 其他未识别：默认按 .lut 处理
    with open(path, "wb") as f:
        f.write(blob)
    print(f"saved raw LUT (default) to {path} bytes={len(blob)}")


def main(palette_csv: str, out_lut_path: str, ignore: Optional[List[int]], use_uint16: bool):
    pal_rgb, pal_idx = load_palette_csv(palette_csv, ignore)
    print(f"palette loaded: {pal_idx.size} usable entries "
          f"({'ignored '+str(ignore) if ignore else 'no ignore'})")
    lut = build_oklab_lut_write_original_index(
        palette_rgb255=pal_rgb,
        palette_orig_idx=pal_idx,
        output_path=out_lut_path,
        dtype_uint16=use_uint16,
    )
    save_lut_by_ext(lut, out_lut_path)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Build OKLab RGB->palette 3D LUT (single file, values=original indices)")
    p.add_argument("--palette", "-p", type=str, default="palette.csv", help="path to palette.csv (index,r,g,b)")
    p.add_argument("--output", "-o", type=str, default="colormap_oklab.npy", help="output LUT .npy (shape 256x256x256)")
    p.add_argument("--ignore", "-i", type=int, nargs="*", default=None,
                   help="palette indices to ignore during nearest-color search (e.g. 0 1 2 3)")
    p.add_argument("--uint16", action="store_true",
                   help="store LUT as uint16 (default uint8). Only needed if your indices go beyond 0..255.")
    args = p.parse_args()
    main(args.palette, args.output, args.ignore, args.uint16)

# 用法：
#   python prepare_colormap.py --palette palette.csv --output colormap_oklab.npy --ignore 0 1 2 3

# python prepare_colormap.py -p palette.csv -o colormap.lut --ignore 0 1 2 3
