from __future__ import annotations

import os
import shutil
import struct
import subprocess
from pathlib import Path
from typing import Iterable


BITRATES = {
    # MPEG1 Layer3
    (3, 1): [
        None,
        32,
        40,
        48,
        56,
        64,
        80,
        96,
        112,
        128,
        160,
        192,
        224,
        256,
        320,
        None,
    ],
    # MPEG2/2.5 Layer3
    (2, 1): [
        None,
        8,
        16,
        24,
        32,
        40,
        48,
        56,
        64,
        80,
        96,
        112,
        128,
        144,
        160,
        None,
    ],
    (0, 1): [
        None,
        8,
        16,
        24,
        32,
        40,
        48,
        56,
        64,
        80,
        96,
        112,
        128,
        144,
        160,
        None,
    ],
}

SAMPLE_RATES = {
    3: [44100, 48000, 32000, None],  # MPEG1
    2: [22050, 24000, 16000, None],  # MPEG2
    0: [11025, 12000, 8000, None],  # MPEG2.5
}


def validate_mp3_and_get_duration_seconds(path: Path) -> tuple[int, str | None]:
    if not path.exists():
        return 0, "file-not-found"
    if path.stat().st_size == 0:
        return 0, "empty-file"
    try:
        duration = _probe_with_ffprobe(path)
        if duration > 0:
            return duration, None
    except Exception:
        pass
    try:
        duration = _estimate_duration_from_headers(path)
        if duration > 0:
            return duration, None
        return 0, "unable-to-parse-mp3-duration"
    except Exception as exc:
        return 0, f"mp3-parse-error:{exc}"


def _probe_with_ffprobe(path: Path) -> int:
    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        return 0
    proc = subprocess.run(
        [
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return 0
    raw = proc.stdout.strip()
    if not raw:
        return 0
    return max(0, int(float(raw)))


def _skip_id3v2(data: bytes) -> int:
    if len(data) < 10 or not data.startswith(b"ID3"):
        return 0
    size_bytes = data[6:10]
    size = (
        ((size_bytes[0] & 0x7F) << 21)
        | ((size_bytes[1] & 0x7F) << 14)
        | ((size_bytes[2] & 0x7F) << 7)
        | (size_bytes[3] & 0x7F)
    )
    return 10 + size


def _find_frame_header(data: bytes, start: int = 0) -> int:
    for i in range(start, len(data) - 4):
        if data[i] == 0xFF and (data[i + 1] & 0xE0) == 0xE0:
            return i
    return -1


def _estimate_duration_from_headers(path: Path) -> int:
    with path.open("rb") as f:
        head = f.read(65536)

    offset = _skip_id3v2(head)
    frame_pos = _find_frame_header(head, offset)
    if frame_pos < 0:
        return 0

    header = struct.unpack(">I", head[frame_pos : frame_pos + 4])[0]
    version_bits = (header >> 19) & 0x3
    layer_bits = (header >> 17) & 0x3
    bitrate_idx = (header >> 12) & 0xF
    sample_idx = (header >> 10) & 0x3
    channel_mode = (header >> 6) & 0x3

    if layer_bits != 1:
        return 0

    version_key = {0: 0, 2: 2, 3: 3}.get(version_bits)
    if version_key is None:
        return 0

    bitrate_kbps = BITRATES.get((version_key, 1), [None] * 16)[bitrate_idx]
    sample_rate = SAMPLE_RATES.get(version_key, [None] * 4)[sample_idx]
    if bitrate_kbps is None or sample_rate is None:
        return 0

    # Try VBR frame count from Xing/Info if available.
    mpeg1 = version_key == 3
    side_info = 17 if channel_mode == 3 else 32
    xing_offset = frame_pos + 4 + (side_info if mpeg1 else (9 if channel_mode == 3 else 17))
    if xing_offset + 16 < len(head):
        tag = head[xing_offset : xing_offset + 4]
        if tag in (b"Xing", b"Info"):
            flags = struct.unpack(">I", head[xing_offset + 4 : xing_offset + 8])[0]
            if flags & 0x1:
                frames = struct.unpack(">I", head[xing_offset + 8 : xing_offset + 12])[0]
                samples_per_frame = 1152
                duration = int((frames * samples_per_frame) / sample_rate)
                if duration > 0:
                    return duration

    audio_bytes = max(1, os.path.getsize(path) - frame_pos)
    duration = int((audio_bytes * 8) / (bitrate_kbps * 1000))
    return max(0, duration)


def merge_mp3_files(
    file_paths: Iterable[Path],
    output_path: Path,
    loudnorm_enabled: bool,
    ffmpeg_path: str | None = None,
) -> str:
    files = [Path(p) for p in file_paths]
    if not files:
        raise ValueError("no input files")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    ffmpeg = ffmpeg_path or shutil.which("ffmpeg")
    if ffmpeg:
        list_file = output_path.with_suffix(".concat.txt")

        def _quote_concat_path(p: Path) -> str:
            return str(p.resolve()).replace("'", "'\\''")

        list_file.write_text(
            "".join(f"file '{_quote_concat_path(p)}'\n" for p in files),
            encoding="utf-8",
        )
        try:
            cmd = [ffmpeg, "-y", "-f", "concat", "-safe", "0", "-i", str(list_file)]
            if loudnorm_enabled:
                cmd += ["-af", "loudnorm=I=-14:TP=-1.5:LRA=11"]
            cmd += ["-c:a", "libmp3lame", "-b:a", "192k", str(output_path)]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:
            raise RuntimeError(f"ffmpeg failed: {exc}") from exc
        finally:
            list_file.unlink(missing_ok=True)
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg failed: {proc.stderr.strip()}")
        return "merged-with-ffmpeg"

    # Fallback for environments without ffmpeg: byte concatenation.
    with output_path.open("wb") as out:
        for p in files:
            out.write(p.read_bytes())
    return "merged-with-byte-concat-fallback"
