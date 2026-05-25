'''Repair stored driver-license scans with the current crop/scale logic.'''

import argparse
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from PIL import Image, ImageOps, UnidentifiedImageError

import config as cfg
from club_admin.app import (
    DRIVER_LICENSE_IMAGE_SIZE,
    SUPPORTED_DOCUMENT_IMAGE_SUFFIXES,
    _driver_license_crop_mask,
    _prepare_driver_license_image,
)


LICENSE_CARD_ASPECT = 1.58


@dataclass(frozen=True, kw_only=True)
class RepairResult:
    path: Path
    status: str
    backup_path: Path | None = None
    error: str = ""


def _normalized_rgb(image: Image.Image) -> Image.Image:
    normalized = ImageOps.exif_transpose(image)
    if normalized.mode not in {"RGB", "L"}:
        normalized = normalized.convert("RGBA")
        background = Image.new("RGBA", normalized.size, "WHITE")
        background.alpha_composite(normalized)
        normalized = background.convert("RGB")
    else:
        normalized = normalized.convert("RGB")
    return normalized


def _image_paths(scan_dir: Path) -> list[Path]:
    if not scan_dir.is_dir():
        raise ValueError(f"Directory is not readable: {scan_dir}")
    return sorted(
        (
            path
            for path in scan_dir.iterdir()
            if path.is_file() and path.suffix.casefold() in SUPPORTED_DOCUMENT_IMAGE_SUFFIXES
        ),
        key=lambda path: str(path).casefold(),
    )


def _backup_path_for(path: Path, documents_dir: Path, backup_root: Path) -> Path:
    try:
        relative_path = path.relative_to(documents_dir)
    except ValueError:
        relative_path = Path(path.name)
    return backup_root / relative_path


def _clusters(values: list[int], *, max_gap: int, min_extent: int) -> list[tuple[int, int]]:
    if not values:
        return []
    clusters: list[tuple[int, int]] = []
    start = values[0]
    end = values[0]
    for value in values[1:]:
        if value - end > max_gap:
            if end - start + 1 >= min_extent:
                clusters.append((start, end + 1))
            start = value
        end = value
    if end - start + 1 >= min_extent:
        clusters.append((start, end + 1))
    return clusters


def _stored_scan_content_bbox(image: Image.Image) -> tuple[int, int, int, int] | None:
    mask = _driver_license_crop_mask(image)
    mask_pixels = mask.load()
    width, height = mask.size
    edge_x = max(12, int(width * 0.025))
    inner_left = min(edge_x, width)
    inner_right = max(inner_left, width - edge_x)
    inner_width = max(1, inner_right - inner_left)
    min_row_pixels = max(3, int(inner_width * 0.0015))
    row_counts = {
        y: sum(1 for x in range(inner_left, inner_right) if mask_pixels[x, y])
        for y in range(height)
    }
    content_rows = [
        y
        for y, count in row_counts.items()
        if count >= min_row_pixels
    ]
    row_clusters = _clusters(
        content_rows,
        max_gap=max(10, int(height * 0.015)),
        min_extent=max(18, int(height * 0.025)),
    )
    if not row_clusters:
        return None

    top_half = height // 2
    row_clusters.sort(
        key=lambda cluster: (
            cluster[0] >= top_half,
            -sum(row_counts[y] for y in range(cluster[0], cluster[1])),
            cluster[0],
        )
    )
    top, bottom = row_clusters[0]

    min_column_pixels = max(3, int((bottom - top) * 0.015))
    column_counts = {
        x: sum(1 for y in range(top, bottom) if mask_pixels[x, y])
        for x in range(inner_left, inner_right)
    }
    content_columns = [
        x
        for x, count in column_counts.items()
        if count >= min_column_pixels
    ]
    if not content_columns:
        return None

    left = min(content_columns)
    right = max(content_columns) + 1
    detected_width = right - left
    detected_height = bottom - top
    expected_height = int(detected_width / LICENSE_CARD_ASPECT)
    expected_width = int(detected_height * LICENSE_CARD_ASPECT)
    if expected_height > detected_height:
        bottom = max(bottom, top + expected_height)
    if expected_width > detected_width:
        expansion = expected_width - detected_width
        left -= expansion // 2
        right += expansion - (expansion // 2)

    horizontal_padding = max(12, int((right - left) * 0.04))
    vertical_padding = max(12, int((bottom - top) * 0.04))
    return (
        max(0, left - horizontal_padding),
        max(0, top - vertical_padding),
        min(width, right + horizontal_padding),
        min(height, bottom + vertical_padding),
    )


def _prepare_stored_driver_license_image(image: Image.Image) -> Image.Image:
    normalized = _normalized_rgb(image)
    content_bbox = _stored_scan_content_bbox(normalized)
    if content_bbox is not None:
        normalized = normalized.crop(content_bbox)

    contained = ImageOps.contain(
        normalized,
        DRIVER_LICENSE_IMAGE_SIZE,
        method=Image.Resampling.LANCZOS,
    )
    canvas = Image.new("RGB", DRIVER_LICENSE_IMAGE_SIZE, "WHITE")
    canvas.paste(
        contained,
        (
            (DRIVER_LICENSE_IMAGE_SIZE[0] - contained.width) // 2,
            (DRIVER_LICENSE_IMAGE_SIZE[1] - contained.height) // 2,
        ),
    )
    return canvas


def _repair_one(
    path: Path,
    *,
    documents_dir: Path,
    apply: bool,
    backup_root: Path | None,
    mode: str,
) -> RepairResult:
    try:
        with Image.open(path) as image:
            if mode == "original-scan":
                repaired = _prepare_driver_license_image(image)
            else:
                repaired = _prepare_stored_driver_license_image(image)
    except (OSError, UnidentifiedImageError) as error:
        return RepairResult(path=path, status="error", error=str(error))

    if not apply:
        return RepairResult(path=path, status="would repair")

    backup_path = None
    if backup_root is not None:
        backup_path = _backup_path_for(path, documents_dir, backup_root)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, backup_path)

    temp_path = path.with_name(f".{path.name}.repairing")
    try:
        repaired.save(temp_path, format="JPEG", quality=92, optimize=True)
        temp_path.replace(path)
    except OSError as error:
        if temp_path.exists():
            temp_path.unlink()
        return RepairResult(path=path, status="error", backup_path=backup_path, error=str(error))

    return RepairResult(path=path, status="repaired", backup_path=backup_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair stored driver-license scans using the club app crop/scale logic.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Specific image files to repair. If omitted, repair image files directly inside --dir.",
    )
    parser.add_argument(
        "--dir",
        dest="scan_dir",
        default=cfg.USER_MANAGEMENT_DOCUMENTS_DIR,
        help="Directory containing image files to repair. Defaults to USER_MANAGEMENT_DOCUMENTS_DIR.",
    )
    parser.add_argument(
        "--documents-dir",
        dest="scan_dir",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Rewrite files. Without this, only prints what would be repaired.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not copy originals before rewriting. Only valid with --apply.",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        help="Backup directory for originals. Defaults to a timestamped sibling of the documents dir.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Repair at most this many files.",
    )
    parser.add_argument(
        "--mode",
        choices=("stored-scan", "original-scan"),
        default="stored-scan",
        help=(
            "stored-scan repairs files already saved with whitespace around the ID; "
            "original-scan uses the app upload cropper for raw scanner uploads."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    scan_dir_value = str(args.scan_dir or "").strip()
    if not scan_dir_value and not args.paths:
        print("Directory is not configured. Pass --dir or file paths.")
        return 2

    scan_dir = Path(scan_dir_value or ".").expanduser().resolve(strict=False)
    if args.paths:
        target_paths = [path.expanduser().resolve(strict=False) for path in args.paths]
    else:
        try:
            target_paths = _image_paths(scan_dir)
        except ValueError as error:
            print(error)
            return 2

    if args.limit is not None:
        target_paths = target_paths[: max(0, args.limit)]

    backup_root = None
    if args.apply and not args.no_backup:
        if args.backup_dir is not None:
            backup_root = args.backup_dir.expanduser().resolve(strict=False)
        else:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            backup_root = (
                scan_dir.parent
                / f"{scan_dir.name}-driver-license-repair-backups"
                / timestamp
            )

    if not target_paths:
        print("No image files found.")
        return 0

    print(
        f"{'Repairing' if args.apply else 'Dry run for'} {len(target_paths)} file(s)."
    )
    if backup_root is not None:
        print(f"Backups: {backup_root}")

    error_count = 0
    for path in target_paths:
        result = _repair_one(
            path,
            documents_dir=scan_dir,
            apply=args.apply,
            backup_root=backup_root,
            mode=args.mode,
        )
        if result.status == "error":
            error_count += 1
            print(f"error: {result.path}: {result.error}")
        elif result.backup_path is not None:
            print(f"{result.status}: {result.path} (backup: {result.backup_path})")
        else:
            print(f"{result.status}: {result.path}")

    if not args.apply:
        print("No files changed. Re-run with --apply to write repairs.")
    return 1 if error_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
