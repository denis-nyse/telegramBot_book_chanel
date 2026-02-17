import json
import mimetypes
import re
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from time import sleep
from urllib import error, request

try:
    from PIL import Image
except ImportError as exc:
    raise RuntimeError(
        "Pillow is required. Install it in venv: "
        "telegram_book/venv/bin/pip install -r telegram_book/requirements.txt"
    ) from exc

from config import BOT_TOKEN, CHANNEL_ID, FILES_DIR, MAX_FILE_SIZE_MB, POST_DELAY_SECONDS

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff"}
SCRIPT_FILES = {"upload_to_channel.py", "config.py", "__pycache__"}
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
SKIPPED_TOO_LARGE_REPORT = Path(__file__).parent / "skipped_too_large.txt"


class TelegramRequestTooLargeError(RuntimeError):
    pass


def build_pairs(folder: Path):
    items = [p for p in folder.iterdir() if p.is_file() and p.name not in SCRIPT_FILES]
    by_stem = {}
    for file_path in items:
        stem = normalize_stem(file_path.stem)
        by_stem.setdefault(stem, []).append(file_path)

    pairs = []
    missing = []

    for stem in sorted(by_stem.keys(), key=str.lower):
        group = sorted(by_stem[stem], key=lambda p: p.suffix.lower())
        images = [p for p in group if p.suffix.lower() in IMAGE_EXTENSIONS]
        books = [p for p in group if p.suffix.lower() not in IMAGE_EXTENSIONS]

        if not images or not books:
            missing.append(stem)
            continue

        pairs.append((stem, images[0], books[0]))

    return pairs, missing


def normalize_stem(stem: str) -> str:
    normalized = stem.strip()
    # Preview files are often named like "<book>.cover.tiff".
    if normalized.lower().endswith(".cover"):
        normalized = normalized[: -len(".cover")]
    return normalized.strip()


def encode_multipart(fields, files):
    boundary = f"----TelegramBoundary{uuid.uuid4().hex}"
    chunks = []

    for key, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode(),
                f"{value}\r\n".encode(),
            ]
        )

    for field_name, file_path in files:
        mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        with file_path.open("rb") as f:
            file_data = f.read()
        chunks.extend(
            [
                f"--{boundary}\r\n".encode(),
                (
                    f'Content-Disposition: form-data; name="{field_name}"; '
                    f'filename="{file_path.name}"\r\n'
                ).encode(),
                f"Content-Type: {mime_type}\r\n\r\n".encode(),
                file_data,
                b"\r\n",
            ]
        )

    chunks.append(f"--{boundary}--\r\n".encode())
    body = b"".join(chunks)
    content_type = f"multipart/form-data; boundary={boundary}"
    return body, content_type


def telegram_call(method: str, fields: dict, files: list):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    body, content_type = encode_multipart(fields, files)

    req = request.Request(
        url=url,
        data=body,
        headers={"Content-Type": content_type, "Content-Length": str(len(body))},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=60) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        if exc.code == 413:
            raise TelegramRequestTooLargeError(f"HTTP {exc.code}: {details}") from exc
        raise RuntimeError(f"HTTP {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Network error: {exc}") from exc

    if not payload.get("ok"):
        raise RuntimeError(f"Telegram API error: {payload}")
    return payload


def upload_pair(stem: str, image_path: Path, book_path: Path):
    book_size = book_path.stat().st_size

    if book_size > MAX_FILE_SIZE_BYTES:
        raise TelegramRequestTooLargeError(
            f"Book is too large: {book_path.name} ({format_size(book_size)})"
        )

    with prepared_image_for_upload(image_path, stem) as ready_image_path:
        image_size = ready_image_path.stat().st_size
        if image_size > MAX_FILE_SIZE_BYTES:
            raise TelegramRequestTooLargeError(
                f"Image is too large: {ready_image_path.name} ({format_size(image_size)})"
            )

        telegram_call(
            "sendPhoto",
            fields={"chat_id": CHANNEL_ID, "caption": stem},
            files=[("photo", ready_image_path)],
        )

    telegram_call(
        "sendDocument",
        fields={"chat_id": CHANNEL_ID},
        files=[("document", book_path)],
    )


@contextmanager
def prepared_image_for_upload(image_path: Path, stem: str):
    if image_path.suffix.lower() not in {".tif", ".tiff"}:
        yield image_path
        return

    safe_name = sanitize_filename(stem) or "cover"
    with tempfile.TemporaryDirectory(prefix="telegram_cover_") as tmp_dir:
        jpg_path = Path(tmp_dir) / f"{safe_name}.jpg"
        with Image.open(image_path) as src:
            rgb = src.convert("RGB")
            rgb.save(jpg_path, format="JPEG", quality=90, optimize=True)
        yield jpg_path


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", name).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned[:180]


def format_size(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.2f} MB"


def main():
    if BOT_TOKEN.startswith("PASTE_") or CHANNEL_ID.startswith("PASTE_"):
        raise RuntimeError("Set BOT_TOKEN and CHANNEL_ID in telegram_book/config.py")

    folder = Path(FILES_DIR)
    if not folder.exists():
        raise RuntimeError(f"Folder not found: {folder}")

    pairs, missing = build_pairs(folder)

    if not pairs:
        print(f"No valid pairs found in: {folder}")
        return

    print(f"Found pairs: {len(pairs)}")
    if missing:
        print(f"Skipped (missing image or book): {len(missing)}")

    uploaded = 0
    too_large = 0
    failed = 0
    too_large_details = []

    for index, (stem, image_path, book_path) in enumerate(pairs, start=1):
        print(f"[{index}/{len(pairs)}] {stem}")
        try:
            upload_pair(stem, image_path, book_path)
            uploaded += 1
        except TelegramRequestTooLargeError as exc:
            too_large += 1
            reason = str(exc)
            too_large_details.append(f"{stem} | {reason}")
            print(f"  skipped (too large): {reason}")
        except Exception as exc:
            failed += 1
            print(f"  failed: {exc}")
        sleep(POST_DELAY_SECONDS)

    if too_large_details:
        SKIPPED_TOO_LARGE_REPORT.write_text(
            "\n".join(too_large_details) + "\n",
            encoding="utf-8",
        )
        print(f"Saved skipped list: {SKIPPED_TOO_LARGE_REPORT}")

    print("Done.")
    print(f"Uploaded: {uploaded}")
    print(f"Skipped (too large): {too_large}")
    print(f"Failed (other errors): {failed}")


if __name__ == "__main__":
    main()
