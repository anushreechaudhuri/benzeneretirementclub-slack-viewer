#!/usr/bin/env python3
"""
Export Discord images via DiscordChatExporter (Docker), classify with a VLM
(Gemini Flash), and upload matching ones to S3.

Required .env variables:
    DISCORD_TOKEN           - Discord user token (from browser DevTools)
    DISCORD_GUILD_ID        - Discord server ID
    GEMINI_API_KEY          - Google AI API key
    AWS_ACCESS_KEY_ID       - AWS credentials
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION      - e.g. us-east-1
    S3_BUCKET               - target S3 bucket name
    S3_PREFIX_DISCORD       - prefix/folder in bucket (e.g. "shared-photos-discord/")
    CLASSIFICATION_PROMPT   - (optional) custom VLM prompt, must expect YES/NO answer

Usage:
    python3 classify_discord_images.py export     # Export from Discord via Docker
    python3 classify_discord_images.py classify    # Classify exported images
    python3 classify_discord_images.py upload      # Upload matches to S3
    python3 classify_discord_images.py all         # Do everything
"""

import base64
import json
import subprocess
import sys
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading

warnings.filterwarnings("ignore")

SCRIPT_DIR = Path(__file__).parent
DISCORD_EXPORT_DIR = SCRIPT_DIR / "discord_export"
DISCORD_PROGRESS_FILE = SCRIPT_DIR / "discord_classify_progress.json"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".gif"}
MIN_FILE_SIZE = 10 * 1024  # 10KB
GEMINI_MODEL = "gemini-2.5-flash-lite"
CONCURRENCY = 20
SAVE_EVERY = 50

DEFAULT_PROMPT = (
    "Does this image contain a person? "
    "Answer ONLY 'YES' or 'NO'."
)


def load_env():
    env = {}
    env_file = SCRIPT_DIR / ".env"
    if not env_file.exists():
        print("ERROR: .env file not found. See README for required variables.")
        sys.exit(1)
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip().strip("'\"")
    return env


def run_discord_export(env):
    """Export all channels from a Discord server using DiscordChatExporter via Docker."""
    token = env.get("DISCORD_TOKEN")
    guild_id = env.get("DISCORD_GUILD_ID")

    if not token:
        print("ERROR: DISCORD_TOKEN not set in .env")
        print("To get your token:")
        print("  1. Open Discord in browser")
        print("  2. Press F12 > Network tab, filter by 'science'")
        print("  3. Click any request, find the Authorization header")
        sys.exit(1)

    if not guild_id:
        print("ERROR: DISCORD_GUILD_ID not set in .env")
        print("Enable Developer Mode (Settings > Advanced),")
        print("then right-click server name > Copy Server ID")
        sys.exit(1)

    DISCORD_EXPORT_DIR.mkdir(exist_ok=True)

    print(f"Exporting all channels from guild {guild_id}...")
    print(f"Output directory: {DISCORD_EXPORT_DIR}")

    cmd = [
        "docker", "run", "--rm",
        "-v", f"{DISCORD_EXPORT_DIR}:/out",
        "tyrrrz/discordchatexporter",
        "exportguild",
        "-t", token,
        "-g", guild_id,
        "-f", "Json",
        "--media",
        "-o", "/out",
    ]

    print("Running: docker run tyrrrz/discordchatexporter exportguild ...")
    result = subprocess.run(cmd, capture_output=False, text=True)

    if result.returncode != 0:
        print(f"Export failed with return code {result.returncode}")
        print("If this is a DM-only workspace, try exportdm instead:")
        print(f"  docker run --rm -v {DISCORD_EXPORT_DIR}:/out tyrrrz/discordchatexporter "
              f"exportdm -t TOKEN -f Json --media -o /out")
        return False

    images = list(collect_discord_images())
    print(f"Export complete! Found {len(images)} images.")
    return True


def collect_discord_images():
    """Find all images in the Discord export directory."""
    if not DISCORD_EXPORT_DIR.exists():
        return []
    images = []
    for path in DISCORD_EXPORT_DIR.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in IMAGE_EXTENSIONS:
            continue
        if path.stat().st_size < MIN_FILE_SIZE:
            continue
        images.append(path)
    return sorted(images)


def get_mime_type(path):
    ext = path.suffix.lower()
    return {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".webp": "image/webp",
        ".heic": "image/heic", ".gif": "image/gif",
    }.get(ext, "image/jpeg")


def resize_for_classification(image_path, max_dim=512):
    """Resize image to reduce upload size for VLM classification."""
    from PIL import Image
    import io

    try:
        with Image.open(image_path) as img:
            img.thumbnail((max_dim, max_dim))
            buf = io.BytesIO()
            img.convert("RGB").save(buf, format="JPEG", quality=70)
            return buf.getvalue(), "image/jpeg"
    except Exception:
        return image_path.read_bytes(), get_mime_type(image_path)


def classify_image(client, image_path, prompt):
    """Classify a single image using Gemini. Returns True if YES."""
    raw_bytes, mime = resize_for_classification(image_path)
    img_data = base64.b64encode(raw_bytes).decode()

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[{
            "role": "user",
            "parts": [
                {"inline_data": {"mime_type": mime, "data": img_data}},
                {"text": prompt}
            ]
        }],
    )
    text = response.text or ""
    return "YES" in text.strip().upper()


def load_progress():
    if DISCORD_PROGRESS_FILE.exists():
        with open(DISCORD_PROGRESS_FILE) as f:
            return json.load(f)
    return {"classified": {}, "errors": []}


def save_progress(progress):
    with open(DISCORD_PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def run_classification(env):
    """Classify all images using Gemini with concurrent requests."""
    from google import genai

    client = genai.Client(api_key=env["GEMINI_API_KEY"])
    prompt = env.get("CLASSIFICATION_PROMPT", DEFAULT_PROMPT)
    images = collect_discord_images()
    progress = load_progress()
    classified = progress["classified"]

    remaining = [img for img in images if str(img) not in classified]
    print(f"Total Discord images: {len(images)}")
    print(f"Already classified: {len(classified)}")
    print(f"Remaining: {len(remaining)}")
    print(f"Prompt: {prompt[:80]}...")

    if not remaining:
        print("All images already classified!")
        return progress

    yes_count = sum(1 for v in classified.values() if v)
    no_count = sum(1 for v in classified.values() if not v)
    done_count = len(classified)
    error_count = 0
    lock = threading.Lock()

    def process_one(img_path):
        nonlocal yes_count, no_count, done_count, error_count
        try:
            result = classify_image(client, img_path, prompt)
            with lock:
                classified[str(img_path)] = result
                done_count += 1
                if result:
                    yes_count += 1
                else:
                    no_count += 1
                label = "YES" if result else "NO"
                print(f"[{done_count}/{len(images)}] {label} - {img_path.name}")
                if done_count % SAVE_EVERY == 0:
                    save_progress(progress)
                    print(f"  ... saved. YES: {yes_count}, NO: {no_count}")
        except Exception as e:
            with lock:
                error_count += 1
                progress["errors"].append({"file": str(img_path), "error": str(e)[:200]})
                print(f"ERROR - {img_path.name}: {str(e)[:80]}")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        list(executor.map(process_one, remaining))

    save_progress(progress)
    print(f"\nDone! YES: {yes_count}, NO: {no_count}, Errors: {error_count}")
    return progress


def upload_to_s3(env, progress):
    """Upload YES-classified images to S3."""
    import boto3

    bucket = env["S3_BUCKET"]
    prefix = env.get("S3_PREFIX_DISCORD", "shared-photos-discord/")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
        region_name=env.get("AWS_DEFAULT_REGION", "us-east-1"),
    )

    yes_files = [Path(p) for p, v in progress["classified"].items() if v]
    print(f"\nUploading {len(yes_files)} Discord images to s3://{bucket}/{prefix}")

    uploaded = 0
    for i, img_path in enumerate(yes_files, 1):
        if not img_path.exists():
            continue
        try:
            rel = img_path.relative_to(DISCORD_EXPORT_DIR)
        except ValueError:
            rel = img_path.name
        s3_key = f"{prefix}{rel}"

        try:
            s3.upload_file(
                str(img_path), bucket, s3_key,
                ExtraArgs={"ContentType": get_mime_type(img_path)}
            )
            uploaded += 1
            print(f"[{i}/{len(yes_files)}] Uploaded {s3_key}")
        except Exception as e:
            print(f"[{i}/{len(yes_files)}] FAILED: {e}")

    print(f"\nUpload complete: {uploaded}/{len(yes_files)}")


def main():
    env = load_env()
    command = sys.argv[1] if len(sys.argv) > 1 else "all"

    if command in ("export", "all"):
        print("=" * 60)
        print("STEP 1: Exporting Discord images")
        print("=" * 60)
        run_discord_export(env)

    if command in ("classify", "all"):
        print("\n" + "=" * 60)
        print("STEP 2: Classifying Discord images")
        print("=" * 60)
        progress = run_classification(env)
    else:
        progress = load_progress()

    if command in ("upload", "all"):
        print("\n" + "=" * 60)
        print("STEP 3: Uploading to S3")
        print("=" * 60)
        upload_to_s3(env, progress)


if __name__ == "__main__":
    main()
