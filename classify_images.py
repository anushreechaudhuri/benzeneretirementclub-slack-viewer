#!/usr/bin/env python3
"""
Classify images from a Slack export using a VLM (Gemini Flash), then upload
matching ones to S3 for sharing.

Scans all images in the export directory, resizes them for efficient API usage,
classifies each with a configurable prompt, and uploads matches to S3.
Supports resume via a progress file.

Required .env variables:
    GEMINI_API_KEY          - Google AI API key
    AWS_ACCESS_KEY_ID       - AWS credentials
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION      - e.g. us-east-1
    S3_BUCKET               - target S3 bucket name
    S3_PREFIX               - prefix/folder in bucket (e.g. "shared-photos/")
    CLASSIFICATION_PROMPT   - (optional) custom VLM prompt, must expect YES/NO answer

Usage:
    python3 classify_images.py classify   # Classify images with VLM
    python3 classify_images.py upload     # Upload YES-classified images to S3
    python3 classify_images.py iam        # Create a read-only IAM user for sharing
    python3 classify_images.py all        # Do everything
"""

import base64
import json
import sys
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading

warnings.filterwarnings("ignore")

# --- Configuration ---
SCRIPT_DIR = Path(__file__).parent
EXPORT_DIR = SCRIPT_DIR / "official_export_with_files"
PROGRESS_FILE = SCRIPT_DIR / "classify_progress.json"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".heic"}
MIN_FILE_SIZE = 10 * 1024  # 10KB - skip thumbnails
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


def collect_images():
    """Find all candidate images (photos/screenshots, not thumbnails)."""
    images = []
    for path in EXPORT_DIR.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in IMAGE_EXTENSIONS:
            continue
        if path.stat().st_size < MIN_FILE_SIZE:
            continue
        images.append(path)
    return sorted(images)


def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"classified": {}, "errors": []}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def get_mime_type(path):
    ext = path.suffix.lower()
    return {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".webp": "image/webp",
        ".heic": "image/heic",
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


def classify_one(client, img_path, prompt):
    """Wrapper returning (path_str, result_or_None, error_or_None)."""
    try:
        result = classify_image(client, img_path, prompt)
        return (str(img_path), result, None)
    except Exception as e:
        return (str(img_path), None, str(e))


def run_classification(env):
    """Classify all images using Gemini with concurrent requests."""
    from google import genai

    client = genai.Client(api_key=env["GEMINI_API_KEY"])
    prompt = env.get("CLASSIFICATION_PROMPT", DEFAULT_PROMPT)
    images = collect_images()
    progress = load_progress()
    classified = progress["classified"]

    remaining = [img for img in images if str(img) not in classified]
    print(f"Total candidate images: {len(images)}")
    print(f"Already classified: {len(classified)}")
    print(f"Remaining: {len(remaining)}")
    print(f"Prompt: {prompt[:80]}...")

    if not remaining:
        print("All images already classified!")
        return progress

    yes_count = sum(1 for v in classified.values() if v)
    no_count = sum(1 for v in classified.values() if not v)
    done_count = len(classified)
    lock = threading.Lock()
    error_count = 0

    def process_result(future):
        nonlocal yes_count, no_count, done_count, error_count
        path_str, result, error = future.result()
        with lock:
            if error:
                error_count += 1
                progress["errors"].append({"file": path_str, "error": error[:200]})
                print(f"[{done_count}/{len(images)}] ERROR - {Path(path_str).name}: {error[:80]}")
            else:
                classified[path_str] = result
                done_count += 1
                if result:
                    yes_count += 1
                else:
                    no_count += 1
                label = "YES" if result else "NO"
                print(f"[{done_count}/{len(images)}] {label} - {Path(path_str).name}")

            if done_count % SAVE_EVERY == 0:
                save_progress(progress)
                print(f"  ... saved. YES: {yes_count}, NO: {no_count}, Errors: {error_count}")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = []
        for img_path in remaining:
            f = executor.submit(classify_one, client, img_path, prompt)
            f.add_done_callback(process_result)
            futures.append(f)
        for f in futures:
            f.result()

    save_progress(progress)
    print(f"\nClassification complete! YES: {yes_count}, NO: {no_count}, Errors: {error_count}")
    return progress


def upload_to_s3(env, progress):
    """Upload YES-classified images to S3."""
    import boto3

    bucket = env["S3_BUCKET"]
    prefix = env.get("S3_PREFIX", "shared-photos/")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
        region_name=env.get("AWS_DEFAULT_REGION", "us-east-1"),
    )

    yes_files = [Path(p) for p, v in progress["classified"].items() if v]
    print(f"\nUploading {len(yes_files)} images to s3://{bucket}/{prefix}")

    uploaded = 0
    for i, img_path in enumerate(yes_files, 1):
        if not img_path.exists():
            print(f"  SKIP (missing): {img_path.name}")
            continue

        rel = img_path.relative_to(EXPORT_DIR)
        s3_key = f"{prefix}{rel}"

        try:
            s3.upload_file(
                str(img_path), bucket, s3_key,
                ExtraArgs={"ContentType": get_mime_type(img_path)}
            )
            uploaded += 1
            print(f"[{i}/{len(yes_files)}] Uploaded {s3_key}")
        except Exception as e:
            print(f"[{i}/{len(yes_files)}] FAILED {img_path.name}: {e}")

    print(f"\nUpload complete: {uploaded}/{len(yes_files)} files")
    return uploaded


def create_readonly_iam_user(env):
    """Create an IAM user with read-only access to the S3 bucket for sharing."""
    import boto3

    bucket = env["S3_BUCKET"]
    prefix = env.get("S3_PREFIX", "shared-photos/")

    iam = boto3.client(
        "iam",
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
        region_name=env.get("AWS_DEFAULT_REGION", "us-east-1"),
    )

    username = f"{bucket}-readonly"
    policy_name = f"{bucket}-readonly-policy"

    policy_doc = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                f"arn:aws:s3:::{bucket}",
                f"arn:aws:s3:::{bucket}/{prefix}*"
            ]
        }]
    })

    try:
        iam.create_user(UserName=username)
        print(f"Created IAM user: {username}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"IAM user {username} already exists")

    iam.put_user_policy(
        UserName=username,
        PolicyName=policy_name,
        PolicyDocument=policy_doc,
    )
    print(f"Attached read-only S3 policy")

    try:
        key_response = iam.create_access_key(UserName=username)
        access_key = key_response["AccessKey"]
        print(f"\n{'='*60}")
        print(f"READ-ONLY CREDENTIALS:")
        print(f"{'='*60}")
        print(f"AWS Access Key ID:     {access_key['AccessKeyId']}")
        print(f"AWS Secret Access Key: {access_key['SecretAccessKey']}")
        print(f"Region:                {env.get('AWS_DEFAULT_REGION', 'us-east-1')}")
        print(f"Bucket:                {bucket}")
        print(f"Prefix:                {prefix}")
        print(f"")
        print(f"To download, install AWS CLI and run:")
        print(f"  aws configure  (enter the keys above)")
        print(f"  aws s3 sync s3://{bucket}/{prefix} ./downloaded-photos")
        print(f"{'='*60}")

        creds_file = SCRIPT_DIR / "shared_credentials.txt"
        with open(creds_file, "w") as f:
            f.write(f"AWS Access Key ID: {access_key['AccessKeyId']}\n")
            f.write(f"AWS Secret Access Key: {access_key['SecretAccessKey']}\n")
            f.write(f"Region: {env.get('AWS_DEFAULT_REGION', 'us-east-1')}\n")
            f.write(f"Bucket: {bucket}\n")
            f.write(f"Download command: aws s3 sync s3://{bucket}/{prefix} ./downloaded-photos\n")
        print(f"\nCredentials also saved to: {creds_file}")

    except Exception as e:
        print(f"Error creating access key: {e}")


def main():
    env = load_env()

    if len(sys.argv) > 1:
        command = sys.argv[1]
    else:
        command = "all"

    if command in ("classify", "all"):
        print("=" * 60)
        print("STEP 1: Classifying images with Gemini Flash")
        print("=" * 60)
        progress = run_classification(env)
    else:
        progress = load_progress()

    if command in ("upload", "all"):
        print("\n" + "=" * 60)
        print("STEP 2: Uploading matching images to S3")
        print("=" * 60)
        upload_to_s3(env, progress)

    if command in ("iam", "all"):
        print("\n" + "=" * 60)
        print("STEP 3: Creating read-only IAM user")
        print("=" * 60)
        create_readonly_iam_user(env)


if __name__ == "__main__":
    main()
