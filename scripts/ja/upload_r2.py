"""
ja_r2_processor.py: Upload of ja360.pmtiles file to Cloudflare R2

Required environment variables:
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET
"""

import hashlib
import logging
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv


def setup_logger():
    """Configure the logging system"""
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)


class ProgressPercentage:
    """Class to display upload progress"""

    def __init__(self, filename):
        self._size = float(Path(filename).stat().st_size)
        self._seen = 0
        self._lock = threading.Lock()
        self._start_time = time.time()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen += bytes_amount
            percentage = (self._seen / self._size) * 100

            elapsed = time.time() - self._start_time
            if elapsed > 0:
                speed_mbps = (self._seen / (1024 * 1024)) / elapsed
                remaining_bytes = self._size - self._seen
                eta_seconds = remaining_bytes / (self._seen / elapsed) if self._seen > 0 else 0
                eta_str = (
                    f"{int(eta_seconds // 60)}m{int(eta_seconds % 60)}s"
                    if eta_seconds > 0
                    else "0s"
                )
            else:
                speed_mbps = 0
                eta_str = "calculating..."

            sys.stdout.write(
                f"\r🚀 {self._seen / (1024 * 1024):.1f}/{self._size / (1024 * 1024):.1f} MB "
                f"({percentage:.1f}%) - {speed_mbps:.1f} MB/s - ETA: {eta_str}     "
            )
            sys.stdout.flush()


def load_env():
    """Load environment variables from .env files"""
    possible_env_paths = [
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parents[1] / ".env",
        Path(__file__).resolve().parents[2] / ".env",
    ]

    for env_path in possible_env_paths:
        if env_path.is_file():
            load_dotenv(dotenv_path=env_path)
            break
    else:
        load_dotenv()


def get_client_and_bucket():
    """Configure and return R2 client and bucket name"""
    access_key = os.getenv("R2_ACCESS_KEY_ID") or os.getenv("Access_Key_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("Secret_Access_Key")
    account = os.getenv("R2_ACCOUNT_ID")
    endpoint = os.getenv("Endpoint")
    bucket = os.getenv("R2_BUCKET")

    if not account and endpoint:
        host = urlparse(endpoint).hostname
        account = host.split(".")[0] if host else None

    if not all([access_key, secret_key, account, bucket]):
        logging.error(
            "❌ Missing R2 variables: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET"
        )
        sys.exit(1)

    url = endpoint or f"https://{account}.r2.cloudflarestorage.com"

    client = boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )

    return client, bucket


def find_ja360_pmtiles():
    """Search for ja360.pmtiles file in possible paths"""
    custom_path = os.getenv("JA360_PMTILES_PATH")
    if custom_path and Path(custom_path).is_file():
        return Path(custom_path)

    search_paths = [
        Path.cwd() / "ja360.pmtiles",
        Path(__file__).resolve().parents[2] / "data" / "ja360" / "tile" / "ja360.pmtiles",
        Path(__file__).resolve().parents[1] / "data" / "ja360" / "tile" / "ja360.pmtiles",
        Path(__file__).resolve().parent / "data" / "ja360" / "tile" / "ja360.pmtiles",
        Path(__file__).resolve().parent / "ja360.pmtiles",
        Path(__file__).resolve().parents[1] / "data" / "ja360.pmtiles",
    ]

    for path in search_paths:
        if path.is_file():
            return path

    return None


def calculate_file_hash(file_path):
    """Calculate MD5 hash of the file"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def verify_upload(client, bucket, object_key, expected_size):
    """Verify that upload completed successfully"""
    try:
        response = client.head_object(Bucket=bucket, Key=object_key)
        return response["ContentLength"] == expected_size
    except Exception:
        return False


def main():
    """Main function for R2 upload"""
    setup_logger()
    load_env()

    pmtiles_path = find_ja360_pmtiles()
    if not pmtiles_path:
        logging.error("❌ ja360.pmtiles file not found")
        logging.error("💡 Generate PMTiles first with: python scripts/ja/ja_tile_generator.py")
        sys.exit(1)

    file_size = pmtiles_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)

    logging.info(f"📁 {pmtiles_path} ({file_size_mb:.1f} MB)")

    file_hash = calculate_file_hash(pmtiles_path)

    part_size_mb = max(int(os.getenv("PART_SIZE_MB", "64")), 5)
    part_size = part_size_mb * 1024 * 1024

    transfer_config = TransferConfig(
        multipart_threshold=part_size,
        multipart_chunksize=part_size,
        max_concurrency=10,
        use_threads=True,
    )

    upload_key = os.getenv("UPLOAD_KEY", "ja360.pmtiles")
    client, bucket = get_client_and_bucket()
    progress = ProgressPercentage(str(pmtiles_path))

    start_time = time.time()

    try:
        extra_args = {
            "ContentType": "application/x-protobuf",
            "Metadata": {
                "original-filename": pmtiles_path.name,
                "file-hash": file_hash,
                "upload-time": datetime.now().isoformat(),
                "file-size": str(file_size),
                "data-source": "JA360",
                "tile-type": "raster-coverage",
                "zoom-range": "1-16",
            },
        }

        client.upload_file(
            Filename=str(pmtiles_path),
            Bucket=bucket,
            Key=upload_key,
            Config=transfer_config,
            Callback=progress,
            ExtraArgs=extra_args,
        )

        sys.stdout.write("\n")

        upload_time = time.time() - start_time
        avg_speed = file_size_mb / upload_time if upload_time > 0 else 0

        logging.info(f"✅ Upload completed in {upload_time:.1f}s ({avg_speed:.1f} MB/s)")

        if verify_upload(client, bucket, upload_key, file_size):
            logging.info(f"✅ File available: s3://{bucket}/{upload_key}")
            domain = os.getenv("R2_DOMAIN", "tiles.streetradar.com")
            logging.info(f"🌐 Future URL: https://{domain}/tiles/ja360")
            logging.info("📊 Metadata:")
            logging.info("   • Source: JA360 street coverage data")
            logging.info("   • Type: Raster tiles (PNG)")
            logging.info("   • Zoom: 1-16")
            logging.info(f"   • Size: {file_size_mb:.1f} MB")
        else:
            logging.error("❌ Verification failed")
            sys.exit(1)

    except Exception as e:
        sys.stdout.write("\n")
        logging.error(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
