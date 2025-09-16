"""
naver_r2_upload.py: Upload du fichier naver.pmtiles vers Cloudflare R2

Variables d'environnement requises:
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET
"""

import os
import sys
import logging
import threading
import hashlib
import time
from pathlib import Path
from dotenv import load_dotenv
import boto3
from boto3.s3.transfer import TransferConfig
from urllib.parse import urlparse
from datetime import datetime


def setup_logger():
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)


class ProgressPercentage:
    def __init__(self, filename):
        self._size = float(Path(filename).stat().st_size)
        self._seen = 0
        self._lock = threading.Lock()
        self._start_time = time.time()
        
    def __call__(self, bytes_amount):
        with self._lock:
            self._seen += bytes_amount
            pct = (self._seen / self._size) * 100
            
            elapsed = time.time() - self._start_time
            if elapsed > 0:
                speed_mbps = (self._seen / (1024 * 1024)) / elapsed
                remaining_bytes = self._size - self._seen
                eta_seconds = remaining_bytes / (self._seen / elapsed) if self._seen > 0 else 0
                eta_str = f"{int(eta_seconds//60)}m{int(eta_seconds%60)}s" if eta_seconds > 0 else "0s"
            else:
                speed_mbps = 0
                eta_str = "calculating..."
            
            sys.stdout.write(
                f"\r🚀 {self._seen/(1024*1024):.1f}/{self._size/(1024*1024):.1f} MB "
                f"({pct:.1f}%) - {speed_mbps:.1f} MB/s - ETA: {eta_str}     "
            )
            sys.stdout.flush()


def load_env():
    possible_env_paths = [
        Path(__file__).resolve().parent / '.env',
        Path(__file__).resolve().parents[1] / '.env',
        Path(__file__).resolve().parents[2] / '.env',
    ]
    
    for env_path in possible_env_paths:
        if env_path.is_file():
            load_dotenv(dotenv_path=env_path)
            break
    else:
        load_dotenv()


def get_client_and_bucket():
    access = os.getenv('R2_ACCESS_KEY_ID') or os.getenv('Access_Key_ID')
    secret = os.getenv('R2_SECRET_ACCESS_KEY') or os.getenv('Secret_Access_Key')
    account = os.getenv('R2_ACCOUNT_ID')
    endpoint = os.getenv('Endpoint')
    bucket = os.getenv('R2_BUCKET')
    
    if not account and endpoint:
        host = urlparse(endpoint).hostname
        account = host.split('.')[0] if host else None
    
    if not all([access, secret, account, bucket]):
        logging.error("❌ Variables R2 manquantes: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET")
        sys.exit(1)
    
    url = endpoint or f"https://{account}.r2.cloudflarestorage.com"
    
    client = boto3.client(
        's3',
        endpoint_url=url,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name='auto'
    )
    
    return client, bucket


def find_naver_pmtiles():
    custom_path = os.getenv('NAVER_PMTILES_PATH')
    if custom_path and Path(custom_path).is_file():
        return Path(custom_path)
    
    root = Path(__file__).resolve().parents[2]
    # Prioritize new pipeline output location
    search_paths = [
        root / "data" / "naver" / "tile" / "naver.pmtiles",
        Path.cwd() / "naver.pmtiles",
        # Backward-compat legacy paths
        root / "data" / "naver" / "usage" / "tiles" / "naver.pmtiles",
        Path(__file__).resolve().parents[1] / "data" / "naver" / "usage" / "tiles" / "naver.pmtiles",
        Path(__file__).resolve().parent / "data" / "naver" / "usage" / "tiles" / "naver.pmtiles",
        Path(__file__).resolve().parent / "naver.pmtiles",
        Path(__file__).resolve().parents[1] / "data" / "naver.pmtiles",
    ]
    
    for path in search_paths:
        if path.is_file():
            return path
    
    return None


def calculate_file_hash(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def verify_upload(client, bucket, object_key, expected_size):
    try:
        response = client.head_object(Bucket=bucket, Key=object_key)
        return response['ContentLength'] == expected_size
    except:
        return False


def main():
    setup_logger()
    load_env()
    
    pmtiles_path = find_naver_pmtiles()
    if not pmtiles_path:
        logging.error("❌ Fichier naver.pmtiles introuvable")
        sys.exit(1)
    
    file_size = pmtiles_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    
    logging.info(f"📁 {pmtiles_path} ({file_size_mb:.1f} MB)")
    
    file_hash = calculate_file_hash(pmtiles_path)
    
    part_size_mb = max(int(os.getenv('PART_SIZE_MB', '64')), 5)
    part_size = part_size_mb * 1024 * 1024
    
    config = TransferConfig(
        multipart_threshold=part_size,
        multipart_chunksize=part_size,
        max_concurrency=10,
        use_threads=True
    )
    
    upload_key = os.getenv('UPLOAD_KEY', 'naver.pmtiles')
    client, bucket = get_client_and_bucket()
    progress = ProgressPercentage(str(pmtiles_path))
    
    start_time = time.time()
    
    try:
        extra_args = {
            'ContentType': 'application/octet-stream',
            'Metadata': {
                'original-filename': pmtiles_path.name,
                'file-hash': file_hash,
                'upload-time': datetime.now().isoformat(),
                'file-size': str(file_size)
            }
        }
        
        client.upload_file(
            Filename=str(pmtiles_path),
            Bucket=bucket,
            Key=upload_key,
            Config=config,
            Callback=progress,
            ExtraArgs=extra_args
        )
        
        sys.stdout.write("\n")
        
        upload_time = time.time() - start_time
        avg_speed = file_size_mb / upload_time if upload_time > 0 else 0
        
        logging.info(f"✅ Upload terminé en {upload_time:.1f}s ({avg_speed:.1f} MB/s)")
        
        if verify_upload(client, bucket, upload_key, file_size):
            logging.info(f"✅ Fichier disponible: s3://{bucket}/{upload_key}")
            domain = os.getenv('R2_DOMAIN', 'tiles.streetradar.com')
            logging.info(f"🌐 URL future: https://{domain}/tiles/naver")
        else:
            logging.error("❌ Échec de la vérification")
            sys.exit(1)
            
    except Exception as e:
        sys.stdout.write("\n")
        logging.error(f"❌ Erreur: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()