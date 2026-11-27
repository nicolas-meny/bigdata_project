import os
from pathlib import Path
import boto3


def get_s3_client():
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    if not endpoint_url and os.environ.get("AWS_S3_ENDPOINT"):
        endpoint_url = f"https://{os.environ['AWS_S3_ENDPOINT']}"

    profile_name = os.environ.get("AWS_PROFILE") or "default"
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        session = boto3.Session(profile_name=profile_name)
    else:
        session = boto3.Session()

    return session.client("s3", endpoint_url=endpoint_url)


def upload_file(local_path: str, bucket: str, s3_key: str) -> None:
    get_s3_client().upload_file(str(local_path), bucket, s3_key)
    print(f"[S3] Uploaded {local_path} -> s3://{bucket}/{s3_key}")


def upload_directory(local_dir: str, bucket: str, s3_prefix: str) -> None:
    s3 = get_s3_client()
    base = Path(local_dir)
    for file_path in base.rglob("*"):
        if file_path.is_file() and not file_path.name.startswith(".") and file_path.name != "_SUCCESS":
            rel_key = f"{s3_prefix.rstrip('/')}/{file_path.relative_to(base).as_posix()}"
            s3.upload_file(str(file_path), bucket, rel_key)
            print(f"[S3] Uploaded {file_path} -> s3://{bucket}/{rel_key}")


def download_file(bucket: str, s3_key: str, local_path: str) -> None:
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    get_s3_client().download_file(bucket, s3_key, str(local_path))
    print(f"[S3] Downloaded s3://{bucket}/{s3_key} -> {local_path}")


def download_prefix(bucket: str, s3_prefix: str, local_dir: str) -> None:
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    base = Path(local_dir)
    base.mkdir(parents=True, exist_ok=True)
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            rel = Path(key).relative_to(s3_prefix)
            target = base / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(target))
            print(f"[S3] Downloaded s3://{bucket}/{key} -> {target}")