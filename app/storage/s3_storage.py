import logging
from pathlib import Path
from typing import Optional, List
import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from app.config import (
    AWS_REGION,
    AWS_S3_BUCKET,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY
)

logger = logging.getLogger("S3StorageWriter")

class S3StorageWriter:
    def __init__(
        self,
        bucket_name: str = AWS_S3_BUCKET,
        region: str = AWS_REGION,
        access_key: str = AWS_ACCESS_KEY_ID,
        secret_key: str = AWS_SECRET_ACCESS_KEY
    ):
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = None

        if access_key and secret_key:
            try:
                self.s3_client = boto3.client(
                    "s3",
                    region_name=self.region,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key
                )
            except Exception as e:
                logger.error(f"[S3] Failed to initialize S3 client with provided credentials: {e}")
        else:
            # Fall back to standard boto3 credential resolution (IAM roles, env vars, ~/.aws/credentials)
            try:
                self.s3_client = boto3.client("s3", region_name=self.region)
            except Exception as e:
                logger.warning(f"[S3] Initialized default S3 client (may fail without credentials): {e}")

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """
        Upload a single local file to S3 bucket.
        Returns True if successful, False otherwise.
        """
        if not self.s3_client:
            logger.error("[S3] S3 client is not configured. Upload skipped.")
            return False

        if not local_path.exists():
            logger.error(f"[S3] Local file does not exist: {local_path}")
            return False

        try:
            logger.info(f"[S3] Uploading {local_path.name} -> s3://{self.bucket_name}/{s3_key}")
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
            logger.info(f"[S3] Upload successful: s3://{self.bucket_name}/{s3_key}")
            return True
        except NoCredentialsError:
            logger.error("[S3] AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
            return False
        except ClientError as e:
            logger.error(f"[S3] AWS ClientError during upload: {e}")
            return False
        except BotoCoreError as e:
            logger.error(f"[S3] BotoCoreError during upload: {e}")
            return False
        except Exception as e:
            logger.error(f"[S3] Unexpected error uploading to S3: {e}")
            return False

    def upload_local_batch_paths(self, file_paths: List[Path], base_dir: Path) -> List[str]:
        """
        Upload a list of local batch files to S3, preserving relative partition folder structure.
        Returns a list of successfully uploaded S3 keys.
        """
        uploaded_keys = []
        for file_path in file_paths:
            try:
                relative_path = file_path.relative_to(base_dir)
                s3_key = str(relative_path).replace("\\", "/")
                if self.upload_file(file_path, s3_key):
                    uploaded_keys.append(s3_key)
            except ValueError:
                s3_key = f"processed/{file_path.name}"
                if self.upload_file(file_path, s3_key):
                    uploaded_keys.append(s3_key)

        return uploaded_keys
