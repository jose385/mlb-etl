#!/usr/bin/env python3
"""
s3_storage.py - S3 integration for MLB data storage
"""
import boto3
import pandas as pd
from pathlib import Path
from typing import Optional, List
import tempfile
import os
from botocore.exceptions import ClientError

class S3DataManager:
    """Manage MLB data storage in S3"""
    
    def __init__(self, bucket_name: str, prefix: str = "mlb-data"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3_client = boto3.client('s3')
        
    def upload_parquet(self, local_file: Path, s3_key: str = None) -> bool:
        """Upload parquet file to S3"""
        if s3_key is None:
            s3_key = f"{self.prefix}/{local_file.name}"
        
        try:
            self.s3_client.upload_file(str(local_file), self.bucket_name, s3_key)
            print(f"✅ Uploaded {local_file.name} → s3://{self.bucket_name}/{s3_key}")
            return True
        except Exception as e:
            print(f"❌ S3 upload failed for {local_file.name}: {e}")
            return False
    
    def download_parquet(self, s3_key: str, local_file: Path) -> bool:
        """Download parquet file from S3"""
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, str(local_file))
            print(f"✅ Downloaded s3://{self.bucket_name}/{s3_key} → {local_file.name}")
            return True
        except Exception as e:
            print(f"❌ S3 download failed for {s3_key}: {e}")
            return False
    
    def list_parquet_files(self, date_prefix: str = None) -> List[str]:
        """List parquet files in S3"""
        try:
            search_prefix = f"{self.prefix}/"
            if date_prefix:
                search_prefix += f"{date_prefix}"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=search_prefix
            )
            
            files = []
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    files.append(obj['Key'])
            
            return files
        except Exception as e:
            print(f"❌ S3 list failed: {e}")
            return []
    
    def load_parquet_from_s3(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Load parquet file directly from S3 into DataFrame"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                self.s3_client.download_file(self.bucket_name, s3_key, tmp_file.name)
                df = pd.read_parquet(tmp_file.name)
                os.unlink(tmp_file.name)  # Clean up temp file
                return df
        except Exception as e:
            print(f"❌ Failed to load {s3_key} from S3: {e}")
            return None
    
    def upload_directory(self, local_dir: Path, s3_prefix: str = None) -> int:
        """Upload all parquet files from a directory to S3"""
        if s3_prefix is None:
            s3_prefix = self.prefix
        
        uploaded_count = 0
        parquet_files = list(local_dir.glob("*.parquet"))
        
        for file_path in parquet_files:
            s3_key = f"{s3_prefix}/{file_path.name}"
            if self.upload_parquet(file_path, s3_key):
                uploaded_count += 1
        
        print(f"📁 Uploaded {uploaded_count}/{len(parquet_files)} files to S3")
        return uploaded_count