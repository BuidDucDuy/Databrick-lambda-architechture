#!/usr/bin/env python3
"""
Upload Data to S3
Uploads both batch and streaming CSV files to S3 buckets with proper partitioning
"""
import boto3
import argparse
import sys
from pathlib import Path
from datetime import datetime
import os


def upload_batch_data(file_path, bucket_name, prefix='data'):
    """Upload batch data (item properties) to S3 with date partitioning"""
    
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        return False
    
    s3 = boto3.client('s3')
    file_size = Path(file_path).stat().st_size / (1024**2)  # MB
    
    # Date-based partitioning for batch
    today = datetime.now()
    s3_key = f"{prefix}/batch/{today.year:04d}/{today.month:02d}/{today.day:02d}/{Path(file_path).name}"
    
    print(f"📤 Uploading batch data...")
    print(f"   File: {file_path}")
    print(f"   Size: {file_size:.2f} MB")
    print(f"   S3 Path: s3://{bucket_name}/{s3_key}")
    
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"✅ Batch upload successful!")
        print(f"   Location: s3://{bucket_name}/{s3_key}")
        return True
    
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False


def upload_streaming_data(file_path, bucket_name, prefix='data'):
    """Upload streaming data (events) to S3 - triggers Lambda processing"""
    
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        return False
    
    s3 = boto3.client('s3')
    file_size = Path(file_path).stat().st_size / (1024**2)  # MB
    
    # Date-based partitioning for streaming
    today = datetime.now()
    s3_key = f"{prefix}/streaming/{today.year:04d}/{today.month:02d}/{today.day:02d}/{Path(file_path).name}"
    
    print(f"📤 Uploading streaming data...")
    print(f"   File: {file_path}")
    print(f"   Size: {file_size:.2f} MB")
    print(f"   S3 Path: s3://{bucket_name}/{s3_key}")
    print(f"   ℹ️  Lambda will be triggered automatically to process events")
    
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"✅ Streaming upload successful!")
        print(f"   Location: s3://{bucket_name}/{s3_key}")
        print(f"   Next: Lambda function 's3_streaming_processor' will process events to Kinesis")
        return True
    
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Upload data files to S3 (batch and streaming)'
    )
    
    parser.add_argument(
        'file_path',
        help='Path to CSV file (data/batch/*.csv or data/streaming/*.csv)'
    )
    
    parser.add_argument(
        '--bucket',
        required=True,
        help='S3 bucket name (from Terraform output)'
    )
    
    parser.add_argument(
        '--type',
        choices=['batch', 'streaming', 'auto'],
        default='auto',
        help='Data type: batch (item properties) or streaming (events). With "auto", detects from file path.'
    )
    
    parser.add_argument(
        '--prefix',
        default='data',
        help='S3 prefix (default: data)'
    )
    
    args = parser.parse_args()
    
    if not Path(args.file_path).exists():
        print(f"❌ File not found: {args.file_path}")
        sys.exit(1)
    
    # Auto-detect type
    data_type = args.type
    if data_type == 'auto':
        if 'batch' in args.file_path.lower() or 'properties' in args.file_path.lower():
            data_type = 'batch'
        elif 'streaming' in args.file_path.lower() or 'events' in args.file_path.lower():
            data_type = 'streaming'
        else:
            print("⚠️  Could not auto-detect file type. Use --type batch or --type streaming")
            sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"S3 DATA UPLOAD")
    print(f"{'='*60}\n")
    
    if data_type == 'batch':
        success = upload_batch_data(args.file_path, args.bucket, args.prefix)
    else:  # streaming
        success = upload_streaming_data(args.file_path, args.bucket, args.prefix)
    
    print(f"\n{'='*60}\n")
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
