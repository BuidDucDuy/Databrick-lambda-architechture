import json
import boto3
import logging
import csv
import io
import os
from datetime import datetime
from uuid import uuid4

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    # Lấy tên luồng Kinesis từ biến môi trường đã định nghĩa trong Terraform
    kinesis_stream = os.environ.get('KINESIS_STREAM') [cite: 10]
    
    try:
        
        records = event.get('Records', [])
        if not records:
            return {'statusCode': 400, 'body': 'No records in event'}
        
        s3_record = records[0]['s3']
        bucket = s3_record['bucket']['name']
        key = s3_record['object']['key']
        
        # Đọc dữ liệu từ S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        
        ingestion_id = str(uuid4())
        ingestion_time = int(datetime.utcnow().timestamp() * 1000)
        
        successful = 0
        for row in csv_reader:
            event_obj = {
                'timestamp': int(row['timestamp']),
                'visitorid': row['visitorid'].strip(),
                'event': row['event'].strip(),
                'itemid': row['itemid'].strip(),
                '_ingestion_id': ingestion_id,
                '_ingestion_timestamp': ingestion_time
            }
            
            # push kinesis
            kinesis_client.put_record(
                StreamName=kinesis_stream,
                Data=json.dumps(event_obj),
                PartitionKey=str(event_obj['itemid'])
            )
            successful += 1
            
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Thành công', 'records': successful})
        }
    
    except Exception as e:
        logger.error(f"Lỗi : {str(e)}")
        return {'statusCode': 500, 'body': str(e)}