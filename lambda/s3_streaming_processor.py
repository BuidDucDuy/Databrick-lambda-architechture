import json
import boto3
import csv
import io
import os
from uuid import uuid4

s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    kinesis_stream = os.environ.get('KINESIS_STREAM')
    
    try:
        records = event.get('Records', [])
        if not records: return {'statusCode': 400, 'body': 'No records'}

        bucket = records[0]['s3']['bucket']['name']
        key = records[0]['s3']['object']['key']

        # DÙNG STREAMING ĐỂ TIẾT KIỆM RAM
        response = s3_client.get_object(Bucket=bucket, Key=key)
        lines = (line.decode('utf-8') for line in response['Body'].iter_lines())
        csv_reader = csv.DictReader(lines)
        
        batch = []
        ingestion_id = str(uuid4())

        for row in csv_reader:
            event_obj = {
                'timestamp': int(row['timestamp']),
                'visitorid': row['visitorid'].strip(),
                'event': row['event'].strip(),
                'itemid': row['itemid'].strip(),
                '_ingestion_id': ingestion_id
            }
            
            # Gom 500 dòng vào 1 lô
            batch.append({
                'Data': json.dumps(event_obj),
                'PartitionKey': str(event_obj['itemid'])
            })

            if len(batch) == 500:
                kinesis_client.put_records(StreamName=kinesis_stream, Records=batch)
                batch = []

        # Gửi những dòng còn lại
        if batch:
            kinesis_client.put_records(StreamName=kinesis_stream, Records=batch)
            
        return {'statusCode': 200, 'body': 'Processed with batching!'}
        
    except Exception as e:
        return {'statusCode': 500, 'body': str(e)}