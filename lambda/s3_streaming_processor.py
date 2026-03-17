#!/usr/bin/env python3

import json
import boto3
import logging
import csv
import io
import os
from datetime import datetime
from uuid import uuid4
from typing import Dict, List, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    """
    Process S3 event and ingest streaming events to Kinesis
    
    Event structure (S3 Put):
    {
        "Records": [{
            "s3": {
                "bucket": {"name": "streaming-data-bucket"},
                "object": {"key": "data/streaming/2026/03/17/events.csv"}
            }
        }]
    }
    """
    
    logger.info(f"Processing S3 event: {json.dumps(event)}")
    
    kinesis_stream = os.environ.get('KINESIS_STREAM_NAME', 'events-stream')
    
    try:
        # Extract S3 bucket and key from event
        records = event.get('Records', [])
        if not records:
            logger.warning("No records in event")
            return {'statusCode': 400, 'body': 'No records in event'}
        
        s3_record = records[0]['s3']
        bucket = s3_record['bucket']['name']
        key = s3_record['object']['key']
        
        logger.info(f"Processing file s3://{bucket}/{key}")
        
        # Read CSV from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Parse CSV
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        
        ingestion_id = str(uuid4())
        ingestion_time = int(datetime.utcnow().timestamp() * 1000)  # ms
        
        successful = 0
        failed = 0
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=1):
            try:
                # Build event
                event_obj = {
                    'timestamp': int(row['timestamp']),
                    'visitorid': row['visitorid'].strip(),
                    'event': row['event'].strip(),
                    'itemid': row['itemid'].strip(),
                    'transactionid': row.get('transactionid', '').strip() or None,
                    '_ingestion_id': ingestion_id,
                    '_ingestion_timestamp': ingestion_time,
                    '_source_file': key
                }
                
                # Validate
                required = ['timestamp', 'visitorid', 'event', 'itemid']
                if not all(event_obj.get(f) for f in required):
                    failed += 1
                    errors.append(f"Row {row_num}: Missing required fields")
                    continue
                
                # Put to Kinesis
                response = kinesis_client.put_record(
                    StreamName=kinesis_stream,
                    Data=json.dumps(event_obj),
                    PartitionKey=str(event_obj['itemid'])  # Partition by item_id
                )
                
                successful += 1
                
                if successful % 100 == 0:
                    logger.info(f"Processed {successful} records")
            
            except Exception as e:
                failed += 1
                errors.append(f"Row {row_num}: {str(e)}")
                logger.error(f"Row {row_num} error: {e}")
        
        # Summary
        total = successful + failed
        logger.info(f"Completed: {successful}/{total} successful, {failed} failed")
        
        if errors and len(errors) <= 10:
            logger.warning(f"Errors: {errors[:10]}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Events ingested successfully',
                'total': total,
                'successful': successful,
                'failed': failed,
                'ingestion_id': ingestion_id
            })
        }
    
    except Exception as e:
        logger.error(f"Error processing S3 event: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

if __name__ == '__main__':
    # For local testing
    import os
    test_event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'streaming-data-bucket'},
                'object': {'key': 'data/streaming/events.csv'}
            }
        }]
    }
    print(lambda_handler(test_event, None))
