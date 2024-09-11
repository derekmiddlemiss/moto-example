from __future__ import annotations

from datetime import datetime

import boto3
from aws_lambda_powertools.utilities.typing import LambdaContext

from .event_type import EventType
from .get_count_from_object import get_count_from_object

s3_client = boto3.client('s3')


def lambda_handler(event: dict, context: LambdaContext) -> dict[str, int]:
    input_bucket = 'moto_example_input_dsm'
    output_bucket = 'moto_example_output_dsm'
    mapped_event = EventType(**event)
    prefix = mapped_event.prefix

    # List all objects with the given prefix
    list_objects = s3_client.list_objects_v2(Bucket=input_bucket, Prefix=prefix).get('Contents', [])

    max_count = float('-inf')

    # Process each file
    for list_object in list_objects:
        obj = s3_client.get_object(Bucket=input_bucket, Key=list_object['Key'])
        count_matches = get_count_from_object(obj)
        if count_matches:
            count = int(count_matches[0])
            max_count = max(max_count, count)
    
    # Write the result to the output bucket
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_key = f"{timestamp}_max_count_{prefix[:-1]}"
    s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=str(max_count))

    return {"statusCode": 200}
