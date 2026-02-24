import boto3
import json
import os
import logging

from swm_logger.swm_logger import create_log_stream, custom_log

logger = logging.getLogger("a4_landmark_sch_event_receiver")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info(f"Start Event Receiver Lambda Landler")

    logger.info(event)
    logger.info(context)

    records = [x for x in event.get("Records", [])]
    sorted_events = sorted(records, key=lambda e: e.get("eventTime"))
    latest_event = sorted_events[-1] if sorted_events else {}
    info = latest_event.get("s3", {})
    s3_req_id = latest_event.get("responseElements", {}).get("x-amz-request-id")
    logger.info(f"s3_req_id: {s3_req_id}")
    file_key = info.get("object", {}).get("key")
    bucket_name = info.get("bucket", {}).get("name")
    logger.info(file_key)
    file_key = file_key.replace("+", " ")
    logger.info(file_key)
    s3client = boto3.client("s3")
    response = s3client.get_object(Bucket=bucket_name, Key=file_key)
    logger.info(response)
    log_group = response["Metadata"]["log_group"]
    log_stream = response["Metadata"]["log_stream"]
    logger.info(log_group)
    logger.info(log_stream)
    custom_log("Entering lambda_handler()", log_group, log_stream)

    file_name = file_key.replace("sftp/", "")
    logger.info(file_name)
    logger.info(bucket_name)
    error_bucket = os.environ["Error_Bucket"]

    fileDetails = {
        "filedetails": [
            {
                "bucket": bucket_name,
                "name": file_name,
                "error_bucket": error_bucket,
                "id": s3_req_id,
            }
        ]
    }

    logger.info(fileDetails)
    sf_client = boto3.client("stepfunctions")
    step_function_name = os.environ["Program_Schedule_StateMachine"]
    logger.info(step_function_name)

    response = sf_client.start_execution(
        stateMachineArn=step_function_name, input=json.dumps(fileDetails)
    )

    logger.info(f"End Event Receiver Lambda Landler")
    return "a4_landmark_sch_event_receiver"
