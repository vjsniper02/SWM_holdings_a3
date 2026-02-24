import json
import boto3
import paramiko
from io import BytesIO
from stat import S_ISDIR, S_ISREG
from botocore.exceptions import ClientError
from io import StringIO
import logging
import datetime
import time
import pytz
import os

logger = logging.getLogger("a3_landmark_sftp")
logger.setLevel(logging.INFO)


def put_log_events(log_group_name, log_stream_name, message):
    # Initialize the CloudWatch Logs client
    client = boto3.client("logs")

    # Serialize dictionary messages to JSON strings
    if isinstance(message, dict):
        message = json.dumps(message)

    log_event = {"message": message, "timestamp": int(round(time.time() * 1000))}
    # Get the sequence token for the log stream
    response = client.describe_log_streams(
        logGroupName=log_group_name, logStreamNamePrefix=log_stream_name
    )
    log_streams = response.get("logStreams", [0])
    sequence_token = log_streams[0].get("uploadSequenceToken", None)

    log_event_request = {
        "logGroupName": log_group_name,
        "logStreamName": log_stream_name,
        "logEvents": [log_event],
    }
    if sequence_token:
        log_event_request["sequenceToken"] = sequence_token

    client.put_log_events(**log_event_request)


def get_aest_datetime():
    """
    Returns the current date and time in Australian Eastern Standard Time (AEST).

    Returns:
      datetime.datetime: The current date and time in AEST.
    """
    aest = pytz.timezone("Australia/Sydney")
    now_utc = datetime.datetime.now(pytz.utc)
    now_aest = now_utc.astimezone(aest)
    return now_aest


def lambda_handler(event, context):
    logger.info(f"Start lambda_handler()")
    payload = {}

    try:
        logger.info(event)
        id = event["filedetails"][0]["id"]
        fileDetails = event["filedetails"]
        logger.info(fileDetails)
        fileName = fileDetails[0]["name"]
        bucket = fileDetails[0]["bucket"]
        error_bucket = fileDetails[0]["error_bucket"]
        logger.info(fileName)

        fileNameSplit = fileName.split(" ")
        logger.info(fileNameSplit)
        logger.info(fileNameSplit[2])

        fileNameSplit = fileNameSplit[2].split("_")
        logger.info(fileNameSplit)

        logger.info(fileNameSplit[0])
        agencyCode = fileNameSplit[0]
        if agencyCode == "" or agencyCode == None:
            aest_date_time = get_aest_datetime()
            # # Set up the CloudWatch log group name
            CEE_LOG_GROUP_NAME = os.environ["CEE_LOG_GROUP_NAME"]
            # # Set up the log stream name
            CEE_LOG_STREAM_NAME = os.environ["CEE_LOG_STREAM_NAME"]
            message = f"REPORT|email|Holdings|failure||||SFERRORDEFAULTED Landmark BCC External Reference not populated|{fileName}|{id}|{aest_date_time}|DEFAULTED"
            put_log_events(CEE_LOG_GROUP_NAME, CEE_LOG_STREAM_NAME, message)
            logger.error("Landmark BCC External Reference not populated")
            payload = {
                "status": "failure",
                "bucket": error_bucket,
                "copySource": bucket + "/" + fileName,
                "target_key": fileName,
            }
        else:
            payload = {
                "status": "success",
                "bucket": bucket,
                "file": fileName,
                "id": id,
                "agency_code": agencyCode,
            }

    except IndexError as e:
        logger.exception(f"Error in File Parsing for {id}:  {str(e)}")
        logger.error("Error in file parsing")
        s3Client = boto3.client("s3")
        copy_source = {"Bucket": bucket, "Key": fileName}
        payload = {
            "status": "failure",
            "bucket": error_bucket,
            "copySource": bucket + "/" + fileName,
            "target_key": fileName,
        }

    return payload
