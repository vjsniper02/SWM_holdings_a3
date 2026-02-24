import json
import boto3
import paramiko
import io
from stat import S_ISDIR, S_ISREG
from botocore.exceptions import ClientError
from io import StringIO
import logging
import os
import zipfile


logger = logging.getLogger("a3_landmark_sftp")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info(f"Start lambda_handler()")
    logger.info(event)
    id = event["id"]
    bucket = event["bucket"]
    logger.info(bucket)
    name = event["file"]
    name = name.replace("+", " ")
    logger.info(name)
    # Holdings_2023-11-29 0209 12345_SEVNET.h

    zip_bucket = os.environ["HOLDING_ZIP_BUCKET"]
    user_bucket = os.environ["USER_BUCKET"]
    s3client = boto3.client("s3")
    copy_source = {"Bucket": bucket, "Key": name}
    response = s3client.get_object(Bucket=bucket, Key=name)
    content = response["Body"].read()

    try:
        s3client.copy_object(CopySource=copy_source, Bucket=user_bucket, Key=name)
        logger.info(f"Holding file {name} copied to {user_bucket}")
    except Exception as e:
        logger.info(f"Error occurred while copying the object: {id} {e}")

    try:
        # Create a zip file in-memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(name, content)

        # Upload the zip file to the target bucket
        zip_buffer.seek(0)
        target_key = name + ".zip"
        s3client.upload_fileobj(zip_buffer, zip_bucket, target_key)
        event["id"] = id
        event["holding_zip_bucket"] = zip_bucket
        event["holding_zip_file"] = target_key
        return event
    except Exception as e:
        logger.info(f"Error occurred while uploading zip file: {id} {e}")
