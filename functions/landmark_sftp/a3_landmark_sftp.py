"""Module provide functionality to Pull file from Landmark Server and copy it to S3 Bucket."""

import os
import boto3
import json
from swm_logger.swm_logger import create_log_stream, custom_log


def lambda_handler(event, context):
    event_id = event["id"]
    lambdaClient = boto3.client("lambda")

    log_group = os.environ["LOG_GROUP_NAME"]
    log_group, log_stream = create_log_stream(log_group, event_id)
    custom_log("Entering lambda_handler()", log_group, log_stream)

    s3_bucket_pa = os.environ["SFTP_S3_BUCKET_PA"]
    s3_bucket_aua = os.environ["SFTP_S3_BUCKET_AUA"]
    sftp_path = os.environ["LANDMARK_SFTP_PATH"]
    lmk_sftp_adaptor_function = os.environ["LMK_SFTP_ADAPTOR_FUNCTION"]

    s3 = boto3.client("s3")

    ssm_client = boto3.client("ssm")
    lmk_sftp_adaptor_function = (
        ssm_client.get_parameter(Name=lmk_sftp_adaptor_function)
        .get("Parameter")
        .get("Value")
    )

    request = {
        "sftp_path": sftp_path,
        "s3_bucket": s3_bucket_pa,
        "log_group": log_group,
        "log_stream": log_stream,
    }

    try:
        custom_log("Invoke Techone Adapator for A3 Account", log_group, log_stream)

        lmk_sftp_response = lambdaClient.invoke(
            FunctionName=lmk_sftp_adaptor_function,
            InvocationType="RequestResponse",
            Payload=json.dumps(request),
        )

    except Exception as e:
        # logger.exception(f"Server error - {str(e)}")
        custom_log("Error in A3 SFTP Invoke" + str(e), log_group, log_stream)

    custom_log("Existing lambda_handler()", log_group, log_stream)

    return "a3_landmark_sftp"
