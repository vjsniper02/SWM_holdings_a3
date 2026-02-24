import logging
import json
import os
import boto3

logger = logging.getLogger("a3_send_file_agencies")
logger.setLevel(logging.INFO)


# Construct message structure for CEE interface
def construct_cee_message(details):

    caller_input = {
        "id": details["id"],
        "body": {
            "externalHoldingsId": details["agency_code"],
            "type": "Holdings",
            "files": [
                {
                    "path": "s3://"
                    + details["holding_zip_bucket"]
                    + "/"
                    + details["holding_zip_file"]
                }
            ],
        },
    }

    return caller_input


def invokeCEE(inputMessage):
    # Invoke Techone Adapatar Lambda function using boto3 invoke method
    try:
        logger.info(f"Invoke CEE for A3 holdings")
        logger.info("CEE Function")
        logger.info(os.environ["CEE_FUNCTION"])
        ssm_client = boto3.client("ssm")
        cee = os.environ["CEE_FUNCTION"]
        client_sf = boto3.client("stepfunctions")
        cee_sm = ssm_client.get_parameter(Name=cee).get("Parameter").get("Value")
        ceeResponse = client_sf.start_execution(
            stateMachineArn=cee_sm, input=json.dumps(inputMessage)
        )

        logger.info(ceeResponse)
        logger.info(ceeResponse["ResponseMetadata"]["HTTPStatusCode"])

    except Exception as e:
        logger.exception(f"Server error - {str(e)}")
        logger.error(f"Error in invoking CEE")

    return "Success"


def lambda_handler(event, context):
    logger.info("Inside send File Agencies")
    logger.info(event)
    input = construct_cee_message(event)
    logger.info(input)
    ceeResponse = invokeCEE(input)
    return ceeResponse
