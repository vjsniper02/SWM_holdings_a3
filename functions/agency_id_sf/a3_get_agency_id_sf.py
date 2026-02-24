"""Module provide functionality to Pull Agency id from Salesforce based on Agency / Media Code."""

import logging
import json
import os
import boto3

logger = logging.getLogger("a4_send_file_agencies")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info(f"Entering Get Agency Id from Salesforce")

    input = []

    input["invocationType"] == "QUERY"
    input["query"] = (
        "SELECT Id, Name from Account WHERE SWM_External_Holdings_ID__c="
        + event["agency_code"]
    )

    lambdaClient = boto3.client("lambda")
    sf_response = lambdaClient.invoke(
        FunctionName=os.environ["SF_ADAPTOR"],
        InvocationType="RequestResponse",
        Payload=json.dumps(input),
    )
    logger.info(sf_response)

    logger.info(f"Existing Get Agency Id from Salesforce")

    return event
