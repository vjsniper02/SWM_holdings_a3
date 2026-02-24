import json
import boto3
import paramiko
from io import BytesIO
from stat import S_ISDIR, S_ISREG
from botocore.exceptions import ClientError
from io import StringIO
import logging
import os

logger = logging.getLogger("a4_landmark_sftp")
logger.setLevel(logging.INFO)


def get_secret_name():
    logger.info(f"Start get_secret_name()")
    ssmClient = boto3.client("ssm")
    secret_name = (
        ssmClient.get_parameter(Name=os.environ["LANDMARK_SFTP_SECRET_NAME"])
        .get("Parameter")
        .get("Value")
    )
    return secret_name


# Get Landmark SFTP details from Secret Manager
def get_landmark_secret():
    logger.info(f"Start get_landmark_secret()")

    secret_name = get_secret_name()
    logger.info(secret_name)

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response["SecretString"])

    secret_dict = {
        "ftp_url": secret["ftp_url"],
        "user_id": secret["user_id"],
        "password": secret["password"],
        "key_value": secret["key_value"],
    }

    logger.info(f"End get_landmark_secret()")
    return secret_dict


def get_ftp_path():
    logger.info(f"Start get_ftp_path()")
    ssmClient = boto3.client("ssm")
    sftp_path = (
        ssmClient.get_parameter(Name=os.environ["LANDMARK_SFTP_PATH"])
        .get("Parameter")
        .get("Value")
    )
    return sftp_path


def lambda_handler(event, context):
    logger.info(f"Start lambda_handler()")
    ftp_url = os.environ["LANDMARK_SFTP_URL"]
    user_id = os.environ["LANDMARK_SFTP_USERID"]
    password = os.environ["LANDMARK_SFTP_PASSWORD"]
    key_value = os.environ["LANDMARK_SFTP_KEY_VALUE"]

    sftp_path = os.environ["LANDMARK_SFTP_PATH"]

    s3client = boto3.client("s3")

    private_key_file = StringIO()
    private_key_file.write(key_value)
    private_key_file.seek(0)

    ssh_key = paramiko.RSAKey.from_private_key(private_key_file)
    # sshClient = paramiko.SSHClient()
    # sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        sshClient.connect(
            hostname=ftp_url,
            username=user_id,
            password=password,
            port=22,
            allow_agent=False,
            pkey=ssh_key,
        )
        sftpClient = sshClient.open_sftp()
        logger.info(f"Landmark SFTP Connection Estabilished")

        logger.info(dir(sftpClient))
        s3_bucket = os.environ["SFTP_S3_BUCKET"]
        logger.info(s3_bucket)
        logger.info(sftp_path)
        sftpClient.chdir(sftp_path)

        try:
            # File Upload to S3
            for entry in sftpClient.listdir_attr(""):
                logger.info(f"Inside File upload to S3")
                mode = entry.st_mode
                if S_ISREG(mode):
                    f = entry.filename
                    with BytesIO() as data:
                        sftpClient.getfo(f, data)
                        data.seek(0)

                        s3client.upload_fileobj(data, s3_bucket, "sftp/{0}".format(f))
        except IOError:
            logger.error("Error copying file from SFTP to S3 bucket")

        sftpClient.close()
    except paramiko.SSHException:
        logger.error("Connection Error")

    logger.info(f"File Upload Success")

    return "a4_landmark_sftp"
