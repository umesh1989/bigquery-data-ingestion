"""this is going to be single access point for all the AWS related activities."""

from botocore.exceptions import ClientError
import datetime
import boto3
import os


class AwsUtils:
    def __int__(self):
        pass

    @staticmethod
    def get_secrets(secret_name, service_name="secretsmanager"):
        """this method deals with getting credentials from the secret manager"""
        session = boto3.session.Session()
        client = session.client(
            service_name=service_name,
        )
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise e
        # Decrypts secret using the associated KMS key.
        secret = get_secret_value_response["SecretString"]
        return secret

    def upload_file(self, api_name, logger):
        log_dir = "../logs/"
        files = os.listdir(log_dir + api_name)
        files.sort()
        file_path = log_dir + api_name + "/" + files[-1]

        s3_client = boto3.client("s3")
        key_parts = [
            api_name,
            str(datetime.datetime.now().year),
            str(datetime.datetime.now().month),
            str(datetime.datetime.now().day),
        ]
        file_key = "/".join(key_parts)
        try:
            s3_client.upload_file(
                Filename=file_path,
                Bucket="dataingestion-logs",
                Key=file_key + "/" + files[-1],
            )
        except Exception as e:
            logger.exception("uploading logs file to s3 failed")
            raise Exception


if __name__ == "__main__":
    awsu = AwsUtils()
    # ss = awsu.get_secrets("snowflake")
    awsu.upload_file("bigquery", "")
