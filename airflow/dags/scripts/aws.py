import boto3
from config import CONFIG


class AWS:
    def __init__(self):
        aws_config = CONFIG.get_aws_config()
        self.aws_access_key_id = aws_config['aws_access_key_id']
        self.aws_secret_access_key = aws_config['aws_secret_access_key']
        self.aws_session_token = aws_config['aws_session_token']
        self.aws_region = aws_config['aws_region']
        self.boto_client = self.create_boto_client()

    def create_boto_client(self):
        return boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            region_name=self.aws_region
        )
