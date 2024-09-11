import boto3
from moto import mock_aws
import pytest
from freezegun import freeze_time
from aws_lambda_powertools.utilities.typing import LambdaContext


@pytest.fixture
def mock_aws_credentials(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fake_access_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fake_secret_key")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "fake_session_token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def input_bucket():
    return "test_input_bucket"


@pytest.fixture
def output_bucket():
    return "test_output_bucket"


@pytest.fixture
def mock_environment(monkeypatch, input_bucket, output_bucket):
    monkeypatch.setenv("INPUT_BUCKET", input_bucket)
    monkeypatch.setenv("OUTPUT_BUCKET", output_bucket)


@pytest.fixture
def s3_setup(mock_aws_credentials, input_bucket, output_bucket):
    with mock_aws():
        # Create S3 client and mock buckets
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=output_bucket)
        
        # Add mock files to the input bucket
        s3_client.put_object(Bucket=input_bucket, Key='test_prefix/file1.txt', Body='count: 10')
        s3_client.put_object(Bucket=input_bucket, Key='test_prefix/file2.txt', Body='count: 20')
        s3_client.put_object(Bucket=input_bucket, Key='test_prefix/file3.txt', Body='count: 5')

        yield s3_client


@freeze_time("2024-01-01 02:03:04")
@mock_aws
def test_lambda_handler(s3_setup, mock_environment, output_bucket):
    from src.handler import lambda_handler
    # Run the Lambda function
    event = {'prefix': 'test_prefix/'}
    result = lambda_handler(event, LambdaContext())
    
    # Verify status_code
    assert result['statusCode'] == 200

    # Verify the file is written to the output bucket
    s3_client = s3_setup
    output_key = '20240101T020304_max_count_test_prefix'
    response = s3_client.get_object(Bucket=output_bucket, Key=output_key)
    content = response['Body'].read().decode('utf-8')
    
    assert content == '20'
