import boto3
from botocore.exceptions import NoCredentialsError


def upload_to_s3(file_path, bucket_name):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(file_path, bucket_name, file_path)
        print(f"File {file_path} successfully uploaded to bucket {bucket_name}")
    except FileNotFoundError as e:
        print(f"File {file_path} was not found.")
    except NoCredentialsError:
        print("Error: AWS credentials not available.")

if __name__ == "__main__":
    pass 