import tarfile
import logging
import boto3
from botocore.exceptions import ClientError
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def get_s3_client(aws_access_key_id: str, aws_secret_access_key: str) -> boto3.client:
    """
    Create and return an S3 client; log an error and return None if unsuccessful.
    Args:
        aws_access_key_id: AWS access key ID.
        aws_secret_access_key: AWS secret access key.
    Returns:
        boto3.client object or None if an error occurred.
    """
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        return s3
    except ClientError as e:
        logging.error(e)
        return None


def upload_to_s3(
    s3_client: boto3.client, file_name: str, bucket_name: str, object_name: str = None
) -> bool:
    """
    Upload a file to an S3 bucket.
    Args:
        s3_client: Initialized S3 client.
        file_name: Name of the file to upload.
        bucket_name: Name of the S3 bucket.
        object_name: S3 object name. If not provided, file_name is used.
    Returns:
        True if the file was successfully uploaded, False otherwise.
    """
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        s3_client.upload_file(file_name, bucket_name, object_name)
        return True
    except FileNotFoundError:
        logging.error(f"The file {file_name} was not found!")
        return False
    except ClientError as e:
        logging.error(e)
        return False


def extract_tar(file_path: str, destination_path: str) -> None:
    """
    Extract a tar file to a specified destination directory.
    Args:
        file_path: Path to the tar file.
        destination_path: Directory to extract the tar file contents.
    """
    with tarfile.open(file_path) as tar:
        tar.extractall(destination_path)


def main() -> None:
    """
    Main function to extract a tar file and upload its contents to an S3 bucket.
    """
    parent_dir = Path(__file__).parents[1]
    extract_tar(f"{parent_dir}/data/yelp_dataset.tar", f"{parent_dir}/data/json_files")

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("BUCKET_NAME")
    s3 = get_s3_client(aws_access_key_id, aws_secret_access_key)
    if s3 is not None:
        yelp_data_dir = parent_dir / "data" / "json_files"
        for pth in yelp_data_dir.iterdir():
            upload_to_s3(s3, str(pth), bucket_name, f"raw_data/{pth.name}")


if __name__ == "__main__":
    main()
