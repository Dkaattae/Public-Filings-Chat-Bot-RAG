import os
from pathlib import Path
from dotenv import load_dotenv

def get_storage_folder():
    """Return 's3' if credentials exist, else return local folder path."""
    load_dotenv()
    s3_key = os.getenv("AWS_ACCESS_KEY_ID")
    s3_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    region_name = os.getenv("AWS_REGION")

    if s3_key and s3_secret:
        # S3 is available
        return "s3"
    else:
        # Fallback to local folder
        local_folder = Path(__file__).parent.parent / "data"  # <project>/data
        local_folder.mkdir(parents=True, exist_ok=True)
        return str(local_folder)

def upload_file(body_bytes: bytes, filename: str, storage: str, bucket_name=None, s3_folder=""):
    """Upload to S3 if storage=='s3', else save locally."""
    if storage == "s3":
        import boto3
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        s3_client.put_object(Bucket=bucket_name, Key=s3_folder + filename, Body=body_bytes)
    else:
        local_folder = Path(storage)
        dest_path = local_folder / filename
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as f_out:
            f_out.write(body_bytes)


if __name__ == "__main__":
    print(get_storage_folder())