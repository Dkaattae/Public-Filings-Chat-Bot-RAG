import dlt
import os
import json
import boto3
from dotenv import load_dotenv
from dlt.destinations import qdrant
from dlt.destinations.adapters import qdrant_adapter
import storage_utils
from pathlib import Path


@dlt.resource(table_name="public_filings_docs", max_table_nesting=0)
def public_filing_data():
    # storage = storage_utils.get_storage_folder()
    storage = '/workspaces/Public-Filings-Chat-Bot-RAG/data'
    if storage == 's3':
        import boto3
        load_dotenv()
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region_name = os.getenv("AWS_REGION")

        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        bucket_name = "public-filings-text-by-section"
        s3_folder = "filing_text_json/"
        file_count = 20
        for obj in s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_folder).get("Contents", []):
            s3_obj = s3.get_object(Bucket=bucket_name, Key=obj["Key"])
            data = s3_obj["Body"].read().decode("utf-8")
            records = json.loads(data)
            for doc in records:
                yield doc
    else:
        local_folder = Path(storage)
        for file_path in local_folder.glob("*.json"):
            with open(file_path, "r", encoding="utf-8") as f:
                yield json.load(f)

if __name__ == "__main__":
    # qclient = QdrantClient(path="db.qdrant")

    client = qdrant(
        host="localhost",  
        port=6333,
        grpc_port=6334
    )

    pipeline = dlt.pipeline(
        pipeline_name="public_filing_pipeline",
        destination=client,
        dataset_name="public_filing_data"

    )
    
    filing_docs = public_filing_data()
    qdrant_adapter(filing_docs, embed=["text"])
    load_info = pipeline.run(filing_docs)
    # print(pipeline.last_trace)