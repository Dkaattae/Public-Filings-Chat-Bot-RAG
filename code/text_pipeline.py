import dlt
import os
import json
import boto3
from dotenv import load_dotenv
from dlt.destinations import qdrant
from dlt.destinations.adapters import qdrant_adapter

load_dotenv()

@dlt.resource(table_name="public_filings_docs", max_table_nesting=0)
def public_filing_data():
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
    for i in range(file_count):
        docs_path = f'public_filing_text_by_section{i}.json'
        obj = s3.get_object(Bucket=bucket_name, Key=s3_folder+docs_path)
        data = obj["Body"].read().decode("utf-8")
        records = json.loads(data)
        for doc in records:
            yield doc

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
    print(pipeline.last_trace)