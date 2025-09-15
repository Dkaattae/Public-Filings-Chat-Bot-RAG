import dlt
import os
import requests
from dotenv import load_dotenv
from dlt.destinations import qdrant
from dlt.destinations.adapters import qdrant_adapter

load_dotenv()

@dlt.resource(table_name="public_filings_docs", max_table_nesting=0)
def public_filing_data():
    bucket_name = "public-filings-text-by-section"
    s3_folder = "filing_text_json/"
    bucket_number = 20
    for i in range(bucket_number):
        docs_path = f'public_filing_text_by_section{i}.json'
        s3_path = f's3://{bucket_name}/{s3_folder}{docs_path}'
        with fsspec.open(s3_path, "r") as f:
            file = json.load(f)
    
            for doc in file:
                yield doc

if __name__ == "__main__":
    # qclient = QdrantClient(path="db.qdrant")

    client = qdrant(
        host="localhost",  
        port=6333,
        grpc_port=6334
    )

    os.environ["DLT_EMBEDDINGS__PROVIDER"] = "sentence_transformers"
    os.environ["DLT_EMBEDDINGS__MODEL_NAME"] = "all-MiniLM-L6-v2"

    pipeline = dlt.pipeline(
        pipeline_name="public_filing_pipeline",
        destination=client,
        dataset_name="public_filing_data"

    )
    
    filing_docs = public_filing_data()
    qdrant_adapter(filing_docs, embed=["text"])
    load_info = pipeline.run(filing_docs)
    print(pipeline.last_trace)