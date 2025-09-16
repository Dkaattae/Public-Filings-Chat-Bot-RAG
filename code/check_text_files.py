import json
import pandas as pd
import boto3
import os
from dotenv import load_dotenv

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

data_df = pd.DataFrame()
for i in range(20):
    docs_path = f'public_filing_text_by_section{i}.json'
    obj = s3.get_object(Bucket=bucket_name, Key=s3_folder+docs_path)
    data = obj["Body"].read().decode("utf-8")
    records = json.loads(data)
    df = pd.DataFrame(records)
    df = df[['ticker', 'year', 'section']]
    data_df = pd.concat([data_df, df], ignore_index=True)


company_count = data_df.groupby('ticker')['year'].count()
print(f'company count: {len(company_count)}')
print(f'document count: {company_count.sum()}')

