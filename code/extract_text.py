import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import json
import sys
import time
import boto3
import storage_utils
import extract_text_10k

def get_cik(ticker):
    cik_ticker_df = pd.read_csv('../files/cik_ticker_dictionary.csv')
    width = 10
    cik_ticker_df['cik_str'] = cik_ticker_df['cik_str'].astype(str).str.zfill(width)
    cik = cik_ticker_df[cik_ticker_df['ticker'] == ticker]['cik_str'].iloc[0]
    return cik

def get_submission_data(cik):
    headers = {
            'User-Agent': 'xchencws@citibank.com'
            }

    # Step 1: Get the list of filings
    submission_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    response = requests.get(submission_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed with {response.status_code}: {response.text}")

    submission_data = response.json()
    return submission_data

def get_accession_number(CIK, ticker, file_type_list, year_list, filings_data):
    headers = {
            'User-Agent': 'xchencws@citibank.com',  # Replace with your details
            'Accept-Encoding': 'application/json',
            'Host': 'www.sec.gov'
        }

    # Step 2: Find all filings type in type list and year in year list
    forms = filings_data["filings"]["recent"]["form"]
    accessions_all = filings_data["filings"]["recent"]["accessionNumber"]
    filing_dates = filings_data["filings"]["recent"]["filingDate"]
    primary_docs = filings_data["filings"]["recent"]["primaryDocument"]

    accessions = []

    for idx, form in enumerate(forms):
        year = filing_dates[idx].split("-")[0]  # e.g., "2023-02-01" → "2023"
        quarter = (int(filing_dates[idx].split("-")[1]) - 1) // 3 + 1
        if form in file_type_list and year in year_list:
            accession_no = accessions_all[idx].replace("-", "")
            doc_name = primary_docs[idx]
            accessions.append({
                "form": form,
                "year": year,
                "quarter": quarter,
                "accession": accession_no,
                "filing_date": filing_dates[idx],
                "doc_url": f"https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_no}/{doc_name}"
            })
    return accessions

def extract_texts_8k(CIK, ticker, accesion):
    return []

def extract_texts_10q(CIK, ticker, accesion):
    return []

def get_text_in_json(ticker, file_type_list, year_list):
    CIK = get_cik(ticker)
    submission_data = get_submission_data(CIK)
    accessions = get_accession_number(CIK, ticker, file_type_list, year_list, submission_data)
    try:
        print(accessions[0])
    except NameError:
        return []
    except IndexError:
        return []
    all_data = []
    for accession in accessions:
        json_list_10k = extract_text_10k.extract_texts_10k(CIK, ticker, accession)
        json_list_8k = extract_texts_8k(CIK, ticker, accession)
        json_list_10q = extract_texts_10q(CIK, ticker, accession)
        all_data_cik = json_list_10k + json_list_8k + json_list_10q
        all_data += all_data_cik

    return all_data


if __name__ == "__main__":
    storage = storage_utils.get_storage_folder()
    bucket_name = "public-filings-text-by-section"
    s3_folder = "filing_text_json/"

    file_type_list = ['10-K']
    year_list = ['2023', '2024', '2025']
    ticker_df = pd.read_csv('../files/Nasdaq100List.csv')
    ticker_list = ticker_df['Symbol'].to_list()
    text_doc_data = []
    file_idx = 0
    bucket_size = 5
    for idx, ticker in enumerate(ticker_list):
        if idx / bucket_size <= file_idx+1:
            json_data = get_text_in_json(ticker, file_type_list, year_list)
            text_doc_data += json_data
        if idx / bucket_size > file_idx+1:
            filename = f'public_filing_text_by_section{file_idx}.json'
            # with open(filename, "w", encoding="utf-8") as f:
            #     json.dump(text_doc_data, f, indent=2, ensure_ascii=False)
            json_bytes = json.dumps(text_doc_data).encode("utf-8")
            storage_utils.upload_file(json_bytes, filename, storage, bucket_name=bucket_name, s3_folder=s3_folder)
            file_idx = file_idx + 1 
            text_doc_data = []
            json_data = get_text_in_json(ticker, file_type_list, year_list)
            text_doc_data = text_doc_data + json_data
        time.sleep(1)
        

    print("Section content extracted successfully.")
