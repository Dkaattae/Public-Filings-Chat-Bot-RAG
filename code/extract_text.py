import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import json
import sys
import json

def get_accession_number(CIK, ticker, file_type_list, year_list, submission_file):
    headers = {
            'User-Agent': 'xchencws@citibank.com',  # Replace with your details
            'Accept-Encoding': 'application/json',
            'Host': 'www.sec.gov'
        }
    
    # Step 1: Get the list of filings
    with open(submission_file, 'r') as file:
        filings_data = json.load(file)

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

def extract_texts_10k(CIK, ticker, accession):
    headers = {
            'User-Agent': 'xchencws@citibank.com',  # Replace with your details
            'Accept-Encoding': 'application/json',
            'Host': 'www.sec.gov'
        }
    json_list = []

    accession_no = accession['accession']
    year = accession['year']
    quarter = accession['quarter']

    doc_url = f"https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_no}/index.json"
    response = requests.get(doc_url, headers=headers)
    doc_data = response.json()

    # Step 3: Get the raw 10-K document
    doc_index_df = pd.DataFrame(doc_data["directory"]["item"])
    include_file_type = "htm"
    exclude_file_type = 'xml'
    size_threshold = 10000
    doc_index_filtered_df = doc_index_df[doc_index_df['name'].str.contains(ticker.lower(), na=False)]
    doc_index_filtered_df = doc_index_filtered_df[doc_index_filtered_df['name'].str.contains(include_file_type, na=False)]
    doc_index_filtered_df = doc_index_filtered_df[~doc_index_filtered_df['name'].str.contains(exclude_file_type, na=False)]
    doc_index_filtered_df = doc_index_filtered_df[doc_index_filtered_df['size'].replace('', 0).astype(int) > size_threshold]

    file_url_path = doc_index_filtered_df.iloc[0]['name']
    last_modified_date = doc_index_filtered_df.iloc[0]['last-modified']
    last_modified_date = last_modified_date.split(' ')[0]

    txt_url = f"https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_no}/{file_url_path}"
    response = requests.get(txt_url, headers=headers)
    doc_text = response.text
    print(txt_url)
    # Parse the HTML
    soup = BeautifulSoup(doc_text, "html.parser")

    section_name_10k = ["Business", "Risk Factors", "Unresolved Staff Comments", \
        "Cybersecurity", "Properties", "Legal Proceedings", "Mine Safety Disclosures", \
        "Market for Registrant's Common Equity, Related Stockholder Matters and Issuer Purchases of Equity Securities", \
        "Management's Discussion and Analysis of Financial Condition and Results of Operations", \
        "Quantitative and Qualitative Disclosures About Market Risk", \
        "Financial Statements and Supplementary Data", \
        "Changes in and Disagreements With Accountants on Accounting and Financial Disclosure", \
        "Controls and Procedures", "Other Information", "Disclosure Regarding Foreign Jurisdictions that Prevent Inspections", \
        "Directors, Executive Officers and Corporate Governance", \
        "Executive Compensation", \
        "Security Ownership of Certain Beneficial Owners and Management and Related Stockholder Matters", \
        "Certain Relationships and Related Transactions, and Director Independence", \
        "Principal Accountant Fees and Services", \
        "Exhibits, Financial Statement Schedules"]
    # Find the link to the Business section
    
    for first_section, second_section in zip(section_name_10k, (section_name_10k[1:]+['Exhibits'])):
        first_link = soup.find("a", string=first_section)
        second_link = soup.find("a", string=second_section)

        # If found, extract the href attribute
        if first_link:
            first_href = first_link.get("href")
            print(f"{first_section} section href:", first_href)
        else:
            print(f"{first_section} section link not found")
            continue
        if second_link:
            second_href = second_link.get("href")
            print(f"{second_section} section href:", second_href)
        else:
            print(f"{second_section} section link not found")
            continue
        
        # Step 4: Go to base_url + second_href and find the start of the second section
        second_url =  txt_url + second_href
        response_second = requests.get(second_url, headers = headers)
        soup_second = BeautifulSoup(response_second.text, 'html.parser')

        second_tag = soup.find(attrs={"id": second_href.lstrip('#')}) or soup.find(attrs={"name": second_href.lstrip('#')})
        # print(second_tag)
        # second_id = second_tag.get("id")

        if not second_tag:
            print(f"Could not find the {second_section} section in the document")
            exit()

        # Step 5: Find first Section Start
        first_start_tag = soup.find(attrs={"id": first_href.lstrip('#')}) or soup.find(attrs={"name": first_href.lstrip('#')})
        # print(first_start_tag)

        if not first_start_tag:
            print(f"Could not find the {first_section} section in the document")
            exit()

        # Step 6: Extract Text from Business Section Until second section
        content = []
        seen_text = set()
        for tag in first_start_tag.find_all_next():
            if tag == second_tag:
                break
    
            text = tag.get_text(strip=True)
            if text and text not in seen_text:  # Avoid duplicates
                seen_text.add(text)
                content.append(text)

        # Step 7: Save or print the extracted content
        current_data = {
            "CIK": CIK,
            "ticker": ticker,
            "year": year,
            "quarter": quarter,
            "filing_type": '10-K',
            "section": first_section,
            "text": content
        }
        json_list.append(current_data)

    return json_list

def extract_texts_8k(CIK, ticker, accesion):
    return []

def extract_texts_10q(CIK, ticker, accesion):
    return []

def get_text_in_json(CIK, ticker, file_type_list, year_list, submission_file):
    accessions = get_accession_number(CIK, ticker, file_type_list, year_list, submission_file)
    try:
        print(accessions[0])
    except NameError:
        return (None, 0, 0)
    all_data = []
    for accession in accessions:
        json_list_10k = extract_texts_10k(CIK, ticker, accession)
        json_list_8k = extract_texts_8k(CIK, ticker, accession)
        json_list_10q = extract_texts_10q(CIK, ticker, accession)
        all_data_cik = json_list_10k + json_list_8k + json_list_10q
        all_data += all_data_cik

    filename = f'public_filing_text_by_section_{ticker}.json'
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print("Section content extracted successfully.")

    return all_data


if __name__ == "__main__":
    input_data = sys.argv[1:]  # Read from command-line args
    output_data = get_text_in_json(*input_data)
    outputs = {
        'last_modified_date': output_data[0]
    }
    # Kestra.outputs(outputs)