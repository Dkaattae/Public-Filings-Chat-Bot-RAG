import os
import csv
import time
import json
from google import genai
from dotenv import load_dotenv
import build_number_search_prompt


load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

duckdb_schema = build_number_search_prompt.get_schema()

qdrant_schema = """
    payload schema columns include CIK, ticker, year, quarter, 
    filing_type(10-K, 10-Q, 8-K), section(only for 10-K), text
    """.strip()

def csv_to_json(csv_file_path):
    with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
        data = list(csv.DictReader(csvfile))
    json_str = json.dumps(data, indent=4)
    return json_str

def load_prompt(path):
    with open(path, "r") as f:
        return f.read()

def llm(prompt):
    response = client.models.generate_content(
        model="gemini-2.0-flash", contents=prompt
    )
    return response.text

def accuracy(relevance_total):
    return sum(relevance_total) / len(relevance_total)

gt_router_file_path = '../files/ground_truth_router.json'
with open(gt_router_file_path, "r") as f:
    gt_router = json.load(f)

company_list = csv_to_json('../files/Nasdaq100List.csv')
routing_prompt_template = load_prompt("../prompts/routing_prompt.txt")
relevance_total = []
results = []
for i, gt_row in enumerate(gt_router):
    
    sentence = gt_row['query']
    rounting_prompt = routing_prompt_template.format(question=sentence, \
        duckdb_schema=duckdb_schema, qdrant_schema=qdrant_schema, \
        company_list=company_list)
    routing_results = llm(rounting_prompt)
    if routing_results.startswith("```"):
        routing_results = "\n".join(routing_results.split("\n")[1:-1])
    # print(routing_results)
    routing_results_json = json.loads(routing_results)
    expected = gt_row['route']
    predicted =routing_results_json['target']
    result = (sentence, expected, predicted)
    print(result)
    results.append(result)

    relevance = 0
    if expected == predicted:
        relevance = 1
    relevance_total.append(relevance)
    time.sleep(5)
    

router_accuracy = accuracy(relevance_total)
print(router_accuracy)