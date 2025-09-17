import os
import csv
import json
from google import genai
from dotenv import load_dotenv
import vector_search
import build_vector_search_prompt

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


routing_prompt_template = """
You are a query router. You must decide where to direct a user question. 
user question must be about public company filings include 10k, 10q and 8k.
- If it asks about structured/tabular data, choose Postgres. 
- If it asks about unstructured text or documents, choose Qdrant. 
- If both are needed, choose both. 
- If unrelated, mark as irrelevant.

postgres database schema: {postgres_schema}
qdrant vector database schema: {qdrant_schema}

user question: {question}
JSON list of companies: {company_list}

Given a user question and a JSON list of companies with their tickers, 
return a JSON object. Provide the JSON output directly, as if I were reading it from a file, 
without extra characters or formatting.

{{
  "ticker_list": ["TSLA", "AAPL"] or [],
  "target": "qdrant" or "postgres" or "both" or "irrelevant"
}}
""".strip()

postgres_schema = """
    not exist yet
    """.strip()

qdrant_schema = """
    payload schema columns include CIK, ticker, year, quarter, 
    filing_type(10-K, 10-Q, 8-K), section(only for 10-K), text
    """.strip()

def csv_to_json(csv_file_path):
    with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
        data = list(csv.DictReader(csvfile))
    json_str = json.dumps(data, indent=4)
    return json_str

def build_prompt(routing_target, question, search_results):
    if routing_target == 'qdrant':
        answer_prompt = build_vector_search_prompt.build_vector_search_prompt(question, search_results[0])
    if routing_target == 'postgres':
        answer_prompt = build_number_search_prompt(question, search_results)
    if routing_target == 'both':
        answer_prompt = """
        not yet build
        """
    if routing_target == 'irrelevant':
        answer_prompt = """
        you are a chat bot, answering questions related to public company reports. 
        user asked an irrelevant question, reject friendly
        """.strip()
    return answer_prompt

def llm(prompt):
    response = client.models.generate_content(
        model="gemini-2.0-flash", contents=prompt
    )
    return response.text

def rag(sentence):
    company_list = csv_to_json('../files/Nasdaq100List.csv')
    rounting_prompt = routing_prompt_template.format(question=sentence, \
        postgres_schema=postgres_schema, qdrant_schema=qdrant_schema, \
        company_list=company_list)
    routing_results = llm(rounting_prompt)
    if routing_results.startswith("```"):
        routing_results = "\n".join(routing_results.split("\n")[1:-1])
    # print(routing_results)
    routing_results_json = json.loads(routing_results)
    search_results = []
    if routing_results_json["target"] in ['qdrant', 'both']:
        vector_search_result = vector_search.vector_search(sentence, routing_results_json["ticker_list"])
        search_results.append(vector_search_result)
    if routing_results_json["target"] in ['postgres', 'both']:
        database_search_result = number_search(sentence, routing_results_json["ticker_list"])
        search_results.append(database_search_result)
    answer_prompt = build_prompt(routing_results_json["target"], sentence, search_results)
    answer = llm(answer_prompt)

    return answer

if __name__ == "__main__":
    sentence = 'what did tesla report in 2024?'
    answer = rag(sentence)
    print(answer)