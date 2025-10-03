import os
import csv
import json
from itertools import chain
from google import genai
from dotenv import load_dotenv
import vector_search
import build_vector_search_prompt
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

def build_prompt(routing_target, question, search_results):
    if routing_target == 'qdrant':
        answer_prompt = build_vector_search_prompt.build_vector_search_prompt(question, search_results[0])
    if routing_target == 'duckdb':
        result_dicts = search_results[0]
        duckdb_prompt_template = load_prompt("../prompts/duckdb_prompt.txt")
        answer_prompt = duckdb_prompt_template.format(question=question, result_dicts=result_dicts, \
            duckdb_schema=build_number_search_prompt.get_schema())
    if routing_target == 'both':
        both_prompt_template = load_prompt("../prompts/vector_and_number_prompt.txt")
        answer_prompt = both_prompt_template.format(question=question, \
            qdrant_results=search_results[0], duckdb_results=search_results[1])
    if routing_target == 'irrelevant':
        answer_prompt = """
        you are a financial analyst, answering questions related to public company reports. 
        user asked an irrelevant question, reject friendly
        """.strip()
    if routing_target == 'not_in_list':
        answer_prompt = """
        you are a financial analyst, answering questions related to public company reports.
        user asked a question beyond your knowledge scope. 
        reply politely saying the company or the year of filing is not in our database. 
        we will add them soon. 
        and then try to answer user's question as possible as you can. 
        """.strip()
    return answer_prompt

def llm(prompt):
    response = client.models.generate_content(
        model="gemini-2.0-flash", contents=prompt
    )
    return response.text

def rag(sentence):
    company_list = csv_to_json('../files/Nasdaq100List.csv')
    routing_prompt_template = load_prompt("../prompts/routing_prompt.txt")
    rounting_prompt = routing_prompt_template.format(question=sentence, \
        duckdb_schema=duckdb_schema, qdrant_schema=qdrant_schema, \
        company_list=company_list)
    routing_results = llm(rounting_prompt)
    if routing_results.startswith("```"):
        routing_results = "\n".join(routing_results.split("\n")[1:-1])
    # print(routing_results)
    routing_results_json = json.loads(routing_results)
    search_results = []
    if routing_results_json["target"] in ['qdrant', 'both']:
        vector_search_result = vector_search.vector_search(sentence, \
            routing_results_json["ticker_list"], routing_results_json["year_list"])
        # print(routing_results_json)
        context_texts = [d.payload["text"] for d in vector_search_result]
        # print(context_texts)
        flat_contexts = list(chain.from_iterable(context_texts))
        search_results.append("\n".join(flat_contexts))
    if routing_results_json["target"] in ['duckdb', 'both']:
        duckdb_search_prompt = build_number_search_prompt.number_search_prompt(sentence, \
            routing_results_json["ticker_list"], build_number_search_prompt.get_schema())
        result_dicts = build_number_search_prompt.get_duckdb_results(duckdb_search_prompt)
        search_results.append(result_dicts)
    answer_prompt = build_prompt(routing_results_json["target"], sentence, search_results)
    answer = llm(answer_prompt)

    return answer

if __name__ == "__main__":
    # user_input = 'What is the debt to equity ratio for Tesla in fiscal year 2024?'
    # user_input = 'What is the year over year growth of net income for Nvidia from year 2023 to year 2024? '
    # user_input = "What are the risk factors for Apple in year 2024"
    user_input = "Summarize Google’s discussion of advertising risks and show ad revenue growth from 2023 to 2024."
    answer = rag(user_input)
    print(answer)