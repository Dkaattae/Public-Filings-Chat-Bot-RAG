import os
import duckdb
from google import genai
from dotenv import load_dotenv


con = duckdb.connect("company_public_info.duckdb")
load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def number_search_prompt(sentence, ticker_list, duckdb_schema):
    with open("../prompts/number_search_prompt.txt", "r") as f:
        number_search_prompt_template = f.read()
    duckdb_search_prompt = number_search_prompt_template.format(question=sentence, \
    ticker_list=ticker_list, duckdb_schema=duckdb_schema)
    return duckdb_search_prompt

def llm(prompt):
    response = client.models.generate_content(
        model="gemini-2.0-flash", contents=prompt
    )
    return response.text

def get_schema():    
    dataset_name = "edgar_data"
    tables = con.execute(f"SHOW TABLES FROM {dataset_name}").fetchall()
    table_names = [t[0] for t in tables]
    internal_tables = ['_dlt_loads', '_dlt_pipeline_state', '_dlt_version']
    user_tables = [t for t in table_names if t not in internal_tables]
    duckdb_schema = {}
    for table_name in user_tables:
        schema = con.execute(f"DESCRIBE {dataset_name}.{table_name}").fetchall()
        columns = [col[0] for col in schema]
        duckdb_schema[table_name] = columns
    return duckdb_schema

def get_duckdb_results(duckdb_search_prompt):
    sql_query = llm(duckdb_search_prompt)
    if sql_query.startswith("```"):
        sql_query = "\n".join(sql_query.split("\n")[1:-1])
    # print(sql_query)
    result_dicts = con.execute(sql_query).fetchall()
    columns = [desc[0] for desc in con.description]
    result_dicts = [dict(zip(columns, row)) for row in result_dicts]
    return result_dicts

if __name__ == "__main__":
    sentence = "what was the cost of revenue of tesla in 2024"
    # sentence = "Who is the current CEO of Tesla?"
    ticker_list = ['TSLA']
    # print(get_schema())
    duckdb_search_prompt = number_search_prompt(sentence, ticker_list, get_schema())
    result_dicts = get_duckdb_results(duckdb_search_prompt)
    print(result_dicts)

