import os
import duckdb
from google import genai
from dotenv import load_dotenv


con = duckdb.connect("company_public_info.duckdb")
load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


number_search_prompt_template = """
You are a helpful assistant for answering questions about public company XBRL data.

Your task is to **generate a valid SQL query** for DuckDB that answers the user’s question.

My DuckDB dataset name is edgar_data, add it before table name
Here is the DuckDB schema (tables and their columns):
{duckdb_schema}

You also have a list of tickers to filter by: {ticker_list}.
All queries should only include rows where `ticker` is in this list.

QUESTION: {question}

Please provide only the SQL query, without additional explanation or text,
as plain text, without any markdown formatting, code fences (```), or language tags (like sql).
Please add alias in computed column
if cast filing date is needed, please use extract instead of strftime
""".strip()


def number_search_prompt(sentence, ticker_list, duckdb_schema):
    duckdb_search_prompt = number_search_prompt_template.format(question=sentence, \
    ticker_list=ticker_list, duckdb_schema=duckdb_schema)
    return duckdb_search_prompt

def llm(prompt):
    response = client.models.generate_content(
        model="gemini-2.0-flash", contents=prompt
    )
    return response.text

def get_schema():    
    table_names = ["company_info", "financial_statement", "balance_sheet", "cashflow"]
    dataset_name = "edgar_data"
    duckdb_schema = {}
    for table_name in table_names:
        schema = con.execute(f"DESCRIBE {dataset_name}.{table_name}").fetchall()
        columns = [col[0] for col in schema]
        duckdb_schema[table_name] = columns
    return duckdb_schema

if __name__ == "__main__":
    sentence = "what was the cost of revenue of tesla in 2024"
    ticker_list = ["TSLA"]
    duckdb_search_prompt = number_search_prompt(sentence, ticker_list, get_schema())
    sql_query = llm(duckdb_search_prompt)
    if sql_query.startswith("```"):
        sql_query = "\n".join(sql_query.split("\n")[1:-1])
    print(sql_query)
    df = con.execute(sql_query).df()
    print(df)

