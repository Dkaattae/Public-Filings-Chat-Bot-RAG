import os
import duckdb
from fastembed import TextEmbedding
from qdrant_client import QdrantClient, models
    
qd_client = QdrantClient(host="localhost", port=6333)
collection_name = "public_filing_data_public_filings_docs"
embed_model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")

def last_fiscal_year_end_date(ticker_list):
    duckdb_name = "company_public_info.duckdb"
    sql_query_template = '''
        select 
            ticker,
            EXTRACT(year FROM to_timestamp(last_fiscal_year_end))as last_fiscal_year
        from edgar_data.company_info
        where ticker in {ticker_list}
    '''
    with duckdb.connect(duckdb_name) as con:
        sql_query = sql_query_template.format(ticker_list=ticker_list)
        result_dicts = con.execute(sql_query).fetchall()
        columns = [desc[0] for desc in con.description]
        result_dicts = [dict(zip(columns, row)) for row in result_dicts]
    year_list = [d['last_fiscal_year'] for d in result_dicts]
    res = []
    [res.append(val) for val in year_list if val not in res]
    return max(res)

def vector_search(sentence, ticker_list, year_list, limit=3):
    # qdrant_filter = None
    last_year = last_fiscal_year_end_date(ticker_list)
    if year_list == []:
        year_list = [str(last_year)]
    numerical_year_list = [int(y) for y in year_list]
    if max(numerical_year_list) > last_year:
        numerical_year_list = [str(y) for y in numerical_year_list if y <= last_year]
    year_list = [str(x) if isinstance(x, (int, float)) else x for x in year_list]
    if len(ticker_list) > 0:
        qdrant_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="ticker",
                    match=models.MatchAny(any=ticker_list)
                ),
                models.FieldCondition(
                    key="year",
                    match=models.MatchAny(any=year_list)
                )
            ]
        )
    else:
        qdrant_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="year",
                    match=models.MatchAny(any=year_list)
                )
            ]
        )
    print(year_list)
    query_embedding = embed_model.embed([sentence])
    query_vector = list(query_embedding)[0]
    query_points = qd_client.query_points(
        collection_name=collection_name,
        query=query_vector,
        using="fast-bge-small-en",
        query_filter=qdrant_filter,
        limit=limit,
        with_payload=True
        )
    print(query_points)
    return query_points.points

if __name__ == "__main__":
    question = "What are the risk factors for Apple in year 2024"
    ticker_list = ['AAPL']
    year_list = ['2024']
    related_docs = vector_search(question, ticker_list, year_list)
    related_text = related_docs[0].payload['text']
    print(related_docs)