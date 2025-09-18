import pandas as pd
import random
import duckdb

con = duckdb.connect("company_public_info.duckdb")
nasdaq_tickers_df = pd.read_csv('../files/Nasdaq100List.csv')
nasdaq_ticker_list = nasdaq_tickers_df['Symbol'].tolist()
ticker_list = random.sample(nasdaq_ticker_list, 5)

sql_query_template = """
    select t1.ticker, t1.ceo_name, t1.fiscal_year, t2.ceo_name
    from (
    SELECT company_info.ticker, title, name AS ceo_name, fiscal_year
    FROM edgar_data.company_info__company_officers
    inner join edgar_data.company_info
        on company_info__company_officers._dlt_parent_id = company_info._dlt_id
    WHERE (title like '%CEO%' or title like '%Chief Executive Officer%')
        and extract(year from filing_date) = 2024
    ) t1
    inner join (
    SELECT company_info.ticker, title, name AS ceo_name, fiscal_year
    FROM edgar_data.company_info__company_officers
    inner join edgar_data.company_info
        on company_info__company_officers._dlt_parent_id = company_info._dlt_id
    WHERE (title like '%CEO%' or title like '%Chief Executive Officer%')
        and extract(year from filing_date) = 2023
    ) t2
        on t1.ticker = t2.ticker
    where t1.ceo_name != t2.ceo_name
    
"""

sql_query = sql_query_template.format(ticker_list=ticker_list)
result_dicts = con.execute(sql_query).fetchall()
columns = [desc[0] for desc in con.description]
result_dicts = [dict(zip(columns, row)) for row in result_dicts]
print(result_dicts)