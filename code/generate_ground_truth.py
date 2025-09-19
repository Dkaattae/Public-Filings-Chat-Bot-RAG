import pandas as pd
import random
import duckdb

con = duckdb.connect("company_public_info.duckdb")
nasdaq_tickers_df = pd.read_csv('../files/Nasdaq100List.csv')
nasdaq_ticker_list = nasdaq_tickers_df['Symbol'].tolist()
ticker_list = random.sample(nasdaq_ticker_list, 5)

sql_query_template = """
select t1.oe as oe2023, t2.oe as oe2022, t1.oe-t2.oe as oe_diff, t1.ticker
from (
SELECT
    ticker,
    extract(year from fiscal_year_end_date) as fiscal_year,
    operating_expense as oe
FROM edgar_data.financial_statement
where extract(year from fiscal_year_end_date) = 2024 
  and ticker = 'GOOG'
) t1
left outer join (
SELECT
    ticker,
    extract(year from fiscal_year_end_date) as fiscal_year,
    operating_expense as oe
FROM edgar_data.financial_statement
where extract(year from fiscal_year_end_date) = 2023 
  and ticker = 'GOOG'
) t2
  on t1.ticker = t2.ticker
"""

sql_query = sql_query_template.format(ticker_list=ticker_list)
result_dicts = con.execute(sql_query).fetchall()
columns = [desc[0] for desc in con.description]
result_dicts = [dict(zip(columns, row)) for row in result_dicts]
print(result_dicts)