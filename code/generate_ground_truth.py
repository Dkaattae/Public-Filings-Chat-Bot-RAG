import pandas as pd
import random
import duckdb

con = duckdb.connect("company_public_info.duckdb")
nasdaq_tickers_df = pd.read_csv('../files/Nasdaq100List.csv')
nasdaq_ticker_list = nasdaq_tickers_df['Symbol'].tolist()
ticker_list = random.sample(nasdaq_ticker_list, 5)

sql_query_template = """
select 
  t1.ticker, company_info.short_name, 
  max(fiscal_year) as last_fiscal_year, 
  sum(t1.total_revenue) as cum_revenue
from (
SELECT
    ticker,
    extract(year from fiscal_year_end_date) as fiscal_year,
    total_revenue
FROM edgar_data.financial_statement
where ticker in {ticker_list}
order by ticker, fiscal_year desc
limit 3
) t1
left outer join edgar_data.company_info
  on t1.ticker = company_info.ticker
group by t1.ticker, short_name
"""

sql_template1 = """
select EXTRACT(year FROM to_timestamp(last_fiscal_year_end)) as fiscal_year
from edgar_data.company_info
where ticker = 'TSLA'
"""

sql_query = sql_template1.format(ticker_list=ticker_list)
result_dicts = con.execute(sql_query).fetchall()
columns = [desc[0] for desc in con.description]
result_dicts = [dict(zip(columns, row)) for row in result_dicts]
print(result_dicts)