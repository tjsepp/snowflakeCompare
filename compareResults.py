from snowflakeCompare import *
from sample_sql_queries import QUERY_LIST

x = SnowflakeComparison()
for query in QUERY_LIST:
    x.send_results_to_db(query)