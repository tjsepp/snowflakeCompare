import snowflake.connector
from localSettings import *
import time
#https://www.youtube.com/watch?v=KYdg9Bfi0X4
conn = snowflake.connector.connect(user=SNOWFLAKE_USER,password=SNOWFLAKE_PW,account=SNOWFLAKE_ACCOUNT,role='ACCOUNTADMIN',warehouse=
                                   SNOWFLAKE_WAREHOUSE,schema=SNOWFLAKE_SCHEMA, database=SNOWFLAKE_DATABASE)

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()
    return cursor

def execute_snowflake_query(query):
    try:
        try:
            sql = 'alter warehouse {} resume'.format(SNOWFLAKE_WAREHOUSE)
            execute_query(conn, sql)
            print('Altered warehouse')
        except:
            pass

        sql1 = query
        sql2 ='select count(*) from({})z'.format(sql1)
        cursor=conn.cursor()
        start = time.time()
        cursor.execute(sql2)
        end = time.time()
        elapse=end-start
        rows=cursor.fetchone()[0]
        return {'query': query, 'row_count': rows, 'time': elapse, 'database': 'Snowflake'}
    except Exception as e:
        print(e)






print(execute_snowflake_query("select * from company where FIC='GBR'"))