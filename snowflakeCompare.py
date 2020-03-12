import snowflake.connector
import pyodbc
from settings import *
import time




class SnowflakeComparison(object):
    def __init__(self,):
        self.sf_server = SNOWFLAKE_ACCOUNT
        self.sf_user = SNOWFLAKE_USER
        self.sf_pw = SNOWFLAKE_PW
        self.sf_db = SNOWFLAKE_DATABASE
        self.sf_wh = SNOWFLAKE_WAREHOUSE
        self.sf_schema = SNOWFLAKE_SCHEMA
        self.sf_connection = self.connect_to_snowflake()
        self.local_server = LOCAL_SERVER
        self.local_db = LOCAL_DB
        self.local_un = LOCAL_UN
        self.local_pw = LOCAL_PW
        self.local_connection = self.connect_to_local_db()
        self.storage_server=STORAGE_SERVER
        self.storage_db=STORAGE_DB
        self.storage_un=STORAGE_UN
        self.storage_pw=STORAGE_PW
        self.storage_connection = self.connect_to_storage_db()

    def __del__(self):
        print('Data has been sent to the database')



    def connect_to_snowflake(self):
        '''
        This method will build the connection to snowflake and start up the service
        '''
        conn = snowflake.connector.connect(user=SNOWFLAKE_USER, password=SNOWFLAKE_PW, account=SNOWFLAKE_ACCOUNT,
                                           role='ACCOUNTADMIN', warehouse=
                                           SNOWFLAKE_WAREHOUSE, schema=SNOWFLAKE_SCHEMA, database=SNOWFLAKE_DATABASE)

        try:
            sql = 'alter warehouse {} resume'.format(SNOWFLAKE_WAREHOUSE)
            self.execute_sf_query(conn, sql)
            print('Altered warehouse')
        except:
            pass
        return conn

    def connect_to_local_db(self):
        '''
        Connects to the local database
        '''
        connectionString = 'DRIVER={{ODBC Driver 17 for SQL Server}};server={};database={};uid={};pwd={}' \
            .format(self.local_server, self.local_db, self.local_un, self.local_pw)
        con = pyodbc.connect(connectionString)
        print('Connected to database {} on {}'.format(self.local_db,self.local_server))
        return con

    def connect_to_storage_db(self):
        '''
        Connects to the local database
        '''
        connectionString = 'DRIVER={{ODBC Driver 17 for SQL Server}};server={};database={};uid={};pwd={}' \
            .format(self.storage_server, self.storage_db, self.storage_un, self.storage_pw)
        con = pyodbc.connect(connectionString)
        print('Connected to database {} on {}'.format(self.storage_db,self.storage_server))
        cursor = con.cursor()
        cursor.execute(CREATE_STORAGE_TABLE)
        cursor.commit()
        return con


    def execute_sf_query(self, query):
        cursor = self.sf_connection.cursor()
        cursor.execute(query)
        return cursor

    def execute_local_query(self, query):
        cursor = self.local_connection.cursor()
        cursor.execute(query)
        return cursor

    def test_query_snowflake(self,query):
        '''
        This method will run the query past to it and return a dictionary with the query, row count and execution time.
        If there is an error, it will provide the error message.
        '''
        try:
            sql1 = query
            sql2 = 'select count(*) from({})z'.format(sql1)
            start = time.time()
            retVal = self.execute_sf_query(sql2)
            end = time.time()
            elapse = end - start
            rows = retVal.fetchone()[0]
            return {'query': query, 'row_count': rows, 'time': elapse, 'error':None}

        except Exception as e:
            return {'query': None, 'row_count': None, 'time': None, 'error':str(e).replace("'","''")}


    def test_query_local(self,query):
        '''
        This method will run the query past to it and return a dictionary with the query, row count and execution time.
        If there is an error, it will provide the error message.
        '''
        try:
            sql1 = query
            sql2 = 'select count(*) from({})z'.format(sql1)
            start = time.time()
            #cursor.execute(sql2)
            retVal = self.execute_local_query(sql2)
            end = time.time()
            elapse = end - start
            rows = retVal.fetchone()[0]
            return {'query': query, 'row_count': rows, 'time': elapse, 'error':None}

        except Exception as e:
            return {'query': None, 'row_count': None, 'time': None, 'error':str(e).replace("'","''")}


    def compare_results(self,query):
        retdict ={}
        try:
            sflake = self.test_query_snowflake(query)
            local = self.test_query_local(query)
            return {'query':query,'local_record_count':local['row_count'],'local_exec_time':local['time'],'snow_record_count':sflake['row_count'],
                    'snow_exec_time':sflake['time'],'localError':local['error'],'snowError':sflake['error']}

        except:
            pass


    def send_results_to_db(self,query):
        results = self.compare_results(query)
        cursor = self.storage_connection.cursor()
        cursor.execute("INSERT INTO SnowflakeCompareResults(query,local_rec_count,local_exec_time,snow_rec_count,snow_exec_time,local_error,snow_error) "
                       "VALUES (?,?,?,?,?,?,?)",
                       (results['query'].replace("'","''"), results['local_record_count'],results['local_exec_time'],results['snow_record_count'],
                        results['snow_exec_time'],results['localError'],results['snowError']))
        #cursor.execute(query)
        cursor.commit()
