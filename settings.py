'''
Snowflake connection information
'''
SNOWFLAKE_ACCOUNT =''
SNOWFLAKE_USER =''
SNOWFLAKE_PW=''
SNOWFLAKE_DATABASE= ''
SNOWFLAKE_WAREHOUSE=''
SNOWFLAKE_SCHEMA=''

'''
SQL server connections information
'''
LOCAL_SERVER=''
LOCAL_DB=''
LOCAL_UN=''
LOCAL_PW=''

'''
Connections for local storage
'''
STORAGE_SERVER=''
STORAGE_DB=''
STORAGE_UN=''
STORAGE_PW=''

'''
Create Storage table
'''
CREATE_STORAGE_TABLE='''
IF object_id('SnowflakeCompareResults') is null
CREATE TABLE [dbo].[SnowflakeCompareResults](
	[queryId] int IDENTITY(1,1) PRIMARY KEY,
	[query] [nvarchar](max) NULL,
	[local_rec_count] [int] NULL,
	[local_exec_time] [float] NULL,
	[snow_rec_count] [int] NULL,
	[snow_exec_time] [float] NULL,
	[local_error] [nvarchar](max) NULL,
	[snow_error] [nvarchar](max) NULL
)
'''



try:
    from localSettings import *
except ImportError:
    pass
