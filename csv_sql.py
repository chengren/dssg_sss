import os, glob
import pandas as pd
from sqlalchemy import create_engine, select
import perprocess
import file_combine

# create sqlalchemy db connetion
engine = create_engine('sqlite:///dssg.sqlite', echo=False)
sqlite_connection = engine.connect()

# get current working directory path
# get list of files in current working directory
path = os.getcwd()+'/data/'
xls_files = glob.glob(os.path.join(path, "*.xls*"))
df_combine = file_combine.combine_table(xls_files)
# write dataframe to sqlite, overwrite table if it exists
df_combine.to_sql('SSS', sqlite_connection, if_exists='replace')

# after the process completes, closes the databasse connection
sqlite_connection.close()