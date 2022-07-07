
import os, glob
import pandas as pd
import perprocess
from itertools import chain
from collections import Counter

path = os.getcwd()+'/data/'
xls_files = glob.glob(os.path.join(path, "*.xls*"))

file_names = []
col_names = []
for file in xls_files:
    # read in excel as pandas dataframe
    try:
        xl = pd.ExcelFile(file)
        print(file)
        n_sheets = len(xl.sheet_names)
        if n_sheets == 1 :
            df = pd.read_excel(file, sheet_name = 0,nrows= 5)
        if n_sheets == 2 :
            df = pd.read_excel(file, sheet_name = 1,nrows= 5)
        if n_sheets > 2:
            family_frame = [sheet for sheet in xl.sheet_names if 'Fam' in sheet]
            df = pd.read_excel(file, sheet_name = family_frame[0], nrows= 5)
        # call the packages from preprocess
        df = perprocess.std_col_names (df)
        col_name = df.columns
        file_name = file.split('/')[-1]
        file_names.append(file_name)
        col_names.append(col_name)
    except:
        print('This file cannot be read' + ' ' +file.split('/')[-1])


#Count frequncy of colnames
d = Counter(chain.from_iterable(col_names))
#to dataframe
col_count = pd.DataFrame.from_dict(d, orient='index').reset_index()
col_count.columns = ['col_name','count']
#write out csv
col_count.to_csv('col_count.csv',index=False)

####Generate a dataframe to reflect columns distribution in each table####
####
dic = dict(zip(file_names, col_names))
#create pair from nested dictionary
pairs = []
for key in dic.keys():
    for value in dic.get(key):
        pair = [key, value]
        pairs.append(pair)
table_cols = pd.DataFrame(pairs)
table_cols.columns = ['file_names','column_names']
table_cols['value']=1
#Long table to wide table so the distribution become dummy
table_cols_wide = table_cols.pivot_table(index='file_names', columns ='column_names', values='value',fill_value = 0)
table_cols_wide.loc['Total']= table_cols_wide.sum()
table_cols_wide.to_csv('table_cols_wide.csv')