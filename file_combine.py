import os, glob
import pandas as pd
import perprocess

def combine_table(excel_files):
	df_combine = pd.DataFrame()
	n_col_max = 0
	for file in excel_files:
		# read in excel as pandas dataframe
		df = pd.read_excel(file, sheet_name = 'By Family')
		#count # of columns
		n_col = df.shape[1]
		if n_col > n_col_max:
			n_col_max = n_col
		# call the packages from preprocess
		df = perprocess.std_col_names (df)
		#combine excel file
		df_combine = pd.concat([df_combine,df],axis=0)
		#NOTE: if the columns names are the same, they will connnect. Otherwiese creating new column
	n_col_combine = df_combine.shape[1]
	if n_col_combine > n_col_max:
		print('There might be some typos in columns names')
	return df_combine

	#BUG Warning: Typo in preshooler in WA2021_SSS_Partial.xlsb