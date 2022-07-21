import os, glob
import pandas as pd
import perprocess

def combine_table(excel_files):
	df_combine = pd.DataFrame()
	n_col_max = 0
	for file in excel_files:
		# read in excel as pandas dataframe
		try:
			xl = pd.ExcelFile(file)
			n_sheets = len(xl.sheet_names)
			if n_sheets == 1 :
				df = pd.read_excel(file, sheet_name = 0)
			if n_sheets == 2 :
				df = pd.read_excel(file, sheet_name = 1)
			if n_sheets > 2:
				family_frame = [sheet for sheet in xl.sheet_names if 'Fam' in sheet]
				df = pd.read_excel(file, sheet_name = family_frame[0])
			# call the packages from preprocess
			df = perprocess.std_col_names (df)
			# get number of cloumns in 
			n_col = df_combine.shape[1]
			#combine excel file df_combine
			df_combine = pd.concat([df_combine,df],axis=0)
			#NOTE: if the columns names are the same, they will connnect. 
			#Otherwiese creating new column
			if df_combine.shape[1] > n_col:
				print('Extra columns added and the file is' + ' '+ file.split('/')[-1])
		except:
			print('This file cannot be read' + ' ' +file.split('/')[-1])
	return df_combine