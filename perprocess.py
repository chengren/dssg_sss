import re
# std_col_names is to standardlize the column names in the columns
def std_col_names(pd_dataframe):
	#if dataframe headers have parentheses and content within , replace with empty
	pd_dataframe.columns = [re.sub('\(.*?\)','',col) for col in pd_dataframe.columns]
	# trim the column name
	pd_dataframe.columns = [col.strip().lower() for col in pd_dataframe.columns]
	# if dataframe headers have spaces, replace with underscores
	pd_dataframe.columns = [col.replace(' ', '_').lower() for col in pd_dataframe.columns]
	return pd_dataframe
	
