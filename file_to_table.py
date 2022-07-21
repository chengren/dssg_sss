"""
Creates a primary table for SSS data.

First, reads SSS data, conducts pre-processing and creates a primary table for SSS data.

"""

import os, glob
import preprocess
import numpy as np
import pandas as pd
from sqlalchemy.orm import declarative_base 
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, ForeignKey, Boolean

"""
Path is a variable that accesses the folder containing the data of interest.

xls_files is a variabale that gets all the files in this folder containing "xls."

"""
path = os.getcwd()+'/data/'
xls_files = glob.glob(os.path.join(path, "*.xls*"))

def read_file(file):
    """
    Takes in a xls file and creates pandas dataframe.

    First, it reads the sheet of interest (family_type) and standardize the column names.

    Parameters
    ----------
    file: str
        path of the file that is added to database

    Returns
    -------
    df: pandas.dataframe
        dataframe including columns similar to the primary table 

    """
    try:
        xl = pd.ExcelFile(file)
        
        # print the file (or path name) to show which file we are working with
        n_sheets = len(xl.sheet_names)
        
        if n_sheets == 1 :
            df = pd.read_excel(file, sheet_name = 0)
        
        # if there are two sheets, then we read the sheetname that does not contain note
        if n_sheets == 2 :
            state_frame = [y for y in range(len(xl.sheet_names)) if "note" not in xl.sheet_names[y].lower()]
            df = pd.read_excel(file, sheet_name = state_frame[0])
        if n_sheets > 2:
            family_frame = [sheet for sheet in xl.sheet_names if 'Fam' in sheet]
            df = pd.read_excel(file, sheet_name = family_frame[0])
        
        # call the packages from preprocess.py
        df = preprocess.std_col_names(df)
    except:
        print('This file cannot be read' + ' ' +file.split('/')[-1])
    return df, file

def check_extra_columns(df):
    """
    Takes pandas dataframe. If there are additional special columns, adds a boolean column. 
    
    In particular, if broadband_&_cell_phone and health_care are included. 

    Parameters
    ----------
    df: pandas.dataframe
        this is the dataframe 


    Returns
    -------
    df: pandas.dataframe
        dataframe has additional columns that indicate whether there is a secondary table to match the finalized columns of the table.

    additional columns as:
    - 'broadband_&_cell_phone'
    - 'miscellaneous_is_secondary'
    - 'health_care_is_secondary'

    """
    if 'broadband_&_cell_phone' in df.columns:
        df['miscellaneous_is_secondary'] = True
    else:
        df['miscellaneous_is_secondary'] = False
    if 'health_care' in  df.columns:
        df['health_care_is_secondary'] = True
    else:
        df['health_care_is_secondary'] = False
    return df 



"""
Loop through the list of xls_files which contains the path of the files of interest.
Creates the pandas.dataframe from the file being read. 
Then, call the previous function to add boolean columns.
Update the column names that were causing issues when reading into the table (specifically, infant and emergency_savings).
At last, create the SQL table by creating sss class.

"""
for i in xls_files:  

    # read file and conduct pre-processing
    df, file = read_file(i)
    df = check_extra_columns(df)
    # need to have more precise solution for this specific problem
    # df['infant'] = pd.to_numeric(df['infant'],errors='coerce')
    # df['emergency_savings'] = pd.to_numeric(df['emergency_savings'],errors='coerce')

    # Create a 'weighted_child_count' column that takes the a*c* values from infant NaNs the rest
    df['weighted_child_count'] = df['infant']
    df.loc[df.loc[:,'family_type'].isin([i for i in df['family_type'] if 'c' not in i]),'weighted_child_count'] = np.nan

    df = df.drop_duplicates(subset=['analysis_type','family_type','state','year','place'])
    df.reset_index(inplace=True, drop=True)

    # access sqlite
    engine = create_engine('sqlite:///sss.sqlite', echo = False)
    m = MetaData(bind=engine)
    Base = declarative_base(metadata=m)

    # create container 
    class sss(Base):

        """
        Container that creates table using SSS data
        """
        __tablename__ = 'self_sufficiency_standard'

        family_type = Column('family_type', String, primary_key = True)
        state = Column('state', String, primary_key = True)
        place = Column('place', String, primary_key = True)
        year = Column('year', Integer, primary_key = True)
        analysis_type =  Column('analysis_type', String, primary_key = True)
        adult = Column('adult', Integer)
        infant = Column('infant', Integer)
        preschooler = Column('preschooler', Integer)
        schoolager = Column('schoolager', Integer)
        teenager = Column('teenager', Integer)
        weighted_child_count = Column('weighted_child_count', Integer)
        housing = Column('housing', Float)
        child_care = Column('child_care', Float)
        transportation = Column('transportation', Float)
        health_care = Column('health_care', Float)
        miscellaneous = Column('miscellaneous', Float)
        taxes = Column('taxes', Float)
        earned_income_tax_credit = Column('earned_income_tax_credit', Float)
        child_care_tax_credit = Column('child_care_tax_credit', Float)
        child_tax_credit = Column('child_tax_credit', Float)
        hourly_self_sufficiency_wage = Column('hourly_self_sufficiency_wage', Float)
        monthly_self_sufficiency_wage = Column('monthly_self_sufficiency_wage', Float)
        annual_self_sufficiency_wage = Column('annual_self_sufficiency_wage', Float)
        emergency_savings = Column('emergency_savings', Float)
        miscellaneous_is_secondary = Column('miscellaneous_is_secondary', Boolean)
        health_care_is_secondary = Column('health_care_is_secondary', Boolean)

    # print(file.split('/')[-1])

    Base.metadata.create_all(engine) 
    Session = sessionmaker(bind=engine)
    session = Session()
    df_dic = df.to_dict(orient="records")

    session.bulk_insert_mappings(sss, df_dic)
    session.commit()


# TODO: Having issues with reading OR 2021, need to debug, everything else work (specifically handling AARPA file)