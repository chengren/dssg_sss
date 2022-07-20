import os, glob
from tabnanny import check
import pandas as pd
import preprocess
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, ForeignKey, Boolean
import os, glob
import pandas as pd
from sqlalchemy import create_engine, select
import preprocess
import file_combine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm import declarative_base
import numpy as np


path = os.getcwd()+'/test_data/'
xls_files = glob.glob(os.path.join(path, "*.xls*"))

def read_file(file):
    # read in excel as pandas dataframe
    try:
        xl = pd.ExcelFile(file)
        print(file)
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
        # call the packages from preprocess
        df = preprocess.std_col_names(df)
    except:
        print('This file cannot be read' + ' ' +file.split('/')[-1])
    return df

# create a miscellanoues and all other secondary columns for each df, to be zero and one 

def check_extra_columns(dataf):
    table_columns = ['family_type', 'state', 'place','year','analysis_type', 'adult', 'infant','preschooler', 'schoolager', 'teenager','weighted_child_count', 'housing','child_care', 
    'transportation', 'health_care', 'miscellaneous', 'taxes','earned_income_tax_credit','child_care_tax_credit','child_tax_credit', 'hourly_self_sufficiency_wage','monthly_self_sufficiency_wage', 'annual_self_sufficiency_wage', 'emergency_savings'] 
    if 'broadband_&_cell_phone' in dataf.columns:
        dataf['miscellaneous_is_secondary'] = True
    else:
        dataf['miscellaneous_is_secondary'] = False
    if 'health_care' in  dataf.columns:
        dataf['health_care_is_secondary'] = True
    else:
        dataf['health_care_is_secondary'] = False
    return dataf

for i in xls_files:
    df = read_file(i)
    df = check_extra_columns(df)
    df = df.drop_duplicates(subset=['analysis_type','family_type','state','year','place'])
    df.reset_index(inplace=True, drop=True)
    engine = create_engine('sqlite:///sss.sqlite', echo = False)
    m = MetaData(bind=engine)
    Base = declarative_base(metadata=m)
    class sss(Base):
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


    Base.metadata.create_all(engine) 
    Session = sessionmaker(bind=engine)
    session = Session()
    df_dic = df.to_dict(orient="records")

    session.bulk_insert_mappings(sss, df_dic)
    session.commit()

# df['infant'] = pd.to_numeric(df['infant'],errors='coerce')
# df['emergency_savings'] = pd.to_numeric(df['emergency_savings'],errors='coerce')


# need to ask Aziza when are you creating the column for the weighted children




