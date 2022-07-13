from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, ForeignKey
import os, glob
import pandas as pd
from sqlalchemy import create_engine, select
import perprocess
import file_combine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy_utils import get_mapper
import numpy as np


df_combine = pd.read_csv('df_combine_sss.csv')
df_combine['infant'] = pd.to_numeric(df_combine['infant'],errors='coerce')
df_combine['emergency_savings'] = pd.to_numeric(df_combine['emergency_savings'],errors='coerce')
df_combine = df_combine.drop_duplicates(subset=['analysis','family_type','state','year','place'])
df_combine.reset_index(inplace=True, drop=True)

engine = create_engine('sqlite:///sss.sqlite', echo = False)
m = MetaData(bind=engine)
Base = declarative_base(metadata=m)
class sss(Base):
    __tablename__ = 'self_sufficiency_standard'
    family_type = Column('family_type', String, primary_key = True)
    state = Column('state', String, primary_key = True)
    place = Column('place', String, primary_key = True)
    year = Column('year', Integer, primary_key = True)
    analysis =  Column('analysis', String, primary_key = True)
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

Base.metadata.create_all(engine) 
Session = sessionmaker(bind=engine)
session = Session()
df_com_dic = df_combine.to_dict(orient="records")

for i in range(0,len(df_com_dic),100000):
    print(i)
    #print(df_com_dic[i])
    session.bulk_insert_mappings(get_mapper(m.tables['self_sufficiency_standard']), df_com_dic[i:i+100000])
    session.commit()
