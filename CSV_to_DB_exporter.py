from sqlalchemy import create_engine, Column, String, Float, select, or_, and_, Table, MetaData, inspect, text, types
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects import oracle
import pandas as pd
import sys
import oracledb

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

# Connection header DO NOT TOUCH
username = 'username'
password = 'password'
dsn = 'dsn'

conection_string = f'oracle+cx_oracle://{username}:{password}@{dsn}'  # Opening sql
engine = create_engine(conection_string)  # Engine

# Creating a session
Session = sessionmaker(bind=engine)
session = Session()

import gc
import ctypes
import time

# ctypes is used to access objects by their memory address
class PyObject(ctypes.Structure):
    _fields_ = [("refcnt", ctypes.c_long)]

gc.disable()  # disabling cyclic GC

import pandas as pd

print("Let's start reading 1 file. 2 minutes")
with pd.read_csv("vins_1800.csv", 
                 chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=',', encoding='cp1251', quoting=3, low_memory=False, nrows=1
                ) as reader:
    chunks = []
    for chunk in reader:
        chunks.append(chunk)
    df_doc_23 = pd.concat(chunks, ignore_index=True)

print("Reading completed")

df = pd.read_csv("vins_end.csv",  
                 on_bad_lines='skip', encoding_errors='ignore', sep=',', encoding='cp1251', quoting=3, low_memory=False, nrows=1
                )
                
df = df.drop(df.columns[0], axis=1)
print('CREATE TABLE AL_BABKINA_VINS_END (')
for a in df.columns.tolist():
    print(a + ' varchar2(2000),')
print(');')
print('commit;')

# df_doc_23 = df_doc_23.drop(df_doc_23.columns[0], axis=1)
# df_doc_23.columns = [col.upper() for col in df_doc_23.columns]
# df_doc_23['DDATE'] = pd.to_datetime(df_doc_23['DDATE'])
df_doc_23

column_types = {col: str(df_doc_23[col].dtype) for col in df_doc_23.columns}

for key, value in column_types.items():
    print(f"{key}: {value}")
    

column_types = {col: str(df_doc_23[col].dtype) for col in df_doc_23.columns}

oracle_types = {
    'int64': 'NUMBER',
    'object': 'VARCHAR2(400)',
    'float64': 'NUMBER',
    'datetime64[ns]': 'DATE'
}

oracle_columns = []

for key, value in column_types.items():
    oracle_type = oracle_types.get(value, 'VARCHAR2(200)')
    oracle_columns.append(f"{key} {oracle_type}")

print('CREATE TABLE PANFILOV_FEATURES_70891_1_900 (')
print(", ".join(oracle_columns))
print(');')

# Create table if it does not exist
df_doc_23.to_sql('al_babkina_900', engine, if_exists='replace', index=False, dtype={col: df_doc_23[col].dtype for col in df_doc_23.columns})

# Insert data from DataFrame into the table
session.execute(text("INSERT INTO al_babkina_900 VALUES (" + ",".join(["?"] * len(df_doc_23.columns)) + ")"), df_doc_23.values.tolist())
session.commit()

only_in_24_df.to_sql('al_babkin', engine, if_exists='append', index=False)
session.commit()
print("Done")

del chunks
del chunk
del reader
print(PyObject.from_address(id(df_doc_23)).refcnt)