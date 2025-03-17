from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import sys
import oracledb

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

# Connection header DO NOT TOUCH
username = 'username'
password = 'password'
dsn = 'dsn'

connection_string = f'oracle+cx_oracle://{username}:{password}@{dsn}'  # Opening sql
engine = create_engine(connection_string)  # Engine

# Creating a session
Session = sessionmaker(bind=engine)
session = Session()

import gc
import ctypes

# ctypes is used to access objects by their memory address
class PyObject(ctypes.Structure):
    _fields_ = [("refcnt", ctypes.c_long)]

gc.disable()  # disabling cyclic GC

# Read CSV with chunking
print("Let's start reading 1 file. 2 minutes")
with pd.read_csv("vins_1800.csv", 
                 chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=',', encoding='cp1251', quoting=3, low_memory=False) as reader:
    df_doc_23 = pd.concat(reader, ignore_index=True)

print("Reading completed")

# Reading another file
df = pd.read_csv("vins_end.csv",  
                 on_bad_lines='skip', encoding_errors='ignore', sep=',', encoding='cp1251', quoting=3, low_memory=False)
df = df.drop(df.columns[0], axis=1)

# Print the SQL create table command
print('CREATE TABLE AL_BABKINA_VINS_END (')
for col in df.columns.tolist():
    print(f"{col} varchar2(2000),")
print(');')
print('commit;')

# Get column types
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

print('CREATE TABLE al_babkina_future_70891_1_900 (')
print(", ".join(oracle_columns))
print(');')

# Create table if it does not exist
df_doc_23.to_sql('al_babkina_900', engine, if_exists='replace', index=False)

# Use context manager for session to ensure it closes
with session.begin():
    for chunk in pd.read_csv("vins_end.csv", chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=',', encoding='cp1251'):
        # Insert each chunk into Oracle
        session.execute(
            text("INSERT INTO al_babkina_900 VALUES (" + ",".join([f":{col}" for col in chunk.columns]) + ")"), 
            {col: val for col, val in zip(chunk.columns, chunk.values.tolist()[0])}
        )
    session.commit()

print("Done")

# Explicitly delete large objects to free memory
del df_doc_23
del chunk
gc.collect()

# Check reference count
print(PyObject.from_address(id(df_doc_23)).refcnt)
