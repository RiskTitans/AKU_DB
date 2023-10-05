import pandas as pd
from sqlalchemy import create_engine, MetaData

db_url = 'postgresql://postgres:postgres@localhost:5432/AKU_DB'
excel_file = pd.ExcelFile(r'C:\Users\yuriy\Desktop\db_tables\AKUDB\DB2.xlsx')

# Create the SQLAlchemy engine and MetaData
engine = create_engine(db_url)
metadata = MetaData()
# Reflect the database and load table names
metadata.reflect(bind=engine)


table_names_postgres = metadata.tables.keys()
excel_sheet_names = pd.ExcelFile(excel_file).sheet_names

# Find differences between table names and Excel sheet names
postgres_only = set(table_names_postgres) - set(excel_sheet_names)
excel_only = set(excel_sheet_names) - set(table_names_postgres)

for table_name in excel_sheet_names:
    df = excel_file.parse(table_name)
    df.to_sql(table_name, engine, if_exists='replace', index=False)