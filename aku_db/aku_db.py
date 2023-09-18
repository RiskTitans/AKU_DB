import pandas as pd
import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="AKUDB",
    user="postgres",
    password="postgres"
)

# Create a cursor object to execute SQL statements
cursor = conn.cursor()

# Load the Excel file
excel_file = pd.ExcelFile(r'C:\Users\yuriy\Desktop\db_tables\AKUDB\DB.xlsx')

# Iterate over all sheets in the Excel file
for sheet_name in excel_file.sheet_names:
    # Read the sheet into a DataFrame
    df = excel_file.parse(sheet_name)

    # Sanitize column names
    df.columns = df.columns.str.replace(' ', '_', regex=True).str.replace('.', '', regex=True)

    # Define the table name based on the sheet name
    table_name = sheet_name.lower().replace(' ', '_')

    # Check if the table exists
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print(f"Table '{table_name}' already exists. Skipping creation.")
        continue

    # Generate the CREATE TABLE statement based on the DataFrame columns
    columns = ', '.join([f'"{column}" VARCHAR' for column in df.columns])
    create_table_query = f'CREATE TABLE {table_name} ({columns})'

    # Execute the CREATE TABLE statement
    cursor.execute(create_table_query)

    # Prepare the INSERT statement
    insert_query = f'INSERT INTO {table_name} ({", ".join([f"{column}" for column in df.columns])}) ' \
                   f'VALUES ({", ".join(["%s" for _ in df.columns])})'

    # Insert the data from the DataFrame into the table
    cursor.executemany(insert_query, df.values.tolist())

    # Commit the changes for each sheet
    conn.commit()

# Close the cursor and the connection
cursor.close()
conn.close()
