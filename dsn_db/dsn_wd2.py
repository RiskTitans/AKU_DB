import pandas as pd
import os
import time
from sqlalchemy import create_engine
from dsn_db.cleaner import clean_excel

# Connect to the PostgreSQL database
db_url = 'postgresql://postgres:postgres@localhost:5432/DSN_DB'
engine_dsn = create_engine(db_url)


folder_path = r'C:\Users\yuriy\Desktop\db_tables\DSNDB\13_work_documentation\xlsx_files'
dd_log_cols = {
    'id_obj': 'id_obj',
    'Текущая ревизия': 'current_rev',
    'Статус текущей ревизии': 'status_current_rev',
    'Дата статуса': 'status_date',
    'Письма': 'letter',
    'Статус Заказчика': 'status_company',
    'Дата статуса Заказчика': 'status_date_company',
    'Первый статус по выпуску РД': 'first_status_dd',
    'Дата выпуска РД': 'release_date_dd',
    'Ревизия, выданная в производство': 'rev_onproduction',
    'Дата выдачи в производство': 'date_toproduction',
    'Письмо о выдаче в производство': 'letter_toproduction',
    'Код KKS базисной сметы': 'basesmeta_kks',
    'Код KKS ресурсной сметы': 'resourcesmeta_kks',
    'Ожидаемая дата получения РД от разработчика': 'expected_developer_date',
    'Источник информации': 'info_source',
    'Ожидаемая дата выдачи РД в производство': 'expected_toproduction_date',
    'Дата выпуска РД по договору подрядчика': 'contract_toproduction_date',
    'Дата выпуска РД по графику с Заказчиком': 'company_toproduction_date',
    'Статус РД в 1С': 'status_1c',
    'expected_developer_status': 'expected_developer_status',
    'expected_toproduction_status': 'expected_toproduction_status',
    # 'source_file': 'source_file'
}
dd_cols = {
    'id_obj': 'id_obj',
    'Наименование объекта/комплекта РД': 'dd_name',
    'Коды работ по выпуску РД': 'dd_code',
    'Пакет РД': 'dd_batch',
    'Код KKS документа': 'dd_kks',
    'designer_code': 'designer_code',
    # 'Объект': 'object',
    # 'object_name': 'object_name',
    # 'Группа': 'unit',
    # 'WBS': 'wbs',
    # 'wbs_name': 'wbs_name',
    # 'Код работы СМР': 'smr_code',
    # 'Вид работ': 'dd_type',
}
smr_cols = {
    'id_obj': 'id_obj',
    'Код работы СМР': 'smr_code',
    'Наименование работы СМР': 'smr_name',
    'Дата старта СМР': 'smr_start_date',
}
designers_cols = {
    'designer_code': 'designer_code',
    'Разработчики РД (только основной договор)': 'developer_contract',
    'Разработчики РД (актуальные)': 'developer_actual',
}
base_smeta_cols = {
    'Код KKS базисной сметы': 'basesmeta_kks',
    'Номер базисной сметы': 'basesmeta_no',
    'Ревизия базисной сметы': 'basesmeta_rev',
    'Статус базисной сметы': 'basesmeta_status',
    'Дата статуса базисной сметы': 'basesmeta_date',
}
resource_smeta_cols = {
    'Код KKS ресурсной сметы': 'resourcesmeta_kks',
    'Номер ресурсной сметы': 'resourcesmeta_no',
    'Ревизия ресурсной сметы': 'resourcesmeta_rev',
    'Статус ресурсной сметы': 'resourcesmeta_status',
    'Дата статуса ресурсной сметы': 'resourcesmeta_date',
}
all_cols = {**dd_log_cols,
            **dd_cols,
            **smr_cols,
            **designers_cols,
            **base_smeta_cols,
            **resource_smeta_cols}


def compare_dfs(df, db_df, table_name, pk):
    # df is excel data, db_df is database data
    # find new rows and changed rows in df_dd_log
    start = time.time()
    df = df.dropna(how ='all').where(pd.notna(df), None)
    aligned_df = db_df[[col for col in df.columns]].copy()

    df['source'] = 'new'
    aligned_df['source'] = 'old'
    # source 'new' and 'old' need for getting rows only from Excel file
    # (some Excel files have missing rows compared to database
    if db_df.empty:
        concatenated_df = df
    else:
        concatenated_df = pd.concat([df, aligned_df], ignore_index=True)

    if table_name == 'designers':
        new_rows = concatenated_df.drop_duplicates(subset=pk, keep='last', ignore_index=True)
        new_rows = new_rows[new_rows['source'] != 'old']
        new_rows = new_rows.drop('source', axis=1)
    else:
        new_rows = concatenated_df.drop_duplicates(subset=pk, keep='last', ignore_index=True)
        new_rows = new_rows[new_rows['source'] != 'old']
        new_rows = new_rows.drop('source', axis=1)

    # na filled with 'test' because in comparison (none == none) -> False
    concatenated_df = concatenated_df.drop(['source'], axis=1).fillna('test')
    duplicate_rows = (concatenated_df[concatenated_df.duplicated(keep=False)])
    changed_rows = df[~df[pk].isin(new_rows[pk]) & ~df[pk].isin(duplicate_rows[pk])]
    changed_rows = changed_rows.drop(['source'], axis=1)
    end = time.time()
    print('Comparison time:', end - start)
    return new_rows, changed_rows, db_df


def excel_dfs(file_path):

    df_excel = clean_excel(file_path, all_cols)
    #df_excel = pd.read_excel(file_path, sheet_name='Итого')

    # split df into df for each table from db
    start = time.time()
    df_dd_log = df_excel[[col for col in dd_log_cols.values() if col in df_excel.columns]]
    df_designers = (df_excel[[col for col in designers_cols.values() if col in df_excel.columns]]
                    .dropna(axis=0, how='any')
                    .drop_duplicates(subset="designer_code", keep='last', ignore_index=True)
                    .set_index(["designer_code"])
                    .sort_index()
                    .reset_index())

    df_dd = df_excel[[col for col in dd_cols.values() if col in df_excel.columns]]
    df_smr = df_excel[[col for col in smr_cols.values() if col in df_excel.columns]]
    df_base_smeta = df_excel[[col for col in base_smeta_cols.values() if col in df_excel.columns]]
    df_resource_smeta = df_excel[[col for col in resource_smeta_cols.values() if col in df_excel.columns]]

    #TODO : add smeta log to database and code
    col1 = {
        'basesmeta_kks': 'smeta_kks',
        'basesmeta_no': 'smeta_no',
        'basesmeta_rev': 'smeta_rev',
        'basesmeta_status': 'smeta_status',
        'basesmeta_date': 'smeta_date',
    }
    col2 = {
        'resourcesmeta_kks': 'smeta_kks',
        'resourcesmeta_no': 'smeta_no',
        'resourcesmeta_rev': 'smeta_rev',
        'resourcesmeta_status': 'smeta_status',
        'resourcesmeta_date': 'smeta_date',
    }
    df1 = df_base_smeta.copy().rename(columns=col1)
    df1['smeta_type'] = 'base_smeta'
    df2 = df_resource_smeta.copy().rename(columns=col2)
    df2['smeta_type'] = 'resource_smeta'
    df_smeta_log = pd.concat([df1, df2], ignore_index=True)
    del df1, df2

    # filter_smeta = 'DD sent FOR CONSTRUCTION / Комплект РД выдан В ПРОИЗВОДСТВО подрядчику'
    # df_base_smeta = df_base_smeta[df_base_smeta['basesmeta_status'].str.contains(filter_smeta)]
    # df_resource_smeta = df_resource_smeta[df_resource_smeta['resourcesmeta_status'].str.contains(filter_smeta)]

    df_excel_list = {
        'dd_log': df_dd_log,
        'dd': df_dd,
        'smr': df_smr,
        'designers': df_designers,
        'smeta_base': df_base_smeta,
        'smeta_resource': df_resource_smeta,
        'smeta_log': df_smeta_log
    }
    end = time.time()
    print('Excel dataframes init time:', end - start)
    return df_excel_list


def sql_dfs():
    start = time.time()
    # sql queries
    sql_dd_log = 'SELECT * FROM dd_log'
    sql_dd = 'SELECT * FROM dd'
    sql_smr = 'SELECT * FROM smr'
    sql_designers = 'SELECT * FROM designers'
    sql_basesmeta = 'SELECT * FROM smeta_base'
    sql_resourcesmeta = 'SELECT * FROM smeta_resource'
    sql_smeta_log = 'SELECT * FROM smeta_log'

    df_dd_log_db = (pd.read_sql_query(sql_dd_log, engine_dsn)
                    .drop(columns='source_file')
                    .drop_duplicates(keep='last')
                    .set_index(['id_obj'])
                    .sort_index()
                    .reset_index()
                    )
    df_dd_db = pd.read_sql_query(sql_dd, engine_dsn)
    df_smr_db = pd.read_sql_query(sql_smr, engine_dsn)
    df_designers_db = pd.read_sql_query(sql_designers, engine_dsn)
    df_smeta_base_db = pd.read_sql_query(sql_basesmeta, engine_dsn)
    df_smeta_resource_db = pd.read_sql_query(sql_resourcesmeta, engine_dsn)
    df_smeta_log_db = pd.read_sql_query(sql_smeta_log, engine_dsn)

    df_sql_list = {
        'dd_log': df_dd_log_db,
        'dd': df_dd_db,
        'smr': df_smr_db,
        'designers': df_designers_db,
        'smeta_base': df_smeta_base_db,
        'smeta_resource': df_smeta_resource_db,
        'smeta_log': df_smeta_log_db
    }
    end = time.time()
    print('sql dataframes init time:', end - start)
    return df_sql_list


def upload_to_sql(file_name, file_path):

    excel_df = excel_dfs(file_path)  # dict table name: table df
    sql_df = sql_dfs()  # dict table name: table df
    start5 = time.time()

    primary_key = {
        'dd_log': 'id_obj',
        'dd': 'id_obj',
        'designers': 'designer_code',
        'smeta_base': ['basesmeta_kks', 'basesmeta_rev'],
        'smeta_resource': ['resourcesmeta_kks', 'resourcesmeta_rev'],
        'smr': 'id_obj',
        'wbs': 'id',
        'smeta_log': ['smeta_kks', 'smeta_rev']
    }

    for table_name in sql_df.keys():

        pk_column = primary_key[table_name]
        if table_name not in ['smeta_base', 'smeta_resource']:
            new_data, changed_data, db_df = compare_dfs(excel_df[table_name], sql_df[table_name], table_name, pk_column)
            print('Table name to upload:', table_name)
        else:
            # upload to sql!
            concatenated_df = pd.concat([excel_df[table_name], sql_df[table_name]])
            concatenated_df = concatenated_df.drop_duplicates(subset=pk_column[0], keep='last')

            concatenated_df.to_sql(table_name, engine_dsn, if_exists='replace', index=False)
            print(f'{table_name} is uploaded to db!')
            continue

        if not new_data.empty or not changed_data.empty:
            if table_name == 'dd_log':
                new_data['source_file'] = file_name
                changed_data['source_file'] = file_name

                # upload to sql!
                new_data.to_sql(table_name, engine_dsn, if_exists='append', index=False)
                changed_data.to_sql(table_name, engine_dsn, if_exists='append', index=False)
                print(f'{table_name} is uploaded to db!')

            elif table_name == 'smeta_log':

                # upload to sql!
                new_data.to_sql(table_name, engine_dsn, if_exists='append', index=False)
                changed_data.to_sql(table_name, engine_dsn, if_exists='append', index=False)
            else:
                all_data = pd.concat([db_df, changed_data, new_data], ignore_index=True)
                all_data = all_data.drop_duplicates(subset=pk_column, keep='last', ignore_index=True)

                # upload to sql!
                all_data.to_sql(table_name, engine_dsn, if_exists='replace', index=False)
                print(f'{table_name} is uploaded to db!')
        else:
            print(f'{table_name} does not have any changes or new rows')

    end5 = time.time()
    print('upload to SQL time:', end5 - start5)


start1 = time.time()

# Load the uploaded file, if it exists
if os.path.exists('uploaded_files.txt'):
    with open('uploaded_files.txt', 'r', encoding="utf-8") as file:
        uploaded_files = file.read().splitlines()
else:
    uploaded_files = []
end1 = time.time()

for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    if os.path.isfile(file_path) and filename not in uploaded_files:
        print(f"Processing {filename}...")
        try:
            upload_to_sql(filename, file_path)
            print(f'{filename} Done')
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            continue

        uploaded_files.append(filename)
start2 = time.time()
with open('uploaded_files.txt', 'w', encoding="utf-8") as file:
    file.write('\n'.join(uploaded_files))
end2 = time.time()

print('Upload file read/write time:', end1-start1+end2-start2)
