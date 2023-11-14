import os
import pandas as pd
import time
# folder_path = r'C:\Users\yuriy\Desktop\db_tables\13_work_documentation\test'
dd_list_cols = {
    'id_obj': 'id_obj',
    'Текущая ревизия': 'current_rev',
    'Статус текущей ревизии': 'status_current_rev',
    'Дата статуса': 'status_date',
    'Письма': 'letter',
    'Статус Заказчика': 'status_company',
    'Дата статуса Заказчика': 'status_date_company',
    'Первый статус по выпуску РД': 'first_status_wd',
    'Дата выпуска РД': 'release_date_wd',
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
wd_cols = {
    'id_obj': 'id_obj',
    'Наименование объекта/комплекта РД': 'wd_name',
    'Коды работ по выпуску РД': 'wd_code',
    'Пакет РД': 'wd_batch',
    'Код KKS документа': 'wd_kks',
    'designer_code': 'designer_code',
    'Объект': 'object',
    'object_name': 'object_name',
    'Группа': 'unit',
    'WBS': 'wbs',
    'wbs_name': 'wbs_name',
    'Код работы СМР': 'smr_code',
    'Вид работ': 'dd_type',
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
    'id_obj': 'id_obj',
    'Код KKS базисной сметы': 'basesmeta_kks',
    'Номер базисной сметы': 'basesmeta_no',
    'Ревизия базисной сметы': 'basesmeta_rev',
    'Статус базисной сметы': 'basesmeta_status',
    'Дата статуса базисной сметы': 'basesmeta_date',
}
resource_smeta_cols = {
    'id_obj': 'id_obj',
    'Код KKS ресурсной сметы': 'resourcesmeta_kks',
    'Номер ресурсной сметы': 'resourcesmeta_no',
    'Ревизия ресурсной сметы': 'resourcesmeta_rev',
    'Статус ресурсной сметы': 'resourcesmeta_status',
    'Дата статуса ресурсной сметы': 'resourcesmeta_date',
}
all_cols = {**dd_list_cols,
            **wd_cols,
            **smr_cols,
            **designers_cols,
            **base_smeta_cols,
            **resource_smeta_cols}


def clean_excel(file_path, all_cols):
    save_path = r'C:\Users\yuriy\Desktop\db_tables\DSNDB\13_work_documentation\upload_files'
    filename = os.path.basename(file_path)
    save_file_path = os.path.join(save_path, filename)

    def compare_excel_columns(file_cols, base_cols):
        # file_cols and base_cols are set format

        # finds differences in columns
        only_in_base = base_cols.difference(file_cols)
        only_in_file = file_cols.difference(base_cols)

        return only_in_base, only_in_file

    try:
        start = time.time()
        # read xlsx Excel file
        excel_df = pd.read_excel(file_path, sheet_name=None)

        # check sheet names if Итого exist drop other sheets else merge Blok 1-2-3-4
        sheet_names = list(excel_df.keys())
        if 'Итого' in sheet_names:
            df = (excel_df['Итого'])
        elif all(block in sheet_names for block in ['блок 1', 'блок 2', 'блок 3', 'блок 4']):
            df = (pd.concat([excel_df[block] for block in ['блок 1', 'блок 2', 'блок 3', 'блок 4']],
                            axis=0, ignore_index=True, sort=False))
        else:
            return None

        # check column names compare with basefile columns (basefile columns added manually)
        base_columns = {
            'Наименование объекта/комплекта РД', 'Коды работ по выпуску РД',
            'Пакет РД', 'Код KKS документа', 'Текущая ревизия',
            'Статус текущей ревизии', 'Дата статуса', 'Письма', 'Статус Заказчика',
            'Дата статуса Заказчика', 'Первый статус по выпуску РД',
            'Дата выпуска РД', 'Ревизия, выданная в производство',
            'Дата выдачи в производство', 'Письмо о выдаче в производство',
            'Код KKS базисной сметы', 'Номер базисной сметы',
            'Ревизия базисной сметы', 'Статус базисной сметы',
            'Дата статуса базисной сметы', 'Код KKS ресурсной сметы',
            'Номер ресурсной сметы', 'Ревизия ресурсной сметы',
            'Статус ресурсной сметы', 'Дата статуса ресурсной сметы',
            'Ожидаемая дата получения РД от разработчика', 'Источник информации',
            'Ожидаемая дата выдачи РД в производство',
            'Разработчики РД (только основной договор)',
            'Разработчики РД (актуальные)',
            'Дата выпуска РД по договору подрядчика',
            'Дата выпуска РД по графику с Заказчиком', 'Дата старта СМР',
            'Код работы СМР', 'Наименование работы СМР',
            'id_obj', 'Объект', 'Группа', 'WBS',
            'Вид работ', 'Статус РД в 1С', 'Нужна в графике СМР'}

        file_columns = set(df.columns)

        only_base, only_file = compare_excel_columns(file_columns, base_columns)
        if only_file and only_base:
            print(f'\nDifferences found in file: {filename}\n')
            print("Columns only in base_file :\n")
            print("\n".join(only_base) + '\n')
            print('-' * 30)
            print("\nColumns only in file :\n")
            print("\n".join(only_file) + '\n')
            print('=' * 30 + '\n')
            return None
        else:
            print('all columns are same')

        # rename columns, align columns and drop unused and duplicated columns
        df = (df.rename(columns=all_cols))

        # Drop empty cells in 'wd_code' and align column types according to database
        df['id_obj'] = pd.to_numeric(df['id_obj'], errors='coerce')
        df.dropna(subset=['wd_code', 'id_obj'], inplace=True)
        df['id_obj'] = df['id_obj'].astype('int64')
        df = (df.drop_duplicates(subset="id_obj", keep='last', ignore_index=True)
              .set_index(["id_obj"])
              .sort_index()
              .reset_index())

        # df = df.drop_duplicates(subset='id_obj',  keep='last', ignore_index=True).reset_index(drop=True)

        date_cols = ['status_date',
                     'status_date_company',
                     'release_date_wd',
                     'date_toproduction',
                     'contract_toproduction_date',
                     'company_toproduction_date',
                     'smr_start_date',
                     'basesmeta_date',
                     'resourcesmeta_date'
                     ]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Separate columns ['expected_developer_date', 'expected_toproduction_date']
        #seperate mixed data datetime and str
        df['expected_developer_status'] = df['expected_developer_date'].where(
            pd.to_datetime(df['expected_developer_date'],
                           errors='coerce').isna())
        df['expected_developer_date'] = pd.to_datetime(df['expected_developer_date'],
                                                       format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

        df['expected_toproduction_status'] = df['expected_toproduction_date'].where(
            pd.to_datetime(df['expected_toproduction_date'],
                           errors='coerce').isna())
        df['expected_toproduction_date'] = pd.to_datetime(df['expected_toproduction_date'],
                                                          errors='coerce')

        #organize code
        df['designer_code'] = df['wd_kks'].str.extract(r'AKU\.(\d{4})')
        # TODO 1: solve for multiple hyphens texts
        # df[['object', 'object_name']] = df['object'].str.split(' - ', 1, expand=True)
        #df[['wbs', 'wbs_name']] = df['wbs'].str.split(' - ', 1, expand=True)
        df['object_name'] = ''
        df['wbs_name'] = ''

        end = time.time()
        print('Total time: ', end-start)

        df_cols = list(all_cols.values())
        df = df[df_cols]

        df.to_excel(save_file_path, sheet_name='Итого', columns=all_cols.values(), index=False)

    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")
        return None


#debug
# if __name__ == "__main__":
#     folder_path = r'C:\Users\yuriy\Desktop\db_tables\13_work_documentation\xlsx_files'
#     file_name = 'Отчет 20.1 по статусам РД (бл.1-4) на 2023.09.11.xlsx'
#     file_path = os.path.join(folder_path, file_name)
#     clean_excel(file_path, all_cols)
# Load the uploaded file, if it exists
if os.path.exists('uploaded_files.txt'):
    with open('uploaded_files.txt', 'r', encoding="utf-8") as file:
        uploaded_files = file.read().splitlines()
else:
    uploaded_files = []


folder_path = r'C:\Users\yuriy\Desktop\db_tables\DSNDB\13_work_documentation\upload_files'

for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    if os.path.isfile(file_path) and filename not in uploaded_files:
        clean_excel(file_path, all_cols)
        print(f'{filename} is cleaned and saved')
