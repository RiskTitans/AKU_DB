import os
import pandas as pd

folder_path = r'C:\Users\yuriy\Desktop\db_tables\EQPDB\19_Таблица поставок\Выгрузка SPF'
dfs = []

for filename in os.listdir(folder_path):
    if filename.endswith('.xlsx'):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_excel(file_path)
        dfs.append(df)

merged_df = pd.concat(dfs, ignore_index=True)

