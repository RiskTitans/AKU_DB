import pandas as pd
import json
import os


def read_excel(path):
    try:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            excel_df = pd.read_excel(file_path, sheet_name=None)
            print('file is exist')
            return excel_df

    except FileNotFoundError:
        print(f"The file '{path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def json_convert(df):
    structure = {}
    df_section = df['Section']
    df_subsection = df['Sub_section']
    df_part = df['Part']
    df_volume = df['Volume']
    df_appendix = df['Appendix']
    df_book = df['Book']

    # section
    for index, row in df_section.iterrows():
        section = row['Раздел']

        if section not in structure:
            structure[section] = {
                "section_name": row['Название раздела'],
                "section_developer": row['Разработчик книги/ раздела'],
                "sub_sections": {},
                "parts": {},
                "volumes": {},
                "appendixes": {},
                "books": {}
            }

    # sub_section
    for index, row in df_subsection.iterrows():
        section = row['Раздел']
        sub_section = row['Подраздел']

        if sub_section not in structure[section]["sub_sections"]:
            structure[section]["sub_sections"][sub_section] = {
                "sub_section_name": row['Название раздела'],
                "parts": {},
                "volumes": {},
                "appendixes": {},
                "books": {}
            }

    # part
    for index, row in df_part.iterrows():
        section = row['Раздел']
        sub_section = row['Подраздел']
        part = row['Часть']

        if (sub_section != 0) and (part not in structure[section]["sub_sections"][sub_section]["parts"]):
            structure[section]["sub_sections"][sub_section]["parts"][part] = {
                "part_name": row['Название раздела'],
                "volumes": {},
                "appendixes": {},
                "books": {}
            }
        else:
            structure[section]["parts"][part] = {
                "part_name": row['Название раздела'],
                "volumes": {},
                "appendixes": {},
                "books": {}
            }

    # appendix
    for index, row in df_appendix.iterrows():
        section = row['Раздел']
        part = row['Часть']
        appendix = row['Приложение']

        # section - part - appendix
        if appendix not in structure[section]["parts"][part]["appendixes"]:
            structure[section]["parts"][part]["appendixes"][appendix] = {
                "appendix_name": row['Название раздела'],
                "section_developer": row['Разработчик книги/ раздела'],
                "comments": row['Комментарии'],
                "volumes": {},
                "books": {}
            }

    # volume
    for index, row in df_volume.iterrows():
        section = row['Раздел']
        sub_section = row['Подраздел']
        part = row['Часть']
        volume = row['Том']
        appendix = row['Приложение']

        # section - subsection - part - volume
        if ((sub_section != 0) and (part != 0) and (appendix == 0) and (volume != 0) and
                volume not in structure[section]["sub_sections"][sub_section]["parts"][part]["volumes"]):
            structure[section]["sub_sections"][sub_section]["parts"][part]["volumes"][volume] = {
                "volume_name": row['Название раздела'],
                "comments": row['Комментарии'],
                "appendixes": {},
                "books": {}
            }
        # section - part - volume
        elif ((sub_section == 0) and (part != 0) and (appendix == 0) and (volume != 0) and
              volume not in structure[section]["parts"][part]["volumes"]):
            structure[section]["parts"][part]["volumes"][volume] = {
                "volume_name": row['Название раздела'],
                "comments": row['Комментарии'],
                "appendixes": {},
                "books": {}
            }
        # section - subsection - volume
        elif ((sub_section != 0) and (part == 0) and (appendix == 0) and (volume != 0) and
              volume not in structure[section]["sub_sections"][sub_section]["volumes"]):
            structure[section]["sub_sections"][sub_section]["volumes"][volume] = {
                "volume_name": row['Название раздела'],
                "comments": row['Комментарии'],
                "books": {}
            }
        # section - volume
        elif ((sub_section == 0) and (part == 0) and (appendix == 0) and (volume != 0) and
              volume not in structure[section]["volumes"]):
            structure[section]["volumes"][volume] = {
                "volume_name": row['Название раздела'],
                "comments": row['Комментарии'],
                "books": {}
            }
        # section - part - appendix - volume
        elif ((appendix != 0) and (part != 0) and (volume != 0) and (volume != 0) and
              (volume not in structure[section]["parts"][part]["appendixes"][appendix]["volumes"])):
            structure[section]["parts"][part]["appendixes"][appendix]["volumes"][volume] = {
                "volume_name": row['Название раздела'],
                "comments": row['Комментарии'],
                "books": {}
            }

    # book
    for index, row in df_book.iterrows():
        section = row['Раздел']
        sub_section = row['Подраздел']
        part = row['Часть']
        volume = row['Том']
        appendix = row['Приложение']
        book = row['Книга']
        book_info = {
            "book_code": row['Обозначение Комплекта'],
            "section_developer": row['Разработчик книги/ раздела'],
            "book_name": row['Название раздела'],
            "comments": row['Комментарии'],
            "construction_phase": row['Этап строительства'],
        }
        # Section - Part - Volume - Book
        if (sub_section == 0) and (part != 0) and (volume != 0) and (appendix == 0):
            structure[section]["parts"][part]["volumes"][volume][book] = book_info
        # Section - Part - Volume - Appendix - Book
        if (sub_section == 0) and (part != 0) and (volume != 0) and (appendix != 0):
            structure[section]["parts"][part]["appendixes"][appendix]["volumes"][volume][book] = book_info
        # Section - Subsection - Part - Volume - Book
        if (sub_section != 0) and (part != 0) and (volume != 0) and (appendix == 0):
            structure[section]["sub_sections"][sub_section]["parts"][part]["volumes"][volume][book] = book_info
        # Section - Subsection - Part - Book
        if (sub_section == 0) and (part != 0) and (volume == 0) and (appendix == 0):
            structure[section]["parts"][part][book] = book_info
        # Section - Part - Book
        if (sub_section == 0) and (part != 0) and (volume == 0) and (appendix != 0):
            structure[section]["parts"][part]["appendixes"][appendix][book] = book_info
        # Section - Book
        if (sub_section == 0) and (part == 0) and (volume == 0) and (appendix == 0):
            structure[section][book] = book_info


    def remove_empty_values(d):
        """
        Recursively remove empty key-value pairs from a dictionary.

        Args:
            d (dict): The dictionary from which to remove empty values.

        Returns:
            dict: The cleaned dictionary with no empty values.
        """
        if not isinstance(d, dict):
            return d

        cleaned_dict = {}
        for key, value in d.items():
            if isinstance(value, dict):
                nested_dict = remove_empty_values(value)
                if nested_dict:  # Only add non-empty dictionaries
                    cleaned_dict[key] = nested_dict
            elif isinstance(value, list):
                cleaned_list = [remove_empty_values(item) for item in value if remove_empty_values(item)]
                if cleaned_list:  # Only add non-empty lists
                    cleaned_dict[key] = cleaned_list
            elif value:  # Only add non-empty values
                cleaned_dict[key] = value

        return cleaned_dict

    # clear empty nested dicts
    clear_structure = remove_empty_values(structure)
    # Convert the dictionary to a JSON string with ensure_ascii=False
    json_data = json.dumps(clear_structure, ensure_ascii=False, indent=4)
    return json_data


folder_path = r'C:\Users\yuriy\Desktop\db_tables\100_AKU_DB\2_DSNDB\1_BD'

if __name__ == '__main__':
    df = read_excel(folder_path)
    json_data = json_convert(df)

    # Save the JSON string to a file with UTF-8 encoding
    with open('books.json', 'w', encoding='utf-8') as json_file:
        json_file.write(json_data)

    print("JSON file created successfully")


# Name is empty if book is cancelled

# Section -
# Section - Subsection
# Section - Subsection - Part
# Section - Subsection - Part - Volume
# Section - Subsection - Part - Volume - Book
# Section - Subsection - Part - Volume - Appendix - Book
# Section - Part
# Section - Part - Book
# Section - Part - Appendix
# Section - Part - Volume
# Section - Part - Volume - Book
# Section - Part - Volume - Appendix - Book
