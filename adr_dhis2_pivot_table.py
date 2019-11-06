import argparse
import json
import os
import sys

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin


def get_metadata(from_pickle=False):
    global category_combos
    global data_elements
    if from_pickle:
        category_combos = pd.read_pickle("build/category_combos.pickle")
        data_elements = pd.read_pickle("build/data_elements.pickle")
        return
    if not os.path.exists("build"):
        os.makedirs("build")
    cc_resource = "categoryOptionCombos?paging=false&fields=id,name"
    de_resource = "dataElements?paging=false&fields=id,name"
    r_cc = __get_dhis2_api_resource(cc_resource)
    r_de = __get_dhis2_api_resource(de_resource)
    cc_list = json.loads(r_cc.text)['categoryOptionCombos']
    de_list = json.loads(r_de.text)['dataElements']
    category_combos = pd.DataFrame(cc_list)
    data_elements = pd.DataFrame(de_list)
    category_combos.to_pickle("build/category_combos.pickle")
    data_elements.to_pickle("build/data_elements.pickle")


def get_dhis2_pivot_table_data(dhis2_pivot_table):
    r_pt = __get_dhis2_api_resource(dhis2_pivot_table)
    json_pt = json.loads(r_pt.text)
    df = pd.DataFrame(json_pt['dataValues'])
    return df


def export_category_config(df: pd.DataFrame) -> pd.DataFrame:
    categories_names = df['categoryOptionCombo'].replace(category_combos.set_index('id')['name'])
    categories_ids = df['categoryOptionCombo']
    categories_map = pd.DataFrame()
    categories_map['name'] = categories_names
    categories_map['id'] = categories_ids
    categories_map = categories_map.drop_duplicates(subset='id')
    with open(os.path.join(OUTPUT_DIR_NAME, f"{table_type}_config.json"), 'w') as f:
        f.write("[\n")
        for i, row in categories_map.iterrows():
            line = f'''{{
    "id": "{row["id"]}",
    "name": "{row["name"]}",
    "mapping": {{
        "age": "",
        "gender": ""
    }}
}},
'''
            f.write(line)
        f.write("]\n")
    return df


def extract_data_elements_names(df: pd.DataFrame) -> pd.DataFrame:
    df['dataElement'] = df['dataElement'].replace(data_elements.set_index('id')['name'])
    return df


def extract_categories(df: pd.DataFrame) -> pd.DataFrame:
    with open(PROGRAM_DATA_CONFIG, 'r') as f:
        program_config = json.loads(f.read())
        program_config = {x['id']: x['mapping'] for x in program_config}
    df['age'] = ''
    df['gender'] = ''
    for i, row in df.iterrows():
        category_id = row['categoryOptionCombo']
        categories = program_config[category_id]
        for c_name, c_value in categories.items():
            row[c_name] = c_value

    output_df = df[['orgUnit', 'period', 'age', 'gender']]
    output_df.columns = ['area_id', 'period', 'age', 'gender']

    empty_cols = [col for col in ['age', 'gender'] if (df[col] == '').all()]
    output_df.drop(empty_cols, axis=1, inplace=True)

    pivot = df[['dataElement', 'value']].pivot(columns='dataElement', values='value')
    out = pd.concat([output_df, pivot], axis=1)
    return out


def __get_dhis2_api_resource(resource):
    return requests.get(urljoin(DHIS2_URL, resource), auth=HTTPBasicAuth(DHIS2_USERNAME, DHIS2_PASSWORD))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pull geo data from a DHIS2 to be uploaded into ADR.')
    argv = sys.argv[1:]
    parser.add_argument('-e', '--env-file',
                        default='.env',
                        help='env file to read config from')

    args = parser.parse_args()

    load_dotenv(args.env_file)
    OUTPUT_DIR_NAME = f"output/{os.environ.get('OUTPUT_DIR_NAME', 'default')}"
    DHIS2_URL = os.getenv("DHIS2_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")
    PROGRAM_DATA = os.getenv('PROGRAM_DATA')
    PROGRAM_DATA_CONFIG = os.getenv("PROGRAM_DATA_CONFIG")

    get_metadata(from_pickle=True)

    tables = json.loads(PROGRAM_DATA)
    for table in tables:
        table_type = table['name']
        table_dhis2_resource = table['dhis2_resource']
        out = (
            get_dhis2_pivot_table_data(table_dhis2_resource)
                .pipe(export_category_config)
                .pipe(extract_data_elements_names)
                .pipe(extract_categories)
        )
        out.to_csv(os.path.join(OUTPUT_DIR_NAME, f"{table_type}.csv"), index=None)
