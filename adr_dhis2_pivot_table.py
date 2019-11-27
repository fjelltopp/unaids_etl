import argparse
import json
import os
import sys

import etl
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin


def get_metadata(from_pickle=False):
    global category_combos
    global data_elements
    global org_units
    if from_pickle:
        category_combos = pd.read_pickle("build/category_combos.pickle")
        data_elements = pd.read_pickle("build/data_elements.pickle")
        org_units = pd.read_pickle("build/org_units.pickle")
        return
    if not os.path.exists("build"):
        os.makedirs("build")
    cc_resource = "categoryOptionCombos?paging=false&fields=id,name"
    de_resource = "dataElements?paging=false&fields=id,name"
    ou_resource = "organisationUnits?paging=false&fields=id,name"
    r_cc = __get_dhis2_api_resource(cc_resource)
    r_de = __get_dhis2_api_resource(de_resource)
    r_ou = __get_dhis2_api_resource(ou_resource)
    cc_list = json.loads(r_cc.text)['categoryOptionCombos']
    de_list = json.loads(r_de.text)['dataElements']
    ou_list = json.loads(r_ou.text)['organisationUnits']
    category_combos = pd.DataFrame(cc_list)
    data_elements = pd.DataFrame(de_list)
    org_units = pd.DataFrame(ou_list)
    category_combos.to_pickle("build/category_combos.pickle")
    data_elements.to_pickle("build/data_elements.pickle")
    org_units.to_pickle("build/org_units.pickle")


def get_dhis2_pivot_table_data(pivot_table_id):
    dhis2_pivot_table_resource = __get_dhis2_table_api_resource(pivot_table_id)
    r_pt = __get_dhis2_api_resource(dhis2_pivot_table_resource)
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

    data_elements_names = df['dataElement'].replace(data_elements.set_index('id')['name'])
    data_elements_ids = df['dataElement']
    data_elements_map = pd.DataFrame()
    data_elements_map['name'] = data_elements_names
    data_elements_map['id'] = data_elements_ids
    data_elements_map = data_elements_map.drop_duplicates(subset='id')

    config_output_dir = os.path.join(OUTPUT_DIR_NAME, "configs")
    os.makedirs(config_output_dir, exist_ok=True)
    with open(os.path.join(config_output_dir, f"{table_type}_category_config.json"), 'w') as f:
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
    with open(os.path.join(config_output_dir, f"{table_type}_column_config.json"), 'w') as f:
        f.write("[\n")
        for i, row in data_elements_map.iterrows():
            line = f'''{{
    "id": "{row["id"]}",
    "name": "{row["name"]}",
    "mapping": "",
    "categoryMapping": {{
        "age": "",
        "gender": ""
    }}
}},
'''
            f.write(line)
        f.write("]\n")

    return df


def extract_data_elements_names(df: pd.DataFrame) -> pd.DataFrame:
    df['dataElementName'] = df['dataElement']
    if PROGRAM_DATA_COLUMN_CONFIG:
        with open(PROGRAM_DATA_COLUMN_CONFIG, 'r') as f:
            program_config = json.loads(f.read())
            de_id_map = {x['id']: x['mapping'] if x['mapping'] else x['id'] for x in program_config}
        df['dataElementName'] = df['dataElementName'].replace(de_id_map)
    # use default dhis2 de names for ids not in config
    df['dataElementName'] = df['dataElementName'].replace(data_elements.set_index('id')['name'])
    return df


def extract_areas_names(df: pd.DataFrame) -> pd.DataFrame:
    df['area_id'] = df['orgUnit']
    df['area_name'] = df['orgUnit'].replace(org_units.set_index('id')['name'])
    return df


def sort_by_area_name(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by=['area_name', 'period']).reset_index(drop=True)


def extract_categories_and_aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    with open(PROGRAM_DATA_CONFIG, 'r') as f:
        program_config = json.loads(f.read())
        program_config = {x['id']: x['mapping'] for x in program_config}
    if PROGRAM_DATA_COLUMN_CONFIG:
        with open(PROGRAM_DATA_COLUMN_CONFIG, 'r') as f:
            column_config = json.loads(f.read())
            column_categories_map = {x['id']: x.get('categoryMapping') for x in column_config}
    else:
        column_categories_map = {}
    metadata_cols = ['area_id', 'area_name', 'period']
    for i, row in df.iterrows():
        category_id = row['categoryOptionCombo']
        de_id = row['dataElement']
        categories = column_categories_map.get(de_id) or program_config[category_id]
        for c_name, c_value in categories.items():
            if c_name not in metadata_cols:
                metadata_cols.append(c_name)
            df.loc[i, c_name] = c_value

    df['value'] = df['value'].astype(float)
    df[metadata_cols] = df[metadata_cols].fillna('')

    aggregated_rows =  df[metadata_cols + ['dataElementName', 'value']].groupby(metadata_cols + ['dataElementName']).sum().reset_index()
    pivot = aggregated_rows.pivot(columns='dataElementName', values='value')
    semi_wide_format_df = pd.concat([aggregated_rows[metadata_cols], pivot], axis=1)

    data_cols = [x for x in semi_wide_format_df if x not in set(metadata_cols)]

    joined_rows = semi_wide_format_df.copy().drop_duplicates(subset=metadata_cols).set_index(metadata_cols)
    for i, row in semi_wide_format_df.iterrows():
        index = list(row[metadata_cols].values)
        for col_name, val in row[data_cols].items():
            if pd.notna(val):
                joined_rows.loc[tuple(index), col_name] = val
    output_df = joined_rows.reset_index()
    return output_df


def map_dhis2_id_area_id(df: pd.DataFrame) -> pd.DataFrame:
    if AREA_ID_MAP:
        area_id_df = pd.read_csv(AREA_ID_MAP, index_col=False)
        df['area_id'] = df['area_id'].replace(area_id_df.set_index('map_id')['area_id'])
    return df

def __get_dhis2_api_resource(resource):
    r = requests.get(urljoin(DHIS2_URL, resource), auth=HTTPBasicAuth(DHIS2_USERNAME, DHIS2_PASSWORD))
    etl.requests_util.check_if_response_is_ok(r)
    return r


def __fetch_pivot_table_details(dhis2_pivot_table_id):
    reportTableReport = f"reportTables/{dhis2_pivot_table_id}"
    rt_r = __get_dhis2_api_resource(reportTableReport)
    return json.loads(rt_r.text)

def __get_dhis2_table_api_resource(pivot_table_id):
    pivot_table_metadata = __fetch_pivot_table_details(pivot_table_id)
    dimensions_dx = [x['dataElement']['id'] for x in pivot_table_metadata['dataDimensionItems'] if x['dataDimensionItemType'] == "DATA_ELEMENT"]
    ou_elms = [x['id'] for x in pivot_table_metadata['organisationUnits']]
    ou_level = [f"LEVEL-{x!r}" for x in pivot_table_metadata.get('organisationUnitLevels', [])]
    pivot_table_resource = f"analytics/dataValueSet.json?" \
                           f"dimension=dx:{';'.join(dimensions_dx)}&" \
                           f"dimension=co&" \
                           f"dimension=ou:{';'.join(ou_elms + ou_level)}&" \
                           f"dimension=pe:2019;LAST_5_YEARS&" \
                           f"displayProperty=NAME"
    return pivot_table_resource

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
    PROGRAM_DATA_COLUMN_CONFIG = os.getenv("PROGRAM_DATA_COLUMN_CONFIG")
    AREA_ID_MAP = os.getenv("AREA_ID_MAP")

    get_metadata()
    tables = json.loads(PROGRAM_DATA)
    for table in tables:
        table_type = table['name']
        dhis2_pivot_table_id = table['dhis2_pivot_table_id']
        (get_dhis2_pivot_table_data(dhis2_pivot_table_id)
            .pipe(export_category_config)
        )
    for table in tables:
        table_type = table['name']
        dhis2_pivot_table_id = table['dhis2_pivot_table_id']
        out = (get_dhis2_pivot_table_data(dhis2_pivot_table_id)
                .pipe(extract_data_elements_names)
                .pipe(extract_areas_names)
                .pipe(extract_categories_and_aggregate_data)
                .pipe(sort_by_area_name)
                .pipe(map_dhis2_id_area_id)
        )
        os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'program'), exist_ok=True)
        out.to_csv(os.path.join(OUTPUT_DIR_NAME, 'program', f"{table_type}.csv"), index=None)
