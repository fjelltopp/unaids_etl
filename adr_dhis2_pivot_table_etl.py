#!/usr/bin/env python3

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

import credentials

etl.LOGGER = etl.logging.get_logger(log_name="DHIS2 pivot table pull", log_group="etl")


@etl.decorators.log_start_and_finalisation("getting DHIS2 metadata")
def get_metadata(from_pickle=False):
    global category_combos
    global data_elements
    global org_units
    build_dir_ = os.path.join(OUTPUT_DIR_NAME, "build")
    os.makedirs(build_dir_, exist_ok=True)
    cc_pickle_path = os.path.join(build_dir_, "category_combos.pickle")
    de_pickle_path = os.path.join(build_dir_, "data_elements.pickle")
    ou_pickle_path = os.path.join(build_dir_, "org_units.pickle")
    p_exist = all(map(os.path.exists, [cc_pickle_path, de_pickle_path, ou_pickle_path]))
    if from_pickle and p_exist:
        category_combos = pd.read_pickle(cc_pickle_path)
        data_elements = pd.read_pickle(de_pickle_path)
        org_units = pd.read_pickle(ou_pickle_path)
        return
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
    category_combos.to_pickle(cc_pickle_path)
    data_elements.to_pickle(de_pickle_path)
    org_units.to_pickle(ou_pickle_path)


@etl.decorators.log_start_and_finalisation("get DHIS2 pivot table data")
def get_dhis2_pivot_table_data(pivot_table_id, from_pickle=False):
    build_dir_ = os.path.join(OUTPUT_DIR_NAME, "build")
    os.makedirs(build_dir_, exist_ok=True)
    pt_pickle_path = os.path.join(build_dir_, f"pivot_table_{pivot_table_id}.pickle")
    if from_pickle and os.path.exists(pt_pickle_path):
        df = pd.read_pickle(pt_pickle_path)
        return df
    dhis2_pivot_table_resource = __get_dhis2_table_api_resource(pivot_table_id)
    r_pt = __get_dhis2_api_resource(dhis2_pivot_table_resource)
    json_pt = json.loads(r_pt.text)
    df = pd.DataFrame(json_pt['dataValues'])
    df.to_pickle(pt_pickle_path)
    return df

@etl.decorators.log_start_and_finalisation("export category config")
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
    with open(os.path.join(config_output_dir, f"{TABLE_TYPE}_category_config.json"), 'w') as f:
        f.write("[")
        first = True
        for i, row in categories_map.iterrows():
            if not first:
                f.write(',')
            else:
                first = False
            line = f'''
{{
    "id": "{row["id"]}",
    "name": "{row["name"]}",
    "mapping": {{
        "age_group": "",
        "sex": ""
    }}
}}'''
            f.write(line)
        f.write("\n]\n")
    with open(os.path.join(config_output_dir, f"{TABLE_TYPE}_column_config.json"), 'w') as f:
        f.write("[")
        first = True
        for i, row in data_elements_map.iterrows():
            if not first:
                f.write(',')
            else:
                first = False
            line = f'''
{{
    "id": "{row["id"]}",
    "name": "{row["name"]}",
    "mapping": "",
    "categoryMapping": {{
        "age_group": "",
        "sex": ""
    }}
}}'''
            f.write(line)
        f.write("\n]\n")

    return df

@etl.decorators.log_start_and_finalisation("extract data elements names")
def extract_data_elements_names(df: pd.DataFrame) -> pd.DataFrame:
    df['dataElementName'] = df['dataElement']
    if PROGRAM_DATA_COLUMN_CONFIG:
        de_id_map = {}
        for column_config_filename in PROGRAM_DATA_COLUMN_CONFIG.split(','):
            with open(column_config_filename, 'r') as f:
                program_config = json.loads(f.read())
                for config_ in program_config:
                    mapping = config_.get('mapping')
                    if not mapping:
                        continue
                    elif type(mapping) != list:
                        mapping = [mapping]
                    de_id_map[config_['id']] = mapping
        extra_rows = pd.DataFrame(columns=list(df))
        for i, row in df.iterrows():
            de_id = row['dataElementName']
            mappings = de_id_map.get(de_id, [])
            if len(mappings) == 0:
                continue
            df.loc[i, 'dataElementName'] = mappings[0]
            if len(mappings) > 1:
                for mapping in mappings[1:]:
                    extra_row = row.copy()
                    extra_row['dataElementName'] = mapping
                    extra_rows = extra_rows.append(extra_row)
        df = df.append(extra_rows, ignore_index=True)

    # use default dhis2 de names for ids not in config
    df['dataElementName'] = df['dataElementName'].replace(data_elements.set_index('id')['name'])
    return df


@etl.decorators.log_start_and_finalisation("extract areas names")
def extract_areas_names(df: pd.DataFrame) -> pd.DataFrame:
    df['area_id'] = df['orgUnit']
    df['area_name'] = df['orgUnit'].replace(org_units.set_index('id')['name'])
    return df


@etl.decorators.log_start_and_finalisation("sort by area name")
def sort_by_area_name(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by=['area_name', 'period']).reset_index(drop=True)


@etl.decorators.log_start_and_finalisation("extract categories and aggregate data")
def extract_categories_and_aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    category_mapping = {}
    for category_config_filename in PROGRAM_DATA_CATEGORY_CONFIG.split(','):
        with open(category_config_filename, 'r') as f:
            category_config = json.loads(f.read())
            map_ = {x['id']: x.get('mapping', {}) for x in category_config}
            category_mapping.update(map_)
    if PROGRAM_DATA_COLUMN_CONFIG:
        column_categories_map = {}
        for column_config_filename in PROGRAM_DATA_COLUMN_CONFIG.split(','):
            with open(column_config_filename, 'r') as f:
                column_config = json.loads(f.read())
                map_ = {x['id']: x.get('categoryMapping') for x in column_config}
                column_categories_map.update(map_)
    else:
        column_categories_map = {}
    categories_to_remove = set([x['id'] for x in category_config if bool(x.get('remove'))])
    df = df.loc[~df['categoryOptionCombo'].isin(categories_to_remove)]
    metadata_cols = ['area_id', 'area_name', 'period']

    # Restructure mapping dictionaries to support direct usage of pandas map function
    category_maps = {}
    for category_id in category_mapping:
        for category_name in category_mapping[category_id]:
            if category_name not in category_maps:
                category_maps[category_name] = {}
            category_maps[category_name][category_id] = category_mapping[category_id][category_name]

    column_categories_maps = {}
    for de_id in column_categories_map:
        if column_categories_map.get(de_id, {}):
            for de_name in column_categories_map.get(de_id, {}):
                if de_name not in column_categories_maps:
                    column_categories_maps[de_name] = {}
                column_categories_maps[de_name][de_id] = column_categories_map[de_id][de_name]

    # Map DHIS2 id's according to maps created above
    for category in category_maps:
        df[category] = df['categoryOptionCombo'].map(category_maps[category])

    # Fill missing category combos with data element maps in case they were created as separate data elements
    for column in column_categories_maps:
        df[column].fillna(value=df['dataElement'].map(column_categories_maps[column]),
                          inplace=True)

    df['value'] = pd.to_numeric(df['value'], errors='coerce', downcast='integer')
    df[metadata_cols] = df[metadata_cols].fillna('')

    aggregated_rows = df[metadata_cols + ['dataElementName', 'value']].groupby(metadata_cols + ['dataElementName']).sum().reset_index()
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


@etl.decorators.log_start_and_finalisation("trimming period strings")
def trim_period_strings(df: pd.DataFrame) -> pd.DataFrame:
    df['period'] = df['period'].str[:4]
    df = df.rename(columns={'period': 'year'})
    return df


@etl.decorators.log_start_and_finalisation("map dhis2 id to area id")
def map_dhis2_id_area_id(df: pd.DataFrame) -> pd.DataFrame:
    if AREA_ID_MAP:
        area_id_df = pd.read_csv(AREA_ID_MAP, index_col=False)
        if 'map_id' in list(area_id_df):
            mapping_column_name = 'map_id'
        else:
            mapping_column_name = 'dhis2_id'
        df['area_id'] = df['area_id'].replace(area_id_df.set_index(mapping_column_name)['area_id'])

        cols = df.columns.tolist()

        # check if the DataFrame has duplicate mappings
        if 'sex' in df:
            group_by = ['area_id', 'period', 'age_group', 'sex']
        else:
            group_by = ['area_id', 'period', 'age_group']
        df_count = df.groupby(group_by).count()

        any_many_to_one_mappings = df_count[df_count > 1].any().any()
        # In case there are many to one location mappings, sum aggregate data and fetch name from map
        if any_many_to_one_mappings:
            df_grouped = df.groupby(group_by).sum()
            df = df_grouped.reset_index(inplace=False)
            area_id_df_grouped = area_id_df.groupby(['area_id']).min().reset_index(inplace=False)
            df['map_name'] = df['area_id'].map(lambda x: area_id_df_grouped.set_index('area_id').at[x, 'map_name'])

            # Reorder DF columns to preferred order
            if 'sex' in df:
                cols2 = ['area_id', 'map_name', 'period', 'age_group', 'sex', 'current_art']
            else:
                cols2 = ['area_id', 'map_name', 'period', 'age_group', 'anc_clients', 'ancrt_known_pos', 'ancrt_already_art',
                         'ancrt_tested', 'ancrt_test_pos']

            df = df[cols2]

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
    periods = [x['id'] for x in pivot_table_metadata['periods']]
    if len(dimensions_dx) < 1:
        raise ValueError(f"No data elements configured for pivot table {pivot_table_id}")
    if len(ou_elms + ou_level) < 1:
        raise ValueError(f"No org units configured for pivot table {pivot_table_id}")
    if len(periods) < 1:
        raise ValueError(f"No periods configured for pivot table {pivot_table_id}")
    pivot_table_resource = f"analytics/dataValueSet.json?" \
                           f"dimension=dx:{';'.join(dimensions_dx)}&" \
                           f"dimension=co&" \
                           f"dimension=ou:{';'.join(ou_elms + ou_level)}&" \
                           f"dimension=pe:{';'.join(periods)}&" \
                           f"displayProperty=NAME"
    return pivot_table_resource


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pull geo data from a DHIS2 to be uploaded into ADR.')
    argv = sys.argv[1:]
    parser.add_argument('-e', '--env-file',
                        default='.env',
                        help='env file to read config from')
    parser.add_argument('-p', '--pickle',
                        dest='pickle',
                        action='store_true',
                        help='fetch data from local pickle instead http call to DHIS2')
    parser.add_argument('-t', '--pivot-table-config',
                        dest='pt_config',
                        action='store_true',
                        help='fetch pivot table configuration data from DHIS2')
    args = parser.parse_args()

    load_dotenv(args.env_file)
    EXPORT_NAME = os.environ.get('OUTPUT_DIR_NAME', 'default')
    OUTPUT_DIR_NAME = f"output/{EXPORT_NAME}"
    DHIS2_URL = os.getenv("DHIS2_URL")
    credentials.read_credentials(os.getenv("DHIS2_CREDENTIALS_FILE"))
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")
    PROGRAM_DATA = os.getenv('PROGRAM_DATA')
    PROGRAM_DATA_CATEGORY_CONFIG = os.getenv("PROGRAM_DATA_CATEGORY_CONFIG")
    # Legacy env name support
    if not PROGRAM_DATA_CATEGORY_CONFIG:
        PROGRAM_DATA_CATEGORY_CONFIG = os.getenv("PROGRAM_DATA_CONFIG")
    PROGRAM_DATA_COLUMN_CONFIG = os.getenv("PROGRAM_DATA_COLUMN_CONFIG")
    AREA_ID_MAP = os.getenv("AREA_ID_MAP")

    get_metadata(from_pickle=args.pickle)
    tables = json.loads(PROGRAM_DATA)
    if args.pt_config:
        for table in tables:
            TABLE_TYPE = table['name']
            etl.LOGGER.info(f"Starting fetching metadata for table \"{TABLE_TYPE}\"")
            dhis2_pivot_table_id = table['dhis2_pivot_table_id']
            (get_dhis2_pivot_table_data(dhis2_pivot_table_id, from_pickle=args.pickle)
             .pipe(export_category_config)
             )
            etl.LOGGER.info(f"Finished fetching metadata for table \"{TABLE_TYPE}\"")
    else:
        for table in tables:
            TABLE_TYPE = table['name']
            etl.LOGGER.info(f"Starting data fetch for table \"{TABLE_TYPE}\"")
            dhis2_pivot_table_id = table['dhis2_pivot_table_id']
            out = (get_dhis2_pivot_table_data(dhis2_pivot_table_id, from_pickle=True)
                    .pipe(extract_data_elements_names)
                    .pipe(extract_areas_names)
                    .pipe(extract_categories_and_aggregate_data)
                    .pipe(sort_by_area_name)
                    .pipe(map_dhis2_id_area_id)
                    .pipe(trim_period_strings)
                   )
            output_file_path = os.path.join(OUTPUT_DIR_NAME, 'program', f"{EXPORT_NAME}_dhis2_pull_{TABLE_TYPE}.csv")
            etl.LOGGER.info(f"Saving \"{TABLE_TYPE}\" data to file {output_file_path}")
            os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'program'), exist_ok=True)
            out.to_csv(output_file_path, index=None, float_format='%.f')
            etl.LOGGER.info(f"Finished processing table \"{TABLE_TYPE}\"")
