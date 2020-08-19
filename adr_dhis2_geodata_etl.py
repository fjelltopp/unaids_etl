#!/usr/bin/env python3

import argparse
import json
import os
import io
import sys
from collections import Sequence, defaultdict
from urllib.parse import urljoin

from itertools import chain, count
import shapely.wkt
import geojson
import etl
import errno

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

import credentials

etl.LOGGER = etl.logging.get_logger(log_name="DHIS2 geo data pull", log_group="dhis2_geo_etl")


@etl.decorators.log_start_and_finalisation("get dhis2 org data")
def get_dhis2_org_data(pickle_path=None):
    dhis2_url = os.environ.get("DHIS2_URL")
    credentials.read_credentials(os.environ.get("DHIS2_CREDENTIALS_FILE"))
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")
    org_resource_url = "organisationUnits.csv?paging=false&includeDescendants=true&includeAncestors=true&withinUserHierarchy=true&fields=id,name,displayName,shortName,path,ancestors,featureType,coordinates"
    r = requests.get(urljoin(dhis2_url, org_resource_url), auth=HTTPBasicAuth(username, password))
    etl.requests_util.check_if_response_is_ok(r)
    f = io.StringIO(r.text)
    df = pd.read_csv(f)
    if pickle_path:
        df.to_pickle(pickle_path)
    return df


@etl.decorators.log_start_and_finalisation("get dhis2 org data from local pickle file")
def get_dhis2_org_data_from_pickle(pickle_path):
    return pd.read_pickle(pickle_path)


@etl.decorators.log_start_and_finalisation("get dhis2 org data from local csv file")
def get_dhis2_org_data_from_csv(csv_path, pickle_path=None):
    df = pd.read_csv(csv_path, dtype=str)
    if pickle_path:
        df.to_pickle(pickle_path)
    return df


def extract_geo_data(df):
    if 'featureType' in list(df):
        cords = df[df['featureType'] == 'POINT']['coordinates'].str.strip('[]').str.split(',', expand=True)
        if not cords.empty:
            cords = cords.astype(float, errors='ignore')
            cords, df = _drop_faulty_facilities(cords, df)
            cords.columns = ['long', 'lat']
            df = pd.concat([df, cords], axis=1, sort=False)
        else:
            df['lat'] = ''
            df['long'] = ''
        df['geoshape'] = df['coordinates']
        df = df.drop(['coordinates'], axis=1)

        # only (multi)polygons in geoshape column
        def __remove_points_from_geoshape(row):
            if row.featureType in ['POINT', 'NONE']:
                return []
            return row.geoshape
        df['geoshape'] = df.apply(__remove_points_from_geoshape, axis=1)

        __convert_str_to_list(df, 'geoshape')
        __fillna_with_empty_list(df, 'geoshape')

        if bool(os.environ.get("FLIP_COORDS")):
            df['geoshape'] = df['geoshape'].apply(__flip_coordinates)
        df['geoshape'] = df.apply(__flatten, axis=1)
    else:
        # deal with WKT geometry
        def __extract_geoshape(row):
            if not pd.isnull(row['geometry']):
                geometry_str = row['geometry']

                shape = shapely.wkt.loads(geometry_str)
                __geojson = geojson.Feature(geometry=shape, properties={})
                geometry = __geojson.geometry
                if geometry.type == 'Point':
                    row['lat'] = str(geometry.coordinates[0])
                    row['long'] = str(geometry.coordinates[1])
                row['geojson'] = str(__geojson)
            return row
        df['geojson'] = ''
        df['lat'] = ''
        df['long'] = ''
        return df.apply(__extract_geoshape, axis=1)

    return df


def _drop_faulty_facilities(cords, df):
    cords_isna = cords.isna()
    faulty_cords = cords_isna[cords_isna[0] | cords_isna[1]]
    if not os.path.exists(os.path.join(OUTPUT_DIR_NAME, 'geodata_errors')):
        os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'geodata_errors'))
    df.loc[faulty_cords.index].to_csv(f"{OUTPUT_DIR_NAME}/geodata_errors/dropped_facilities.csv", index=False)
    cords = cords.drop(faulty_cords.index)
    if len(list(cords)) > 2:
        cords = cords.drop([2, 3], axis=1)
    df = df.drop(faulty_cords.index)
    return cords, df


@etl.decorators.log_start_and_finalisation("convert cords to int")
def convert_cords_str_to_int(df: pd.DataFrame) -> pd.DataFrame:
    for cord in ['lat', 'long']:
        if df[cord].dtype != pd.np.float64:
            df[cord] = df[cord].str.strip('\"').str.strip('\'').astype(float, errors='ignore')
    return df


def __flip_coordinates(cords):
    # to be used if you want to flip coords in nested collection
    # e.g. polygon: [[[12.0, -2], [12.0, -2.5], ... ], [[12,3], [...]]]
    if type(cords) == list and len(cords):
        nested_list = any([type(item) == list for item in cords])
        if nested_list:
            for item in cords:
                __flip_coordinates(item)
        else:
            swap = cords[0]
            cords[0] = cords[1]
            cords[1] = swap
    return cords


def __depth(seq):
    for level in count():
        if not seq:
            return level
        seq = list(chain.from_iterable(s for s in seq if isinstance(s, Sequence)))


def __flatten(row):
    cords = row['geoshape']
    if row['featureType'] == "MULTI_POLYGON":
        depth_limit = 4
    else:
        depth_limit = 3
    if __depth(cords) > depth_limit:
        nested_list = any([type(item) == list for item in cords])
        is_cord = all([type(item) != list for sublist in cords for item in sublist])
        if len(cords) and nested_list and not is_cord:
            flatten_list = [item for sublist in cords for item in sublist]
            return flatten_list
    return cords


@etl.decorators.log_start_and_finalisation("extract admin level")
def extract_admin_level(df: pd.DataFrame) -> pd.DataFrame:
    paths: pd.Series = df['path'].str.lstrip('/').str.split('/')
    admin_level = paths.apply(len) - 1
    df['admin_level'] = admin_level
    return df


@etl.decorators.log_start_and_finalisation("extract parent")
def extract_parent(df: pd.DataFrame) -> pd.DataFrame:
    paths: pd.Series = df['path'].str.lstrip('/').str.split('/')

    def get_parent_id(path):
        if len(path) < 1:
            return ''
        elif len(path) == 1:
            return path[0]
        # parent id must be within AREAS_ADMIN_LEVEL
        elif len(path) > AREAS_ADMIN_LEVEL + 1:
            return path[AREAS_ADMIN_LEVEL]
        else:
            return path[-2]
    parent_ids: pd.Series = paths.apply(get_parent_id)
    parent_df = pd.DataFrame(parent_ids)
    parent_df.columns = ['parent_dhis2_id']
    df['parent_id'] = parent_df.merge(df, how='left', left_on='parent_dhis2_id', right_on='dhis2_id')['id']
    return df


@etl.decorators.log_start_and_finalisation("save locations in wide format")
def save_locations_in_wide_format(df: pd.DataFrame) -> pd.DataFrame:
    ancestors = df['path'].str.lstrip('/').str.split('/', expand=True)
    ancestor_col_names = [f"admin_{i}" for i in list(ancestors)]
    ancestors.columns = ancestor_col_names
    id_to_name = df[['dhis2_id', 'name']]
    ancestors = ancestors[ancestors[list(ancestors)[-1]].notnull()]
    for column in list(ancestors):
        ancestors[f'{column}_name'] = ancestors[column].apply(lambda x: __get_name(x, id_to_name))
    if not os.path.exists(OUTPUT_DIR_NAME):
        os.makedirs(OUTPUT_DIR_NAME)
    ancestors.to_csv(f"{OUTPUT_DIR_NAME}/locations_wide.csv", index=False)
    return df


def __get_name(dhis2_id, ids_map):
    values = ids_map[ids_map['dhis2_id'] == dhis2_id]['name'].values
    if len(values) > 0:
        return values[0]
    else:
        return


@etl.decorators.log_start_and_finalisation("create index column")
def create_index_column(df: pd.DataFrame) -> pd.DataFrame:
    df['dhis2_id'] = df['id']
    counters = defaultdict(int)

    def create_index(admin_level):
        counters[admin_level] += 1
        return f"{ISO_CODE}_{admin_level}_{counters[admin_level]}"

    df['id'] = df['admin_level'].apply(create_index)
    return df


@etl.decorators.log_start_and_finalisation("sort by admin level")
def sort_by_admin_level(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by='admin_level').reset_index()


@etl.decorators.log_start_and_finalisation("save location hierarchy")
def save_location_hierarchy(df: pd.DataFrame) -> pd.DataFrame:
    lh_df = df[df['admin_level'] <= AREAS_ADMIN_LEVEL][['id', 'name', 'admin_level', 'parent_id', 'area_sort_order']]
    lh_df.columns = ['area_id', 'area_name', 'area_level', 'parent_area_id', 'area_sort_order']
    if not os.path.exists(os.path.join(OUTPUT_DIR_NAME, 'geodata')):
        os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'geodata'))
    lh_df.to_csv(f"{OUTPUT_DIR_NAME}/geodata/location_hierarchy.csv", index=False)
    return df


@etl.decorators.log_start_and_finalisation("save facilities list")
def save_facilities_list(df: pd.DataFrame) -> pd.DataFrame:
    fl_df = df[df['admin_level'] > AREAS_ADMIN_LEVEL].reindex(columns=['id', 'name', 'parent_id', 'lat', 'long', 'type', 'area_sort_order'])
    fl_df['type'] = 'health facility'
    fl_df.columns = ['facility_id', 'facility_name', 'parent_area_id', 'lat', 'long', 'type', 'area_sort_order']
    if not os.path.exists(os.path.join(OUTPUT_DIR_NAME, 'geodata')):
        os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'geodata'))
    fl_df.to_csv(f"{OUTPUT_DIR_NAME}/geodata/facility_list.csv", index=False)
    return df


@etl.decorators.log_start_and_finalisation("save dhis2 ids")
def save_dhis2_ids(df: pd.DataFrame) -> pd.DataFrame:
    dhis2_ids = df[['id', 'admin_level', 'name', 'dhis2_id']]
    dhis2_ids['map_source'] = "DHIS2"
    dhis2_ids.columns = ["area_id", "map_level", "map_name", "map_id", "map_source"]
    if not os.path.exists(os.path.join(OUTPUT_DIR_NAME, 'geodata')):
        os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'geodata'))
    dhis2_ids.to_csv(f"{OUTPUT_DIR_NAME}/geodata/dhis2_id_mapping.csv", index=False)
    return df


@etl.decorators.log_start_and_finalisation("save ids mapping")
def save_ids_mapping(df: pd.DataFrame) -> pd.DataFrame:
    fl_df = df.reindex(columns=['id', 'dhis2_id'])
    fl_df['pepfar_id'] = ''
    fl_df.columns = ["area_id", "dhis2_id", "pepfar_id"]
    if not os.path.exists(OUTPUT_DIR_NAME):
        os.makedirs(OUTPUT_DIR_NAME)
    fl_df.to_csv(f"{OUTPUT_DIR_NAME}/ids_mapping.csv", index=False)
    return df


@etl.decorators.log_start_and_finalisation("save area geometries")
def save_area_geometries(df: pd.DataFrame) -> pd.DataFrame:
    incorrect_geojson_areas = defaultdict(list)
    features = []
    for level in range(1, AREAS_ADMIN_LEVEL + 1):
        is_geojson = 'geojson' in list(df)
        area_level_df = df[df['admin_level'] == level]
        if not is_geojson:
            valid_area_df = area_level_df[area_level_df['geoshape'].apply(lambda x: len(x)) > 0]
            for i, area in valid_area_df.iterrows():
                features.append({
                    "type": "Feature",
                    "geometry": __prepare_geometry(area),
                    "properties": __prepare_properties(area)
                })
            error_area_df = area_level_df[area_level_df['geoshape'].apply(lambda x: len(x)) == 0]
            for i, area in error_area_df.iterrows():
                incorrect_geojson_areas[f"admin_{level}"].append(__prepare_properties_error(area))
        else:
            area_df = area_level_df[['id', 'name', 'admin_level', 'geojson', 'dhis2_id']]
            for i, area in area_df.iterrows():
                try:
                    item_gj = json.loads(area['geojson'])
                except json.decoder.JSONDecodeError:
                    incorrect_geojson_areas[f"admin_{level}"].append(__prepare_properties_error(area))
                    continue
                item_gj['properties'] = __prepare_properties(area)
                features.append(item_gj)
    geojson_str = {
        "type": "FeatureCollection",
        "features": features
    }

    if not os.path.exists(os.path.join(OUTPUT_DIR_NAME, 'geodata')):
        os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'geodata'))
    with open(f'{OUTPUT_DIR_NAME}/geodata/areas.json', 'w') as f:
        f.write(json.dumps(geojson_str))

    if not os.path.exists(os.path.join(OUTPUT_DIR_NAME, 'geodata_errors')):
        os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'geodata_errors'))
    with open(f'{OUTPUT_DIR_NAME}/geodata_errors/areas_geoshapes_errors.json', 'w') as f:
        f.write(json.dumps(incorrect_geojson_areas, indent=2))
    with open(f'{OUTPUT_DIR_NAME}/geodata_errors/areas_geoshapes_errors.txt', 'w') as f:
        w_ = [9, 13, 13, 45]
        separation_line_ = f"|{'':-^{w_[0]}}+{'':-^{w_[1]}}+{'':-^{w_[2]}}+{'':-^{w_[3]}}|\n"
        f.write(separation_line_)
        f.write(f"|{'area_id': ^{w_[0]}}|{'dhis2_id': ^{w_[1]}}|{'admin_level': ^{w_[2]}}|{'name': ^{w_[3]}}|\n")
        f.write(separation_line_)
        for admin_level, areas in incorrect_geojson_areas.items():
            for area in areas:
                line = f"|{area['area_id']: >{w_[0]}}|{area['dhis2_id']: ^{w_[1]}}|{admin_level: ^{w_[2]}}|{area['name']: <{w_[3]}}|\n"
                f.write(line)
        f.write(separation_line_)
    with open(f'{OUTPUT_DIR_NAME}/geodata_errors/areas_geoshapes_errors_markdown.txt', 'w') as f:
        for admin_level, areas in incorrect_geojson_areas.items():
            for area in areas:
                line = f"1. area id: {area['area_id']}, name: {area['name']}, dhis2_id: {area['dhis2_id']}\n"
                f.write(line)

    return df


def __empty_polygon():
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": []
        }
    }


def __fillna_with_empty_list(df, column_name):
    for i in df.loc[df[column_name].isnull(), column_name].index:
        df.at[i, column_name] = []


def __convert_str_to_list(df, column_name):
    df[column_name] = df[df[column_name].apply(type) == str][column_name].apply(json.loads)


def __prepare_geometry(area: pd.Series) -> dict:
    _type = area['featureType']
    if _type == 'MULTI_POLYGON':
        _type = 'MultiPolygon'
    else:
        _type = 'Polygon'
    return {
        "type": _type,
        "coordinates": area['geoshape']
    }


def __prepare_properties(area: pd.Series) -> dict:
    return {
        "area_id": str(area['id']),
        "area_name": area['name'],
        "area_level": area['admin_level']
    }


def __prepare_properties_error(area: pd.Series) -> dict:
    return {
        "area_id": str(area['id']),
        "name": area['name'],
        "dhis2_id": area['dhis2_id']
    }


def __get_init_df():
    if not os.path.exists(os.path.join(OUTPUT_DIR_NAME, 'build')):
        os.makedirs(os.path.join(OUTPUT_DIR_NAME, 'build'), exist_ok=True)
    geodata_pickle = os.path.join(OUTPUT_DIR_NAME, 'build/dhis2_orgs.pickle')
    if args.csv:
        return get_dhis2_org_data_from_csv(args.csv, geodata_pickle)

    if args.pickle and os.path.exists(geodata_pickle):
        return get_dhis2_org_data_from_pickle(geodata_pickle)
    else:
        return get_dhis2_org_data(geodata_pickle)


@etl.decorators.log_start_and_finalisation("extract location subtree")
def extract_location_subtree(df: pd.DataFrame) -> pd.DataFrame:
    if not SUBTREE_ORG_NAME:
        return df
    root_candidates = df[df['name'] == SUBTREE_ORG_NAME].pipe(
        extract_admin_level
    ).sort_values(by='admin_level')
    if root_candidates.empty:
        raise(ValueError(f"Failed to find subtree org unit for '{SUBTREE_ORG_NAME}'\n"
                         f"Please verify your config file."))
    root_id = root_candidates.iloc[0]['id']
    subtree_indexes = df.path.apply(lambda x: root_id in x)
    df = df.loc[subtree_indexes]
    lstrip_path_column = f"/{root_id}" + df['path'].str.split(root_id, expand=True)[1]
    df['path'] = lstrip_path_column
    return df


@etl.decorators.log_start_and_finalisation("validate admin level")
def validate_admin_level(df: pd.DataFrame) -> pd.DataFrame:
    df['is_leaf'] = ''
    for i, row in df.iterrows():
        has_children = len(df[df['parent_id'] == row['id']]) > 0
        df.loc[i, 'is_leaf'] = not has_children
    return df


def run_pipeline():
    df_ = __get_init_df()
    run_steps(df_)


def run_steps(df_):
    (df_
     .pipe(extract_location_subtree)
     .pipe(extract_admin_level)
     .pipe(extract_geo_data)
     .pipe(convert_cords_str_to_int)
     .pipe(sort_by_admin_level)
     .pipe(create_index_column)
     .pipe(extract_parent)
     # .pipe(validate_admin_level)
     .pipe(etl.add_empty_column('area_sort_order'))
     # .pipe(save_locations_in_wide_format)
     .pipe(save_location_hierarchy)
     .pipe(save_facilities_list)
     .pipe(save_dhis2_ids)
     .pipe(save_area_geometries)
     )


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
    parser.add_argument('-c', '--csv-file',
                        dest='csv',
                        help='Fetch data from a CSV file')
    args = parser.parse_args()

    if not os.path.exists(args.env_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), args.env_file)
    load_dotenv(args.env_file)
    SUBTREE_ORG_CONFIGS = json.loads(os.environ.get("SUBTREE_ORG_CONFIGS", "{}"))

    if SUBTREE_ORG_CONFIGS:
        for subtree_config in SUBTREE_ORG_CONFIGS:
            OUTPUT_DIR_NAME = f"output/{os.environ.get('OUTPUT_DIR_NAME', 'default')}/{subtree_config['name']}"
            SUBTREE_ORG_NAME = subtree_config['name']
            AREAS_ADMIN_LEVEL = int(subtree_config['areas_admin_level'])
            ISO_CODE = subtree_config['iso_code']
            run_pipeline()
    else:
        OUTPUT_DIR_NAME = f"output/{os.environ.get('OUTPUT_DIR_NAME', 'default')}"
        SUBTREE_ORG_NAME = os.environ.get("SUBTREE_ORG_NAME", False)
        AREAS_ADMIN_LEVEL = int(os.environ.get("AREAS_ADMIN_LEVEL", 2))
        ISO_CODE = os.environ.get("ISO_CODE", os.environ.get('OUTPUT_DIR_NAME', 'XXX'))
        run_pipeline()
