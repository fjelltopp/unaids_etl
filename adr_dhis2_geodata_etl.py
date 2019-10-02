#!/usr/bin/env python3

import argparse
import json
import os
import io
import sys
from collections import Sequence, defaultdict
from itertools import chain, count
import shapely.wkt
import geojson
import etl

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv


def get_dhis2_org_data(pickle_path=None):
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")
    resource_url = os.environ.get("DHIS2_URL")
    r = requests.get(resource_url, auth=HTTPBasicAuth(username, password))
    f = io.StringIO(r.text)
    df = pd.read_csv(f)
    if pickle_path:
        df.to_pickle(pickle_path)
    return df

def get_dhis2_org_data_from_pickle(pickle_path):
    return pd.read_pickle(pickle_path)


def get_dhis2_org_data_from_csv(csv_path, pickle_path=None):
    df = pd.read_csv(csv_path)
    if pickle_path:
        df.to_pickle(pickle_path)
    return df


def extract_geo_data(df):
    if 'featureType' in list(df):
        cords = df[df['featureType'] == 'POINT']['coordinates'].str.strip('[]').str.split(',', expand=True)
        cords.columns = ['long', 'lat']
        df = pd.concat([df, cords], axis=1, sort=False)
        df['geoshape'] = df['coordinates']
        df = df.drop(['coordinates'], axis=1)
        # only (multi)polygons in geoshape column
        def __remove_points_from_geoshape(row):
            if row.featureType in  ['POINT', 'NONE']:
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


def convert_cords_str_to_int(df:pd.DataFrame) -> pd.DataFrame:
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


def extract_admin_level(df:pd.DataFrame) -> pd.DataFrame:
    paths: pd.Series = df['path'].str.lstrip('/').str.split('/')
    admin_level = paths.apply(len) - 1
    df['admin_level'] = admin_level
    return df


def extract_parent(df: pd.DataFrame) -> pd.DataFrame:
    paths: pd.Series = df['path'].str.lstrip('/').str.split('/')
    def get_parent_id(path):
        if len(path) < 1:
            return ''
        elif len(path) == 1:
            return path[0]
        elif len(path) <= AREAS_ADMIN_LEVEL:
            return path[-2]
        else:
            return path[AREAS_ADMIN_LEVEL]
    parent_ids: pd.Series = paths.apply(get_parent_id)
    parent_df = pd.DataFrame(parent_ids)
    parent_df.columns = ['parent_dhis2_id']
    df['parent_id'] = parent_df.merge(df, how='left', left_on='parent_dhis2_id', right_on='dhis2_id')['id']

    return df


def create_index_column(df:pd.DataFrame) -> pd.DataFrame:
    df['dhis2_id'] = df['id']
    df['id'] = df.index + 1
    return df


def sort_by_admin_level(df:pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by='admin_level').reset_index()


def save_location_hierarchy(df:pd.DataFrame) -> pd.DataFrame:
    lh_df = df[df['admin_level'] <= AREAS_ADMIN_LEVEL][['id', 'name', 'admin_level', 'parent_id', 'sort_order', 'dhis2_id']]
    lh_df.columns = ['area_id', 'area_name', 'area_level', 'parent_area_id', 'sort_order', 'dhis2_id']
    if not os.path.exists(OUTPUT_DIR_NAME):
        os.makedirs(OUTPUT_DIR_NAME)
    lh_df.to_csv(f"{OUTPUT_DIR_NAME}/location_hierarchy.csv", index=False)
    return df


def save_facilities_list(df:pd.DataFrame) -> pd.DataFrame:
    fl_df = df[df['admin_level'] > AREAS_ADMIN_LEVEL].reindex(columns=['id', 'name', 'parent_id', 'lat', 'long', 'type', 'sort_order', 'dhis2_id'])
    fl_df['type'] = 'health facility'
    fl_df.columns = ['facility_id', 'facility_name', 'parent_area_id', 'lat', 'long', 'type', 'sort_order', 'dhis2_id']
    if not os.path.exists(OUTPUT_DIR_NAME):
        os.makedirs(OUTPUT_DIR_NAME)
    fl_df.to_csv(f"{OUTPUT_DIR_NAME}/facility_list.csv", index=False)
    return df

def save_ids_mapping(df:pd.DataFrame) -> pd.DataFrame:
    fl_df = df.reindex(columns=['id', 'dhis2_id'])
    fl_df['pepfar_id'] = ''
    fl_df.columns = ["area_id", "dhis2_id", "pepfar_id"]
    if not os.path.exists(OUTPUT_DIR_NAME):
        os.makedirs(OUTPUT_DIR_NAME)
    fl_df.to_csv(f"{OUTPUT_DIR_NAME}/ids_mapping.csv", index=False)
    return df


def save_area_geometries(df:pd.DataFrame) -> pd.DataFrame:
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
                    incorrect_geojson_areas[f"admin_{level}"].append( __prepare_properties_error(area))
                    continue
                item_gj['properties'] = __prepare_properties(area)
                features.append(item_gj)
    geojson_str = {
        "type": "FeatureCollection",
        "features": features
    }

    if not os.path.exists(OUTPUT_DIR_NAME):
        os.makedirs(OUTPUT_DIR_NAME)
    with open(f'{OUTPUT_DIR_NAME}/areas.json', 'w') as f:
        f.write(json.dumps(geojson_str))

    with open(f'{OUTPUT_DIR_NAME}/areas_geoshapes_errors.json', 'w') as f:
        f.write(json.dumps(incorrect_geojson_areas, indent=2))
    with open(f'{OUTPUT_DIR_NAME}/areas_geoshapes_errors.txt', 'w') as f:
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


def __prepare_geometry(area:pd.Series) -> dict:
    type = area['featureType']
    if type == 'MULTI_POLYGON':
        type = 'MultiPolygon'
    else:
        type = 'Polygon'
    return {
        "type": type,
        "coordinates": area['geoshape']
    }

def __prepare_properties(area:pd.Series) -> dict:
    return {
        "area_id": str(area['id']),
        "name": area['name'],
        "level": area['admin_level']
    }

def __prepare_properties_error(area:pd.Series) -> dict:
    return {
        "area_id": str(area['id']),
        "name": area['name'],
        "dhis2_id": area['dhis2_id']
    }


def __get_init_df():
    if not os.path.exists(OUTPUT_DIR_NAME):
        os.makedirs(OUTPUT_DIR_NAME)

    if args.csv:
        pickle_path = f"{OUTPUT_DIR_NAME}/orgs.pickle"
        return get_dhis2_org_data_from_csv(args.csv, pickle_path)

    if args.pickle:
        pickle_path = f"{OUTPUT_DIR_NAME}/orgs.pickle"
        if os.path.exists(pickle_path):
            return get_dhis2_org_data_from_pickle(pickle_path)
        else:
            return get_dhis2_org_data(f"{OUTPUT_DIR_NAME}/orgs.pickle")

    else:
        return get_dhis2_org_data()


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

    load_dotenv(args.env_file)
    AREAS_ADMIN_LEVEL = int(os.environ.get("AREAS_ADMIN_LEVEL", 2))
    OUTPUT_DIR_NAME = f"output/{os.environ.get('OUTPUT_DIR_NAME', 'default')}"

    (__get_init_df()
        .pipe(extract_admin_level)
        .pipe(extract_geo_data)
        .pipe(convert_cords_str_to_int)
        .pipe(sort_by_admin_level)
        .pipe(create_index_column)
        .pipe(extract_parent)
        .pipe(etl.add_empty_column('sort_order'))
        .pipe(save_location_hierarchy)
        .pipe(save_facilities_list)
        .pipe(save_area_geometries)
    )
