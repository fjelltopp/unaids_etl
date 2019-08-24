import json
import os
import io

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

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

def get_dhis2_org_data_from_picke(pickle_path):
    return pd.read_pickle(pickle_path)

def extract_ancestors(df):
    org_path = df['path'].str.split('/', expand=True)
    id_to_name_dict = df[['id', 'name']].set_index('id')['name'].to_dict()
    org_name_path = org_path.replace(id_to_name_dict)

    col_count = len(list(org_name_path))
    for i in range(1, col_count):
        df.insert(0, f'region_{i}', org_name_path.iloc[:, -i])
    # df.insert(0, 'District', org_name_path.iloc[:, -2])
    # df.insert(0, 'Region', org_name_path.iloc[:, -3])
    return df


def extract_geo_data(df):
    cords = df[df['featureType'] == 'POINT']['coordinates'].str.strip('[]').str.split(',', expand=True)
    cords.columns = ['lat', 'long']
    df = pd.concat([df, cords], axis=1, sort=False)
    df['geoshape'] = df[df['featureType'] == 'POLYGON']['coordinates']
    __convert_str_to_list(df, 'geoshape')
    __fillna_with_empty_list(df, 'geoshape')
    df = df.drop(['featureType', 'coordinates'], axis=1)

    return df


def extract_admin_level(df:pd.DataFrame) -> pd.DataFrame:
    paths: pd.Series = df['path'].str.lstrip('/').str.split('/')
    admin_level = paths.apply(len) - 1
    df['admin_level'] = admin_level
    return df


def extract_parent(df: pd.DataFrame) -> pd.DataFrame:
    paths: pd.Series = df['path'].str.lstrip('/').str.split('/')
    def get_parent_id(path):
        if len(path) < 2:
            return path[0]
        else:
            return path[-2]
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
    return df.sort_values(by='admin_level')


def save_location_hierarchy(df:pd.DataFrame) -> pd.DataFrame:
    lh_df = df[df['admin_level'] <= 2][['id', 'name', 'admin_level', 'parent_id', 'dhis2_id']]
    lh_df.columns = ['area_id', 'area_name', 'area_level', 'parent_area_id', 'dhis2_id']
    lh_df['pepfar_id'] = ''
    if not os.path.exists('output'):
        os.makedirs('output')
    lh_df.to_csv("output/location_hierarchy.csv")
    return df


def save_facilities_list(df:pd.DataFrame) -> pd.DataFrame:
    fl_df = df[df['admin_level'] > 2].reindex(columns=['id', 'name', 'parent_id', 'lat', 'long', 'type', 'dhis2_id'])
    fl_df['type'] = 'health facility'
    fl_df.columns = ['facility_id', 'facility_name', 'parent_area_id', 'lat', 'long', 'type', 'dhis2_id']
    fl_df.to_csv("output/facility_list.csv")
    if not os.path.exists('output'):
        os.makedirs('output')
    return df


def save_area_geometries(df:pd.DataFrame) -> pd.DataFrame:
    area_df = df[df['admin_level'] <= 2][['id', 'name', 'admin_level', 'geoshape']]
    features = []
    for i, area in area_df.iterrows():
        features.append({
            "type": "Feature",
            "geometry": __prepare_geometry(area.geoshape),
            "properties": __prepare_properties(area)
        })
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    with open('output/areas.json', 'w') as f:
        f.write(json.dumps(geojson))
    return df


def __fillna_with_empty_list(df, column_name):
    for i in df.loc[df[column_name].isnull(), column_name].index:
        df.at[i, column_name] = []


def __convert_str_to_list(df, column_name):
    df[column_name] = df[df[column_name].apply(type) == str][column_name].apply(json.loads)


def __prepare_geometry(coordinates:str) -> dict:
    return {
        "type": "Polygon",
        "coordinates": coordinates
    }

def __prepare_properties(area:pd.Series) -> dict:
    return {
        "properties" : {
            "area_id": area.id,
            "name": area.name,
            "level": area.admin_level
        }
    }


if __name__ == '__main__':

    # get_dhis2_org_data('orgs.pickle')
    df = (get_dhis2_org_data_from_picke('orgs.pickle')
        .pipe(extract_geo_data)
        .pipe(extract_admin_level)
        .pipe(sort_by_admin_level)
        .pipe(create_index_column)
        .pipe(extract_parent)
        # .pipe(save_location_hierarchy)
        # .pipe(save_facilities_list)
        .pipe(save_area_geometries)
    )
