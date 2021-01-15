import json
from pathlib import Path

import pandas as pd
from pandas.io.json import json_normalize

ADR_LEVEL = 2
DHIS2_LEVEL = 1
COUNTRY = 'gmb'
GEOJSON_FILENAME = 'gmb_areas.geojson'

root_path = Path('/home/tomek/work/fjelltopp/unaids_etl/')
geojson_path = root_path / f'inputs/{COUNTRY}/{GEOJSON_FILENAME}'
dhis2_area_id_mapping = root_path / f'output/{COUNTRY}/geodata/dhis2_id_mapping.csv'
output_path = root_path / f'output/{COUNTRY}/geodata/{COUNTRY}_area_id_mapping_level{ADR_LEVEL}.csv'

with open(geojson_path) as gj:
    geojson = json.load(gj)
df = json_normalize(geojson["features"])
adr_df = df[df['properties.area_level'] == ADR_LEVEL][['properties.area_id', 'properties.area_name']]
adr_df.columns = ["area_id", "name"]

with open(dhis2_area_id_mapping) as dhis2_csv:
    df = pd.read_csv(dhis2_csv)
districts_df = df[df['map_level'] == DHIS2_LEVEL]
names_ids_df = districts_df[['map_name', 'map_id']]
names_ids_df.columns = ["name", "dhis2_id"]

# with open(root_path / 'inputs/ethiopia/country_team_mapping.csv') as f:
#     country_team_mapping_df = pd.read_csv(f, header=2)
# country_team_mapping_df = country_team_mapping_df[['organisationunitname', 'MapZoneName']]
# country_team_mapping_df.columns = ['name', 'adr_name']
country_team_mapping_df = pd.DataFrame(columns=['name'])


def string_matching_mapping():
    output_columns = ["area_id", "area_name", "map_level", "map_name", "map_id", "map_source"]
    area_id_map_df = pd.DataFrame(columns=output_columns)
    for i, row in adr_df.iterrows():
        naomi_name = row['name']
        naomi_id = row['area_id']
        matches = names_ids_df[
            into_ascii_only_series(names_ids_df['name'])
                .str.contains(ascii_only(naomi_name), case=False)
        ]
        matches.columns = ['map_name', 'map_id']
        if matches.empty:
            area_id_map_df = area_id_map_df.append({
                'area_id': naomi_id,
                'area_name': naomi_name,
            }, ignore_index=True)
            continue
        matches['area_id'] = naomi_id
        matches['area_name'] = naomi_name
        area_id_map_df = area_id_map_df.append(matches, ignore_index=True)
    # add dhis2 areas with no mapping
    missing_dhis2_ids = (names_ids_df.loc[
                             ~names_ids_df['dhis2_id'].isin(area_id_map_df['map_id'])]
                         .sort_values(by='name'))
    missing_dhis2_ids.columns = ['map_name', 'map_id']
    for i, row in missing_dhis2_ids.iterrows():
        dhis2_id = row['map_id']
        dhis2_name = row['map_name']
        matches = country_team_mapping_df[
            into_ascii_only_series(country_team_mapping_df['name'])
                .str.contains(ascii_only(dhis2_name), case=False)
        ]
        if matches.empty:
            area_id_map_df = area_id_map_df.append(row, ignore_index=True)
            continue
        adr_loc_info = adr_df.loc[adr_df['name'].isin(matches['adr_name'])]
        if adr_loc_info.empty:
            area_id_map_df = area_id_map_df.append(row, ignore_index=True)
            continue
        crosswalk_row = adr_loc_info.iloc[0].append(row)
        crosswalk_row.index = ['area_id', 'area_name', 'map_name', 'map_id']
        area_id_map_df = area_id_map_df.append(crosswalk_row, ignore_index=True)

    area_id_map_df['map_source'] = "dhis2"
    area_id_map_df['map_level'] = ADR_LEVEL
    area_id_map_df = area_id_map_df[output_columns]
    area_id_map_df['area_id'] = area_id_map_df['area_id'].fillna(COUNTRY.upper())
    area_id_map_df = area_id_map_df.fillna('')
    area_id_map_df.sort_values(by='area_id', inplace=True)
    area_id_map_df.to_csv(output_path, index=False)


def exact_matching_mapping():
    names_ids_df['name_short'] = names_ids_df['name'].str.replace(' District', '')
    area_id_map_df = adr_df.set_index("name").join(names_ids_df.set_index("name_short"))
    area_id_map_df.to_csv(output_path, index=False)


def into_ascii_only_series(in_series):
    return in_series.str.encode('ascii', 'ignore').str.decode('ascii')


def ascii_only(input_string):
    return input_string.encode('ascii', 'ignore').decode('ascii')


if __name__ == '__main__':
    string_matching_mapping()
