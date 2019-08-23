import pandas as pd
import shapefile
from fuzzywuzzy import process
from slugify import slugify

def create_long_dhis2(country, dhis2_levels=8):

    dhis2 = pd.read_csv(country+'Dhis2.csv')

    dhis2['orgunit_parent'] = ""
    dhis2['orgunit_name'] = ""
    dhis2['orgunit_level'] = ""
    dhis2['ard_id'] = ""
    level_mapping = dict(zip(range(dhis2_levels-1, 0, -1), range(1, dhis2_levels)))
    seen_ids = set()

    for i, row in dhis2.iterrows():
        print(f'Record {i}')
        for level in range(1, dhis2_levels):
            heading = 'region_'+str(level)
            if row.loc[heading] is not pd.np.nan:

                parent_heading = 'region_' + str(level+1)

                dhis2.loc[i, 'orgunit_parent'] = row.loc[parent_heading].strip()
                dhis2.loc[i, 'orgunit_name'] = row.loc[heading].strip()
                dhis2.loc[i, 'orgunit_level'] = level_mapping[level]

                if country == 'Zambia':
                    dhis2.loc[i, 'orgunit_parent'] = dhis2.loc[i, 'orgunit_parent'][3:]
                    dhis2.loc[i, 'orgunit_name'] = dhis2.loc[i, 'orgunit_name'][3:]

                # Create sensible ids
                slugified_id = slugify(row.loc[heading].strip())
                if slugified_id in seen_ids:
                    raise Exception(f'ID {slugified_id} not unique')
                else:
                    dhis2.loc[i, 'adr_id'] = slugify(dhis2.loc[i, 'orgunit_name'] + " " + dhis2.loc[i, 'orgunit_parent'])
                    seen_ids.add(dhis2.loc[i, 'adr_id'])
                break

    dhis2 = dhis2[['adr_id', 'orgunit_name', 'orgunit_level', 'orgunit_parent', 'id']]
    dhis2.sort_values(['orgunit_level', 'orgunit_parent', 'orgunit_name'], inplace=True)
    dhis2.columns = ['area_id', 'name', 'admin_level', 'parent_name', 'dhis2_id']

    dhis2.to_csv(country+'Dhis2Long.csv', index=False)


def link_parent_ids(filepath):
    df = pd.read_csv(filepath)
    df['parent_id'] = "-"
    first_row = pd.DataFrame(pd.np.array([[
        slugify(df.iloc[0].parent_name),
        df.iloc[0].parent_name,
        0,
        df.iloc[0].parent_name,
        "-",
        slugify(df.iloc[0].parent_name)
    ]]), columns=df.columns)
    if not df.iloc[0].equals(first_row):
        df = pd.concat([first_row, df])
    for i, row in df.iterrows():
        print(str(i) + " - " + str(row.loc['name']))
        try:
            if int(row.loc['admin_level']) > 0:
                parent = df.loc[df['name'] == row.loc['parent_name']].iloc[0]
                df.loc[i, 'parent_id'] = parent.loc['area_id']
        except Exception:
            pass
    df.to_csv(filepath, index=False)


def prep_adr_data(filepath, country, iso3, facility_level=3):
    df = pd.read_csv(filepath)
    df = df[['area_id', 'name', 'admin_level', 'parent_id', 'dhis2_id']]
    areas = df.loc[df['admin_level'] < facility_level]
    areas.insert(4, 'iso3', iso3)
    areas.insert(6, 'pepfar_id', '')
    areas['pepfar_id'] = ''
    areas['admin_level'] = areas['admin_level'].astype(int)
    areas.columns = ['area_id', 'area_name', 'area_level', 'parent_area_id', 'iso3', 'dhis2_id', 'pepfar_id']

    areas.to_csv(f'{country}_location_hierachy.csv', index=False)

    facilities = df.loc[df['admin_level'] == facility_level]
    facilities['lat'] = 0
    facilities['long'] = 0
    facilities['type'] = 'health center'
    facilities = facilities[['area_id', 'name', 'parent_id', 'lat', 'long', 'type', 'dhis2_id']]
    facilities.columns = ['facility_id', 'facility_name', 'parent_area_id', 'lat', 'long', 'type', 'dhis2_id']
    facilities.to_csv(f'{country}_facility_list.csv', index=False)


def prep_gadm_geometry(geometry_fp, location_hierachy_fp, levels=[1, 2]):
    """
    At the moment this makes ONE shp file containing all admin level geometry.
    """

    w = shapefile.Writer('ZMB_GADM')
    w.fields = [['area_id', 'C', 80, 0], ['name', 'C', 80, 0], ['level', 'C', 80, 0]]

    lh = pd.read_csv(location_hierachy_fp)
    choices = list(lh['area_id'].copy())

    for level in levels:
        str_level = str(level)
        str_parent_level = str(level-1)
        sf = shapefile.Reader(geometry_fp + "/" + geometry_fp + "_" + str_level)

        fields = [x[0] for x in sf.fields][1:]
        sf_data = pd.DataFrame(data=sf.records(), columns=fields)
        sf_data.insert(
            0,
            'area_id',
            sf_data['NAME_'+str_level] + " " + sf_data['TYPE_'+str_level] + " " + sf_data['NAME_'+str_parent_level]
        )

        def choose(x):
            chosen = process.extractOne(x, choices)
            choices.remove(chosen[0])
            return chosen[0]

        sf_data['area_id'] = sf_data['area_id'].apply(slugify)
        sf_data['new_area_id'] = sf_data['area_id'].apply(lambda x: choose(x))
        with pd.option_context('display.max_rows', None):
            print(sf_data[['new_area_id', 'area_id']])

        seen_ids = []
        for i, shaperec in enumerate(sf.iterShapeRecords()):
            new_rec = [sf_data.loc[i, 'new_area_id'], sf_data.loc[i, 'NAME_'+str_level], level]
            if sf_data.loc[i, 'new_area_id'] in seen_ids:
                raise Exception("Non unique ID: " + sf_data.loc[i, 'new_area_id'])
            else:
                seen_ids += [sf_data.loc[i, 'new_area_id']]
            w.record(*new_rec)
            w.shape(shaperec.shape)

    w.close()

# create_long_dhis2('Zambia', dhis2_levels=7)
# link_parent_ids('ZambiaDhis2Long.csv')
# prep_adr_data('ZambiaDhis2Long.csv', 'zambia', 'ZMB')
# prep_gadm_geometry('gadm36_ZMB', 'zambia_location_hierachy.csv')

# create_long_dhis2('Kenya')
# link_parent_ids('KenyaDhis2Long.csv')
# prep_adr_data('KenyaDhis2Long.csv', 'kenya', 'KEN')
# prep_gadm_geometry('gadm36_KEN', 'kenya_location_hierachy.csv')
