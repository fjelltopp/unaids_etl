import pandas as pd


POP_FILENAME = '../inputs/botswana/bwa_population.csv'
AREA_HIERARCHY = '../inputs/botswana/bwa_area_hiearchy_2021.csv'

if __name__ == '__main__':
    with open(POP_FILENAME) as f:
        pop = pd.read_csv(f)
    with open(AREA_HIERARCHY) as f:
        area_hierarchy = pd.read_csv(f)
    area_ids = pop['Health District'].replace(area_hierarchy[['area_id', 'area_name']].set_index('area_name')['area_id'])