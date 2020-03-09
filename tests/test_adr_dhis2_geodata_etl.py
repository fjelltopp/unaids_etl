import json
import os
import unittest

import adr_dhis2_geodata_etl as geo_etl
import pandas.util.testing as pd_test
import pandas as pd
from slugify import slugify


class TestGeodataETLGoldenMaster(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        dirname = os.path.dirname(__file__)
        df = geo_etl.get_dhis2_org_data_from_csv(os.path.join(dirname, 'resources/geodata/response.csv'))
        geo_etl.OUTPUT_DIR_NAME = os.path.join(dirname, 'output')
        geo_etl.SUBTREE_ORG_NAME = False
        geo_etl.AREAS_ADMIN_LEVEL = 2
        geo_etl.ISO_CODE = 'play'

        geo_etl.run_steps(df)

    def test_golden_master_geo_areas_json(self):
        dirname = os.path.dirname(__file__)
        with open(os.path.join(dirname, 'resources/geodata/areas.json')) as f:
            expected = json.load(f)
        with open(os.path.join(dirname, 'output/geodata/areas.json')) as f:
            actual = json.load(f)
        self.assertEqual(expected, actual)


def create_test(csv_file):
    dirname = os.path.dirname(__file__)
    def do_test_expected(self):
        actual_path = os.path.join(dirname, 'output/geodata', csv_file)
        expected_path = os.path.join(dirname, 'resources/geodata', csv_file)
        actual = pd.read_csv(actual_path)
        expected = pd.read_csv(expected_path)
        pd_test.assert_frame_equal(expected, actual, by_blocks=True)

    return do_test_expected


csv_filenames = [
    "dhis2_id_mapping.csv",
    "facility_list.csv",
    "location_hierarchy.csv"
]

for k, csv_file in enumerate(csv_filenames):
    test_method = create_test(csv_file)
    test_method.__name__ = f"test_golden_master_geo_{slugify(csv_file, separator='_')}"
    setattr(TestGeodataETLGoldenMaster, test_method.__name__, test_method)

if __name__ == '__main__':
    unittest.main()
