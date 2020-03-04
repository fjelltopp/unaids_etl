import os
import unittest

import adr_dhis2_geodata_etl as geo_etl
import pandas.util.testing as pd_test
import pandas as pd

class MyTestCase(unittest.TestCase):
    def test_etl_artefacts_equal_golden_master(self):
        df = geo_etl.get_dhis2_org_data_from_csv('resources/geodata/response.csv')
        geo_etl.OUTPUT_DIR_NAME = 'output'
        geo_etl.SUBTREE_ORG_NAME = False
        geo_etl.AREAS_ADMIN_LEVEL = 2
        geo_etl.ISO_CODE = 'play'

        geo_etl.run_steps(df)

        csv_filenames = [
            "dhis2_id_mapping.csv",
            "facility_list.csv",
            "location_hierarchy.csv"
        ]
        for csv_file in csv_filenames:
            actual_path = os.path.join('output/geodata', csv_file)
            expected_path = os.path.join('resources/geodata', csv_file)
            actual = pd.read_csv(actual_path)
            expected = pd.read_csv(expected_path)
            pd_test.assert_frame_equal(expected, actual, by_blocks=True)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
