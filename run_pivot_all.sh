#!/usr/bin/env bash
source venv/bin/activate
echo Exporting ken
python3 adr_dhis2_pivot_table_etl.py -e inputs/ken/ken.env
echo Exporting uga
python3 adr_dhis2_pivot_table_etl.py -e inputs/uga/uga.env
echo Exporting zmb
python3 adr_dhis2_pivot_table_etl.py -e inputs/zmb/zmb.env
