#!/usr/bin/env bash
source venv/bin/activate
echo Exporting ken
python3 adr_dhis2_geodata_etl.py -p -e inputs/ken/ken.env
echo Exporting lso
python3 adr_dhis2_geodata_etl.py -p -e inputs/lso/lso.env
echo Exporting mwi
python3 adr_dhis2_geodata_etl.py -p -e inputs/mwi/mwi.env
echo Exporting nam
python3 adr_dhis2_geodata_etl.py -p -e inputs/nam/nam.env
echo Exporting uga
python3 adr_dhis2_geodata_etl.py -p -e inputs/uga/uga.env
echo Exporting zmb
python3 adr_dhis2_geodata_etl.py -p -e inputs/zmb/zmb.env
echo Exporting tza
python3 adr_dhis2_geodata_etl.py -p -e inputs/tza/tza.env -c inputs/tza/tza_location_data.csv
echo Exporting zwe
python3 adr_dhis2_geodata_etl.py -p -e inputs/zwe/zwe.env -c inputs/tza/zwe_location_data.csv
echo Exporting DATIM
python3 adr_dhis2_geodata_etl.py -e inputs/datim/datim.env
