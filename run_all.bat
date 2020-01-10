CALL .\env\Scripts\activate.bat
python adr_dhis2_geodata_etl.py -e inputs/datim/datim.env
python adr_dhis2_geodata_etl.py -e inputs/zmb/zmb.env
pause
