# unaids_etl
ETL scripts to be used in UNAIDS. There are two scripts in the repo:
1. `adr_dhis2_geodata_etl.py` to fetch geographic area information from DHIS2
2. 'adr_dhis2_pivot_table_etl.py` to fetch program data (anc/art) from DHIS2's pivot tables

## Run the scripts:
### Environment setup:
1. Go inside the unaids_etl project directory
    ```
    cd unaids_etl
2. Create venv for your etl and activate it.
    ```
    # Linux/Mac
    python -m venv venv
    source venv/bin/activate
    # Windows
    py -m venv env 
    .\env\Scripts\activate
    ```

3. Install the requirements by
    ```
    pip install -r requirements.txt
    ```
### Geodata fetch: 
4. Script configuration:
    - using `.env` file inside `inputs` directory:
        ```
        DHIS2_URL - specific url to fetch organisation units data in specific format (see example below)
        DHIS2_CREDENTIALS_FILE - path to another env file with DHIS2 credentials (see example below)

        AREAS_ADMIN_LEVEL - level to which consider location as areas, deeper levels will be considered facilities
        OUTPUT_DIR_NAME - name of the subdirectory in `./outputs` where script's artefacts will be stored
        SUBTREE_ORG_NAME - comma separated list of names that should be exported separatelly as subtrees, e.g. 'Uganda,Kenya,Malawi,Tanzania,Zambia,Zimbabwe'
        ```
        Example config file `inputs/play.env`:
        ```
        DHIS2_URL=https://play.dhis2.org/2.33.1/api/26/
        DHIS2_CREDENTIALS_FILE='credentials/play.env'
        DHIS2_USERNAME=admin
        DHIS2_PASSWORD=district

        AREAS_ADMIN_LEVEL=2
        OUTPUT_DIR_NAME=play
        ```
        Example credentials file `credentials/play.env`:
        ```
        DHIS2_USERNAME=admin
        DHIS2_PASSWORD=district
        ```
1. Running the script
    ```
    python adr_dhis2_geodata_etl.py -e path_to/play.env
    ```
    
    - instead of providing DHIS2 url and credentials a csv input file with raw location data can be provided with `-c` flag e.g.
        ```
        python adr_dhis2_geodata_etl.py -e inputs/play/play.env -c play_raw_location_data.csv
        ```
     To fetch the raw location data as csv file use this URL:
     ```
     https://play.dhis2.org/api/26/organisationUnits.csv?paging=false&includeDescendants=true&includeAncestors=true&withinUserHierarchy=true&fields=id,name,displayName,shortName,path,ancestors,featureType,coordinates
    ``` 
     - flag `-p` creates a local cache of DHIS2 data instead of calling DHIS2. It speeds up consecutive runs of the script. Useful for debugging, e.g.
        ```
        python adr_dhis2_geodata_etl.py -p -e inputs/play/play.env 
        ```
### Program data fetch: 
The program anc/art data is fetched as DHIS2 pivot table data pull. This require some interim configuration on how to map pivot table structure into output csv file.
#### Config `env` file:
```
DHIS2_URL=https://play.dhis2.org/api/26/
DHIS2_CREDENTIALS_FILE='credentials/play.env'

OUTPUT_DIR_NAME=play

PROGRAM_DATA='[
{"name": "play_anc", "dhis2_pivot_table_id": "VlppluVonvM"},
{"name": "play_art", "dhis2_pivot_table_id": "A5LfrfQRGmc"}
]'

PROGRAM_DATA_CATEGORY_CONFIG='inputs/play/play_anc_category_config.json,inputs/play/play_art_category_config.json'
PROGRAM_DATA_COLUMN_CONFIG='inputs/play/play_anc_column_config.json,inputs/play/play_art_column_config.json'
AREA_ID_MAP='inputs/play/play_area_map_datim.csv' (Optional)
```
#### Running the pivot table ETL script:
The script should be run first time to fetch configuration and second time to fetch the data:
* Configuration pull
    ```
    python adr_dhis2_pivot_table.py -e path_to/play.env --pivot-table-config
    ```
    Afterwards you should find config files templates (2 for each configured pivot table) in directory `output/play/configs`. You should update those files and put the file paths to the updated files in `env` config with `PROGRAM_DATA_CATEGORY_CONFIG` `PROGRAM_DATA_COLUMN_CONFIG` 
* Second run
    ```
    python adr_dhis2_pivot_table_etl.py -e path_to/play.env
    ```
    This will output fetched pivot tables as csv files in `output/play/program` directory
