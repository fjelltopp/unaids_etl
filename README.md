# unaids_etl
ETL scripts to be used in UNAIDS.
1. Go inside the unaids_etl project directory
    ```
    cd unaids_etl
1. Create venv for your etl and activate it.
    ```
    # Linux/Mac
    python -m venv venv
    source venv/bin/activate
    # Windows
    py -m venv env 
    .\env\Scripts\activate
    ```

2. Install the requirements by
    ```
    pip install -r requirements.txt
    ```
    
3. Script configuration:
    - using `.env` file:
        ```
        DHIS2_URL - specific url to fetch organisation units data in specific format (see example below)
        DHIS2_USERNAME
        DHIS2_PASSWORD

        AREAS_ADMIN_LEVEL - level to which consider location as areas, deeper levels will be considered facilities
        OUTPUT_DIR_NAME - name of the subdirectory in `./outputs` where script's artefacts will be stored
        SUBTREE_ORG_NAME - comma separated list of names that should be exported separatelly as subtrees, e.g. 'Uganda,Kenya,Malawi,Tanzania,Zambia,Zimbabwe'
        ```
        Example config file `play.env`:
        ```
        DHIS2_URL=https://play.dhis2.org/api/26/organisationUnits.csv?paging=false&includeDescendants=true&includeAncestors=true&withinUserHierarchy=true&fields=id,name,displayName,shortName,path,ancestors,featureType,coordinates
        DHIS2_USERNAME=admin
        DHIS2_PASSWORD=district

        AREAS_ADMIN_LEVEL=2
        OUTPUT_DIR_NAME=play
        ```
1. Running the script
    ```
    python adr_dhis2_geodata_etl.py -e path_to/play.env
    ```
    
    - instead of providing DHIS2 url and credentials a csv input file with raw location data can be provided with `-c` flag e.g.
        ```
        python adr_dhis2_geodata_etl.py -e env/play.env -c play_raw_location_data.csv
        ```
     - flag `-p` creates a local cache of DHIS2 data instead of calling DHIS2. It speeds up consecutive runs of the script. Useful for debugging, e.g.
        ```
        python adr_dhis2_geodata_etl.py -p -e env/play.env 
        ```
