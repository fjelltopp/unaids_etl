#!/usr/bin/env bash
set -e
ogr2ogr -F "ESRI Shapefile" $1.shp $1.json
zip $1.zip $1.prj $1.shp $1.shx $1.dbf
rm $1.prj $1.shp $1.shx $1.dbf