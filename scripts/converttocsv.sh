#!/bin/sh
#
# Creates a CSV of total owed by plates sorted by total owed.
#
set -e

fields=plate,totalowed

mkdir -p tmp/scripts

out=data/platessorted.csv
cat playgrounds/convert_to_csv.mongodb | sed 's#use(#//use(#g' > tmp/scripts/convert_to_csv.js
mongo mongodb://localhost:27017/nycparkingviolations tmp/scripts/convert_to_csv.js
mongoexport --db=nycparkingviolations --collection=converttocsv --type=csv --fields=$fields > $out
echo "Written to $out"