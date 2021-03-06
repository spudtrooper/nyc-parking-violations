#!/bin/sh
#
# Creates a CSV of total owed by plates sorted by total owed.
#
set -e

fields=tag,plate,totalowed
out=data/csv/platessorted.csv

mkdir -p tmp/scripts $(dirname $out)
cat playgrounds/convert_to_csv.mongodb | sed 's#use(#//use(#g' > tmp/scripts/convert_to_csv.js
mongo mongodb://localhost:27017/nycparkingviolations tmp/scripts/convert_to_csv.js
mongoexport --db=nycparkingviolations --collection=converttocsv --type=csv --fields=$fields > $out
echo "Written to $out $(wc -l $out | awk '{print $1}' | sed 's/ //g') lines"
cp $out ~/Desktop