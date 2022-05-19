#!/bin/sh
#
# Creates a CSV of total owed by plates sorted by total owed.
#
set -e

fields=tag,plate,totalowed
out=data/csv/vanityplatessorted.csv

mkdir -p tmp/scripts $(dirname $out)
cat playgrounds/vanity_convert_to_csv.mongodb | sed 's#use(#//use(#g' > tmp/scripts/vanity_convert_to_csv.js
mongo mongodb://localhost:27017/nycparkingviolations tmp/scripts/vanity_convert_to_csv.js
mongoexport --db=nycparkingviolations --collection=vanityconverttocsv --type=csv --fields=$fields > $out
echo "Written to $out $(wc -l $out | awk '{print $1}' | sed 's/ //g') lines"
cp $out ~/Desktop