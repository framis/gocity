#!/bin/bash

DUMPS="US.zip CN.zip TH.zip FR.zip"
POSTAL_CODES="allCountries.zip"
FOLDER="`pwd`/download"

unzip -v >/dev/null 2>&1 || { echo >&2 "I require 'unzip' but it's not installed.  Aborting."; exit 1; }
wget -h >/dev/null 2>&1 || { echo >&2 "I require 'wget' but it's not installed.  Aborting."; exit 1; }

mkdir -p $FOLDER/zip

echo "Downloading GeoNames.org data..."
for dump in $DUMPS; do
    wget -c -P "$FOLDER" http://download.geonames.org/export/dump/$dump
done
find $FOLDER -name "*.zip" -print0 | xargs -0 -n1 unzip -u -d$FOLDER


echo "Downloading postalCodes"
for postalCode in $POSTAL_CODES; do
    wget -c -P "$FOLDER/zip" http://download.geonames.org/export/zip/$postalCode
done
unzip -u $FOLDER/zip/*.zip -d $FOLDER/zip -x readme.txt

# Cleanup
find $FOLDER -name "*.zip" -print0 | xargs -0 -n1 rm