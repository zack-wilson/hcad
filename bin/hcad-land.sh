#!/usr/bin/env bash
TAX_YEAR=${1:-$(date +"%Y")}
DATABASE="data/hcad/"
LANDING="$DATABASE/landing/"
STAGING="$DATABASE/staging/"

mkdir -p logs
mkdir -p "$DATABASE/Desc"
mkdir -p "$LANDING"
mkdir -p "$STAGING/$TAX_YEAR/csv"

wget --recursive --no-parent --continue --random-wait \
    -D "pdata.hcad.org" \
    -P "$LANDING" \
    --accept "txt,zip" \
    --reject "Access.zip" \
    --reject-regex ".*/GIS/.*" \
    "https://pdata.hcad.org/index.html"

find "$LANDING" -name "*.txt" -exec cp -v {} "$DATABASE/Desc" \;
find "$LANDING" -path "**/$TAX_YEAR/*" -name "*.zip" -type f -exec unzip -ua -d "$STAGING/$TAX_YEAR/raw" {} \;
find "$STAGING" -name "*.txt" -type f
touch "$LANDING/_SUCCESS"
