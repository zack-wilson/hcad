#!/usr/bin/env bash
CURRENT_YEAR=$(date +"%Y")
TAX_YEAR=${1:-$CURRENT_YEAR}
DATABASE="data/hcad"
DESC="$DATABASE/Desc/$TAX_YEAR"
LANDING="$DATABASE/landing"

if [[ "$TAX_YEAR" == "$CURRENT_YEAR" ]]; then
    URL="https://pdata.hcad.org/download/"
else
    URL="https://pdata.hcad.org/download/${TAX_YEAR}.html"
fi

echo "Downloading $TAX_YEAR Property Tax Data."
mkdir -p {"logs","$DESC","$LANDING"}
wget --recursive --continue --random-wait \
    -D "pdata.hcad.org" \
    -P "$LANDING" \
    --accept "txt,zip" \
    --reject "Access.zip" \
    --reject-regex ".*/GIS/.*" \
    "$URL"

find "$LANDING" -path "**/${TAX_YEAR}/*" -o -path "**/Desc/*" -name "*.txt"  -exec cp {} "$DESC" \;
touch {"$DESC/_SUCCESS","$LANDING/_SUCCESS"}
