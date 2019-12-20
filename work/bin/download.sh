#!/usr/bin/env bash
TAX_YEAR=${1:$(date +"%Y")}

wget --spider --mirror --continue --random-wait \
    --compression="auto" \
    --domains="http://pdata.hcad.org" \
    --directory-prefix="data/landing" \
    --accept=".txt,.zip" \
    "https://pdata.hcad.org"
