#!/usr/bin/env bash
set -x
ZIP_FILE=$1
K=${2:-100}
COMPRESSED_FILES=$(zipinfo -1 "$ZIP_FILE")
EXCTRACT_DIR=$(dirname "$ZIP_FILE")


echo "$COMPRESSED_FILES"
echo "$EXCTRACT_DIR"
#
#unzip "$ZIP_FILE"
#
#for f_in in $(zipinfo -1 "$ZIP_FILE"); do
#    f_out=$(dirname "$ZIP_FILE")/txt/sample."$K"."$f_in"
#    mkdir -p "$(dirname "$f_out")"
#    K_split=$(( K / 2))
#    head -n  "$K_split" "$f_in" > "$f_out"
#    tail -n "$K_split" "$f_in" >> "$f_out"
#done
#
#find "$(dirname "$ZIP_FILE")" -name "*sample*" -type f
