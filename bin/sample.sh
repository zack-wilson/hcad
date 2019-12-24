#!/usr/bin/env bash
GLOB=${$1:"*.*"}
K=${2:-100}

for f_in in $GLOB; do
    dst=$(dirname "$f_in")/sample."$K".$(basename "$f_in")
    K_split=$(("$K" / 2))
    head -n  "$K_split" "$f_in" > "$dst"
    tail -n "$K_split" "$f_in" >> "$dst"
done
