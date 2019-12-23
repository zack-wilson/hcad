#!/usr/bin/env bash

for i in $1; do
    head -n 1000 "$i" > sample.1000."$(basename "$i")"
done
