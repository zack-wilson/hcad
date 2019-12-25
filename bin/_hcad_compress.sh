#!/usr/bin/env bash
for i in ${1:-*.csv}; do
    tar -vacf "$i.tgz" "$i"
    rm -vr "$i"
done
