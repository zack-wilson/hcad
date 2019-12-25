#!/usr/bin/env bash
RESOURCES=${1:-resources/}
wget -c -N -P "$RESOURCES/ddls" -i "$RESOURCES/urls.txt"

