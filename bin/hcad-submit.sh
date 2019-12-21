#!/usr/bin/env bash
spark-submit \
	--py-files hcad.pyz \
	--archives "$(find data/hcad/landing -name "*.zip" | tr "\n" ",")" \
	hcad "${@}"
