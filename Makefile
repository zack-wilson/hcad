--shell=bash
.PHONY: check clean package reformat

package:hcad
	 python -m zipapp -o hcad.pyz -c hcad

clean:
	find . -name "__pycache__" -type d -o -name "*.pyc" -o -name ".cache.?" -type f
	rm -rf *.egg-info/ dist/ build/ *.pyz .*_cache/ .tox/ .eggs/

reformat:
	setup-cfg-fmt setup.cfg
	black -l 79 -t py37 setup.py "hcad" "tests"
	isort -y -rc "hcad" "tests"
	beautysh --files bin/*.sh
	prettier --write --quote-props consistent *.yml *.yaml *.md *.json

check:
	pylint hcad
	mypy hcad
