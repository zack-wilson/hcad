--shell=bash
.PHONY: package clean

package:hcad
	 python -m zipapp -o hcad.pyz -c hcad

clean:
	find . -name __pycache__ -type d -o -name "*.pyc" -type f
	rm -rf *.egg-info/ dist/ build/ *.pyz *_cache/
