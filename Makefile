venv: venv/.touchfile

venv/.touchfile: setup.cfg
	test -d venv || python3 -m venv venv
	. venv/bin/activate
	pip install -e '.[testing]'
	touch venv/.touchfile

test: venv
	pytest --cov=src/shillelagh -vv tests/ --doctest-modules src/shillelagh --without-integration --without-slow-integration

integration: venv
	pytest --cov=src/shillelagh -vv tests/ --doctest-modules src/shillelagh --with-integration --with-slow-integration

clean:
	rm -rf venv

spellcheck:
	codespell -S "*.json" src/shillelagh docs/*rst tests
