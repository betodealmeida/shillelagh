pyenv: .python-version

.python-version: requirements/test.txt
	if [ -z "`pyenv virtualenvs | grep shillelagh`" ]; then\
	    pyenv virtualenv shillelagh;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local shillelagh;\
	fi
	pip install -r requirements/test.txt
	touch .python-version

test: pyenv
	pytest --cov=src/shillelagh -vv tests/ --doctest-modules src/shillelagh --without-integration --without-slow-integration

integration: pyenv
	pytest --cov=src/shillelagh -vv tests/ --doctest-modules src/shillelagh --with-integration --with-slow-integration

clean:
	pyenv virtualenv-delete shillelagh

spellcheck:
	codespell -S "*.json" src/shillelagh docs/*rst tests templates *.rst

check:
	pre-commit run --all-files
