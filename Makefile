requirements:
	pipreqs --force shillelagh --savepath requirements.txt

init: requirements
	pip install -r requirements.txt

test:
	nosetests tests

.PHONY: init test
