build:
	python -m poetry build
	python -m poetry export --output=requirements.txt

install:
	python -m poetry install

deploy: build
	scp dist/* chablis:/usr/local/www/data/pip3/datalogger-to-ml/

clean:
	rm -rf dist __pycache__ **/__pycache__ nanny.log
