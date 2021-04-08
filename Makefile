build:
	poetry build
	poetry export --output=requirements.txt

deploy: build
	scp dist/* chablis:/usr/local/www/data/pip3/datalogger-to-ml/

clean:
	rm -rf dist __pycache__ **/__pycache__ nanny.log
