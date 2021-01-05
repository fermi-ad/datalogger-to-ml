# Data Logger to ML

Scripts for requesting AD Controls data logger data and transforming those to the desired ML output format and destination.

## Dependencies

`pip` is used to manage dependencies. The `acsys` module is hosted on Fermilab machines and requires that a URL be specified for retrieval.

`pip install -r requirements.txt --extra-index-url https://www-bd.fnal.gov/pip3`

## Getting data

`dpmData.py` offers `--help` documentation for specifying requests.

The code below will request the previous 10 minutes of data logger data using the most recent device requests file from GitHub.

`python3 dpmData.py -du 600`
