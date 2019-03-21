# rnaget Microservice

Based on CanDIG demo projects: [OpenAPI variant service demo](https://github.com/ljdursi/openapi_calls_example), [Python Model Service](https://github.com/CanDIG/python_model_service)
This is a Proof-of-Concept implementation of the [GA4GH rnaget API](https://github.com/ga4gh-rnaseq/schema), used to query and download RNA quantification matrix data

## Schema info
For more information about the schemas used visit https://github.com/ga4gh-rnaseq/schema

## Stack

- [Connexion](https://github.com/zalando/connexion) for implementing the API
- [SQLAlchemy](http://sqlalchemy.org), using [Sqlite3](https://www.sqlite.org/index.html) for ORM
- [Bravado-core](https://github.com/Yelp/bravado-core) for Python classes from the spec
- [Dredd](https://dredd.readthedocs.io/en/latest/) and [Dredd-Hooks-Python](https://github.com/apiaryio/dredd-hooks-python) for testing
- [HDF5] (https://www.hdfgroup.org/solutions/hdf5/) for matrix store & operations
- Python 3
- Pytest, tox
- Travis-CI

## Installation

The server software can be installed in a virtual environment:

```
pip install -r requirements.txt
pip install -r requirements_dev.txt
python setup.py develop
```

for automated testing you can install dredd; assuming you already have node and npm installed,

```
npm install -g dredd
```

### Running

By default a demo server can be run with:

```
python3 -m rnaget_service --loglevel=WARN
```

To specify your own database & log files, the server can be started with:

```
python3 -m rnaget_service --database=test.db --logfile=test.log --loglevel=WARN
```

For testing, the dredd config is currently set up to launch the service itself, so no server needs be running:

```
cd tests
dredd --hookfiles=dreddhooks.py
```
