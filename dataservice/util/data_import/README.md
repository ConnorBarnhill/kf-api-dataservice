Kids First Data Utility
=========================

The Kids First Data Utility is currently a simple tool, used internally, to ETL raw Kids First data into the Kids First Data Service. This tool is currently a utility that is part of the
Kids First Data Service API, however, in the near future it will be extracted from here
and made part of a Kids First data import or ETL service.

Additionally, the loading portion of the ETL currently interfaces directly with the Postgres database via the SQlAlchemy ORM.
In the near future the loader will be extended to interface with the Kids First Dataservice API to load data.

These instructions are for local development and experimentation.  

## Preconditions
You have already setup your development environment according to the [README](https://github.com/kids-first/kf-api-dataservice)
in the Kids First Dataservice. At a minimum you should have Postgres running (locally or in docker container).
The Postgres server has a database called `dev` with the Kids First Dataservice entity tables.
Additionally, you have the `FLASK_APP` environment variable set to `manage.py`.

## Import Data
The data importer uses flask commands as the main interface for running data imports and
deleting data.

A user may import a dataset via the `import_data` flask command with the name of the
python ETL package as the positional argument after the command.

### Example
```bash
flask import_data seidman
```

Data is imported by study. So in the example above the argument `seidman` is the name of the Python package which contains the ETL code to import data from the Seidman study.

Each time this command is run, a new set of data for this study is imported into
the Kids First Dataservice. The import tool does not yet support update of
existing data.

### Location of Data
The importer runs locally and expects that the data is in a local folder on the file system.
The path for this is set in the ETL package's extract module.

For example, to update the data directory for the Seidman study ETL, go to:

./seidman/extract.py

and change the `DATA_DIR` variable to the location of the seidman data on your system.

In the near future, this along with other ETL configurations will not be hard coded and will move into a YAML config file.

### Drop Data
Similar to importing the data, data can be dropped by study.

### Example
```bash
flask drop_data seidman
```  
The `drop_data` command will drop all seidman data, even if you've imported the data multiple times.

## Supported Studies
Initial ETL modules have been implemented for the following studies:

1. Seidman FY 2015
2. Schiffman FY 2015
3. Rios FY 2016
4. Chung FY 2015   

## Add a New ETL module
Data for additional studies may be imported by implementing a new ETL module
for the study(s). Once it has been implemented simply follow the examples above
to import data or drop data for that module.

`flask import_data <new ETL module name>`

More later ...
