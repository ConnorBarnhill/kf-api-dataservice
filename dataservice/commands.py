# -*- coding: utf-8 -*-
"""Click commands."""

import click


@click.command()
def test():
    """ Run the unit tests and pep8 checks """
    from subprocess import call
    unit = call(["python", "-m", "pytest", "tests",
                 "--cov", "dataservice.api"])
    lint = call(["python", "-m", "pytest", "--pep8", "dataservice"])
    exit(max(unit, lint))


@click.command()
def erd():
    """ Create an ERD of the current data model """
    import os
    from eralchemy import render_er
    from dataservice.api.participant.models import Participant

    if not os.path.isdir('docs'):
        os.mkdir('docs')

    render_er(Participant, os.path.join('docs', 'erd.png'))


@click.command()
@click.argument('etl_module_name')
@click.argument('config_file', type=click.Path(), required=False)
def import_data(etl_module_name, config_file):
    """
    ETL real data to database via creating new objects

    Arg: etl_module_name is the name of the python module to run in the
    dataservice.util.data_import package. Example: 'seidman'
    """
    from dataservice.util.data_import import main
    from dataservice.util.data_import.config import IMPORT_DATA_OP
    main.run(IMPORT_DATA_OP, etl_module_name, config_file)


@click.command()
@click.argument('etl_module_name')
@click.argument('config_file', type=click.Path(), required=False)
def update_data(etl_module_name, config_file):
    """
    ETL real data to database via updating existing objects

    Arg: etl_module_name is the name of the python module to run in the
    dataservice.util.data_import package. Example: 'seidman'
    """
    from dataservice.util.data_import import main
    from dataservice.util.data_import.config import UPDATE_DATA_OP
    main.run(UPDATE_DATA_OP, etl_module_name, config_file)


@click.command()
@click.argument('etl_module_name')
@click.argument('config_file', type=click.Path(), required=False)
def drop_data(etl_module_name, config_file):
    """
    ETL real data to database

    Arg: etl_module_name is the name of the python module to run in the
    dataservice.util.data_import package. Example: 'seidman'
    """
    from dataservice.util.data_import import main
    main.drop_data(etl_module_name, config_file)


@click.command()
def populate_db():
    """
    Run the dummy data generator

    Populate the database
    """
    from dataservice.util.data_gen.data_generator import DataGenerator
    dg = DataGenerator()
    dg.create_and_publish_all()


@click.command()
def clear_db():
    """
    Run the dummy data generator

    Clear the database
    """
    from dataservice.util.data_gen.data_generator import DataGenerator
    dg = DataGenerator()
    dg.drop_all()
