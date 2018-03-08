# -*- coding: utf-8 -*-
"""The app module, containing the app factory function."""
from flask import Flask
from collections import namedtuple
import datetime

from dataservice import commands
from dataservice.utils import _get_version
from dataservice.extensions import db, ma, migrate
from dataservice.api.investigator.models import Investigator
from dataservice.api.study.models import Study
from dataservice.api.participant.models import Participant
from dataservice.api.family_relationship.models import FamilyRelationship
from dataservice.api.demographic.models import Demographic
from dataservice.api.diagnosis.models import Diagnosis
from dataservice.api.outcome.models import Outcome
from dataservice.api.phenotype.models import Phenotype
from dataservice.api.sample.models import Sample
from dataservice.api.aliquot.models import Aliquot
from dataservice.api.sequencing_experiment.models import SequencingExperiment
from dataservice.api.genomic_file.models import GenomicFile
from dataservice.api.workflow.models import Workflow, WorkflowGenomicFile
from dataservice.api.study_file.models import StudyFile
from dataservice.api.study.models import Study
from dataservice.api.investigator.models import Investigator
from config import config

from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from werkzeug.exceptions import HTTPException


def create_app(config_name):
    """
    An application factory
    """
    app = Flask(__name__)
    app.url_map.strict_slashes = False
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    # Register Flask extensions
    register_extensions(app)
    register_shellcontext(app)
    register_commands(app)
    register_error_handlers(app)
    register_blueprints(app)
    register_spec(app)
    register_admin(app)

    return app


def register_spec(app):
    """
    Creates an API spec and puts it on the app
    """
    from apispec import APISpec

    spec = APISpec(
        title='Kids First Data Service',
        version=_get_version(),
        plugins=[
            'apispec.ext.flask',
            'apispec.ext.marshmallow',
        ],
    )

    from dataservice.api import status_view, views
    from dataservice.api.common.schemas import StatusSchema

    spec.definition('Status', schema=StatusSchema)

    from dataservice.api.common.views import CRUDView
    CRUDView.register_spec(spec)
    with app.test_request_context():
        spec.add_path(view=status_view)
        for view in views:
            spec.add_path(view=view)

    app.spec = spec


def register_shellcontext(app):
    """
    Register shell context objects
    """
    def shell_context():
        """Shell context objects."""
        return {'db': db,
                'Participant': Participant}

    app.shell_context_processor(shell_context)


def register_commands(app):
    """
    Register Click commands
    """
    app.cli.add_command(commands.test)
    app.cli.add_command(commands.erd)
    app.cli.add_command(commands.populate_db)
    app.cli.add_command(commands.clear_db)


def register_extensions(app):
    """
    Register Flask extensions
    """

    # SQLAlchemy
    db.init_app(app)
    ma.init_app(app)

    # If using sqlite, must instruct sqlalchemy to set foreign key constraint
    if app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite"):
        from sqlalchemy.engine import Engine
        from sqlalchemy import event

        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    # Migrate
    migrate.init_app(app, db)


def register_error_handlers(app):
    """
    Register error handlers

    NB: Exceptions to be handled must be imported in the head of this module
    """
    from dataservice.api import errors
    app.register_error_handler(HTTPException, errors.http_error)
    app.register_error_handler(IntegrityError, errors.integrity_error)
    app.register_error_handler(404, errors.http_error)
    app.register_error_handler(400, errors.http_error)


def register_blueprints(app):
    from dataservice.api import api
    app.register_blueprint(api)


def register_admin(app):
    import flask_admin as admin
    from flask_admin.contrib import sqla
    from flask.ext.admin.model.form import InlineFormAdmin
    from flask.ext.admin.contrib.sqla import ModelView
    from flask.ext.admin.contrib.sqla.form import InlineModelConverter
    from flask.ext.admin.contrib.sqla.fields import InlineModelFormList

    from dataservice.api.participant.models import Participant
    from dataservice.api.demographic.models import Demographic
    from dataservice.api.diagnosis.models import Diagnosis
    from dataservice.api.sample.models import Sample
    from dataservice.api.genomic_file.models import GenomicFile
    from dataservice.api.sequencing_experiment.models import (
        SequencingExperiment
    )
    from dataservice.api.aliquot.models import Aliquot

    class IndexView(admin.AdminIndexView):
        @admin.expose('/')
        def index(self):
            # Time metric


            result = db.engine.execute('''SELECT date(created_at) as date, date_part('hour', created_at) as hour, count(created_at) as total_count
                                FROM "participant"
                                WHERE ("participant"."created_at" BETWEEN '2012-10-17 00:00:00.000000' AND '2019-11-07     12:25:04.082224') 
                                GROUP BY date(created_at), date_part('hour', created_at)
                                ORDER BY count(created_at) DESC''')

            Record = namedtuple('Record', result.keys())
            records = [Record(*r) for r in result.fetchall()]
            dts = []
            ts = []
            ts_counts = {}
            for r in records:
                #d = datetime.time(hour=int(r.hour), minute=int(r.minute))
                d = datetime.time(hour=int(r.hour))
                dt = datetime.datetime.combine(r.date, d)
                dts.append(dt)
                ts_counts[dt.isoformat()] = r.total_count

            start = min(dts) - datetime.timedelta(seconds=4*3600)

            dts = [start]
            for i in range(12):
                dts.append(dts[-1] + datetime.timedelta(seconds=3600))
                if dts[-1].isoformat() in ts_counts:
                    continue
                ts_counts[dts[-1].isoformat()] = 0

            ts = list(ts_counts.keys())
            ts_counts = list(ts_counts.values())


            # Participants by Study

            counts = (db.session.query(func.count(Participant.study_id)
                                         .label('count'),
                                         Participant.study_id)
                                         .group_by(Participant.study_id)
                                         .all())

            study_names = []
            participants_by_study = []
            for count, study in counts:
                study_name = Study.query.get(study).investigator
                if study_name is not None:
                    study_name = study_name.name
                else:
                    study_name = 'CBTTC'
                study_names.append(study_name)
                participants_by_study.append(count)

            # Phenotype breakdown
            counts = (db.session.query(func.count(Phenotype.phenotype)
                                         .label('count'),
                                         Phenotype.phenotype)
                                         .group_by(Phenotype.phenotype)
                                         .all())

            pheno_names = []
            pheno_counts = []
            for count, pheno in sorted(counts, key=lambda x: x[0], reverse=True):
                if count <= 20:
                    continue
                #pheno_name = Phenotype.query.get(pheno)
                pheno_names.append(pheno)
                pheno_counts.append(count)

            # Files by acl
            # Entity Counts
            counts = {}
            entities = [Study,
                        Investigator,
                        Participant,
                        Demographic,
                        Diagnosis,
                        Sample,
                        GenomicFile,
                        SequencingExperiment,
                        Aliquot]

            for e in entities:
                counts[e.__name__] = e.query.count()

            quality = {}

            # Demographic metric
            with_dem = Participant.query.join(Demographic).count()
            tot = Participant.query.count()
            msg = '{} / {}'.format(with_dem, tot)
            qual = {'actual': with_dem,
                    'expect': tot}
            quality['participants_with_demographic'] = qual

            with_samp = (Participant.query
                         .filter(Participant.samples.any())
                         .count())
            tot = Participant.query.count()
            msg = '{} / {}'.format(with_samp, tot)
            qual = {'actual': with_samp,
                    'expect': tot}
            quality['participants_with_at_least_one_sample'] = qual

            with_experiment = (Aliquot.query
                               .filter(Aliquot.sequencing_experiments.any())
                               .count())
            tot = Aliquot.query.count()
            msg = '{} / {}'.format(with_experiment, tot)
            qual = {'actual': with_experiment,
                    'expect': tot}
            quality['aliquots_with_at_least_one_sequencing_experiment'] = qual

            with_file = (SequencingExperiment.query
                         .filter(SequencingExperiment.genomic_files.any())
                         .count())
            tot = SequencingExperiment.query.count()
            msg = '{} / {}'.format(with_file, tot)
            qual = {'actual': with_file,
                    'expect': tot}
            quality['sequencing_experiments_with_at_least_one_file'] = qual


            with_phen = (Participant.query
                         .filter(Participant.phenotypes.any())
                         .count())
            tot = Participant.query.count()
            msg = '{} / {}'.format(with_file, tot)
            qual = {'actual': with_phen,
                    'expect': tot}
            quality['participants_with_at_least_one_phenotype'] = qual

            return self.render('admin/index.html',
                               participants_by_study=participants_by_study,
                               studies=study_names,
                               counts=counts,
                               pheno_names=pheno_names,
                               pheno_counts=pheno_counts,
                               ts=ts,
                               ts_counts=ts_counts,
                               quality=quality)

    class GenomicFileAdmin(sqla.ModelView):
        column_display_pk = True
        column_exclude_list = ['uuid', 'md5sum', 'sequencing_experiments']
        column_searchable_list = ('kf_id',)
        column_sortable_list = ('created_at', 'modified_at')

    class InlineDemographic(InlineFormAdmin):
        def __init__(self):
            super(InlineDemographic, self).__init__(Diagnosis)

    class ParticipantAdmin(sqla.ModelView):
        column_display_pk = True
        column_exclude_list = ['uuid', ]
        column_searchable_list = ('external_id', 'kf_id')
        column_sortable_list = ('created_at', 'modified_at')
        # column_auto_select_related = True
        inline_models = (InlineDemographic(),)

    admin = admin.Admin(app, name='Dataservice',
                        index_view=IndexView(),
                        template_mode='bootstrap3')
    admin.add_view(ParticipantAdmin(Participant, db.session))
    admin.add_view(GenomicFileAdmin(GenomicFile, db.session))
