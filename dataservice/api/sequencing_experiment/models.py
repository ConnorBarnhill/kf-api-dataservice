from sqlalchemy import event

from dataservice.extensions import db
from dataservice.api.common.model import Base, KfId
from dataservice.api.genomic_file.models import GenomicFile


class SequencingExperiment(db.Model, Base):
    """
    SequencingExperiment entity.
    :param kf_id: Unique id given by the Kid's First DCC
    :param external_id: Name given to sequencing experiment by contributor
    :param experiment_date : Date of the sequencing experiment conducted
    :param experiment_strategy: Text term that represents the library strategy
    :param library_name: Text term that represents the name of the library
    :param library_strand: Text term that represents the library stranded-ness
    :param is_paired_end: Boolean term specifies whether reads have paired end
    :param platform: Name of the platform used to obtain data
    :param instrument_model: Text term that represents the model of instrument
    :param max_insert_size: Maximum size of the fragmented DNA
    :param mean_insert_size: Mean size of the fragmented DNA
    :param mean_depth: (Coverage)Describes the amount of sequence data that
           is available per position in the sequenced genome territory
    :param total_reads: Total reads of the sequencing experiment
    :param mean_read_length: Mean lenth of the reads
    """
    __tablename__ = 'sequencing_experiment'
    __prefix__ = 'SE'

    external_id = db.Column(db.Text(), nullable=False,
                            doc='Name given to sequencing experiment by'
                            ' contributor')
    experiment_date = db.Column(db.DateTime(),
                                doc='Date of the sequencing experiment'
                                ' conducted')
    experiment_strategy = db.Column(db.Text(), nullable=False,
                                    doc='Text term that represents the'
                                    ' Library strategy')
    library_name = db.Column(db.Text(),
                             doc='Text term that represents the name of the'
                             ' library')
    library_strand = db.Column(db.Text(),
                               doc='Text term that represents the'
                               ' library stranded-ness')
    is_paired_end = db.Column(db.Boolean(), nullable=False,
                              doc='Boolean term specifies whether reads have'
                              ' paired end')
    platform = db.Column(db.Text(), nullable=False,
                         doc='Name of the platform used to obtain data')
    instrument_model = db.Column(db.Text(),
                                 doc='Text term that represents the model of'
                                 ' instrument')
    max_insert_size = db.Column(db.Integer(),
                                doc='Maximum size of the fragmented DNA')
    mean_insert_size = db.Column(db.Float(),
                                 doc='Mean size of the fragmented DNA')
    mean_depth = db.Column(db.Float(),
                           doc='Mean depth or coverage describes the amount of'
                           ' sequence data that is available per position in'
                           ' the sequenced genome territory')
    total_reads = db.Column(db.Integer(),
                            doc='Total reads of the sequencing experiment')
    mean_read_length = db.Column(db.Float(),
                                 doc='Mean lenth of the reads')
    genomic_files = db.relationship(GenomicFile,
                                    backref=db.backref(
                                        'sequencing_experiment',
                                        lazy=True))
    sequencing_center_id = db.Column(KfId(),
                                     db.ForeignKey('sequencing_center.kf_id'),
                                     nullable=False,
                                     doc='The kf_id of the sequencing center')


@event.listens_for(GenomicFile, 'after_delete')
def delete_orphans(mapper, connection, state):
    q = (db.session.query(SequencingExperiment)
         .filter(~SequencingExperiment.genomic_files.any()))
    q.delete(synchronize_session='fetch')
