from dataservice.extensions import db
from dataservice.api.common.model import Base, KfId
from dataservice.api.biospecimen_genomic_file.models import (
    BiospecimenGenomicFile)
from dataservice.api.diagnosis.models import Diagnosis
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import event


class BiospecimenDiagnosis(db.Model, Base):
    """
    Represents association table between biospecimen table and
    diagnosis table. Contains all biospecimen, diagnosis combiniations.
    :param kf_id: Unique id given by the Kid's First DCC
    :param created_at: Time of object creation
    :param modified_at: Last time of object modification
    """
    __tablename__ = 'biospecimen_diagnosis'
    __prefix__ = 'BD'
    __table_args__ = (db.UniqueConstraint('diagnosis_id',
                                          'biospecimen_id'),)
    diagnosis_id = db.Column(KfId(),
                             db.ForeignKey('diagnosis.kf_id'),
                             nullable=False)

    biospecimen_id = db.Column(KfId(),
                               db.ForeignKey('biospecimen.kf_id'),
                               nullable=False)


class Biospecimen(db.Model, Base):
    """
    Biospecimen entity.
    :param kf_id: Unique id given by the Kid's First DCC
    :param external_sample_id: Name given to sample by contributor
    :param external_aliquot_id: Name given to aliquot by contributor
    :param composition : The cellular composition of the biospecimen.
    :param source_text_tissue_type: description of the kind of tissue collected
           with respect to disease status or proximity to tumor tissue
    :param source_text_anatomical_site : The name of the primary disease site
           of the submitted tumor biospecimen
    :param age_at_event_days: Age at the time biospecimen was
           acquired, expressed in number of days since birth
    :param source_text_tumor_descriptor: The kind of disease present in the
           tumor specimen as related to a specific timepoint
    :param shipment_origin : The origin of the shipment
    :param analyte_type: Text term that represents the kind of molecular
           specimen analyte
    :param concentration_mg_per_ml: The concentration of an analyte or aliquot
           extracted from the biospecimen or biospecimen portion, measured in
           milligrams per milliliter
    :param volume_ml: The volume in microliters (ml) of the aliquots derived
           from the analyte(s) shipped for sequencing and characterization
    :param shipment_date: The date item was shipped in YYYY-MM-DD format
    :param uberon_id_anatomical_site: The ID of the term from Uber-anatomy
           ontology which represents harmonized anatomical ontologies
    :param ncit_id_tissue_type: The ID term from the National Cancer Institute
           Thesaurus which represents a harmonized tissue_type
    :param ncit_id_anatomical_site: The ID term from the National Cancer
           Institute Thesaurus which represents a harmonized anatomical_site
    :param spatial_descriptor: Ontology term that harmonizes the spatial
           concepts from Biological Spatial Ontology
    :param dbgap_consent_code: Consent classification code from dbgap
    """

    __tablename__ = 'biospecimen'
    __prefix__ = 'BS'

    external_sample_id = db.Column(db.Text(),
                                   doc='Name given to sample by contributor')
    external_aliquot_id = db.Column(db.Text(),
                                    doc='Name given to aliquot by contributor')
    source_text_tissue_type = db.Column(db.Text(),
                                        doc='Description of the kind of '
                                        'biospecimen collected')
    composition = db.Column(db.Text(),
                            doc='The cellular composition of the biospecimen')
    source_text_anatomical_site = db.Column(db.Text(),
                                            doc='The anatomical location of '
                                            'collection')
    age_at_event_days = db.Column(db.Integer(),
                                  doc='Age at the time of event occurred in '
                                      'number of days since birth.')
    source_text_tumor_descriptor = db.Column(db.Text(),
                                             doc='Disease present in the '
                                             'biospecimen')
    shipment_origin = db.Column(db.Text(),
                                doc='The original site of the aliquot')
    analyte_type = db.Column(db.Text(), nullable=False,
                             doc='The molecular description of the aliquot')
    concentration_mg_per_ml = db.Column(db.Float(),
                                        doc='The concentration of the aliquot')
    volume_ml = db.Column(db.Float(),
                          doc='The volume of the aliquot')
    shipment_date = db.Column(db.DateTime(),
                              doc='The date the aliquot was shipped')
    uberon_id_anatomical_site = db.Column(db.Text(),
                                          doc='The ID of the term from '
                                          'Uber-anatomy ontology which '
                                          'represents harmonized anatomical'
                                          ' ontologies')
    ncit_id_tissue_type = db.Column(db.Text(),
                                    doc='The ID term from the National Cancer'
                                    'Institute Thesaurus which represents a '
                                    'harmonized tissue_type')
    ncit_id_anatomical_site = db.Column(db.Text(),
                                        doc='The ID term from the National'
                                        'Cancer Institute Thesaurus which '
                                        'represents a harmonized'
                                        ' anatomical_site')
    spatial_descriptor = db.Column(db.Text(),
                                   doc='Ontology term that harmonizes the'
                                   'spatial concepts from Biological Spatial'
                                   ' Ontology')
    participant_id = db.Column(KfId(),
                               db.ForeignKey('participant.kf_id'),
                               nullable=False,
                               doc='The kf_id of the biospecimen\'s donor')
    sequencing_center_id = db.Column(KfId(),
                                     db.ForeignKey('sequencing_center.kf_id'),
                                     nullable=False,
                                     doc='The kf_id of the sequencing center')
    dbgap_consent_code = db.Column(db.Text(),
                                   doc='Consent classification code from dbgap'
                                   )
    genomic_files = association_proxy(
        'biospecimen_genomic_files', 'genomic_file',
        creator=lambda genomic_file:
        BiospecimenGenomicFile(genomic_file=genomic_file))

    biospecimen_genomic_files = db.relationship(BiospecimenGenomicFile,
                                                backref='biospecimen',
                                                cascade='all, delete-orphan')
    diagnoses = db.relationship('Diagnosis', secondary='biospecimen_diagnosis',
                                backref=db.backref(
                                    'biospecimens'))


def validate_biospecimen(target):
    """
    Ensure that both the diagnosis and biospecimen
    have the same participant
    If this is not the case then raise DatabaseValidationError
    """
    from dataservice.api.errors import DatabaseValidationError
    # Return if biospecimen is None or
    # if diagnosis doesn't exist, return and
    # let ORM handle non-existent foreign key
    if not target or not target.diagnoses:
        return
    # Get diagnosis by id and bisopecimen by id
    ds = Diagnosis.query.get(target.diagnoses[0].kf_id)
    bsp = Biospecimen.query.get(target.kf_id)
    if ds is None:
        operation = 'modify'
        target_entity = Diagnosis.__tablename__
        message = ('Diagnosis {} does not exist').format(
            target.diagnoses[0].kf_id)
        raise DatabaseValidationError(target_entity, operation, message)
    elif bsp is None:
        operation = 'modify'
        target_entity = Biospecimen.__tablename__
        message = ('Biospecimen {} does not exist').format(target.kf_id)
        raise DatabaseValidationError(target_entity, operation, message)
    # Check if this diagnosis and biospecimen refer to same participant
    if ds.participant_id != target.participant_id:
        operation = 'modify'
        target_entity = Biospecimen.__tablename__
        message = (
            ('a diagnosis cannot be linked with a biospecimen if they '
             'refer to different participants. diagnosis {} '
             'refers to participant {} and '
             'biospecimen {} refers to participant {}')
            .format(ds.kf_id,
                    ds.participant_id,
                    target.kf_id,
                    target.participant_id))
        raise DatabaseValidationError(target_entity, operation, message)


@event.listens_for(Biospecimen, 'before_insert')
@event.listens_for(Biospecimen, 'before_update')
def biospecimen_on_insert(mapper, connection, target):
    """
    Run preprocessing/validation of diagnosis before insert
    """
    validate_biospecimen(target)
