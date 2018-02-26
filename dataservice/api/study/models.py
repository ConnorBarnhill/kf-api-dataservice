from dataservice.extensions import db
from dataservice.api.common.model import Base, KfId
from dataservice.api.participant.models import Participant
from dataservice.api.study_file.models import StudyFile


class Study(db.Model, Base):
    """
    Study entity representing the dbGaP study.

    :param kf_id: Unique id given by the Kid's First DCC
    :param created_at: Time of object creation
    :param modified_at: Last time of object modification
    :param data_access_authority: Name of organization which governs data
    access
    :param external_id: dbGaP accession number
    :param version: dbGaP version
    :param name: Name or title of study
    :param attribution: Link to attribution prose provided by dbGaP
    """
    __tablename__ = 'study'
    __prefix__ = 'SD'

    data_access_authority = db.Column(db.Text(),
                                      nullable=False,
                                      default='dbGaP')

    external_id = db.Column(db.Text(), nullable=False)
    version = db.Column(db.Text())
    name = db.Column(db.Text())
    attribution = db.Column(db.Text())

    participants = db.relationship(Participant,
                                   cascade="all, delete-orphan",
                                   backref='study')
    investigator_id = db.Column(KfId(),
                                db.ForeignKey('investigator.kf_id'))
    study_files = db.relationship(StudyFile,
                                  cascade="all, delete-orphan",
                                  backref='study')

    def __repr__(self):
        return '<Study {}>'.format(self.kf_id)