from sqlalchemy import event, or_, and_


from dataservice.extensions import db
from dataservice.api.common.model import Base, KfId
from dataservice.api.participant.models import Participant

REVERSE_RELS = {
    'mother': 'Child',
    'father': 'Child',
    'sibling': 'Sibling'
}


class FamilyRelationship(db.Model, Base):
    """
    Represents a relationship between two family members.

    The relationship table represents a directed graph. One or more
    relationships may exist between any two participants.
    (P1 -> P2 is different than P2 -> P1)

    :param kf_id: Primary key given by the Kid's First DCC
    :param created_at: Time of object creation
    :param modified_at: Last time of object modification
    :param external_id: Name given to family_relationship by contributor
    :param participant1_id: Kids first id of the first Participant in the
    relationship
    :param participant2_id: Kids first id of the second Participant
    in the relationship
    :param relationship_type: Text describing the nature of the
    relationship (i.e. father, mother, sister, brother)
    :param _rel_name: an autogenerated parameter used to ensure that the
    relationships are not duplicated and the graph is undirected
    """
    __tablename__ = 'family_relationship'
    __prefix__ = 'FR'
    __table_args__ = (db.UniqueConstraint(
        'participant1_id', 'participant2_id',
        'participant1_to_participant2_relation',
        'participant2_to_participant1_relation'),)
    external_id = db.Column(db.Text(),
                            doc='external id used by contributor')
    participant1_id = db.Column(
        KfId(),
        db.ForeignKey('participant.kf_id'),
        nullable=False,
        doc='kf_id of one participant in the relationship')

    participant2_id = db.Column(
        KfId(),
        db.ForeignKey('participant.kf_id'),
        nullable=False,
        doc='kf_id of the other participant in the relationship')

    participant1_to_participant2_relation = db.Column(db.Text(),
                                                      nullable=False)

    participant2_to_participant1_relation = db.Column(db.Text())

    participant1 = db.relationship(
        Participant,
        primaryjoin=participant1_id == Participant.kf_id,
        backref=db.backref('outgoing_family_relationships',
                           cascade='all, delete-orphan'))

    participant2 = db.relationship(
        Participant,
        primaryjoin=participant2_id == Participant.kf_id,
        backref=db.backref('incoming_family_relationships',
                           cascade='all, delete-orphan'))

    @classmethod
    def query_all_relationships(cls, participant_kf_id=None,
                                model_filter_params=None):
        """
        Find all family relationships for a participant

        :param participant_kf_id: Kids First ID of the participant
        :param model_filter_params: Filter parameters to the query

        Given a participant's kf_id, return all of the biological
        family relationships of the participant and the relationships
        of the participant's family members.

        If the participant does not have a family defined, then return
        all of the immediate/direct family relationships of the participant.
        """
        # Apply model property filter params
        if model_filter_params is None:
            model_filter_params = {}
        q = FamilyRelationship.query.filter_by(**model_filter_params)

        # Get family relationships and join with participants
        q = q.join(Participant, or_(FamilyRelationship.participant1,
                                    FamilyRelationship.participant2))

        # Do this bc query.get() errors out if passed None
        if participant_kf_id:
            pt = Participant.query.get(participant_kf_id)

        # Return normal get all query
        else:
            return q

        # Use family to get all family relationships in participant's family
        if pt and pt.family_id:
            q = q.filter(Participant.family_id == pt.family_id)

        # No family provided, use just family relationships
        # to get only immediate family relationships for participant
        else:
            q = q.filter(or_(
                FamilyRelationship.participant1_id == participant_kf_id,
                FamilyRelationship.participant2_id == participant_kf_id))

        # Don't want duplicates - return unique family relationships
        q = q.group_by(FamilyRelationship.kf_id)

        return q

    def __repr__(self):
        return '<{} is {} of {}>'.format(
            self.participant1.external_id,
            self.participant1_to_participant2_relation,
            self.participant2.external_id)


@event.listens_for(FamilyRelationship.participant1_to_participant2_relation,
                   'set')
def set_reverse_relation(target, value, oldvalue, initiator):
    """
    Listen for set 'participant1_to_participant2_relation' events and
    set the reverse relationship, 'participant2_to_participant1_relation'
    attribute
    """
    target.participant2_to_participant1_relation = REVERSE_RELS.get(
        value.lower(),
        None)
