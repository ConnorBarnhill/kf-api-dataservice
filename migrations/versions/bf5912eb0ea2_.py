"""
Merge sample and aliquot as biospecimen
Add uberon_id to biospecimen and update concentration to float
Remove link between biospecimen and sequencing_experiment
Directly Link genomic_file to biospecimen

Revision ID: bf5912eb0ea2
Revises: 6c5b80679ee3
Create Date: 2018-04-10 11:52:17.595497

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'bf5912eb0ea2'
down_revision = '6c5b80679ee3'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('biospecimen',
    sa.Column('uuid', postgresql.UUID(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('modified_at', sa.DateTime(), nullable=True),
    sa.Column('external_sample_id', sa.Text(), nullable=True),
    sa.Column('external_aliquot_id', sa.Text(), nullable=True),
    sa.Column('tissue_type', sa.Text(), nullable=True),
    sa.Column('composition', sa.Text(), nullable=True),
    sa.Column('anatomical_site', sa.Text(), nullable=True),
    sa.Column('age_at_event_days', sa.Integer(), nullable=True),
    sa.Column('tumor_descriptor', sa.Text(), nullable=True),
    sa.Column('shipment_origin', sa.Text(), nullable=True),
    sa.Column('shipment_destination', sa.Text(), nullable=True),
    sa.Column('analyte_type', sa.Text(), nullable=False),
    sa.Column('concentration', sa.Float(), nullable=True),
    sa.Column('volume', sa.Float(), nullable=True),
    sa.Column('shipment_date', sa.DateTime(), nullable=True),
    sa.Column('uberon_id', sa.Text(), nullable=True),
    sa.Column('participant_id', dataservice.api.common.model.KfId(length=11), nullable=False),
    sa.Column('kf_id', dataservice.api.common.model.KfId(length=11), nullable=False),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.kf_id'], ),
    sa.PrimaryKeyConstraint('kf_id'),
    sa.UniqueConstraint('uuid')
    )
    op.drop_table('sample')
    op.drop_table('aliquot')
    op.add_column('genomic_file', sa.Column('biospecimen_id', dataservice.api.common.model.KfId(length=11), nullable=False))
    op.drop_constraint('genomic_file_sequencing_experiment_id_fkey', 'genomic_file', type_='foreignkey')
    op.create_foreign_key(None, 'genomic_file', 'biospecimen', ['biospecimen_id'], ['kf_id'])
    op.drop_column('genomic_file', 'sequencing_experiment_id')
    op.alter_column('participant', 'family_id',
               existing_type=sa.TEXT(),
               type_=dataservice.api.common.model.KfId(length=11),
               existing_nullable=True)
    op.add_column('sequencing_experiment', sa.Column('genomic_file_id', dataservice.api.common.model.KfId(length=11), nullable=False))
    op.drop_constraint('sequencing_experiment_aliquot_id_fkey', 'sequencing_experiment', type_='foreignkey')
    op.create_foreign_key(None, 'sequencing_experiment', 'genomic_file', ['genomic_file_id'], ['kf_id'])
    op.drop_column('sequencing_experiment', 'aliquot_id')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('sequencing_experiment', sa.Column('aliquot_id', sa.VARCHAR(length=11), autoincrement=False, nullable=False))
    op.drop_constraint(None, 'sequencing_experiment', type_='foreignkey')
    op.create_foreign_key('sequencing_experiment_aliquot_id_fkey', 'sequencing_experiment', 'aliquot', ['aliquot_id'], ['kf_id'])
    op.drop_column('sequencing_experiment', 'genomic_file_id')
    op.alter_column('participant', 'family_id',
               existing_type=dataservice.api.common.model.KfId(length=11),
               type_=sa.TEXT(),
               existing_nullable=True)
    op.add_column('genomic_file', sa.Column('sequencing_experiment_id', sa.VARCHAR(length=11), autoincrement=False, nullable=False))
    op.drop_constraint(None, 'genomic_file', type_='foreignkey')
    op.create_foreign_key('genomic_file_sequencing_experiment_id_fkey', 'genomic_file', 'sequencing_experiment', ['sequencing_experiment_id'], ['kf_id'])
    op.drop_column('genomic_file', 'biospecimen_id')
    op.create_table('aliquot',
    sa.Column('uuid', postgresql.UUID(), autoincrement=False, nullable=True),
    sa.Column('created_at', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('modified_at', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('external_id', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('shipment_origin', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('shipment_destination', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('analyte_type', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('concentration', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('volume', postgresql.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=True),
    sa.Column('shipment_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('sample_id', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
    sa.Column('kf_id', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['sample_id'], ['sample.kf_id'], name='aliquot_sample_id_fkey'),
    sa.PrimaryKeyConstraint('kf_id', name='aliquot_pkey'),
    sa.UniqueConstraint('uuid', name='aliquot_uuid_key')
    )
    op.create_table('sample',
    sa.Column('uuid', postgresql.UUID(), autoincrement=False, nullable=True),
    sa.Column('created_at', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('modified_at', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('external_id', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('tissue_type', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('composition', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('anatomical_site', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('age_at_event_days', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('tumor_descriptor', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('participant_id', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
    sa.Column('kf_id', sa.VARCHAR(length=11), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['participant_id'], ['participant.kf_id'], name='sample_participant_id_fkey'),
    sa.PrimaryKeyConstraint('kf_id', name='sample_pkey'),
    sa.UniqueConstraint('uuid', name='sample_uuid_key')
    )
    op.drop_table('biospecimen')
    # ### end Alembic commands ###
