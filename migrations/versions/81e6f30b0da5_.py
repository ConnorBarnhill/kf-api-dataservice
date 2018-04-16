"""
Add sequencing_center entity to model
Remove center from sequencing_experiment and shipment_destination from biospecimen

Revision ID: 81e6f30b0da5
Revises: e3ee14b87b2e
Create Date: 2018-04-16 11:00:09.846946

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '81e6f30b0da5'
down_revision = 'e3ee14b87b2e'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('sequencing_center',
    sa.Column('uuid', postgresql.UUID(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('modified_at', sa.DateTime(), nullable=True),
    sa.Column('name', sa.Text(), nullable=False),
    sa.Column('sequencing_experiment_id', dataservice.api.common.model.KfId(length=11), nullable=False),
    sa.Column('kf_id', dataservice.api.common.model.KfId(length=11), nullable=False),
    sa.ForeignKeyConstraint(['sequencing_experiment_id'], ['sequencing_experiment.kf_id'], ),
    sa.PrimaryKeyConstraint('kf_id'),
    sa.UniqueConstraint('uuid')
    )
    op.add_column('biospecimen', sa.Column('sequencing_center_id', dataservice.api.common.model.KfId(length=11), nullable=False))
    op.create_foreign_key(None, 'biospecimen', 'sequencing_center', ['sequencing_center_id'], ['kf_id'])
    op.drop_column('biospecimen', 'shipment_destination')
    op.drop_column('sequencing_experiment', 'center')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('sequencing_experiment', sa.Column('center', sa.TEXT(), autoincrement=False, nullable=False))
    op.add_column('biospecimen', sa.Column('shipment_destination', sa.TEXT(), autoincrement=False, nullable=True))
    op.drop_constraint(None, 'biospecimen', type_='foreignkey')
    op.drop_column('biospecimen', 'sequencing_center_id')
    op.drop_table('sequencing_center')
    # ### end Alembic commands ###
