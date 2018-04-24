"""empty message

Revision ID: 1cbf860fdd03
Revises: 5c8919540598
Create Date: 2018-04-24 07:39:07.135163

"""
from alembic import op
import sqlalchemy as sa
import dataservice

# revision identifiers, used by Alembic.
revision = '1cbf860fdd03'
down_revision = '5c8919540598'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('diagnosis', sa.Column('ncit_id', sa.Text(), nullable=True))
    op.add_column('diagnosis', sa.Column('source_text_diagnosis', sa.Text(), nullable=True))
    op.add_column('diagnosis', sa.Column('source_text_tumor_location', sa.Text(), nullable=True))
    op.drop_column('diagnosis', 'diagnosis')
    op.drop_column('diagnosis', 'tumor_location')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('diagnosis', sa.Column('tumor_location', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('diagnosis', sa.Column('diagnosis', sa.TEXT(), autoincrement=False, nullable=True))
    op.drop_column('diagnosis', 'source_text_tumor_location')
    op.drop_column('diagnosis', 'source_text_diagnosis')
    op.drop_column('diagnosis', 'ncit_id')
    # ### end Alembic commands ###
