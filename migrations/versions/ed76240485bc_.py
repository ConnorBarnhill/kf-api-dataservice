"""
Rename file_type to data_tpe & Add controlled_access
field to genomic_file

Revision ID: ed76240485bc
Revises: b7852f8aab0a
Create Date: 2018-02-12 17:06:36.156545

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ed76240485bc'
down_revision = 'b7852f8aab0a'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_file', sa.Column('controlled_access', sa.Boolean(), nullable=True))
    op.add_column('genomic_file', sa.Column('data_type', sa.Text(), nullable=True))
    op.drop_column('genomic_file', 'file_type')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('genomic_file', sa.Column('file_type', sa.TEXT(), autoincrement=False, nullable=True))
    op.drop_column('genomic_file', 'data_type')
    op.drop_column('genomic_file', 'controlled_access')
    # ### end Alembic commands ###
