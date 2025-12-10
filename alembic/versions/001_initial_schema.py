"""Initial schema migration - create transactions and uploaded_files tables.

Revision ID: 001_initial_schema
Revises: 
Create Date: 2025-12-10 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001_initial_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create transactions table
    op.create_table(
        'transactions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('amount', sa.Float(), nullable=False),
        sa.Column('transaction_datetime', sa.DateTime(), nullable=False),
        sa.Column('transaction_type', sa.String(length=50), nullable=False),
        sa.Column('atm_terminal', sa.String(length=100), nullable=True),
        sa.Column('card', sa.String(length=50), nullable=True),
        sa.Column('status_response', sa.String(length=50), nullable=True),
        sa.Column('transaction_id', sa.String(length=100), nullable=False),
        sa.Column('channel_type', sa.String(length=50), nullable=True),
        sa.Column('response_code', sa.String(length=50), nullable=True),
        sa.Column('description', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_transactions_id'), 'transactions', ['id'], unique=False)
    op.create_unique_constraint('uq_transactions_transaction_id', 'transactions', ['transaction_id'])

    # Create uploaded_files table
    op.create_table(
        'uploaded_files',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('file_description', sa.Text(), nullable=True),
        sa.Column('uploaded_by', sa.BigInteger(), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
        sa.Column('updated_at', sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_uploaded_files_id'), 'uploaded_files', ['id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_uploaded_files_id'), table_name='uploaded_files')
    op.drop_table('uploaded_files')
    op.drop_constraint('uq_transactions_transaction_id', 'transactions', type_='unique')
    op.drop_index(op.f('ix_transactions_id'), table_name='transactions')
    op.drop_table('transactions')
