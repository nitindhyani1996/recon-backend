"""Add transaction type tables - ATM, Flexcube, and Switch transactions.

Revision ID: 002_add_transaction_types
Revises: 001_initial_schema
Create Date: 2025-12-10 16:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '002_add_transaction_types'
down_revision = '001_initial_schema'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create atm_transactions table
    op.create_table(
        'atm_transactions',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('datetime', sa.TIMESTAMP(), nullable=True),
        sa.Column('terminalid', sa.String(length=50), nullable=True),
        sa.Column('location', sa.String(length=255), nullable=True),
        sa.Column('atmindex', sa.String(length=50), nullable=True),
        sa.Column('pan_masked', sa.String(length=30), nullable=True),
        sa.Column('account_masked', sa.String(length=30), nullable=True),
        sa.Column('transactiontype', sa.String(length=50), nullable=True),
        sa.Column('amount', sa.DECIMAL(precision=15, scale=2), nullable=True),
        sa.Column('currency', sa.String(length=10), nullable=True),
        sa.Column('stan', sa.String(length=20), nullable=True),
        sa.Column('rrn', sa.String(length=50), nullable=False),
        sa.Column('auth', sa.String(length=20), nullable=True),
        sa.Column('responsecode', sa.String(length=10), nullable=True),
        sa.Column('responsedesc', sa.String(length=255), nullable=True),
        sa.Column('uploaded_by', sa.BigInteger(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
        sa.Column('updated_at', sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_atm_transactions_id'), 'atm_transactions', ['id'], unique=False)

    # Create flexcube_transactions table
    op.create_table(
        'flexcube_transactions',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('posted_datetime', sa.TIMESTAMP(), nullable=True),
        sa.Column('fc_txn_id', sa.String(length=100), nullable=False),
        sa.Column('rrn', sa.BigInteger(), nullable=False),
        sa.Column('stan', sa.BigInteger(), nullable=False),
        sa.Column('account_masked', sa.String(length=50), nullable=False),
        sa.Column('dr', sa.Numeric(), nullable=False),
        sa.Column('currency', sa.String(length=10), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=False),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('cr', sa.Numeric(), nullable=False),
        sa.Column('uploaded_by', sa.BigInteger(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
        sa.Column('updated_at', sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_flexcube_transactions_id'), 'flexcube_transactions', ['id'], unique=False)

    # Create switch_transactions table
    op.create_table(
        'switch_transactions',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('datetime', sa.TIMESTAMP(), nullable=True),
        sa.Column('direction', sa.String(length=50), nullable=True),
        sa.Column('mti', sa.Integer(), nullable=True),
        sa.Column('pan_masked', sa.String(length=50), nullable=True),
        sa.Column('processingcode', sa.Integer(), nullable=True),
        sa.Column('amountminor', sa.Numeric(), nullable=True),
        sa.Column('currency', sa.String(length=10), nullable=True),
        sa.Column('stan', sa.BigInteger(), nullable=True),
        sa.Column('rrn', sa.String(length=50), nullable=False),
        sa.Column('terminalid', sa.String(length=50), nullable=True),
        sa.Column('source', sa.String(length=50), nullable=True),
        sa.Column('destination', sa.String(length=50), nullable=True),
        sa.Column('responsecode', sa.String(length=50), nullable=False),
        sa.Column('authid', sa.String(length=50), nullable=False),
        sa.Column('uploaded_by', sa.BigInteger(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
        sa.Column('updated_at', sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_switch_transactions_id'), 'switch_transactions', ['id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_switch_transactions_id'), table_name='switch_transactions')
    op.drop_table('switch_transactions')
    op.drop_index(op.f('ix_flexcube_transactions_id'), table_name='flexcube_transactions')
    op.drop_table('flexcube_transactions')
    op.drop_index(op.f('ix_atm_transactions_id'), table_name='atm_transactions')
    op.drop_table('atm_transactions')
