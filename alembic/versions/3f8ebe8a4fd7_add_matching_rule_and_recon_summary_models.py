"""add matching rule and recon summary models

Revision ID: 3f8ebe8a4fd7
Revises: 002_add_transaction_types
Create Date: 2025-12-18 14:54:47.598348

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3f8ebe8a4fd7'
down_revision: Union[str, Sequence[str], None] = '002_add_transaction_types'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Create matching_rules and recon_matching_summary tables."""
    from sqlalchemy import inspect
    
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = inspector.get_table_names()
    
    # Create matching_rules table only if it doesn't exist
    if 'matching_rules' not in existing_tables:
        op.create_table(
            'matching_rules',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('basic_details', sa.JSON(), nullable=False),
            sa.Column('classification', sa.JSON(), nullable=False),
            sa.Column('rule_category', sa.Integer(), nullable=False),
            sa.Column('matchcondition', sa.JSON(), nullable=False),
            sa.Column('tolerance', sa.JSON(), nullable=False),
            sa.Column('added_by', sa.String(length=100), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
    
    # Create recon_matching_summary table only if it doesn't exist
    if 'recon_matching_summary' not in existing_tables:
        op.create_table(
            'recon_matching_summary',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('recon_reference_number', sa.String(length=100), nullable=False),
            sa.Column('matched', sa.Text(), nullable=True),
            sa.Column('un_matched', sa.Text(), nullable=True),
            sa.Column('partially_matched', sa.Text(), nullable=True),
            sa.Column('added_by', sa.BigInteger(), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True),
            sa.Column('updated_at', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('recon_reference_number')
        )


def downgrade() -> None:
    """Downgrade schema - Drop matching_rules and recon_matching_summary tables."""
    op.drop_table('recon_matching_summary')
    op.drop_table('matching_rules')
