"""add cr column to flexcube_transactions

Revision ID: 757adc9f8c85
Revises: aac81b8f49b5
Create Date: 2025-12-18 21:21:41.598428

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '757adc9f8c85'
down_revision: Union[str, Sequence[str], None] = 'aac81b8f49b5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - only add cr column."""
    # Only add the cr column, don't alter rrn/stan types
    op.add_column('flexcube_transactions', sa.Column('cr', sa.Numeric(), nullable=True))


def downgrade() -> None:
    """Downgrade schema - remove cr column."""
    # Only remove the cr column
    op.drop_column('flexcube_transactions', 'cr')
