"""merge matching rules migrations

Revision ID: aac81b8f49b5
Revises: 2989ca5179c5, 3f8ebe8a4fd7
Create Date: 2025-12-18 15:09:55.290327

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'aac81b8f49b5'
down_revision: Union[str, Sequence[str], None] = ('2989ca5179c5', '3f8ebe8a4fd7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
