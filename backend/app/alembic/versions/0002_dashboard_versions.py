"""dashboard_versions

Revision ID: 0002_dashboard_versions
Revises: 0001_baseline
Create Date: 2026-07-09 08:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0002_dashboard_versions'
down_revision: Union[str, Sequence[str], None] = '0001_baseline'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('dashboard_versions',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('dashboard_id', sa.String(), nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('definition_json', sa.Text(), nullable=False),
    sa.Column('created_by', sa.String(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('dashboard_versions', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_dashboard_versions_dashboard_id'), ['dashboard_id'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('dashboard_versions', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_dashboard_versions_dashboard_id'))

    op.drop_table('dashboard_versions')
