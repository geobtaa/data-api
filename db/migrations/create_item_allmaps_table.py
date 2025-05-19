"""create item_allmaps table

Revision ID: create_item_allmaps_table
Revises: 
Create Date: 2024-03-19 10:00:00.000000

"""
import asyncio
import logging
import sys
from pathlib import Path

from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from db.config import DATABASE_URL
from db.models import item_allmaps

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# revision identifiers, used by Alembic.
revision = 'create_item_allmaps_table'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'item_allmaps',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('item_id', sa.String(), nullable=False),
        sa.Column('allmaps_id', sa.String(), nullable=True),
        sa.Column('iiif_manifest_uri', sa.String(), nullable=True),
        sa.Column('annotated', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('iiif_manifest', sa.Text(), nullable=True),
        sa.Column('allmaps_annotation', sa.Text(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_item_allmaps_item_id'), 'item_allmaps', ['item_id'], unique=False)
    op.create_index(op.f('ix_item_allmaps_allmaps_id'), 'item_allmaps', ['allmaps_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_item_allmaps_allmaps_id'), table_name='item_allmaps')
    op.drop_index(op.f('ix_item_allmaps_item_id'), table_name='item_allmaps')
    op.drop_table('item_allmaps')


async def create_item_allmaps_table():
    """Create the item_allmaps table."""
    try:
        # Create engine
        engine = create_async_engine(DATABASE_URL)
        
        async with engine.begin() as conn:
            # Check if the table already exists
            def check_table(sync_conn):
                inspector = inspect(sync_conn)
                return inspector.has_table("item_allmaps")
            
            if await conn.run_sync(check_table):
                logger.info("Table item_allmaps already exists. Skipping creation.")
                return

            # Create the table
            await conn.run_sync(item_allmaps.create)
            logger.info("Successfully created item_allmaps table.")

    except Exception as e:
        logger.error(f"Error creating item_allmaps table: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(create_item_allmaps_table()) 