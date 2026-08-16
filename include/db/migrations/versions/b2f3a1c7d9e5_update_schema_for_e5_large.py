"""update schema for e5-large embeddings and initial data load

Revision ID: b2f3a1c7d9e5
Revises: 94ea7aab5db7
Create Date: 2026-08-16 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import pgvector.sqlalchemy.vector
import sqlalchemy as sa

revision: str = 'b2f3a1c7d9e5'
down_revision: Union[str, None] = '94ea7aab5db7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the old vector index (cosine, 768-dim)
    op.drop_index('idx_articles_embedding', table_name='articles',
                  postgresql_using='ivfflat', postgresql_with={'lists': 100},
                  postgresql_ops={'embedding': 'vector_cosine_ops'})

    # Change embedding dimension from 768 to 1024
    op.alter_column('articles', 'embedding',
                    type_=pgvector.sqlalchemy.vector.VECTOR(dim=1024),
                    existing_type=pgvector.sqlalchemy.vector.VECTOR(dim=768),
                    existing_nullable=True)

    # Make scraped_at nullable (for manually prepared datasets)
    op.alter_column('articles', 'scraped_at', nullable=True)

    # Make events.summary and events.topic nullable (populated after clustering)
    op.alter_column('events', 'summary', nullable=True)
    op.alter_column('events', 'topic', nullable=True)

    # Recreate vector index with L2 distance ops
    op.create_index('idx_articles_embedding', 'articles', ['embedding'],
                    unique=False, postgresql_using='ivfflat',
                    postgresql_with={'lists': 100},
                    postgresql_ops={'embedding': 'vector_l2_ops'})


def downgrade() -> None:
    op.drop_index('idx_articles_embedding', table_name='articles',
                  postgresql_using='ivfflat', postgresql_with={'lists': 100},
                  postgresql_ops={'embedding': 'vector_l2_ops'})

    op.alter_column('articles', 'embedding',
                    type_=pgvector.sqlalchemy.vector.VECTOR(dim=768),
                    existing_type=pgvector.sqlalchemy.vector.VECTOR(dim=1024),
                    existing_nullable=True)

    op.alter_column('articles', 'scraped_at', nullable=False)
    op.alter_column('events', 'summary', nullable=False)
    op.alter_column('events', 'topic', nullable=False)

    op.create_index('idx_articles_embedding', 'articles', ['embedding'],
                    unique=False, postgresql_using='ivfflat',
                    postgresql_with={'lists': 100},
                    postgresql_ops={'embedding': 'vector_cosine_ops'})
