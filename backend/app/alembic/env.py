"""Alembic environment — binds Base.metadata and engine_meta from app settings.

Only online mode; nobody generates offline SQL scripts for this SQLite DB.
render_as_batch=True is mandatory: SQLite can't ALTER most things natively.
"""
from alembic import context

from app.models import Base, engine_meta

target_metadata = Base.metadata


def run_migrations_online() -> None:
    with engine_meta.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,  # SQLite ALTER support
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
