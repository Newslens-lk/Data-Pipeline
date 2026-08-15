# Define your SQLAlchemy models here.
# Alembic will compare these against the DB to auto-generate migrations.
#
# Example:
#
# from sqlalchemy import Column, String, Text, Float, DateTime, Integer, ForeignKey
# from sqlalchemy.dialects.postgresql import JSONB, UUID
# from pgvector.sqlalchemy import Vector
# from sqlalchemy.orm import DeclarativeBase
#
# class Base(DeclarativeBase):
#     pass
#
# class Article(Base):
#     __tablename__ = "articles"
#     article_id = Column(String, primary_key=True)
#     ...

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
