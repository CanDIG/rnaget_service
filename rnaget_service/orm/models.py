"""
SQLAlchemy models for the database
"""
from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy import UniqueConstraint, ForeignKey
from sqlalchemy import TypeDecorator
from rnaget_service.orm.guid import GUID
from rnaget_service.orm import Base
import json


class JsonArray(TypeDecorator):
    """
    Custom array type to emulate arrays in sqlite3
    """

    impl = String

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        return json.loads(value)

    def copy(self):
        return JsonArray(self.impl.length)


class Project(Base):
    """
    SQLAlchemy class/table representing projects
    """
    __tablename__ = 'projects'
    id = Column(GUID(), primary_key=True)
    version = Column(String(10), default="")
    tags = Column(JsonArray(), default=[])
    name = Column(String(100), unique=True, nullable=False)
    description = Column(String(100), default="")
    created = Column(DateTime())
    __table_args__ = ()


class Study(Base):
    """
    SQLAlchemy class/table representing studies
    """
    __tablename__ = 'studies'
    id = Column(GUID(), primary_key=True)
    version = Column(String(10), default="")
    tags = Column(JsonArray(), default=[])
    name = Column(String(100), nullable=False)
    description = Column(String(100), default="")
    parentProjectID = Column(GUID(), ForeignKey('projects.id'), nullable=False)
    patientList = Column(JsonArray(), default=[])
    sampleList = Column(JsonArray(), default=[])
    created = Column(DateTime())
    __table_args__ = (
        UniqueConstraint("parentProjectID", "name"),
    )


class Expression(Base):
    """
    SQLAlchemy class/table representing the RNA expression matrix
    """
    __tablename__ = 'expressions'
    __filepath__ = Column(String(100))
    id = Column(GUID(), primary_key=True)
    URL = Column(String(100))
    studyID = Column(GUID(), ForeignKey('studies.id'))
    created = Column(DateTime())
    version = Column(String(10), default="")
    tags = Column(JsonArray())
    __table_args__ = ()


class SearchFilter(Base):
    """
    SQLAlchemy class/table for search filters
    """
    __tablename__ = 'searchfilters'
    version = Column(String(10), default="")
    filter = Column(String(100), primary_key=True)
    description = Column(String(100), default="")
    __table_args__ = ()


class ExpressionSearchFilter(Base):
    """
    SQLAlchemy class/table for expression search filters
    """
    __tablename__ = 'expressionsearchfilters'
    filterType = Column(String(100), primary_key=True)
    filters = Column(JsonArray())
    created = Column(DateTime())
    __table_args__ = ()


class ChangeLog(Base):
    """
    SQLAlchemy class/table for listing changes to the database with version update
    """
    __tablename__ = 'changelogs'
    version =  Column(String(10), primary_key=True)
    log = Column(JsonArray())
    created = Column(DateTime())
    __table_args__ = ()


class File(Base):
    """
    SQLAlchemy class/table for representing files
    TODO: similar schema to Expression data. Merge somehow?
    """
    __tablename__ = 'files'
    id = Column(GUID(), primary_key=True)
    version = Column(String(10))
    tags = Column(JsonArray())
    fileType = Column(String(10))
    studyID = Column(GUID(), ForeignKey('studies.id'))
    URL = Column(String(100))
    created = Column(DateTime())
    __table_args__ = ()

