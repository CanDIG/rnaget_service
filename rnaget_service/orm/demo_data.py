# pylint: disable=invalid-name
# pylint: disable=C0301
"""
Create some mock records for functional tests and DEMO purposes (dev only)
"""
import datetime
import uuid
from shutil import copyfile
import flask
import os

from rnaget_service import orm
from rnaget_service.orm import models
from rnaget_service.api.models import BasePath, Version


def _load_expression(study_id):
    db_session = orm.get_session()
    expressionid = uuid.uuid1()

    # sample expression file:
    # copy local file if demo hdf5 not already in test dir
    expression_path = os.path.join(os.path.dirname(flask.current_app.instance_path), 'data/expression/')
    if not os.path.exists(expression_path):
        os.makedirs(expression_path)
    expression_file = os.path.join(expression_path, 'demo_quants.h5')
    # copyfile(local_file, expression_file)

    with flask.current_app.test_request_context():
        test_expression = {
            'id': expressionid,
            '__filepath__': expression_file,
            'URL': flask.request.url_root[:-1]+BasePath+'/expressions/download/'+str(expressionid),
            'studyID': study_id,
            'version': Version,
            'tags': ['test', 'pog', 'kallisto', 'expressions'],
            "fileType": ".h5",
            'created': datetime.datetime.utcnow()
        }

    try:
        orm_expression = orm.models.File(**test_expression)
    except orm.ORMException as e:
        print(e)
        return

    try:
        db_session.add(orm_expression)
        db_session.commit()
    except orm.ORMException:
        return


def _load_filters():
    db_session = orm.get_session()

    # SEARCH FILTERS
    tags_filter = {
        "filter": "tags",
        "description": "Comma separated tag list to filter by",
        "filter_for": ["projects", "studies"]
    }

    version_filter = {
        "filter": "version",
        "description": "Version to return",
        "filter_for": ["projects", "studies"]
    }

    project_filter = {
        "filter": "projectID",
        "description": "Parent project ID of study",
        "filter_for": ["studies"]
    }

    for filter in [tags_filter, version_filter, project_filter]:
        try:
            orm_filter = orm.models.SearchFilter(**filter)
        except orm.ORMException:
            return

        try:
            db_session.add(orm_filter)
            db_session.commit()
        except orm.ORMException:
            return

    # EXPRESSION SEARCH FILTERS
    sample_filters = [{"filter": "sampleID",
                       "description": "sampleID to match"},
                      {"filter": "projectID",
                       "description": "project to filter on"},
                      {"filter": "studyID",
                       "description": "study to filter on"}]

    sample_filter = {
        "filterType": "sample",
        "filters": sample_filters
    }

    feature_filters = [{"filter": "maxExpression",
                        "description": "return only samples with expression values less than listed threshold for each corresponding feature in the array"},
                       {"filter": "minExpression",
                        "description": "return only samples with expression values greater than listed threshold for each corresponding feature in the array"},
                       {"filter": "featureIDList",
                        "description": "return only values for listed feature ID values"}]

    feature_filter = {
        "filterType": "feature",
        "filters": feature_filters
    }

    for filter in [sample_filter, feature_filter]:
        try:
            orm_filter = orm.models.ExpressionSearchFilter(**filter)
        except orm.ORMException:
            return

        try:
            db_session.add(orm_filter)
            db_session.commit()
        except orm.ORMException:
            return


def _load_project_study():
    db_session = orm.get_session()

    # sample project: profyle
    main_project_id = uuid.uuid1()
    main_study_id = uuid.uuid1()

    test_project = {
        'id': main_project_id,
        'name': 'profyle',
        'created': datetime.datetime.utcnow(),
        'description': 'mock profyle project for testing',
        'tags': ['test', 'candig'],
        'version': Version
    }

    try:
        orm_project = orm.models.Project(**test_project)
    except orm.ORMException:
        return

    try:
        db_session.add(orm_project)
        db_session.commit()
    except orm.ORMException:
        return

    # sample study: pog
    test_study = {
        'id': main_study_id,
        'name': 'pog',
        'parentProjectID': main_project_id,
        'created': datetime.datetime.utcnow(),
        'description': 'mock study for testing',
        'tags': ['test', 'candig', 'pog'],
        'patientList': [str(x) for x in range(30)],
        'sampleList': ['PATIENT_'+str(x) for x in range(30)]
    }

    try:
        orm_study = orm.models.Study(**test_study)
    except orm.ORMException:
        return

    try:
        db_session.add(orm_study)
        db_session.commit()
    except orm.ORMException:
        return

    _load_expression(main_study_id)


def _load_projects():
    db_session = orm.get_session()
    plist = ['tf4cn', 'marathon', 'ga4gh']

    for pname in plist:
        project_id = uuid.uuid1()

        test_project = {
            'id': project_id,
            'name': pname,
            'created': datetime.datetime.utcnow(),
            'description': 'sample project for testing',
            'tags': [pname],
            'version': Version
        }

        try:
            orm_project = orm.models.Project(**test_project)
        except orm.ORMException:
            return

        try:
            db_session.add(orm_project)
            db_session.commit()
        except orm.ORMException:
            return

        _load_study(project_id)

    return


def _load_study(parent_project_id):
    db_session = orm.get_session()

    study_id = uuid.uuid1()

    test_study = {
        'id': study_id,
        'name': 'pilot',
        'parentProjectID': parent_project_id,
        'created': datetime.datetime.utcnow(),
        'description': 'mock study for testing',
        'tags': [],
        'patientList': []
    }

    try:
        orm_study = orm.models.Study(**test_study)
    except orm.ORMException:
        return

    try:
        db_session.add(orm_study)
        db_session.commit()
    except orm.ORMException:
        return

    return


def get_db(app):
    with app.app.app_context():
        demo_db = os.path.join(os.path.dirname(flask.current_app.instance_path), 'data/rnaget_demo.sqlite')
    return demo_db


def setup_db(app):
    with app.app.app_context():
        _load_project_study()
        _load_filters()
        _load_projects()
