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
from rnaget_service.api.logging import logger
from rnaget_service.api.logging import structured_log as struct_log
from rnaget_service.api.models import Error, BasePath, Version


def _report_search_failed(typename, exception, **kwargs):
    """
    Generate standard log message + request error for error:
    Internal error performing search

    :param typename: name of type involved
    :param exception: exception thrown by ORM
    :param **kwargs: arbitrary keyword parameters
    :return: Connexion Error() type to return
    """
    report = typename + ' search failed'
    message = 'Internal error searching for '+typename+'s'
    logger().error(struct_log(action=report, exception=str(exception), **kwargs))
    return Error(message=message, code=500)


def _report_object_exists(typename, **kwargs):
    """
    Generate standard log message + request error for warning:
    Trying to POST an object that already exists

    :param typename: name of type involved
    :param **kwargs: arbitrary keyword parameters
    :return: Connexion Error() type to return
    """
    report = typename + ': Attempt to modify with a POST'
    message = 'Attempt to modify '+typename+' with a POST'
    logger().warning(struct_log(action=report, **kwargs))
    return Error(message=message, code=405)


def _report_conversion_error(typename, exception, **kwargs):
    """
    Generate standard log message + request error for warning:
    Trying to POST an object that already exists

    :param typename: name of type involved
    :param exception: exception thrown by ORM
    :param **kwargs: arbitrary keyword parameters
    :return: Connexion Error() type to return
    """
    report = 'Could not convert '+typename+' to ORM model'
    message = typename + ': failed validation - could not convert to internal representation'
    logger().error(struct_log(action=report, exception=str(exception), **kwargs))
    return Error(message=message, code=400)


def _report_write_error(typename, exception, **kwargs):
    """
    Generate standard log message + request error for error:
    Error writing to DB

    :param typename: name of type involved
    :param exception: exception thrown by ORM
    :param **kwargs: arbitrary keyword parameters
    :return: Connexion Error() type to return
    """
    report = 'Internal error writing '+typename+' to DB'
    message = typename + ': internal error saving ORM object to DB'
    logger().error(struct_log(action=report, exception=str(exception), **kwargs))
    err = Error(message=message, code=500)
    return err


def _load_expression(study_id):
    db_session = orm.get_session()
    expressionid = uuid.uuid1()

    # sample expression file:
    # copy local file if demo hdf5 not already in test dir
    # local_file = '/home/alipski/CanDIG/mock_data/rna_exp/test_seq.h5'
    expression_path = os.path.join(os.path.dirname(flask.current_app.instance_path), 'data/expression/')
    if not os.path.exists(expression_path):
        os.makedirs(expression_path)
    expression_file = os.path.join(expression_path, 'demo_quants.h5')
    # copyfile(local_file, expression_file)

    test_expression = {
        'id': expressionid,
        '__filepath__': expression_file,
        'URL': flask.request.url_root[:-1]+BasePath+'/expressions/download/'+str(expressionid),
        'studyID': study_id,
        'version': Version,
        'tags': ['test', 'pog', 'kallisto'],
        'created': datetime.datetime.utcnow()
    }

    try:
        orm_expression = orm.models.Expression(**test_expression)
    except orm.ORMException as e:
        err = _report_conversion_error('expression', e, **test_expression)
        print(err)
        return

    try:
        db_session.add(orm_expression)
        db_session.commit()
    except orm.ORMException as e:
        err = _report_write_error('expression', e, **test_expression)
        print(err)
        return


def _load_filters():
    db_session = orm.get_session()

    # SEARCH FILTERS
    tags_filter = {
        "filter": "tags",
        "description": "Comma separated tag list to filter by"
    }

    version_filter = {
        "filter": "version",
        "description": "Version to return"
    }

    for filter in [tags_filter, version_filter]:
        try:
            orm_filter = orm.models.SearchFilter(**filter)
        except orm.ORMException as e:
            err = _report_conversion_error('filter', e, **filter)
            print(err)
            return

        try:
            db_session.add(orm_filter)
            db_session.commit()
        except orm.ORMException as e:
            err = _report_write_error('filter', e, **filter)
            print(err)
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
        except orm.ORMException as e:
            err = _report_conversion_error('expression filter', e, **filter)
            print(err)
            return

        try:
            db_session.add(orm_filter)
            db_session.commit()
        except orm.ORMException as e:
            err = _report_write_error('expression filter', e, **filter)
            print(err)
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
        'tags': ['test', 'candig']
    }

    try:
        orm_project = orm.models.Project(**test_project)
    except orm.ORMException as e:
        err = _report_conversion_error('project', e, **test_project)
        print(err)
        return

    try:
        db_session.add(orm_project)
        db_session.commit()
    except orm.ORMException as e:
        err = _report_write_error('project', e, **test_project)
        print(err)
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
    except orm.ORMException as e:
        err = _report_conversion_error('study', e, **test_study)
        print(err)
        return

    try:
        db_session.add(orm_study)
        db_session.commit()
    except orm.ORMException as e:
        err = _report_write_error('study', e, **test_study)
        print(err)
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
        except orm.ORMException as e:
            err = _report_conversion_error('project', e, **test_project)
            print(err)
            return

        try:
            db_session.add(orm_project)
            db_session.commit()
        except orm.ORMException as e:
            err = _report_write_error('project', e, **test_project)
            print(err)
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
    except orm.ORMException as e:
        err = _report_conversion_error('study', e, **test_study)
        print(err)
        return

    try:
        db_session.add(orm_study)
        db_session.commit()
    except orm.ORMException as e:
        err = _report_write_error('study', e, **test_study)
        print(err)
        return

    return


def setup_testdb():
    _load_project_study()
    _load_filters()
    _load_projects()
