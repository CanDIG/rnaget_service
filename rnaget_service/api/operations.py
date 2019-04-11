# pylint: disable=invalid-name
# pylint: disable=C0301
"""
Implement endpoints of model service
"""
import datetime
import uuid
import flask
import os
import json

from sqlalchemy import or_
from sqlalchemy import exc
from rnaget_service import orm
from rnaget_service.orm import models
from rnaget_service.api.logging import apilog, logger
from rnaget_service.api.logging import structured_log as struct_log
from rnaget_service.api.models import Error, BasePath, Version
from rnaget_service.api.exceptions import ThresholdValueError
from rnaget_service.orm.test_data import setup_testdb
from rnaget_service.expression.rnaget_query import ExpressionQueryTool


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


@apilog
def db_test_setup():
    # TODO: DELETE -> DEV ONLY: use for updating the DB for some DEMO
    setup_testdb()
    return db_lookup_projects()


def db_lookup_projects():
    db_session = orm.get_session()
    project = orm.models.Project

    try:
        projects = db_session.query(project)
    except orm.ORMException as e:
        err = _report_search_failed('project', e)
        print(err)
        return err, 500

    return [orm.dump(x) for x in projects], 200


def get_search_filters():
    """
    :return: general filters used for project and study searches
    """

    db_session = orm.get_session()
    search_filter = orm.models.SearchFilter

    try:
        filters = db_session.query(search_filter)
    except orm.ORMException as e:
        err = _report_search_failed('search filter', e)
        return err, 500

    return [orm.dump(x) for x in filters], 200


@apilog
def get_project_by_id(projectId):
    """

    :param projectId:
    :return: all projects or if projectId specified, corresponding project
    """
    db_session = orm.get_session()
    project = orm.models.Project

    try:
        specified_project = db_session.query(project)\
            .get(projectId)
    except exc.StatementError:
        err = Error(message="Query id must be a valid UUID: " + str(projectId), code=404)
        return err, 404
    except orm.ORMException as e:
        err = _report_search_failed('project', e, project_id=str(projectId))
        return err, 500

    if not specified_project:
        err = Error(message="Project not found: "+str(projectId), code=404)
        return err, 404

    return orm.dump(specified_project), 200

@apilog
def post_project(project_record):
    db_session = orm.get_session()

    iid = uuid.uuid1()
    project_record['id'] = iid
    project_record['created'] = datetime.datetime.utcnow()

    try:
        orm_project = orm.models.Project(**project_record)
    except orm.ORMException as e:
        err = _report_conversion_error('project', e, **project_record)
        return err, 400

    try:
        db_session.add(orm_project)
        db_session.commit()
    except exc.IntegrityError:
        err = _report_object_exists('project: '+project_record['name'], **project_record)
        return err, 405
    except orm.ORMException as e:
        err = _report_write_error('project', e, **project_record)
        return err, 500

    logger().info(struct_log(action='post_project', status='created',
                             project_id=str(iid), **project_record))

    return project_record, 201, {'Location': BasePath + '/projects/' + str(iid)}


@apilog
def search_projects(tags=None, version=None):
    """

    :param tags:
    :param version:
    :return:
    """
    db_session = orm.get_session()
    project = orm.models.Project

    try:
        projects = db_session.query(project)
        if version:
            projects = projects.filter(project.version.like('%' + version + '%'))
        if tags:
            # return any project that matches at least one tag
            projects = projects.filter(or_(*[project.tags.contains(tag) for tag in tags]))
    except orm.ORMException as e:
        err = _report_search_failed('project', e)
        return err, 500

    return [orm.dump(x) for x in projects], 200

@apilog
def search_project_filters():
    """
    :return: filters for project searches
    """
    return get_search_filters()


@apilog
def get_study_by_id(studyId):
    """

    :param studyId: required identifier
    :return: a single specified study
    """
    db_session = orm.get_session()
    study = orm.models.Study

    try:
        specified_study = db_session.query(study)\
            .get(studyId)
    except exc.StatementError:
        err = Error(message="Query id must be a valid UUID: " + str(studyId), code=404)
        return err, 404
    except orm.ORMException as e:
        err = _report_search_failed('study', e, study_id=studyId)
        return err, 500

    if not specified_study:
        err = Error(message="Study not found: " + studyId, code=404)
        return err, 404

    return orm.dump(specified_study), 200


@apilog
def post_study(study_record):
    db_session = orm.get_session()

    iid = uuid.uuid1()
    study_record['id'] = iid
    study_record['created'] = datetime.datetime.utcnow()

    try:
        orm_study = orm.models.Study(**study_record)
    except orm.ORMException as e:
        err = _report_conversion_error('study', e, **study_record)
        return err, 400

    try:
        db_session.add(orm_study)
        db_session.commit()
    except exc.IntegrityError as e:
        err = _report_object_exists('study: ' + study_record['name'], **study_record)
        return err, 405
    except orm.ORMException as e:
        err = _report_write_error('study', e, **study_record)
        return err, 500

    logger().info(struct_log(action='post_study', status='created',
                             project_id=str(iid), **study_record))

    return study_record, 201, {'Location': BasePath + '/studies/' + str(iid)}

@apilog
def search_studies(tags=None, version=None):
    """

    :param tags: optional list of tags
    :param version:
    :return: studies that match the filters
    """
    db_session = orm.get_session()
    study = orm.models.Study

    try:
        studies = db_session.query(study)
        if version:
            studies = studies.filter(study.version.like('%' + version + '%'))
        if tags:
            # return any study that matches at least one tag
            studies = studies.filter(or_(*[study.tags.contains(tag) for tag in tags]))
    except orm.ORMException as e:
        err = _report_search_failed('project', e)
        return err, 500

    return [orm.dump(x) for x in studies], 200


@apilog
def search_study_filters():
    """
    :return: filters for study searches
    """
    return get_search_filters()


@apilog
def get_expression_by_id(expressionId):
    """

    :param expressionId: required identifier
    :return: a single specified expression matrix
    """
    db_session = orm.get_session()
    expression = orm.models.Expression

    try:
        expr_matrix = db_session.query(expression).get(expressionId)
    except exc.StatementError:
        err = Error(message="Query id must be a valid UUID: " + str(expressionId), code=404)
        return err, 404
    except orm.ORMException as e:
        err = _report_search_failed('expression', e, expression_id=expressionId)
        return err, 500

    if not expr_matrix:
        err = Error(message="Expression matrix not found: " + expressionId, code=404)
        return err, 404

    return orm.dump(expr_matrix), 200


@apilog
def post_expression(expression_record):
    db_session = orm.get_session()

    iid = uuid.uuid1()
    expression_record['id'] = iid
    expression_record['created'] = datetime.datetime.utcnow()

    try:
        orm_expression = orm.models.Expression(**expression_record)
    except orm.ORMException as e:
        err = _report_conversion_error('project', e, **expression_record)
        return err, 400

    try:
        db_session.add(orm_expression)
        db_session.commit()
    except exc.IntegrityError:
        err = _report_object_exists('expression: ' + expression_record['id'], **expression_record)
        return err, 405
    except orm.ORMException as e:
        err = _report_write_error('expression', e, **expression_record)
        return err, 500

    logger().info(struct_log(action='post_expression', status='created',
                             project_id=str(iid), **expression_record))

    return expression_record, 201, {'Location': BasePath + '/expressions/' + str(iid)}

@apilog
def get_search_expressions(tags=None, sampleID=None, projectID=None, studyID=None,
                      version=None, featureIDList=None, featureNameList=None,
                      featureAccessionList=None, minExpression=None, maxExpression=None):
    """

    :param tags: optional Comma separated tag list
    :param sampleID: optional sample identifier
    :param projectID: optional project identifier
    :param studyID: optional study identifier
    :param version: optional version
    :param featureIDList: optional filter by listed feature IDs
    :param featureNameList: optional filter by listed features
    :param featureAccessionList: optional filter bys by listed accession numbers
    :return: expression matrices matching filters
    """
    db_session = orm.get_session()
    expression = orm.models.Expression

    # TODO: default output for slicing HDF5's will be JSON for now, additional output options in progress
    try:
        # need to collect hdf5 files/expression db entries to pull from
        expressions = db_session.query(expression)

        # db queries
        if version:
            expressions = expressions.filter(expression.version.like('%' + version + '%'))
        if tags:
            # return any project that matches at least one tag
            expressions = expressions.filter(or_(*[expression.tags.contains(tag) for tag in tags]))
        if studyID:
            expressions = expressions.filter(expression.studyID == studyID)
        if projectID:
            # TODO: query performance?
            study_list = get_study_by_project(projectID)
            expressions = expressions.filter(expression.studyID.in_(study_list))

        if not any([sampleID, featureIDList, maxExpression, minExpression]):
            # return expression file list as is
            pass

        else:
            # HDF5 queries
            # TODO: feature name and feature accession lookups; how to store pointer to hdf5 file?
            results = {}
            try:
                for expr in expressions:
                    try:
                        h5query = ExpressionQueryTool(expr.__filepath__)
                    except OSError as err:
                        # file not found... do something?
                        print(err)
                        continue

                    if sampleID or featureIDList:
                        results[str(expr.studyID)] = h5query.search(sample_id=sampleID, feature_list=featureIDList)

                    elif minExpression:
                        threshold_array = convert_threshold_array(minExpression)
                        results[str(expr.studyID)] = h5query.search_threshold(threshold_array, ft_type='min')

                    elif maxExpression:
                        threshold_array = convert_threshold_array(maxExpression)
                        results[str(expr.studyID)] = h5query.search_threshold(threshold_array, ft_type='max')

                    h5query.close()

            except ThresholdValueError as e:
                err = Error(
                    message=str(e),
                    code=400)
                return err, 400

            # file output as JSON for now
            # TODO: if using multiple matrices,
            response = generate_json_url(results)

            return [response], 200

    except orm.ORMException as e:
        err = _report_search_failed('expression', e)
        return err, 500

    return [orm.dump(expr_matrix) for expr_matrix in expressions], 200

@apilog
def search_expression_filters(type=None):
    """

    :param type: optional one of `feature` or `sample`. If blank, both will be returned
    :return: filters for expression searches
    """
    db_session = orm.get_session()
    expression_filter = orm.models.ExpressionSearchFilter
    valid_types = ['feature', 'sample']

    try:
        expression_filters = db_session.query(expression_filter)
        if type in valid_types:
            expression_filters = expression_filters.\
                filter(expression_filter.filterType == type)
        elif not type:
            pass
        else:
            err = Error(message="Invalid type: " + type, code=400)
            return err, 400

    except orm.ORMException as e:
        err = _report_search_failed('expression search filter', e)
        return err, 500

    return [orm.dump(x) for x in expression_filters], 200


@apilog
def get_versions():
    """

    :return: release versions of the database
    """
    db_session = orm.get_session()
    change_log = orm.models.ChangeLog

    try:
        versions = db_session.query(change_log.version)
    except orm.ORMException as e:
        err = _report_search_failed('versions', e)
        return err, 500

    return [entry.version for entry in versions], 200

@apilog
def post_change_log(change_log_record):
    db_session = orm.get_session()
    change_version = change_log_record.get('version')

    change_log_record['created'] = datetime.datetime.utcnow()

    try:
        orm_changelog = orm.models.ChangeLog(**change_log_record)
    except orm.ORMException as e:
        err = _report_conversion_error('changelog', e, **change_log_record)
        return err, 400

    try:
        db_session.add(orm_changelog)
        db_session.commit()
    except exc.IntegrityError:
        err = _report_object_exists('changelog: ' + change_log_record['name'], **change_log_record)
        return err, 405
    except orm.ORMException as e:
        err = _report_write_error('changelog', e, **change_log_record)
        return err, 500

    logger().info(struct_log(action='post_change_log', status='created',
                             change_version=change_version, **change_log_record))

    return change_log_record, 201, {'Location': BasePath + '/changelog/' + change_version}


@apilog
def get_change_log(version):
    """

    :param version: required release version
    :return: changes associated with specified release version
    """
    db_session = orm.get_session()
    change_log = orm.models.ChangeLog

    try:
        log = db_session.query(change_log)\
            .get(version)
    except orm.ORMException as e:
        err = _report_search_failed('change log', e)
        return err, 500

    if not log:
        err = Error(message="Change log not found", code=404)
        return err, 404

    return orm.dump(log), 200


@apilog
def get_file(fileID):
    """

    :param fileID: required identifier
    :return: a single specified file download URL
    """
    db_session = orm.get_session()
    file = orm.models.File

    try:
        get_file = db_session.query(file).get(fileID)
    except orm.ORMException as e:
        err = _report_search_failed('file', e, file_id=fileID)
        return err, 500

    if not get_file:
        err = Error(message="File not found: " + fileID, code=404)
        return err, 404

    return orm.dump(get_file), 200


@apilog
def search_files(tags=None, projectID=None, studyID=None, fileType=None):
    """

    :param tags: optional comma separated tag list
    :param projectID: optional project identifier
    :param studyID: optional study identifier
    :param fileType: optional file type
    :return: manifest of download URL(s) for matching files
    """
    db_session = orm.get_session()
    file = orm.models.File
    valid_file_types = ['bam','csv','vcf']

    try:
        files = db_session.query(file)
        if tags:
            files = files.filter(or_(*[file.tags.contains(tag) for tag in tags]))
        if projectID:
            study_list = get_study_by_project(projectID)
            files = files.filter(file.studyID.in_(study_list))
        if studyID:
            files = files.filter(file.studyID == studyID)
        if fileType in valid_file_types:
            files = files.filter(file.fileType == fileType)
        else:
            err = Error(message="Invalid file type: " + fileType, code=400)
            return err, 400
    except orm.ORMException as e:
        err = _report_search_failed('file', e)
        return err, 500

    return [orm.dump(x) for x in files], 200


def get_study_by_project(projectID):
    """

    :param projectID:
    :return: list of study id's associated with a given project
    """
    db_session = orm.get_session()
    study = orm.models.Study
    study_id_list = []
    studies = db_session.query(study.id)\
        .filter(study.parentProjectID == projectID)
    if studies:
        study_id_list = [x.id for x in studies]
    return study_id_list

# TODO: setup DRS/DOS for temp files and hdf5 storage?

@apilog
def tmp_json_download(token):
    """

    :param token: for now using the file identifier
    :return: json file with type application/json
    """
    return download_file('data/tmp/json', token, ".json")


@apilog
def tmp_h5_download(token):
    """

    :param token: for now using the file identifier
    :return: h5 file with type application/octet-stream
    """
    return download_file('data/tmp/h5', token, ".h5")


@apilog
def expression_download(token):
    """

    :param token: for now using the file identifier
    :return: h5 file with type application/octet-stream
    """
    return download_file('data/expression/', token, ".h5")


def generate_json_url(results):
    file_id = str(uuid.uuid1())

    tmp_dir = os.path.join(os.path.dirname(flask.current_app.instance_path), 'data/tmp/json/')
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    tmp_file_path = os.path.join(tmp_dir, file_id + '.json')
    with open(tmp_file_path, 'w') as outfile:
        json.dump(results, outfile)

    formatted_results = {
        'URL': flask.request.url_root[:-1] + BasePath + '/download/json/' + file_id,
        'version': Version,
        'created': datetime.datetime.utcnow()
    }

    return formatted_results


def download_file(data_path, token, file_ext):
    """
    Generic file exporter
    """

    try:
        if data_path == "data/expression/":
            access_file = get_expression_file(token)
            if not access_file:
                err = Error(message="file not found", code=404)
                return err, 404
            response = flask.send_file(access_file, as_attachment=True)
        # use tmp directory file ID's
        else:
            access_file = token+file_ext
            tmp_dir = os.path.join(
                os.path.dirname(flask.current_app.instance_path), data_path
            )

            if os.path.isfile(os.path.join(tmp_dir, access_file)):
                response = flask.send_from_directory(tmp_dir, access_file, as_attachment=True)
            else:
                err = Error(message="file not found", code=404)
                return err, 404

        response.direct_passthrough = False
        return response

    except Exception as e:
        print(type(e))
        err = _report_search_failed('file', e)
        return err, 404


def convert_threshold_array(threshold_input):
    """
    query parameter threshold array formatted: Feature,Value,Feature,Value
    :param threshold_input: query parameter threshold array
    :return: list of feature/threshold tuples or raise error
    """
    threshold_output = []
    try:
        for i in range(int(len(threshold_input)/2)):
            threshold_output.append((threshold_input[2*i], float(threshold_input[2*i+1])))
    except ValueError as err:
        raise ThresholdValueError from err
    else:
        return threshold_output


def get_expression_file(expressionId):
    """

    :param expressionId: required identifier
    :return: internal expression matrix filepath
    """
    db_session = orm.get_session()
    expression = orm.models.Expression

    try:
        expr_matrix = db_session.query(expression).get(expressionId)
    except exc.StatementError:
        err = Error(message="Query id must be a valid UUID: " + str(expressionId), code=404)
        return err, 404
    except orm.ORMException as e:
        err = _report_search_failed('expression', e, expression_id=expressionId)
        return err, 500

    if not expr_matrix:
        err = Error(message="Expression matrix not found: " + expressionId, code=404)
        return err, 404

    return expr_matrix.__filepath__
