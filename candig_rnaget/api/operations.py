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
import pkg_resources

from sqlalchemy import or_
from sqlalchemy import exc
from candig_rnaget import orm
from candig_rnaget.orm import models
from candig_rnaget.api.logging import apilog, logger
from candig_rnaget.api.logging import structured_log as struct_log
from candig_rnaget.api.models import Error, BasePath, Version
from candig_rnaget.api.exceptions import ThresholdValueError, IdentifierFormatError
from candig_rnaget.expression.rnaget_query import ExpressionQueryTool

app = flask.current_app


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


@apilog
def get_project_by_id(projectId):
    """

    :param projectId:
    :return: all projects or if projectId specified, corresponding project
    """
    db_session = orm.get_session()
    project = orm.models.Project

    try:
        validate_uuid_string('id', projectId)
        specified_project = db_session.query(project)\
            .get(projectId)
    except IdentifierFormatError as e:
        err = Error(
            message=str(e),
            code=400)
        return err, 400
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
    project_record['version'] = Version

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
    valid_filters = ["tags", "version"]

    return get_search_filters(valid_filters)


@apilog
def get_study_by_id(studyId):
    """

    :param studyId: required identifier
    :return: a single specified study
    """
    db_session = orm.get_session()
    study = orm.models.Study

    try:
        validate_uuid_string('studyID', studyId)
        specified_study = db_session.query(study)\
            .get(studyId)

    except IdentifierFormatError as e:
        err = Error(
            message=str(e),
            code=400)
        return err, 400

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
    study_record['version'] = Version

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
def search_studies(tags=None, version=None, projectID=None):
    """

    :param tags: optional list of tags
    :param version:
    :param projectID:
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
        if projectID:
            validate_uuid_string('projectID', projectID)
            studies = studies.filter(study.parentProjectID == projectID)

    except IdentifierFormatError as e:
        err = Error(
            message=str(e),
            code=400)
        return err, 400

    except orm.ORMException as e:
        err = _report_search_failed('project', e)
        return err, 500

    return [orm.dump(x) for x in studies], 200


@apilog
def search_study_filters():
    """
    :return: filters for study searches
    """
    valid_filters = ["tags", "version", "projectID"]

    return get_search_filters(valid_filters)


def get_search_filters(valid_filters):
    filter_file = pkg_resources.resource_filename('candig_rnaget', 'orm/filters_search.json')

    with open(filter_file, 'r') as ef:
        search_filters = json.load(ef)

    response = []

    for search_filter in search_filters:
        if search_filter["filter"] in valid_filters:
            response.append(search_filter)

    return response, 200


@apilog
def get_expression_by_id(expressionId):
    """

    :param expressionId: required identifier
    :return: a single specified expression matrix
    """
    db_session = orm.get_session()
    expression = orm.models.File

    try:
        validate_uuid_string('id', expressionId)
        expr_matrix = db_session.query(expression).get(expressionId)
    except IdentifierFormatError as e:
        err = Error(
            message=str(e),
            code=400)
        return err, 400
    except orm.ORMException as e:
        err = _report_search_failed('file', e, expression_id=expressionId)
        return err, 500

    if not expr_matrix:
        err = Error(message="Expression matrix not found: " + expressionId, code=404)
        return err, 404

    return orm.dump(expr_matrix), 200


@apilog
def post_expression(expression_record):
    db_session = orm.get_session()

    iid = uuid.uuid1()
    base_url = app.config.get('BASE_DL_URL') + BasePath

    expression_record['id'] = iid
    expression_record['created'] = datetime.datetime.utcnow()
    expression_record['version'] = Version
    expression_record['URL'] = base_url+'/expressions/download/'+str(iid)

    if expression_record['fileType'] != '.h5':
        err = Error(message="Expression matrix must be fileType '.h5'", code=400)
        return err, 400

    try:
        orm_expression = orm.models.File(**expression_record)
    except orm.ORMException as e:
        err = _report_conversion_error('file', e, **expression_record)
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
                             expression_id=str(iid), **expression_record))

    return expression_record, 201, {'Location': BasePath + '/expressions/' + str(iid)}


@apilog
def get_search_expressions(tags=None, sampleID=None, projectID=None, studyID=None,
                      version=None, featureIDList=None, featureNameList=None,
                      featureAccessionList=None, minExpression=None, maxExpression=None,
                      featureThresholdLabel="name", file_type=".h5"):
    """

    :param tags: optional Comma separated tag list
    :param sampleID: optional sample identifier
    :param projectID: optional project identifier
    :param studyID: optional study identifier
    :param version: optional version
    :param featureIDList: optional filter by listed feature IDs
    :param featureNameList: optional filter by listed features
    :param featureAccessionList: optional filter bys by listed accession numbers
    :param file_type: output file type (not a part of schema yet)
    :return: expression matrices matching filters
    """
    db_session = orm.get_session()
    expression = orm.models.File
    tmp_dir = app.config.get('TMP_DIRECTORY')

    try:
        # TODO: find better way to filter for expression data?
        expressions = db_session.query(expression)
        expressions = expressions.filter(expression.fileType == ".h5")

        # db queries
        if version:
            expressions = expressions.filter(expression.version.like('%' + version + '%'))
        if tags:
            # return any project that matches at least one tag
            expressions = expressions.filter(or_(*[expression.tags.contains(tag) for tag in tags]))
        if studyID:
            validate_uuid_string('studyID', studyID)
            expressions = expressions.filter(expression.studyID == studyID)
        if projectID:
            validate_uuid_string('projectID', projectID)
            study_list = get_study_by_project(projectID)
            expressions = expressions.filter(expression.studyID.in_(study_list))

        if not any([sampleID, featureIDList, featureNameList, featureAccessionList, maxExpression, minExpression]):
            pass

        else:
            # H5 queries
            # TODO: feature name and feature accession lookups
            responses = []
            try:
                for expr in expressions:
                    output_file_id = uuid.uuid1()
                    output_filepath = tmp_dir+str(output_file_id)+file_type
                    feature_map = pkg_resources.resource_filename('candig_rnaget',
                                                                  'expression/feature_mapping_HGNC.tsv')
                    try:
                        h5query = ExpressionQueryTool(
                            expr.__filepath__,
                            output_filepath,
                            include_metadata=False,
                            output_type=file_type,
                            feature_map=feature_map
                        )
                    except OSError as err:
                        logger().warning(struct_log(action=str(err)))
                        continue

                    if sampleID or featureIDList or featureNameList or featureAccessionList:
                        q = h5query.search(
                            sample_id=sampleID,
                            feature_list_id=featureIDList,
                            feature_list_name=featureNameList,
                            feature_list_accession=featureAccessionList
                        )
                    elif minExpression:
                        threshold_array = convert_threshold_array(minExpression)
                        q = h5query.search_threshold(threshold_array, ft_type='min', feature_label=featureThresholdLabel)
                    else:
                        threshold_array = convert_threshold_array(maxExpression)
                        q = h5query.search_threshold(threshold_array, ft_type='max', feature_label=featureThresholdLabel)

                    h5query.close()
                    responses.append(generate_file_response(q, file_type, output_file_id, expr.studyID))

            except ThresholdValueError as e:
                err = Error(
                    message=str(e),
                    code=400)
                return err, 400

            return responses, 200

    except IdentifierFormatError as e:
        err = Error(
            message=str(e),
            code=400)
        return err, 400

    except orm.ORMException as e:
        err = _report_search_failed('expression', e)
        return err, 500

    return [orm.dump(expr_matrix) for expr_matrix in expressions], 200


@apilog
def search_expression_filters(filterType=None):
    """

    :param filterType: optional one of `feature` or `sample`. If blank, both will be returned
    :return: filters for expression searches
    """
    filter_file = pkg_resources.resource_filename('candig_rnaget','orm/filters_expression.json')

    with open(filter_file, 'r') as ef:
        expression_filters = json.load(ef)

    if filterType:
        response = []
        if filterType not in ['feature', 'sample']:
            err = Error(message="Invalid type: " + filterType, code=400)
            return err, 400

        for expression_filter in expression_filters:
            if expression_filter["filterType"] == filterType:
                response.append(expression_filter)
    else:
        response = expression_filters

    return response, 200


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
        validate_uuid_string('fileID', fileID)
        file_q = db_session.query(file).get(fileID)
    except IdentifierFormatError as e:
        err = Error(
            message=str(e),
            code=400)
        return err, 400
    except orm.ORMException as e:
        err = _report_search_failed('file', e, file_id=fileID)
        return err, 500

    if not file_q:
        err = Error(message="File not found: " + fileID, code=404)
        return err, 404

    return orm.dump(file_q), 200


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

    try:
        files = db_session.query(file)
        if tags:
            files = files.filter(or_(*[file.tags.contains(tag) for tag in tags]))
        if projectID:
            validate_uuid_string('projectID', projectID)
            study_list = get_study_by_project(projectID)
            files = files.filter(file.studyID.in_(study_list))
        if studyID:
            validate_uuid_string('studyID', studyID)
            files = files.filter(file.studyID == studyID)
        if fileType:
            files = files.filter(file.fileType == fileType)
    except IdentifierFormatError as e:
        err = Error(
            message=str(e),
            code=400)
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
def tmp_download(token):
    """

    :param token: for now using the file identifier
    :return: h5 file with type application/octet-stream
    """
    return download_file(token, temp_file=True)


@apilog
def expression_download(token):
    """

    :param token: for now using the file identifier
    :return: file attachment type application/octet-stream
    """
    return download_file(token)


def generate_file_response(results, file_type, file_id, study_id):
    base_url = app.config.get('BASE_DL_URL') + BasePath
    tmp_dir = app.config.get('TMP_DIRECTORY')

    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    if file_type == ".json":
        tmp_file_path = os.path.join(tmp_dir, str(file_id) + file_type)
        with open(tmp_file_path, 'w') as outfile:
            json.dump(results, outfile)

    elif file_type == ".h5":
        # results file written to hdf5 in mem
        tmp_file_path = results.filename
        results.close()

    else:
        raise ValueError("Invalid file type")

    file_record = {
        'id': file_id,
        'URL': base_url + '/download/' + str(file_id),
        'studyID': study_id,
        'fileType': file_type,
        'version': Version,
        'created': datetime.datetime.utcnow(),
        '__filepath__': tmp_file_path
    }

    return create_tmp_file_record(file_record)


def download_file(token, temp_file=False):
    """
    Generic file exporter
    """

    try:
        if not temp_file:
            access_file = get_expression_file_path(token)
            if not access_file:
                err = Error(message="File not found", code=404)
                return err, 404
            response = flask.send_file(access_file, as_attachment=True)
        # use tmp directory file ID's
        else:
            db_session = orm.get_session()
            temp_file = orm.models.TempFile

            try:
                access_file = db_session.query(temp_file).get(token)
            except exc.StatementError:
                err = Error(message="Invalid file token: " + str(token), code=404)
                return err, 404
            except orm.ORMException as e:
                err = _report_search_failed('file', e, expression_id=token)
                return err, 500

            if not access_file:
                err = Error(message="File not found (link may have expired)", code=404)
                return err, 404

            file_path = access_file.__filepath__

            if os.path.isfile(file_path):
                response = flask.send_file(file_path, as_attachment=True)
            else:
                err = Error(message="File not found (link may have expired)", code=404)
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


def get_expression_file_path(expressionId):
    """

    :param expressionId: required identifier
    :return: internal expression matrix filepath
    """
    db_session = orm.get_session()
    expression = orm.models.File

    try:
        validate_uuid_string('id', expressionId)
        expr_matrix = db_session.query(expression).get(expressionId)

    except IdentifierFormatError as e:
        err = Error(
            message=str(e),
            code=400)
        return err, 400

    except orm.ORMException as e:
        err = _report_search_failed('file', e, expression_id=expressionId)
        return err, 500

    if not expr_matrix:
        err = Error(message="Expression matrix not found: " + expressionId, code=404)
        return err, 404

    return expr_matrix.__filepath__


def create_tmp_file_record(file_record):
    db_session = orm.get_session()

    try:
        orm_expression = orm.models.TempFile(**file_record)
    except orm.ORMException as e:
        err = _report_conversion_error('file', e, **file_record)
        return err, 400

    del file_record['__filepath__']

    try:
        db_session.add(orm_expression)
        db_session.commit()
    except exc.IntegrityError:
        err = _report_object_exists('file: ' + file_record['id'], **file_record)
        return err, 405
    except orm.ORMException as e:
        err = _report_write_error('file', e, **file_record)
        return err, 500

    return file_record


def validate_uuid_string(field_name, uuid_str):
    """
    Validate that the id parameter is a valid UUID string
    :param uuid_str: query parameter
    :param field_name: id field name
    """
    try:
        uuid.UUID(uuid_str)
    except ValueError:
        raise IdentifierFormatError(field_name)
    return
