# pylint: disable=invalid-name
# pylint: disable=C0301
# pylint: disable=redefined-outer-name
"""
Tests for ORM module
"""

import uuid
import flask
import os

from candig_rnaget import orm
from candig_rnaget.app import app
from candig_rnaget.api import operations
from candig_rnaget.api.models import BasePath, Version
from candig_rnaget.expression.download import tmp_download, persistent_download

import pytest


@pytest.fixture(name='test_client', scope='module')
def load_test_client(db_filename="operations.db"):  # pylint: disable=too-many-locals
    # delete db if already exists
    try:
        os.remove(db_filename)
    except OSError:
        pass

    context = app.app.app_context()

    with context:
        orm.init_db('sqlite:///' + db_filename)
        project, study, expression = load_test_objects()
        load_supplementary_objects()

        app.app.config['BASE_DL_URL'] = 'http://127.0.0.1'
        app.app.config['TMP_DIRECTORY'] = os.path.join(os.getcwd(), 'tmp/')

    return project, study, expression, context


def test_post_project_exists(test_client):
    """
    post_project
    """
    sample_project, _, _, context = test_client

    with context:
        # get id (200)
        result, code = operations.post_project({'id': sample_project['id'], 'name': 'main_test_project'})
        assert(code == 405)


def test_post_project_orm_error(test_client):
    """
    post_project
    """
    sample_project, _, _, context = test_client

    with context:
        # get id (200)
        result, code = operations.post_project({'id': 12345678910, 'name': 1})
        assert(code == 500)


def test_get_project_by_id(test_client):
    """
    get_project_by_id
    """
    sample_project, _, _, context = test_client

    with context:
        # get id (200)
        result, code = operations.get_project_by_id(sample_project['id'])
        assert(result['id'] == uuid.UUID(sample_project['id']))
        assert(code == 200)

        # get id (400)
        result, code = operations.get_project_by_id("not a uuid")
        assert(code == 400)

        # get id (404)
        result, code = operations.get_project_by_id(str(uuid.uuid1()))
        assert(code == 404)


def test_search_projects(test_client):
    """
    search_projects
    """
    sample_project, _, _, context = test_client

    with context:
        # search by tag
        result, code = operations.search_projects(tags=['test'])
        assert(result[0]['id'] == uuid.UUID(sample_project['id']))
        assert(code == 200)

        # search by version
        result, code = operations.search_projects(version=Version)
        assert(result[0]['id'] == uuid.UUID(sample_project['id']))
        assert(code == 200)


def test_search_project_filters(test_client):
    """
    search_project_filters
    """
    context = test_client[3]

    with context:
        valid_filters = ["tags", "version"]
        result, code = operations.search_project_filters()
        assert(len(result) == len(valid_filters))
        assert(code == 200)
        for filter_obj in result:
            assert(filter_obj["filter"] in valid_filters)


def test_get_study_by_id(test_client):
    """
    get_study_by_id
    """
    _, sample_study, _, context = test_client

    with context:
        # get id (200)
        result, code = operations.get_study_by_id(sample_study['id'])
        assert(result['id'] == uuid.UUID(sample_study['id']))
        assert(code == 200)

        # get id (400)
        result, code = operations.get_study_by_id("not a uuid")
        assert(code == 400)

        # get id (404)
        result, code = operations.get_study_by_id(str(uuid.uuid1()))
        assert(code == 404)


def test_search_studies(test_client):
    """
    search_projects
    """
    sample_project, sample_study, _, context = test_client

    with context:
        # search by tag
        result, code = operations.search_studies(tags=['test'])
        assert(result[0]['id'] == uuid.UUID(sample_study['id']))
        assert(code == 200)

        # search by version
        result, code = operations.search_studies(version=Version)
        assert(result[0]['id'] == uuid.UUID(sample_study['id']))
        assert(code == 200)

        # search by project
        result, code = operations.search_studies(projectID=sample_project['id'])
        assert(result[0]['id'] == uuid.UUID(sample_study['id']))
        assert(code == 200)


def test_search_study_filters(test_client):
    """
    search_study_filters
    """
    context = test_client[3]

    with context:
        valid_filters = ["tags", "version", "projectID"]
        result, code = operations.search_study_filters()
        assert(len(result) == len(valid_filters))
        assert(code == 200)
        for filter_obj in result:
            assert(filter_obj["filter"] in valid_filters)


def test_get_expression_by_id(test_client):
    """
    get_expression_by_id
    """
    _, _, sample_expression, context = test_client

    with context:
        # get id (200)
        result, code = operations.get_expression_by_id(sample_expression['id'])
        assert(result['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)

        # get id (400)
        result, code = operations.get_expression_by_id("not a uuid")
        assert(code == 400)

        # get id (404)
        result, code = operations.get_expression_by_id(str(uuid.uuid1()))
        assert(code == 404)


def test_get_search_expressions_filter_basic(test_client):
    """
    get_search_expressions
    """
    sample_project, sample_study, sample_expression, context = test_client

    with context:
        # basic query (200)
        result, code = operations.get_search_expressions()
        assert(len(result) == 1)
        assert(code == 200)


def test_get_search_expressions_filter_tag(test_client):
    """
    get_search_expressions
    """
    sample_project, sample_study, sample_expression, context = test_client

    with context:
        # search by tag
        result, code = operations.get_search_expressions(tags=['test'])
        assert(result[0]['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)


def test_get_search_expressions_filter_version(test_client):
    """
    get_search_expressions
    """
    sample_project, sample_study, sample_expression, context = test_client

    with context:
        # search by version
        result, code = operations.get_search_expressions(version=Version)
        assert(result[0]['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)


def test_get_search_expressions_filter_study(test_client):
    """
    get_search_expressions
    """
    sample_project, sample_study, sample_expression, context = test_client

    with context:
        # search by studyID
        result, code = operations.get_search_expressions(studyID=sample_study['id'])
        assert(result[0]['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)


def test_get_search_expressions_filter_project(test_client):
    """
    get_search_expressions
    """
    sample_project, sample_study, sample_expression, context = test_client

    with context:
        # search by projectID
        result, code = operations.get_search_expressions(projectID=sample_project['id'])
        assert(result[0]['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)


def test_get_search_expressions_filter_error(test_client):
    """
    get_search_expressions
    """
    sample_project, sample_study, sample_expression, context = test_client

    with context:
        # invalid UUID search
        result, code = operations.get_search_expressions(projectID="not a uuid")
        assert(code == 400)


def test_get_expression_formats(test_client):
    """
    get_expression_formats
    """
    context = test_client[3]

    with context:
        # format list (200)
        result, code = operations.get_expression_formats()
        assert(len(result) > 0)
        assert(code == 200)


def test_get_search_expressions_slice_by_sample(test_client):
    """
    get_search_expressions
    """
    _, _, sample_expression, context = test_client

    with context:
        # bad sample id (200)
        result, code = operations.get_search_expressions(sampleID="blah")
        assert(len(result) == 0)
        assert(code == 200)

        # valid sample id (200)
        result, code = operations.get_search_expressions(sampleID="DO221123", format='json')
        assert(result[0]['studyID'] == uuid.UUID(sample_expression['studyID']))
        assert(result[0]['fileType'] == 'json')
        assert(code == 200)


def test_get_search_expressions_slice_by_feature_id_json(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature ID list (200)
        result, code = operations.get_search_expressions(
            featureIDList=['ENSG00000000003', 'ENSG00000000005'], format='json')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'json')
        assert(code == 200)
        tmp_id = result[0]['id']
        tmp_response = tmp_download(str(tmp_id))
        assert(tmp_response.status_code == 200)


def test_get_search_expressions_slice_by_feature_name_json(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature name list (200)
        result, code = operations.get_search_expressions(featureNameList=['TSPAN6', 'TNMD'], format='json')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'json')
        assert(code == 200)


def test_get_search_expressions_slice_by_feature_sample_json(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature name list (200)
        result, code = operations.get_search_expressions(
            featureNameList=['TSPAN6', 'TNMD'], sampleID='DO221123', format='json')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'json')
        assert(code == 200)


def test_get_search_expressions_slice_by_threshold_json(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # minExpression (200)
        result, code = operations.get_search_expressions(
            minExpression=['TSPAN6', '0.1', 'TNMD', '0.2'], format='json')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'json')
        assert(code == 200)

        # maxExpression (200)
        result, code = operations.get_search_expressions(
            maxExpression=['TSPAN6', '1.0', 'TNMD', '2.0'], format='json')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'json')
        assert(code == 200)

        # Threshold value error (400)
        result, code = operations.get_search_expressions(
            maxExpression=['TSPAN6', 'NotAValue', 'TNMD', '2.0'], format='json')
        assert(code == 400)


def test_get_search_expressions_slice_by_sample_h5(test_client):
    """
    get_search_expressions
    """
    _, _, sample_expression, context = test_client

    with context:
        # bad sample id (200)
        result, code = operations.get_search_expressions(sampleID="blah")
        assert(len(result) == 0)
        assert(code == 200)

        # valid sample id (200)
        result, code = operations.get_search_expressions(sampleID="DO221123", format='h5')
        assert(result[0]['studyID'] == uuid.UUID(sample_expression['studyID']))
        assert(result[0]['fileType'] == 'h5')
        assert(code == 200)


def test_get_search_expressions_slice_by_feature_id_h5(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature ID list (200)
        result, code = operations.get_search_expressions(
            featureIDList=['ENSG00000000003', 'ENSG00000000005'], format='h5')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'h5')
        assert(code == 200)
        tmp_id = result[0]['id']
        with app.app.test_request_context():
            response = tmp_download(str(tmp_id))
            assert(response.status_code == 200)


def test_get_search_expressions_slice_by_feature_name_h5(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature name list (200)
        result, code = operations.get_search_expressions(featureNameList=['TSPAN6', 'TNMD'], format='h5')
        assert(len(result) == 1)
        assert(code == 200)


def test_get_search_expressions_slice_by_feature_sample_h5(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature name list (200)
        result, code = operations.get_search_expressions(
            featureNameList=['TSPAN6', 'TNMD'], sampleID='DO221123', format='h5')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'h5')
        assert(code == 200)


def test_get_search_expressions_slice_by_threshold_h5(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # minExpression (200)
        result, code = operations.get_search_expressions(minExpression=['TSPAN6', '0.1', 'TNMD', '0.2'], format='h5')
        assert(len(result) == 1)
        assert(result[0]['fileType'] == 'h5')
        assert(code == 200)

        # maxExpression (200)
        result, code = operations.get_search_expressions(maxExpression=['TSPAN6', '1.0', 'TNMD', '2.0'], format='h5')
        assert(len(result) == 1)
        assert(result[0]['fileType'] == 'h5')
        assert(code == 200)


def test_get_search_expressions_slice_by_sample_loom(test_client):
    """
    get_search_expressions
    """
    _, _, sample_expression, context = test_client

    with context:
        # bad sample id (200)
        result, code = operations.get_search_expressions(sampleID="blah")
        assert(len(result) == 0)
        assert(code == 200)

        # valid sample id (200)
        result, code = operations.get_search_expressions(sampleID="DO221123", format='loom')
        assert(result[0]['studyID'] == uuid.UUID(sample_expression['studyID']))
        assert(result[0]['fileType'] == 'loom')
        assert(code == 200)


def test_get_search_expressions_slice_by_feature_id_loom(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature ID list (200)
        result, code = operations.get_search_expressions(
            featureIDList=['ENSG00000000003', 'ENSG00000000005'], format='loom')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'loom')
        assert(code == 200)
        tmp_id = result[0]['id']
        with app.app.test_request_context():
            response = tmp_download(str(tmp_id))
            assert(response.status_code == 200)


def test_get_search_expressions_slice_by_feature_name_loom(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature name list (200)
        result, code = operations.get_search_expressions(featureNameList=['TSPAN6', 'TNMD'], format='loom')
        assert(len(result) == 1)
        assert(code == 200)


def test_get_search_expressions_slice_by_feature_sample_loom(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # feature name list (200)
        result, code = operations.get_search_expressions(
            featureNameList=['TSPAN6', 'TNMD'], sampleID='DO221123', format='loom')
        assert(len(result) == 1)
        assert (result[0]['fileType'] == 'loom')
        assert(code == 200)


def test_get_search_expressions_slice_by_threshold_loom(test_client):
    """
    get_search_expressions
    """
    context = test_client[3]

    with context:
        # minExpression (200)
        result, code = operations.get_search_expressions(minExpression=['TSPAN6', '0.1', 'TNMD', '0.2'], format='loom')
        assert(len(result) == 1)
        assert(result[0]['fileType'] == 'loom')
        assert(code == 200)

        # maxExpression (200)
        result, code = operations.get_search_expressions(maxExpression=['TSPAN6', '1.0', 'TNMD', '2.0'], format='loom')
        assert(len(result) == 1)
        assert(result[0]['fileType'] == 'loom')
        assert(code == 200)


def test_post_search_expressions_name_threshold(test_client):
    """
    post_search_expressions
    """
    _, _, sample_expression, context = test_client

    with context:
        # min Expression feature ID h5 format
        result, code = operations.post_search_expressions(
            {
                'version': Version,
                'tags': ['test'],
                'minExpression': [{'featureID': 'ENSG00000000003', 'threshold': 0.1},
                                  {'featureID': 'ENSG00000000005', 'threshold': 0.2}]
            }
        )
        assert(len(result) == 1)
        assert(result[0]['fileType'] == 'h5')
        assert(code == 200)

        # min Expression feature no threshold match
        result, code = operations.post_search_expressions(
            {
                'version': Version,
                'tags': ['test'],
                'minExpression': [{'featureID': 'ENSG00000000003', 'threshold': 1000},
                                  {'featureID': 'ENSG00000000005', 'threshold': 1000}]
            }
        )
        assert(len(result) == 0)
        assert(code == 200)


def test_post_search_expressions_id_threshold(test_client):
    """
    post_search_expressions
    """
    _, _, sample_expression, context = test_client

    with context:
        # max Expression feature name json format
        result, code = operations.post_search_expressions(
            {
                'format': 'json',
                'minExpression': [{'featureName': 'TSPAN6', 'threshold': 0.1},
                                  {'featureName': 'TNMD', 'threshold': 0.2}]
            }
        )
        assert(len(result) == 1)
        assert(result[0]['fileType'] == 'json')
        assert(code == 200)


def test_post_search_expressions_error_threshold(test_client):
    """
    post_search_expressions
    """
    _, _, sample_expression, context = test_client

    with context:
        # threshold error
        result, code = operations.post_search_expressions(
            {
                'format': 'json',
                'minExpression': [{'featureName': 'TSPAN6', 'threshold': 0.1},
                                  {'featureID': 'ENSG00000000005', 'threshold': 0.2}]
            }
        )
        assert (code == 400)

        # threshold error
        result, code = operations.post_search_expressions(
            {
                'format': 'json',
                'minExpression': [{'invalid': 'TSPAN6'},
                                  {'invalid': 'ENSG00000000005', 'threshold': 2.0}]
            }
        )
        assert (code == 400)


def test_post_search_expressions_error_uuid(test_client):
    """
    post_search_expressions
    """
    _, _, sample_expression, context = test_client

    with context:
        # uuid error (400)
        result, code = operations.post_search_expressions(
            {
                'studyID': 'not a uuid',
            }
        )
        assert (code == 400)


def test_expression_filters(test_client):
    """
    get_expression_filters
    """
    context = test_client[3]

    with context:
        # get expression filters (200)
        result, code = operations.search_expression_filters()
        assert(len(result) > 0)
        assert(code == 200)

        # bad Accept parameter (406)
        result, code = operations.search_expression_filters(Accept="yaml")
        assert(code == 406)


def test_get_file(test_client):
    """
    get_file
    """
    _, _, sample_expression, context = test_client

    with context:
        # get id (200)
        result, code = operations.get_file(sample_expression['id'])
        assert(result['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)

        # get id (400)
        result, code = operations.get_file("not a uuid")
        assert(code == 400)

        # get id (404)
        result, code = operations.get_file(str(uuid.uuid1()))
        assert(code == 404)


def test_file_download(test_client):
    """
    expression_download
    """
    _, _, sample_expression, context = test_client

    with context:
        # get id (200)
        result, code = operations.get_file(sample_expression['id'])
        assert(result['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)
        filename = result['URL'].split('/')[-1]
        with app.app.test_request_context():
            response = persistent_download(filename)
            assert(response.status_code == 200)


def test_search_files(test_client):
    """
    search_files
    """
    sample_project, sample_study, sample_expression, context = test_client

    with context:
        # basic query (200)
        result, code = operations.search_files()
        assert(len(result) == 1)
        assert(code == 200)

        # search by tags (200)
        result, code = operations.search_files(tags=['test'])
        assert(result[0]['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)

        # search by project (200)
        result, code = operations.search_files(projectID=sample_project['id'])
        assert(result[0]['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)

        # search by study (200)
        result, code = operations.search_files(studyID=sample_study['id'])
        assert(result[0]['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)

        # search invalid UUID (400)
        result, code = operations.search_files(studyID="not a uuid")
        assert(code == 400)

        # search by file type (200)
        result, code = operations.search_files(fileType='h5')
        assert(result[0]['id'] == uuid.UUID(sample_expression['id']))
        assert(code == 200)

        # invalid parameters should still be an empty 200
        result, code = operations.search_files(fileType='bam')
        assert(len(result) == 0)
        assert(code == 200)


def test_changelog(test_client):
    """
    post_changelog, get_versions, get_changelog
    """
    context = test_client[3]
    sample_version = {'version': Version, 'log': ['unit testing']}

    with context:
        # post log (201)
        result, code, _ = operations.post_change_log(sample_version)
        log_version = result['version']
        assert(code == 201)

        # get versions (200)
        result, code = operations.get_versions()
        assert(result[0] == log_version)
        assert(code == 200)

        # get_changelog
        result, code = operations.get_change_log(version=Version)
        assert(result['version'] == Version)
        assert(code == 200)


def test_continuous(test_client):
    """
    TODO: continuous endpoints are not yet supported. All should return 501 for now
    """
    context = test_client[3]

    with context:
        result, code = operations.get_continuous_by_id('be2ba51c-8dfe-4619-b832-31c4a087a589')
        assert(code == 501)

        result, code = operations.get_continuous_formats()
        assert(code == 501)

        result, code = operations.search_continuous('loom')
        assert(code == 501)


def load_expression(study_id):
    expressionid = uuid.uuid1().hex

    sample_file = os.path.join(
        os.path.dirname(flask.current_app.instance_path), 'tests/data/sample_matrix.h5')

    url_base = 'http://127.0.0.1'+BasePath
    test_expression = {
        'id': expressionid,
        '__filepath__': sample_file,
        'URL': url_base+'/expressions/download/'+os.path.basename(sample_file),
        'studyID': study_id,
        'version': Version,
        'tags': ['expressions', 'test'],
        "fileType": "h5",
    }

    operations.post_expression(test_expression)

    return test_expression


def load_test_objects():
    # sample project: profyle
    main_project_id = uuid.uuid1().hex
    main_study_id = uuid.uuid1().hex

    test_project = {
        'id': main_project_id,
        'name': 'main_test_project',
        'description': 'mock profyle project for testing',
        'tags': ['test', 'candig'],
        'version': Version
    }

    operations.post_project(test_project)

    # sample study: pog
    test_study = {
        'id': main_study_id,
        'name': 'main_test_study',
        'parentProjectID': main_project_id,
        'description': 'mock study for testing',
        'tags': ['test', 'candig'],
        'patientList': [str(x) for x in range(30)],
        'sampleList': ['PATIENT_'+str(x) for x in range(30)]
    }

    operations.post_study(test_study)
    test_expression = load_expression(main_study_id)

    return test_project, test_study, test_expression


def load_supplementary_objects():
    for pname in ['PS1', 'PS2', 'PS3']:
        project_id = str(uuid.uuid1())

        test_project = {
            'id': project_id,
            'name': pname,
            'description': 'sample project for testing',
            'tags': [pname],
            'version': Version
        }

        operations.post_project(test_project)

        load_study(project_id)


def load_study(parent_project_id):
    study_id = uuid.uuid1().hex

    test_study = {
        'id': study_id,
        'name': 'pilot',
        'parentProjectID': parent_project_id,
        'description': 'mock study for testing',
        'tags': [],
        'patientList': []
    }

    operations.post_study(test_study)
