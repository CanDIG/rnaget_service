import json
import dredd_hooks as hooks
import sys

UUID_EXAMPLE = "be2ba51c-8dfe-4619-b832-31c4a087a589"
RO_FIELDS = ["created", "id"]
response_stash = {}

@hooks.before_each
def redact_readonly_fields(transaction):
    """Do not POST readonly (computed) fields"""
    # no action necessary if not a POST, skip demo endpoints
    if transaction['name'].startswith("demo"):
        transaction['skip'] = True
    elif transaction['request']['method'] == "POST":
        # otherwise, remove such fields from the request body
        request_body = json.loads(transaction['request']['body'])
        for ro_field in RO_FIELDS:
            if ro_field in request_body:
                del request_body[ro_field]
        transaction['request']['body'] = json.dumps(request_body)


@hooks.after("projects > /rnaget/projects/search > Search for projects matching filters > 200 > application/json")
def save_projects_response(transaction):
    parsed_body = json.loads(transaction['real']['body'])
    ids = [item['id'] for item in parsed_body]
    response_stash['project_ids'] = ids


@hooks.after("studies > /rnaget/studies/search > Search for studies matching filters > 200 > application/json")
def save_studies_response(transaction):
    parsed_body = json.loads(transaction['real']['body'])
    ids = [item['id'] for item in parsed_body]
    response_stash['study_ids'] = ids


@hooks.after("expressions > /rnaget/expressions/search > Search for expressions matching filters > 200 > application/json")
def save_expressions_response(transaction):
    parsed_body = json.loads(transaction['real']['body'])
    ids = [item['id'] for item in parsed_body]
    response_stash['expression_ids'] = ids


@hooks.after("changelog > /rnaget/changelog/getVersions > Get release versions of database > 200 > application/json")
def save_versions_response(transaction):
    parsed_body = json.loads(transaction['real']['body'])
    versions = [item for item in parsed_body]
    response_stash['versions'] = versions


@hooks.before("projects > /rnaget/projects/{projectId} > Find project by ID > 200 > application/json")
def insert_project_id(transaction):
    transaction['fullPath'] = transaction['fullPath'].replace(UUID_EXAMPLE, response_stash['project_ids'][0])


@hooks.before("studies > /rnaget/studies/{studyId} > Find study by ID > 200 > application/json")
def insert_study_id(transaction):
    if 'project_ids' in response_stash:
        transaction['fullPath'] = transaction['fullPath'].replace(UUID_EXAMPLE, response_stash['study_ids'][0])


@hooks.before("expressions > /rnaget/expressions/{expressionId} > Find expression data by ID > 200 > application/json")
def insert_expression_id(transaction):
    if 'expression_ids' in response_stash:
        transaction['fullPath'] = transaction['fullPath'].replace(UUID_EXAMPLE, response_stash['expression_ids'][0])


@hooks.before("changelog > /rnaget/changelog/{version} > Get change log for a specific release version > 200 > application/json")
def insert_change_version(transaction):
    if 'versions' in response_stash:
        transaction['fullPath'] = transaction['fullPath'].replace("version1", response_stash['versions'][0])


@hooks.before("projects > /rnaget/projects/{projectId} > Find project by ID > 404 > application/json")
@hooks.before("studies > /rnaget/studies/{studyId} > Find study by ID > 404 > application/json")
@hooks.before("expressions > /rnaget/expressions/{expressionId} > Find expression by ID > 404 > application/json")
def let_pass(transaction):
    transaction['skip'] = False


# skipping file download endpoints
@hooks.before("download json > /rnaget/download/json/{token} > Download the file as JSON > 200 > application/json")
@hooks.before("download hdf5 > /rnaget/expressions/download/{token} > Download the file as HDF5 > 200 > application/json")
def skip_test(transaction):
    transaction['skip'] = True
