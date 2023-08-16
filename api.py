import boto3
import app.single_audit_api.env_variables as env_variables
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
import time
import datetime 
import json
from boto3.dynamodb.conditions import Key
import app.single_audit_api.s3_functions as myutils
import app.single_audit_api.dynamodb_functions as dyno


import app.single_audit_api.main as main_audit

from fastapi import APIRouter, HTTPException, Path

router = APIRouter()

access_key = env_variables.dev_aws_access_key
secret_key = env_variables.dev_aws_secret_key
region_name = 'us-east-1'
audit_table = env_variables.dev_dynamo_table
bucket_name = env_variables.dev_s3_bucket


def create_aws_session(access_key, secret_key, region_name):
    # Set up AWS credentials
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name  # Replace with your bucket's region
    )

    # Create an S3 client
    return session.client('s3',config=boto3.session.Config(signature_version='s3v4'))


def create_aws_client(aws_session, AWS_entity):
    # Create an s3 or dynamodb or cloudformation client
    return aws_session.client(AWS_entity)


# This route creates just the report and delivers it as a zip file in s3
@router.get("/create_report")
def run_script():
    zip_complete = main_audit.create_report_route()
    return {"message": "Script executed and zip file is created"}

# This route grabs a specific report record based on just the partition key as denoted by organization_id
@router.get("/read_specifics_table/{organization_id}")
def read_specifics_table(organization_id: str):
    table_name = audit_table
    # Create an AWS session
    session_name = create_aws_session(access_key, secret_key, region_name)

    # Create a DynamoDB resource using the session
    dynamodb_resource = session_name.resource('dynamodb')

    try:
        # Get the table
        table = dynamodb_resource.Table(table_name)

        # Define the query parameters
        query_params = {
            'KeyConditionExpression': Key('organization_id').eq(organization_id)
        }

        # Perform the query operation to read items based on the specified organization_id
        response = table.query(**query_params)

        # Retrieve the items from the response
        items = response['Items']

        # Return the items as a JSON response
        return JSONResponse(content=items)
    except Exception as e:
        return JSONResponse(content={"error": "API failed to return response"})

# This route deletes the row which corresponds to the organization_id and report_id
@router.delete("/delete_row/{organization_id}/{report_id}")
def delete_row(organization_id: str, report_id: str):
    table_name = audit_table
    # Create an AWS session
    session_name = create_aws_session(access_key, secret_key, region_name)

    # Create a DynamoDB resource using the session
    dynamodb_resource = session_name.resource('dynamodb')

    try:
        # Get the table
        table = dynamodb_resource.Table(table_name)

        # Define the key of the item to be deleted
        key = {
            'organization_id': organization_id,
            'report_id': report_id
        }

        # Perform the delete operation
        table.delete_item(Key=key)

        return JSONResponse(content={"message": "Row deleted successfully"})
    except Exception as e:
        return JSONResponse(content={"error": "API failed to delete row"})


# This route is specificaly for creating the presigned url link from existing "completed" statused report
@router.get("/s3_link_generation/{organization_id}/{report_id}")
async def fastapi_s3_link_test(organization_id: str, report_id: str):
    # Replace the access_key, secret_key, region_name, and audit_table with actual values
    AWS_session = create_aws_session(access_key, secret_key, region_name)
    # Check the status of the report in the DynamoDB table
    report_id, report_status, s3_filename = dyno.get_reportid_status(AWS_session, audit_table, report_id, organization_id)

    if report_status == 'completed':
        # If the status is 'completed', generate the pre-signed URL
        presigned_url = myutils.get_s3_presigned_url(bucket_name, s3_filename)
        return {"status": "completed", "download_url": presigned_url}
    else:
        # If the status is not 'Completed', return 'In Progress' message
        return {"status": "In-Progress"}
