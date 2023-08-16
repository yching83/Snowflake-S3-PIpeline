#!/usr/bin/env python
import pandas as pd
import boto3
import sys
import os
import io
import snowflake.connector
import re
import smart_open
import tempfile
import time
import datetime 
import json
import zipfile
from pathlib import Path
import logging
import app.single_audit_api.env_variables as env_variables
import app.single_audit_api.table_question_format as review_stage
import app.single_audit_api.s3_functions as myutils
import app.single_audit_api.dynamodb_functions as dyno
import shutil
import uuid
from urllib.parse import urlparse, parse_qs

# Credentials for Snowflake Connection
example_file_name = 'app/single_audit_api/example_json.json'
read_me_name = 'app/single_audit_api/ReadMe.txt'
user_field = env_variables.snowflake_username 
password_field = env_variables.snowflake_password 
account_identifier = env_variables.snowflake_account_nm 
database_name = env_variables.snowflake_database_nm 
warehouse_field = env_variables.snowflake_warehouse_nm 
schema_name = env_variables.snowflake_schema_nm 
role_field = env_variables.snowflake_role_nm
access_key =  env_variables.dev_aws_access_key
secret_key = env_variables.dev_aws_secret_key
arn= env_variables.dev_arn
dev_dynamo_tbl = env_variables.dev_dynamo_table
dev_s3_bucket = env_variables.dev_s3_bucket
dev_dynamo_region_name = 'us-east-1'
AWS_ACCESS_KEY_ID = env_variables.prod_aws_access_key_id 
AWS_SECRET_ACCESS_KEY = env_variables.prod_aws_secret_access_key 
AWS_SESSION_TOKEN = env_variables.prod_aws_session_token 
aws_session_ID = dyno.create_aws_session(access_key, secret_key, dev_dynamo_region_name)
region_name = 'us-east-1'

# Naming variables to hold directory for the file path in smart open
folder_id =  'FOLDER_DIRECTORY' 
folder_list = [] 

# Create logger file to capture issues during step process
logging.basicConfig(filename = 'AuditProcess.log', encoding = 'utf-8', level=logging.DEBUG)

# Class for creation of snowflake connection
class snowflakeconnection(object):
    def __init__(self):
        self.user = user_field
        self.password = password_field
        self.account = account_identifier
    def connecthandler(self):
        self.ctx = snowflake.connector.connect(user = self.user, password = self.password, account = self.account)
        return self.ctx
    def cursor(self):
        self.cs = self.ctx.cursor()
        return self.cs

# Class to declare variables definitions
class Report:
    def __init__(self,report_name_api, organization_id, organization_id_string, count_per_batch, submission_list, aws_session, table_name, random_uuid, formatted_string_date, submission_list_elements, dev_zip_names, dev_s3, dev_bucket_name):
        self.report_name_api = report_name_api
        self.organization_id = organization_id
        self.organization_id_string = organization_id_string
        self.count_per_batch = count_per_batch
        self.submission_list = submission_list
        self.aws_session = aws_session
        self.table_name = table_name
        self.report_id = random_uuid
        self.formatted_string_date = formatted_string_date
        self.submission_list_elements = submission_list_elements
        self.dev_zip_names = dev_zip_names
        self.dev_s3 = dev_s3
        self.dev_bucket_name = dev_bucket_name 



def try_query_async (name_of_conn, handler_name, cursor_name, execution_query, type_of_form):
    name_of_conn = snowflakeconnection()
    handler_name = name_of_conn.connecthandler()
    cursor_name = handler_name.cursor()
    starts = time.time() #added
    cursor_name.execute_async(execution_query)
    try:
        query_id = cursor_name.sfqid
        while handler_name.is_still_running(handler_name.get_query_status(query_id)):
            time.sleep(1)
    except snowflake.connector.ProgrammingError as err:
        logging.error('try_query_async method error: {0}'.format(err))
        sys.exit(1)
    cursor_name.get_results_from_sfqid(query_id)
    results = cursor_name.fetchall()
    df = pd.DataFrame(results)
    ends = time.time()
    print("Query Timer In Seconds For {}: ".format(type_of_form), (ends - starts))
    return df, query_id, handler_name, cursor_name

# Function that separates out dataframes based on a list of values --ie. folder directory
def print_pd_groupby(X, grp=None):
    '''Display contents of a Panda groupby object
    :param X: Pandas groupby object
    :param grp: a list with one or more group names
    '''
    if grp is None:
        for k,i in X:
            error_group = []
            error_group = list(k)
            logging.error("group is not being displayed correctly:")
            logging.error(error_group)
            return df_frame
    else:
        for j in grp:
            group_labels = []
            group_labels = list(j)
            logging.info("group names being read into a separated label are:")
            logging.info(group_labels)
            df_frame = (X.get_group(j))
            return df_frame

def deliver_files_smartopen(zip_name, dataframes, folder_ids):    
    with smart_open.open(zip_name, 'wb') as fout:
        with zipfile.ZipFile(fout, 'w') as zf:
            starttime  = time.time() 
            for df, folder_id in zip(dataframes, folder_ids):
                folder_list = df[folder_id].drop_duplicates().to_list()
                dfg = df.groupby(folder_id)

                for item in folder_list:
                    df_frame = print_pd_groupby(dfg, grp=[item])
                    writePath = item
                    print(writePath)
                    writePath = os.path.splitext(writePath)[0] + ".csv"
                    #print(os.path.splitext(writePath)[0])
                    csv_data = df_frame.to_csv(index=False)
                    zf.writestr(writePath, csv_data)
            endstime = time.time()
            print("Timer for zipping all files: ", (endstime - starttime))

        zf.close()
    fout.close()

def download_files_from_s3_returns2(bucket_names, object_keys, destination_paths):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )

    zip_memory_downloads = []
    destination_paths_list = []

    for bucket_name, object_key, destination_path in zip(bucket_names, object_keys, destination_paths):
        try:
            file_data = io.BytesIO()  # Create a BytesIO object to store file data
            s3.download_fileobj(bucket_name, object_key, file_data)

            # Create a new zip memory for each downloaded file
            zip_memory_download = io.BytesIO()
            zip_memory_downloads.append(zip_memory_download)
            destination_paths_list.append(destination_path)

            with zipfile.ZipFile(zip_memory_download, 'w') as zf:
                zf.writestr(object_key, file_data.getvalue())

            print(f"File '{object_key}' downloaded from S3 and added to the ZIP archive")

        except Exception as e:
            print(f"Error downloading file '{object_key}' from S3: {str(e)}")

    return zip_memory_downloads, destination_paths_list

def create_zip_from_memory(zip_memory_downloads, destination_paths, dataframes, folder_ids, zip_memory=None): 
    if zip_memory is None:
        zip_memory = io.BytesIO()
    else:
        zip_memory.seek(0)

    with zipfile.ZipFile(zip_memory, 'w') as zf:
        # Adds the downloadable objects from S3 with designated destination_paths
        if not zip_memory_downloads:
            pass
        else:
            for zip_memory_download, destination_path in zip(zip_memory_downloads, destination_paths):
                zf.writestr(destination_path, zip_memory_download.getvalue())

        # Add dataframes in the ZIP archive
        for df, folder_id in zip(dataframes, folder_ids):
            folder_list = df[folder_id].drop_duplicates().to_list()
            dfg = df.groupby(folder_id)
            for item in folder_list:
                df_frame = print_pd_groupby(dfg, grp=[item])
                writePath = item
                writePath = os.path.splitext(writePath)[0] + ".csv"
                csv_data = df_frame.to_csv(index=False)
                zf.writestr(writePath, csv_data)

    zip_memory.seek(0)
    return zip_memory

# Expand on this section later for large file attachments
def add_to_zip (zip_file_name, source_file, destination_path):
    filepath = zip_file_name
    with zipfile.ZipFile(filepath, 'a') as zipf:
        source_path = source_file
        destination = destination_path
        zipf.write(source_path, destination)


def upload_file_to_s3_using_smart_open(zip_memory, bucket_name, path_filename, access_key, secret_key, region_name):
    s3_uri = f"s3://{bucket_name}/{path_filename}"
    s3_options = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
        'region_name': region_name
    }
    with smart_open.open(s3_uri, 'wb', transport_params=s3_options) as s3_file:
        s3_file.write(zip_memory.getvalue())
    print(f"Zip file uploaded successfully to '{bucket_name}' with key '{path_filename}'.")


# Functions to replace multiple values in string -- in this case: our query
def multiple_replacer(*key_values):
    replace_dict = dict(key_values)
    replacement_function = lambda match: replace_dict[match.group(0)]
    pattern = re.compile("|".join([re.escape(k) for k, v in key_values]), re.M)
    return lambda string: pattern.sub(replacement_function, string)

def multiple_replace(string, *key_values):
    return multiple_replacer(*key_values)(string)

def json_parse(json_file_name):
    file = open(json_file_name)
    json_parsed_data = json.load(file)
    return json_parsed_data

def json_variable_list(json_element, json_file_called, variable_clause):
    data = json_parse(json_file_called) 
    list_variable = (data[json_element])
    if type (list_variable) == list:
        list_of_values = variable_clause + ' in (' + ','.join( str(x) for x in list_variable) + ')'
        return list_of_values, list_variable
    else:
         string_value = variable_clause + ' = ' + '\''+ list_variable + '\''
         return string_value

def generate_uuid():
    # Generate a random UUID
    random_uuid = str(uuid.uuid4())
    return random_uuid

def summary_query():
    query_summary = """
    select  'This "Read_Me" file explains how to use the folder structure and going through the files and stages in each folder. 
The folder structure is created based on submission_id as the first level, with all other form types associated with the
particular submission is then created as its own separate folders.  The sub directories of the folders behind each form type 
is then broken down into the creation date and time stamp based on the creation date and timestamp of each of the file designated 
by the "Folder_Directory" path located on the files.  Chronology file will provide an overview of the history behind each 
submission id in association to all the form steps of each submission.  Form_Data.csv provides a focused data set of each form 
type based on each submission, and it is saved by path form type > form_data > and creation datetimestamps. '
as summary, 
CONCAT('Audit_Folder', '/readme.csv' )
as folder_directory;
  """
    return query_summary

def forms_query_string():
    query_form = """
with response_field_value_cte as (
    select * from datamart.gms.response_field_value where organization_id =
),
form_field_cte as (
    select * from datamart.gms.form_field where organization_id =
),
form_field_option_cte as (
    select * from datamart.gms.form_field_option where organization_id =
),
forms_data as (
select
    rfv.submission_id,
    ff.form_type,
    ff.form_name,
    ff.field_type,
    ff.field_label,
    rfv.sub_field_id,
    coalesce(ffo.option_label, GET(TRY_PARSE_JSON(rfv.value), 'value'), rfv.value) as value,
    rcc.completed_at,
    rcc.created_at,
    ms.email as created_user_email,
    CASE WHEN (ff.field_label = 'Table Question' and ff.form_type <> 'review') then (CONCAT('Audit_Folder', '/Audit_' , rfv.SUBMISSION_ID , '/' , FORM_TYPE, '/',
             FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/FORM_DATA_/' , replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/files/Table_Question.csv')) 
         WHEN (ff.field_label = 'Table Question' and ff.form_type = 'review') then 
         CONCAT('Audit_Folder', '/Audit_' , rfv.SUBMISSION_ID , '/' , FORM_TYPE, '/', FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/FORM_DATA_' ,ms.email,'/', replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/files/Table_Question.csv')
         WHEN (ff.form_type = 'review' and ff.field_label <> 'Table Question') then 
          CONCAT('Audit_Folder', '/Audit_' , rfv.SUBMISSION_ID , '/' , FORM_TYPE, '/', FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/FORM_DATA_/' ,ms.email,'/', replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/form_data.csv')
        else
        CONCAT('Audit_Folder', '/Audit_' , rfv.SUBMISSION_ID , '/' , FORM_TYPE, '/', FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/FORM_DATA_' , replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/form_data.csv')
    end
    as folder_directory
from response_field_value_cte rfv
inner join form_field_cte ff on rfv.field_id = ff.field_id
left join form_field_option_cte ffo on ffo.field_id = rfv.field_id and ffo.option_id = rfv.sub_field_id
left join staging.intermediate.response_context rcc on 
(rfv.request_id = rcc.request_id and rfv.response_id = rcc.response_id and rfv.submission_id = rcc.submission_id)
left join staging.base.ms_smmuser ms on rcc.created_by = ms.userid
where rfv.submission_id in
order by rfv.submission_id, ff.form_type, ff.form_name, ff.field_index)
select distinct submission_id,
       form_type,
       form_name,
       field_type,
       field_label,
       sub_field_id,
       value,
       completed_at,
       created_at,
       created_user_email,
       folder_directory
from forms_data
union
(
select distinct submission_id,
       form_type,
       form_name,
       field_type,
       'Path_to_File_Table_Question' as field_label,
       'Please Refer to Value for Path Field' as sub_field_id,
       CASE WHEN form_type = 'review' then 
       (CONCAT('Audit_Folder', '/Audit_' , SUBMISSION_ID , '/' , FORM_TYPE, '/', FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/FORM_DATA_' ,created_user_email,'/', replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/files/Table_Question.csv'))
       else
       (CONCAT('Audit_Folder', '/Audit_' , SUBMISSION_ID , '/' , FORM_TYPE, '/',
             FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/FORM_DATA_' , replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/files/Table_Question.csv')) 
       end
       as value,
       completed_at,
       created_at,
       created_user_email,
       CONCAT('Audit_Folder', '/Audit_' , SUBMISSION_ID , '/' , FORM_TYPE, '/', FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/FORM_DATA_' , replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/form_data.csv') as folder_directory
from forms_data where FIELD_LABEL = 'Table Question'
); 
"""
    return query_form

def review_stage_string():
    query_review_stage = """
with response_field_value_cte as (
    select * from datamart.gms.response_field_value where organization_id = 
),
form_field_cte as (
    select * from datamart.gms.form_field where organization_id = 
),
form_field_option_cte as (
    select * from datamart.gms.form_field_option where organization_id = 
)

select 

rfv.submission_id,
rr.review_stage_id as stage_id, 
rr.review_stage_name stage_name,
rr.form_id, 
rr.review_stage_type as stage_type, 
rr.created_by as reviewer_user_id,
rr.submission_id, 
ms.email as reviewer_email, 
--rr.response_review_score as score,
ff.field_label,
coalesce(ffo.option_label, GET(TRY_PARSE_JSON(rfv.value), 'value'), rfv.value) as value,
    CONCAT('Audit_Folder', '/Audit_' , rfv.SUBMISSION_ID , '/' , FORM_TYPE, '/', FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/STAGE_DATA_/' ,ms.email,'/', replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/review_stage_data.csv')
as folder_directory
from staging.intermediate.response_context rr
left join staging.base.ms_smmuser ms 
on rr.created_by = ms.userid
inner join response_field_value_cte rfv
on (rfv.form_id = rr.form_id
    and rfv.response_id = rr.response_id
    and rfv.submission_id = rr.submission_id)
inner join form_field_cte ff
on rfv.field_id = ff.field_id
inner join form_field_option_cte ffo 
on (ffo.field_id = rfv.field_id 
and ffo.option_id = rfv.sub_field_id)
where 
rr.review_stage_id is not null
and rr.organization_id = 
and rr.submission_id in 
"""
    return query_review_stage


def chronology_string():
    query_chronology = """
with submit_history as (
select
s.submission_id,
s.Activity_Date as Timestamp,
s.description,
s.note,
s.submission_history_id
from datamart.gms.submission_history s
where s.submission_id in
and s.organization_id =
)

select s.submission_id,
s.Timestamp,
s.description,
s.note, 
ss.emailmessage as message,
ss.emailrecipients,
CONCAT('Audit_Folder', '/Audit_' , s.SUBMISSION_ID ,'/chronology.csv')
as folder_directory
from submit_history s
inner join staging.base.ms_submissionhistory ss
on s.submission_history_id = ss.submissionhistoryid

"""
    return query_chronology

def files_string():
    query_files = """
with response_field_value_cte as (
    select * from datamart.gms.response_field_value where organization_id =
),
form_field_cte as (
    select * from datamart.gms.form_field where organization_id =
),
form_field_option_cte as (
    select * from datamart.gms.form_field_option where organization_id =
)
,forms_data as (
select
    rfv.submission_id,
    ff.form_type,
    ff.form_name,
    ff.field_type,
    ff.field_label,
    rfv.sub_field_id,
    coalesce(ffo.option_label, GET(TRY_PARSE_JSON(rfv.value), 'value'), rfv.value) as value,
    rcc.completed_at,
    rcc.created_at,
    ms.email as created_user_email,
    rfv.response_id
from response_field_value_cte rfv
inner join form_field_cte ff on rfv.field_id = ff.field_id
left join form_field_option_cte ffo on ffo.field_id = rfv.field_id and ffo.option_id = rfv.sub_field_id
left join staging.intermediate.response_context rcc on 
(rfv.request_id = rcc.request_id and rfv.response_id = rcc.response_id and rfv.submission_id = rcc.submission_id)
left join staging.base.ms_smmuser ms on rcc.created_by = ms.userid
where rfv.submission_id in
order by rfv.submission_id, ff.form_type, ff.form_name, ff.field_index
)

select ff.submission_id, f.id as file_id, f.file_name, f.file_size_bytes, f.bucket_name, f.storage_key,ff.form_type, ff.form_name,
ff.created_user_email, ff.completed_at,
CONCAT('Audit_Folder', '/Audit_' , ff.SUBMISSION_ID , '/' , FORM_TYPE, '/', FORM_TYPE, '_',replace(FORM_NAME,' ','_') , 
           '/FORM_DATA_' , replace(replace(COMPLETED_AT::varchar(23),' ','_'),':','_'),'/',created_user_email,'/files_/' , f.file_name )
as folder_directory
from staging.base.files_file f join
forms_data ff on f.id = ff.sub_field_id;    
   
"""
    return query_files

def data_definitions():
    table_name = 'Audit_Dynamo'
    # Create AWS session for dyanomodb usage

   
    json_file_nm = example_file_name
    submission_list, submission_list_elements = json_variable_list('submission_ids', json_file_nm, 'submission_id')
    
    # Get the formatted timestamp for each S3 Zip name
    now = datetime.datetime.now()
    formatted_string_date = now.strftime("_%m-%d-%Y_%I_%M_%p")

    # Open the JSON file
    with open(json_file_nm) as file:
        # Load the JSON data
        data = json.load(file)

    # Access the value using the key "report_name"
    report_name_api= data['report_name']
    organization_id = data['organization_id']
    report_json_name = 'Patrick Report'
    organization_id_string = 'organization_id' + ' = ' + '\''+ organization_id + '\''
    count_per_batch = 2
    aws_session = aws_session_ID
    random_uuid = generate_uuid()
    dev_zip_names =  ("Audit" + "Grp" + report_name_api + formatted_string_date + '.zip')
    print("Creating S3 session...")
    dev_s3 = myutils.create_s3_session(access_key, secret_key, region_name)
    dev_bucket_name = dev_s3_bucket

    report = Report(
        report_name_api,
        organization_id,
        organization_id_string,
        count_per_batch,
        submission_list = submission_list,
        aws_session = aws_session_ID,
        table_name = table_name,
        random_uuid = generate_uuid(),
        formatted_string_date = formatted_string_date,
        submission_list_elements =  submission_list_elements,
        dev_zip_names = dev_zip_names,
        dev_s3 = dev_s3,
        dev_bucket_name = dev_bucket_name
        ) 
    
    organization_id_used = organization_id
    report_id_used = random_uuid

    return report, organization_id_used, report_id_used


def create_initial_row():
    # Retrieve the organization_id_used and report_id_used from data_definitions()
    report, organization_id_used, report_id_used = data_definitions()
    report_name_used = report.report_name_api
    submission_list_used = report.submission_list
    item_data = {
        'organization_id': organization_id_used,
        'report_id': report_id_used,
        'created_at': report.formatted_string_date,
        'created_by': 'catherine.ching@submittable.com',
        'is_deleted': False,
        'report_name': report_name_used,
        'submission_list': report.submission_list_elements,
        'total_submissions': len(report.submission_list_elements),
        'processed_submissions': 0,
        'status': 'processing',
        's3_filename': report.dev_zip_names
    }
    dyno.put_item(report.aws_session, report.table_name, item_data)
    
    
    return report.aws_session,report.table_name, organization_id_used, report_id_used, report_name_used, report.submission_list_elements
    
# (AWS_session, table_name, organization_id, report_id, processed_submissions)
def update_dynamodb(aws_session, table_name, organization_id_current, report_id_current,processed_submission_count):
    p = processed_submission_count
    dyno.update_dynamodb(aws_session, table_name, organization_id_current, report_id_current, p)

def update_dyno_percent(aws_session, table_name, organization_id_current, report_id_current,report_percent):
    rp = report_percent
    dyno.update_dyno_percent(aws_session, table_name, organization_id_current, report_id_current,rp)


def complete_dynamodb(aws_session, table_name, organization_id_current, report_id_current, status):
    stat = status
    dyno.complete_dynamodb(aws_session, table_name, organization_id_current, report_id_current, stat)

def complete_dynamodb_link(aws_session, table_name, organization_id_current, report_id_current, link):
    s3link = link
    dyno.complete_dynamodb_link(aws_session, table_name, organization_id_current, report_id_current, s3link)

def complete_dynamodb_s3(aws_session, table_name, organization_id_current, report_id_current, s3_filename):
    s3_dev_filename = s3_filename
    dyno.complete_dynamodb_s3(aws_session, table_name, organization_id_current, report_id_current, s3_dev_filename)    

def get_s3_filename():
    report, organization_id_used, report_id_used = data_definitions()
    s3_filename = report.dev_zip_names
    return s3_filename, report_id_used, organization_id_used


def get_dfs():
    report, organization_id_used, report_id_used = data_definitions()
    replacements = ("organization_id =", report.organization_id_string), ("submission_id in", report.submission_list)
    query_readme = summary_query()
    query_form = forms_query_string()
    result_form_query = (multiple_replace(query_form, *replacements))
    query_review_stage = review_stage_string()
    r_result_review_query = (multiple_replace(query_review_stage, *replacements)) 
    query_chronology = chronology_string()
    c_result_chrnology_query = (multiple_replace(query_chronology, *replacements))
    query_files = files_string()
    rr_query_string = (multiple_replace(query_files, *replacements))
    s_df, s_queryid, conn_handler, cursor_conn = try_query_async('snowflakeconn', 'ctx2', 'cs2',query_readme, "readme_query")
    s_list_columns = ['SUMMARY', 'FOLDER_DIRECTORY']
    if s_df.empty:
        pass
    else:
        s_df.columns = s_list_columns
    df, queryid, conn_handler, cursor_conn = try_query_async('snowflakeconn','ctx2','cs2', result_form_query,"form_query")
    list_columns = ['SUBMISSION_ID', 'FORM_TYPE','FORM_NAME','FIELD_TYPE','FIELD_LABEL','SUB_FIELD_ID', 'VALUE', 'COMPLETED_AT', 'CREATED_AT','CREATED_USER_EMAIL','FOLDER_DIRECTORY']
    if df.empty:
        pass
    else:
        df.columns = list_columns
        final_result_dataframe= df.loc[df["FIELD_LABEL"] != "Table Question"]
    r_df, r_queryid, conn_handler, cursor_conn = try_query_async('snowflakeconn','ctx2','cs2', r_result_review_query, "review_stage_data")
    r_list_columns = ['SUBMISSION_ID','STAGE_ID','STAGE_NAME','FORM_ID','STAGE_TYPE','REVIEWER_USER_ID', 'SUBMISSION_ID', 'REVIEWER_EMAIL','FIELD_LABEL','VALUE','FOLDER_DIRECTORY']
    if r_df.empty:
        pass
    else:
        r_df.columns = r_list_columns
    c_df, c_queryid, conn_handler, cursor_conn = try_query_async('snowflakeconn','ctx2','cs2', c_result_chrnology_query, "chronology_form")
    c_list_columns = ['SUBMISSION_ID','TIMESTAMP','DESCRIPTION', 'NOTE','MESSAGE','EMAILRECIPENTS','FOLDER_DIRECTORY']
    if c_df.empty:
        pass
    else:
        c_df.columns = c_list_columns
    rr_df, rr_queryid,conn_handler, cursor_conn = try_query_async('snowflakeconn','ctx2','cs2', rr_query_string, "s3_bucket_names")
    rr_list_columns = ['SUBMISSION_ID', 'FILE_ID', 'FILE_NAME', 'FILE_SIZE_BYTES', 'BUCKET_NAME', 'STORAGE_KEY', 'FORM_TYPE', 'FORM_NAME','CREATED_USER_EMAIL', 'COMPLETED_AT', 'FOLDER_DIRECTORY']
    if rr_df.empty:
        pass
    else:
        rr_df.columns = rr_list_columns
    final_result_dataframe= df.loc[df["FIELD_LABEL"] != "Table Question"]  
    print(final_result_dataframe)
    cursor_conn.close()
    conn_handler.close()
    # Separated items and cost data frames for Table Questions
    df_matrix = df.loc[df["FIELD_LABEL"] == "Table Question"]  
    df_reviewstage = df_matrix[['SUB_FIELD_ID','VALUE','FOLDER_DIRECTORY']]
    df_reviewstage = df_reviewstage.reset_index(drop=True)
    result_review_df = review_stage.read_dataframe(df_reviewstage)
    # Extract the integer value using string manipulation
    result_review_df['SUBMISSION_ID'] = result_review_df['FOLDER_DIRECTORY'].str.extract(r'Audit_(\d+)/')
    # Move the 'NewColumn' to the first position
    cols = result_review_df.columns.tolist()
    cols = ['SUBMISSION_ID'] + cols[:-1]
    result_review_df = result_review_df[cols]
    return s_df,final_result_dataframe, r_df, c_df, rr_df, result_review_df, report.submission_list_elements 

def create_report_route():
    report, organization_id_used, report_id_used = data_definitions()
    s3_filename, report_id_present, organization_id_present = get_s3_filename()
    submission_download_attachment_sizes = []
    s_df, form_df, review_stage_df, chronology_df, files_df, table_question_df, submission_list = get_dfs()
    dataframes = [ s_df, form_df, review_stage_df, chronology_df, files_df, table_question_df]
    folder_ids = [ 'FOLDER_DIRECTORY', 'FOLDER_DIRECTORY', 'FOLDER_DIRECTORY', 'FOLDER_DIRECTORY', 'FOLDER_DIRECTORY', 'FOLDER_DIRECTORY']
    print("The total inputted submission_ids are: ")
    print(len(submission_list))
    inputted_submission_total = len(submission_list)
    zip_objects_list = []
    zip_dest_paths = []
    print("Creating S3 session...")
    dev_s3 = myutils.create_s3_session(access_key, secret_key, region_name)
    submission_id_set = set(files_df['SUBMISSION_ID'].values)
    submission_id_form_set = set(form_df['SUBMISSION_ID'].values) 


    # Set the S3 AWS connection and first initial creation in DynamoDB
    aws_session, table_name, organization_id_current, report_id_current, report_name_current, submission_list_current = create_initial_row()

    # Set the S3 destinations for each submission ID
    checked_count = 0  # Variable to track the count of checked submission IDs
    total_count = len(submission_list)  # Total count of submission IDs
    zip_memory = io.BytesIO()  # Initialize the zip_memory variable

    # Set the process for each batch, divide the submission IDs into batches
    batches = [submission_list[i:i+report.count_per_batch] for i in range(0, total_count, report.count_per_batch)]

    # Calculate the cumulative progress across all batches
    submissions_adds = 0
    total_submissions_processed = 0

    # Process each batch
    for batch_index, batch in enumerate(batches, start=1):
        checked_count = 0
        total_count = len(batch)  # Update the total count for each batch

        # Process each submission ID within the current batch
        for counter, submission_id in enumerate(batch, start=1):
            checked_count += 1
            submissions_adds += 1
            total_submissions_processed += 1
            if submission_id in submission_id_form_set:
                submission_forms_df = form_df[form_df['SUBMISSION_ID'] == submission_id]

                # Filter review_stage_df based on the submission ID using query
                submission_review_df = review_stage_df.query(f'SUBMISSION_ID == {submission_id}')
                submission_chronology_df = chronology_df[chronology_df['SUBMISSION_ID'] == submission_id]
                submission_table_df = table_question_df[table_question_df['SUBMISSION_ID'] == submission_id]

                if submission_id in submission_id_set:
                    submission_files_df = files_df[files_df['SUBMISSION_ID'] == submission_id]
                    prod_bucket_name = submission_files_df['BUCKET_NAME'].tolist()
                    prod_object_keys = submission_files_df['STORAGE_KEY'].tolist()
                    prod_destination_paths = submission_files_df['FOLDER_DIRECTORY'].tolist()

                    # Check if files_df is empty
                    if files_df.empty:
                        zip_objects_list = []
                        zip_dest_paths = []
                    else:
                        # Append to the existing lists
                        zip_objects_list.extend(prod_object_keys)
                        zip_dest_paths.extend(prod_destination_paths)

                        # Call the function to download files and get the results
                        zip_objects_list, zip_dest_paths = download_files_from_s3_returns2(prod_bucket_name, prod_object_keys, prod_destination_paths)

                # Add the dataframes to the zip archive using the create_zip_from_memory function
                zip_memory = create_zip_from_memory(zip_memory_downloads=[],
                                                    destination_paths=[],
                                                    dataframes=[s_df, submission_forms_df, submission_review_df, submission_chronology_df, submission_table_df],
                                                    folder_ids=folder_ids,
                                                    zip_memory=zip_memory)

                # Rest of the code for processing the submission ID

            # Calculate the percentage of completion within the current batch
            percentage_complete = (counter / total_count) * 100

            # Calculate the overall progress across all batches
            overall_progress = ((batch_index - 1) * total_count + counter) / len(submission_list) * 100

            # Calculate the cumulative progress across all batches
            submissions_adds = (counter)

            # Calculate the percentage of completion for the whole input of submission_ids as it's being processed
            percentage_processed = round((total_submissions_processed / inputted_submission_total) * 100, 2)
            percentage_processed_int = int(percentage_processed)

            # Print the counter, the submission ID being processed, and the percentage of completion
            print(f"Processing item {counter}/{total_count} in Batch {batch_index}/{len(batches)}: {submission_id} ({percentage_complete:.2f}% complete)")
            print("Use this for dynamodb update which represents cumulative submission number")
            print(total_submissions_processed)
            print(f"The percentage of processed submissions {percentage_processed}")

            
            # Update the DynamoDB table after processing all submission IDs in the batch
            update_dynamodb( aws_session, table_name,organization_id_current, report_id_current, total_submissions_processed)
        print(f"The number of submissions completed so far in the batch is {submissions_adds}. The total submissions processed for Batch {batch_index} is {total_submissions_processed}")


        # Check if all submission IDs were checked within the current batch
        if checked_count == total_count:
            # Define the bucket name and file name

            # Complete the ZIP file
            complete_memory_zip = create_zip_from_memory(zip_objects_list, zip_dest_paths, dataframes, folder_ids)

            # Upload the ZIP file to S3 bucket
            myutils.upload_file_to_s3_using_smart_open_with_boto(dev_s3, complete_memory_zip, report.dev_bucket_name, s3_filename)
    
    status = ''
    zip_complete = ''
    # Check if all submission IDs were checked
    if total_submissions_processed == len(submission_list):
        print("All submission IDs were checked")
        status = 'completed'
        zip_complete = s3_filename
    else:
        print("Not all submission IDs were checked")
        status = 'incomplete'
    complete_dynamodb(aws_session, table_name, organization_id_current, report_id_current, status)
    complete_dynamodb_s3(aws_session, table_name, organization_id_current, report_id_current, zip_complete)
    print(f"The total submissions processed is: {total_submissions_processed} ")
    return zip_complete

##########  TEST HERE ###################

# aws_session, table_name, organization_id_current, report_id_current, report_name_current, submission_list_current = create_initial_row()
# print(organization_id_current)
# print(report_id_current)
# for i in range(8):
#     if i < 8:
#         print(i)
#         processed_submissions = i
#         update_dynamodb( aws_session, table_name,organization_id_current, report_id_current, processed_submissions)
#         time.sleep(3)
def create_JSON_s3link():
    # Call the create_report_route() function
    zip_complete = create_report_route()
    report, organization_id_used, report_id_used = data_definitions()

    # Get the pre-signed URL for the uploaded file
    response = myutils.get_s3_presigned_url(report.dev_bucket_name, zip_complete)

    # Check if the response is not empty
    if response:
        # Print the pre-signed URL
        print(f"Please access the file {zip_complete} at the presigned URL: {response}")

        # Extract the S3 link from the response
        s3_link = response

        # Example usage
        presigned_url = s3_link
        print("Presigned URL:", presigned_url)

        # Create a dictionary to store the response and other information
        data = {
            "s3_link": s3_link
        }

        # Print the JSON structure of the data dictionary
        json_structure = json.dumps(data)
        print(json_structure) 


create_JSON_s3link() 
