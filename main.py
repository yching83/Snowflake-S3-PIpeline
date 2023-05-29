#!/usr/bin/env python
import pandas as pd
import sys
import os
import io
import snowflake.connector
import time
import json
import re
import smart_open
import tempfile
import zipfile
from pathlib import Path
import logging
import env_variables
import review_stage
import write_to_s3 as myutils

# Credentials for Snowflake Connection
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


def deliver_files_smartopen_in_memory(dataframes, folder_ids):    
    zip_memory = io.BytesIO()
    zip_uri = "zip://" + zip_memory
    with smart_open.open(zip_uri, 'wb') as fout:
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


def zip_file_in_memory(dataframes, folder_ids):
    zip_memory = io.BytesIO()
    with zipfile.ZipFile(zip_memory, 'w') as zf:
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
    zip_memory.seek(0)
    print("Timer for zipping all files: ", (endstime - starttime))
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
        #list_of_values = variable_clause + ' in (' + ','.join( str(x) for x in list_variable) + ')'
        return list_variable
    else:
         string_value = variable_clause + ' = ' + '\''+ list_variable + '\''
         return string_value


# Program Call Outs
if __name__ == "__main__":
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

    json_file_nm = 'example_json.json'
    submission_clause = json_variable_list('submission_ids', json_file_nm, 'submission_id')
    print(f"Submission clause: {submission_clause}")

    # Open the JSON file
    with open(json_file_nm) as file:
        # Load the JSON data
        data = json.load(file)

    # Access the value using the key "report_name"
    report_name = data['report_name']
    organ_id = data['organization_id']
    # Print the report name
    r_json_name = 'Patrick Report'
    string_value = 'organization_id' + ' = ' + '\''+ organ_id + '\''
    print(string_value)
    num_of_submissions_to_query = 3

    #Forms Data Query and Separation of Table Questionnaire

    if report_name == r_json_name:
        i = 0
        separated_list = ([submission_clause[i:i + num_of_submissions_to_query] for i in range(0, len(submission_clause), num_of_submissions_to_query)])
        for f_list in separated_list:
            i= i + 1
            print(f"f_list: {f_list}")
            list_of_values = 'submission_id' + ' in (' + ','.join( str(x) for x in f_list) + ')'
            print(f"List of values: {list_of_values}")
            replacements = ("organization_id =", string_value), ("submission_id in", (list_of_values))
            result_form_query = (multiple_replace(query_form, *replacements))
            # print(result_form_query)
            df, queryid, conn_handler, cursor_conn = try_query_async('snowflakeconn','ctx2','cs2', result_form_query, "form Data")
            list_columns = ['SUBMISSION_ID', 'FORM_TYPE','FORM_NAME','FIELD_TYPE','FIELD_LABEL','SUB_FIELD_ID', 'VALUE', 'COMPLETED_AT', 'CREATED_AT','CREATED_USER_EMAIL','FOLDER_DIRECTORY']
            df.columns = list_columns
            if i == 1:
                totaldf = df
            else:
                totaldf = pd.concat([totaldf, df], ignore_index = True)
        
        cursor_conn.close()
        conn_handler.close()

    print(totaldf.head(10))
    print(totaldf.tail(10))
    final_result_dataframe= totaldf.loc[totaldf["FIELD_LABEL"] != "Table Question"]  #transform_column_values (df, 'SUB_FIELD_ID', 'SUBMISSION_ID',list_columns )
    logging.info("Merged Number of Rows After Duplicates Removal")
    print(final_result_dataframe.shape[0])
    print(final_result_dataframe.head(10))
    print(final_result_dataframe.tail(10))

    # Review data display

    if report_name == r_json_name:
        j = 0
        separated_list = ([submission_clause[j:j + num_of_submissions_to_query] for j in range(0, len(submission_clause), num_of_submissions_to_query)])
        for r_list in separated_list:
            j= j + 1
            list_of_values = 'submission_id' + ' in (' + ','.join( str(x) for x in r_list) + ')'
            print(f"List of values: {list_of_values}")
            replacements = ("organization_id =", string_value), ("submission_id in", (list_of_values))
            r_result_review_query = (multiple_replace(query_review_stage, *replacements))
            r_df, r_queryid, r_conn_handler, r_cursor_conn = try_query_async('snowflakeconn','ctx2','cs2', r_result_review_query, "review_stage_data")
            r_list_columns = ['SUBMISSION_ID','STAGE_ID','STAGE_NAME','FORM_ID','STAGE_TYPE','REVIEWER_USER_ID', 'SUBMISSION_ID', 'REVIEWER_EMAIL','FIELD_LABEL','VALUE','FOLDER_DIRECTORY']
            r_df.columns = r_list_columns
            if j == 1:
                total_r_df = r_df
            else:
                total_r_df = pd.concat([total_r_df, r_df], ignore_index = True)

        r_cursor_conn.close()
        r_conn_handler.close()
    print(total_r_df.head(10))
    print(total_r_df.tail(10))
       
    # Chonology data display

    if report_name == r_json_name:
        c = 0
        separated_list = ([submission_clause[c:c + num_of_submissions_to_query] for c in range(0, len(submission_clause), num_of_submissions_to_query)])
        for r_list in separated_list:
            c= c + 1
            list_of_values = 'submission_id' + ' in (' + ','.join( str(x) for x in r_list) + ')'
            print(f"List of values: {list_of_values}")
            replacements = ("organization_id =", string_value), ("submission_id in", (list_of_values))
            c_result_chrnology_query = (multiple_replace(query_chronology, *replacements))
            c_df, c_queryid, c_conn_handler, c_cursor_conn = try_query_async('snowflakeconn','ctx2','cs2', c_result_chrnology_query, "chronology_form")
            c_list_columns = ['SUBMISSION_ID','TIMESTAMP','DESCRIPTION', 'NOTE','MESSAGE','EMAILRECIPENTS','FOLDER_DIRECTORY']
            c_df.columns = c_list_columns
            if c == 1:
                total_c_df = c_df
            else:
                total_c_df = pd.concat([total_c_df, c_df], ignore_index = True)
        c_cursor_conn.close()
        c_conn_handler.close()
    print(total_c_df.head(10))
    print(total_c_df.tail(10))

    # s3 bucket path and data outputs

    if report_name == r_json_name:
        r = 0
        separated_list = ([submission_clause[r:r + num_of_submissions_to_query] for r in range(0, len(submission_clause), num_of_submissions_to_query)])
        for r_list in separated_list:
            r = r + 1
            list_of_values = 'submission_id' + ' in (' + ','.join( str(x) for x in r_list) + ')'
            print(f"List of values: {list_of_values}")
            replacements = ("organization_id =", string_value), ("submission_id in", (list_of_values))
            rr_query_string = (multiple_replace(query_files, *replacements))
            rr_df, rr_queryid,rr_conn_handler, rr_cursor_conn = try_query_async('snowflakeconn','ctx2','cs2', rr_query_string, "s3_bucket_names")
            if r == 1:
                total_rr_df = rr_df
            else:
                total_rr_df = pd.concat([total_rr_df, rr_df], ignore_index = True)
        rr_cursor_conn.close()
        rr_conn_handler.close()
    print(total_rr_df.head(10))
    rr_list_columns = ['SUBMISSION_ID', 'FILE_ID', 'FILE_NAME', 'FILE_SIZE_BYTES', 'BUCKET_NAME', 'STORAGE_KEY', 'FORM_TYPE', 'FORM_NAME','CREATED_USER_EMAIL', 'COMPLETED_AT', 'FOLDER_DIRECTORY']
    total_rr_df.columns = rr_list_columns
    print(total_rr_df.head(10))



    # Print out dataframes from datasplit
    #df_matrix = pd.read_csv("result10.csv")
    df_matrix = totaldf.loc[totaldf["FIELD_LABEL"] == "Table Question"]  
    df_reviewstage = df_matrix[['SUB_FIELD_ID','VALUE','FOLDER_DIRECTORY']]
    print(df_reviewstage)
    
    # Smart zip all the data outputs from queries via FOLDER_DIRECTORY column
    df_reviewstage = df_reviewstage.reset_index(drop=True)
    print(df_reviewstage.head(26))
    result_review_df = review_stage.read_dataframe(df_reviewstage)
    
    dataframes = [final_result_dataframe, total_r_df, total_c_df, result_review_df]
    folder_ids = ['FOLDER_DIRECTORY','FOLDER_DIRECTORY', 'FOLDER_DIRECTORY', 'FOLDER_DIRECTORY']



    # Define the bucket name and file name
    filename = 'sample_file.pdf'
    bucket_name = 'testawsaudit'
    access_point_name = 's3auditwrite'  # Access point ARN or name
    region_name = 'us-east-2'

    print("Creating S3 session...")
    s3 = myutils.create_s3_session(access_key, secret_key, region_name)
    print(f"Used access_key: {access_key}, secret_key: {secret_key}, region_name: {region_name}, bucket name: {bucket_name}")
    print("S3 session created!")
    print(f"Listing contents of S3 bucket {bucket_name}:")
    myutils.list_bucket_contents(s3, bucket_name)

    #zip_name = 'audit_y_output.zip'
    #deliver_files_smartopen(zip_name, dataframes, folder_ids)
    zip_memory = zip_file_in_memory(dataframes,folder_ids)

    # Then upload the zip_memory to S3 bucket using whatever method you want.

    # Example usage
    # upload_file_to_s3_using_smart_open(zip_memory, bucket_name, "YC_test.zip", access_key, secret_key)
    object_key = "audit_test_smart_open_20230528_16h20.zip"
    myutils.upload_file_to_s3_using_smart_open_with_boto(s3, zip_memory, bucket_name, object_key)

    # Get the link to the recently created file in S3 bucket
    response = myutils.get_s3_presigned_url(bucket_name, object_key)
    myutils.list_bucket_contents(s3, bucket_name)
    if response:
        print(f"Please access the file {object_key} at the presign URL: {response}")
    
