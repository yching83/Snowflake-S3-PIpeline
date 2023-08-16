#import dotenv
import os #provides ways to access the Operating System and allows us to read the environment variables
#from dotenv import load_dotenv

#load_dotenv()

snowflake_username = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_account_nm = os.getenv("ACCOUNT_IDENTIFIER")
snowflake_database_nm = os.getenv("DATABASE_NAME")
snowflake_warehouse_nm = os.getenv("WAREHOUSE_FIELD")
snowflake_schema_nm = os.getenv("SCHEMA_NAME")
snowflake_role_nm = os.getenv("ROLE_FIELD")
dev_aws_access_key = os.getenv("DEV_ACCESS_KEY")
dev_aws_secret_key = os.getenv("DEV_SECRET_KEY")
dev_arn = os.getenv("DEV_ARN")
dev_dynamo_table = os.getenv("DEV_DYNAMO_TABLE")
dev_s3_bucket = os.getenv("DEV_BUCKET_NAME")
prod_aws_access_key_id = os.getenv("PROD_AWS_ACCESS_KEY_ID")
prod_aws_secret_access_key = os.getenv("PROD_AWS_SECRET_ACCESS_KEY")
prod_aws_session_token = os.getenv("PROD_AWS_SESSION_TOKEN")

