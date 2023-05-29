import dotenv
import os #provides ways to access the Operating System and allows us to read the environment variables
from dotenv import load_dotenv

load_dotenv()

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
