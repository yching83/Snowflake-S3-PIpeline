import os
import boto3
import io
import smart_open

def create_s3_session(access_key, secret_key, region_name):
    # Set up AWS credentials
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name  # Replace with your bucket's region
    )

    # Create an S3 client
    return session.client('s3')

def get_aws_credentials(access_key, secret_key):
    try:
        # Create a session with the provided access key and secret key
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        # Retrieve the AWS credentials from the session
        credentials = session.get_credentials()

        # Extract the access key and secret key from the credentials
        aws_access_key_id = credentials.access_key
        aws_secret_access_key = credentials.secret_key

        return aws_access_key_id, aws_secret_access_key

    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")


def list_bucket_contents(s3, bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print("The bucket is empty.")
    except Exception as e:
        print(f"Error listing bucket contents: {str(e)}")


def create_s3_path(s3, bucket_name, path):
    try:
        # Create an empty object to represent the path
        s3.put_object(Bucket=bucket_name, Key=f"{path}/")
        print(f"S3 path '{path}' created successfully.")
    except Exception as e:
        print(f"Error creating S3 path: {str(e)}")

import boto3

def upload_zip_to_s3_boto(s3, zip_memory, bucket_name, name):
    try:
        # Rewind the BytesIO object to the beginning
        zip_memory.seek(0)
        
        # Upload the BytesIO object to S3
        response = s3.put_object(Body=zip_memory, Bucket=bucket_name, Key=name)
        
        # Check the response for successful upload
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Zip file uploaded successfully to S3 bucket: {bucket_name}, Key: {name}")
        else:
            print("Error uploading zip file to S3.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")



def upload_file_to_s3(s3, bucket_name, subfolder, file_path):
    try:
        # Check if the subfolder exists
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=subfolder)
        if 'Contents' not in response:
            # Create the subfolder if it doesn't exist
            s3.put_object(Bucket=bucket_name, Key=f"{subfolder}/")

        # Upload the file to the subfolder
        file_name = os.path.basename(file_path)
        s3.upload_file(file_path, bucket_name, f"{subfolder}/{file_name}")
        print(f"File '{file_name}' uploaded successfully to '{subfolder}' in S3 bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error uploading file to S3: {str(e)}")

def download_and_save_file_from_s3(s3, bucket_name, subfolder, file_name, destination_path, new_file_name):
    try:
        # Download the file from the subfolder
        s3.download_file(bucket_name, f"{subfolder}/{file_name}", destination_path)

        if new_file_name == "":
            new_file_name = file_name

        # Rename the downloaded file
        new_destination_path = os.path.join(os.path.dirname(destination_path), new_file_name)
        os.rename(destination_path, new_destination_path)
        print(f"File '{file_name}' downloaded from '{subfolder}' and renamed to '{new_file_name}'.")
    except Exception as e:
        print(f"Error downloading and renaming file: {str(e)}")



def download_from_s3_to_memory(s3, bucket_name, subfolder, file_name):
    try:
        # Create an in-memory file object
        file_object = io.BytesIO()

        # Download the file into the in-memory file object
        s3.download_fileobj(bucket_name, f"{subfolder}/{file_name}", file_object)

        # Set the file object's position to the beginning
        file_object.seek(0)

        print(f"File '{file_name}' downloaded from S3 bucket '{bucket_name}' into memory.")
        return file_object
    except Exception as e:
        print(f"Error downloading file to memory: {str(e)}")
        return None
    
def open_pdf_in_memory(file_path):
    with open(file_path, 'rb') as file:
        pdf_bytes = file.read()
    pdf_buffer = io.BytesIO(pdf_bytes)
    return pdf_buffer


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


def get_s3_presigned_url(bucket_name, object_key, expiration=3600):
    """
    Generate a pre-signed URL for accessing an S3 object.
    
    :param bucket_name: The name of the S3 bucket.
    :param object_key: The key (path) of the object in the S3 bucket.
    :param expiration: The expiration time in seconds (default: 1 hour).
    :return: The pre-signed URL as a string.
    """
    s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))

    try:
        response = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
        return response
    except Exception as e:
        print(f"Error generating pre-signed URL: {str(e)}")
        return None

def upload_file_to_s3_using_smart_open_with_boto(s3, zip_memory, bucket_name, path_filename):
    s3_uri = f"s3://{bucket_name}/{path_filename}"
    s3_options = {
        'client': s3}
    zip_memory.seek(0)
    with smart_open.open(s3_uri, 'wb', transport_params=s3_options) as s3_file:
        s3_file.write(zip_memory.getvalue())
    print(f"Zip file uploaded successfully to '{bucket_name}' with key '{path_filename}'.")
