import boto3

s3 = boto3.client(
    's3',
    region_name='ap-south-1'
)

bucket_name = 'ritzzz'
file_path = "dim_customer_400mb.json"
s3_file_name = 'json_Data400mb'

s3.upload_file(file_path, bucket_name, s3_file_name)
print("File uploaded successfully")