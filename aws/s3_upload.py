import boto3
import os

INPUT_FILE_1 = os.path.join(os.path.dirname(__file__), '../data/merged_data/merged_data.csv')
INPUT_FILE_2 = os.path.join(os.path.dirname(__file__), '../data/spark_output/part-00000-296a71e9-ddcf-47d3-8640-b3e78b72ac07-c000.csv')

BUCKET_NAME = 'smart-meter-pipeline-london-hannah'

S3_KEY_1 = 'merged_data.csv'
S3_KEY_2 = 'agg_data.csv'

s3 = boto3.client('s3')

s3.upload_file(INPUT_FILE_1, BUCKET_NAME, S3_KEY_1)
print(f"Upload complete: {S3_KEY_1} uploaded to S3 bucket {BUCKET_NAME}")

s3.upload_file(INPUT_FILE_2, BUCKET_NAME, S3_KEY_2)
print(f"Upload complete: {S3_KEY_2} uploaded to S3 bucket {BUCKET_NAME}")
