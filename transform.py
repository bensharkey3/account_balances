import json
import pandas as pd
import lxml
import boto3


def lambda_handler(event, context):
    
    bucket_name = 'account-balances-scraper'
    object_key = 'market-data-html-raw/2022-09-12--18-01-PM.txt'
    
    s3client = boto3.client('s3')
    response = s3client.get_object(Bucket=bucket_name, Key=object_key)
    df = pd.read_html(response.get("Body"))[0]
    print(df)

    s3resource = boto3.resource('s3')
    bucket = s3resource.Bucket(bucket_name)
    lst = []
    for obj in bucket.objects.filter(Prefix='market-data-html-raw/2'):
        lst.append(obj.key)
    
    print(lst)
