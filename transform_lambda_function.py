import json
import pandas as pd
import boto3
import os
from io import StringIO


def lambda_handler(event, context):

    # os.environ["AWS_ACCESS_KEY_ID"] = ''
    # os.environ["AWS_SECRET_ACCESS_KEY"] = ''

    # AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    # AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3client = boto3.client('s3')

    # s3client = boto3.client(
    #     "s3",
    #     aws_access_key_id=AWS_ACCESS_KEY_ID,
    #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    #       )

    bucket_name = 'account-balances-scraper'

    # get all object keys in bucket
    s3resource = boto3.resource('s3')
    bucket = s3resource.Bucket(bucket_name)

    # get SNS arn
    SNS_ARN = os.environ['SNS_ARN']


    # create marketdata df
    df_marketdata = pd.DataFrame(columns=['Code', 'Security Name', 'Units', 'Price', 'Market Value',
                            'Security Ratio', 'Security Value', 'File Date'])

    for obj in bucket.objects.filter(Prefix='market-data-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]
        
        # clean df
        df_file = df_file[:-1]
        df_file.drop('Guarantor', axis=1, inplace=True)

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_marketdata = pd.concat([df_marketdata, df_file], axis=0)
        df_marketdata.reset_index(drop=True, inplace=True)

        
    # create interest df
    df_interest = pd.DataFrame(columns=['Interest Charged', 'Interest Earned'])

    for obj in bucket.objects.filter(Prefix='interest-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_interest = pd.concat([df_interest, df_file], axis=0)
        df_interest.reset_index(drop=True, inplace=True)


    # create loandetails df
    df_loandetails = pd.DataFrame(columns=['Account Repayment Style', 'Type', 
                                        'Interest Rate', '# of Repayments Remaining', 
                                        'Estimated Expiry Date'])

    for obj in bucket.objects.filter(Prefix='loan-details-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_loandetails = pd.concat([df_loandetails, df_file], axis=0)
        df_loandetails.reset_index(drop=True, inplace=True)


    # create summarydata df
    df_summarydata = pd.DataFrame(columns=[0, 1, 'File Date'])

    for obj in bucket.objects.filter(Prefix='summary-data-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_summarydata = pd.concat([df_summarydata, df_file], axis=0)
        df_summarydata.reset_index(drop=True, inplace=True)


    # create transactions df
    df_transactions = pd.DataFrame(columns=['Date', 'Transaction Details', 'Debit', 'Credit', 'Balance', 'File Date'])

    for obj in bucket.objects.filter(Prefix='transactions-html-raw/202'):
        # create df from file
        response = s3client.get_object(Bucket=bucket_name, Key=obj.key)
        df_file = pd.read_html(response.get("Body"))[0]

        # get date of tile and add it to a column in df
        filedate = obj.key.split('/')[1].split('--')[0]
        df_file.loc[:, 'File Date'] = filedate
        
        # append dfs together
        df_transactions = pd.concat([df_transactions, df_file], axis=0)
        df_transactions.reset_index(drop=True, inplace=True)

    # final cleaning before writing to s3
    # rename columns
    df_summarydata.rename(columns={0: 'Measure', 1: 'Value'}, inplace=True)

    # change data type
    df_marketdata['Market Value'] = df_marketdata['Market Value'].replace({'\$':''}, regex = True)
    df_marketdata['Market Value'] = df_marketdata['Market Value'].replace({'\,':''}, regex = True)
    df_marketdata['Market Value'] = df_marketdata['Market Value'].astype('float')


    # put transformed df's into a new bucket location
    csv_buffer_marketdata = StringIO()
    df_marketdata.to_csv(csv_buffer_marketdata)
    s3resource.Object(bucket_name, 'transformed/' + 'marketdata.csv').put(Body=csv_buffer_marketdata.getvalue())

    csv_buffer_interest = StringIO()
    df_interest.to_csv(csv_buffer_interest)
    s3resource.Object(bucket_name, 'transformed/' + 'interest.csv').put(Body=csv_buffer_interest.getvalue())

    csv_buffer_loandetails = StringIO()
    df_loandetails.to_csv(csv_buffer_loandetails)
    s3resource.Object(bucket_name, 'transformed/' + 'loandetails.csv').put(Body=csv_buffer_loandetails.getvalue())

    csv_buffer_summarydata = StringIO()
    df_summarydata.to_csv(csv_buffer_summarydata)
    s3resource.Object(bucket_name, 'transformed/' + 'summarydata.csv').put(Body=csv_buffer_summarydata.getvalue())

    csv_buffer_transactions = StringIO()
    df_transactions.to_csv(csv_buffer_transactions)
    s3resource.Object(bucket_name, 'transformed/' + 'transactions.csv').put(Body=csv_buffer_transactions.getvalue())

    # send email using SNS
    snsclient = boto3.client('sns')
    response = client.publish(
        TopicArn=SNS_ARN,
        Message='this is a test message... testing 1,2,3...')
